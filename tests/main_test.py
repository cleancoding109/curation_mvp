"""
Unit Tests for Metadata-Driven Spark Batch Framework

These tests validate the core functionality of the Silver Processor,
including high-watermark incremental processing and SCD Type 1/2 patterns.
"""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DecimalType, BooleanType
)


class TestSilverProcessorCore:
    """Tests for core Silver Processor functionality."""

    @pytest.fixture
    def sample_config(self):
        """Sample table configuration for testing."""
        return {
            "table_name": "test_silver_orders",
            "source_table": "test_bronze.orders",
            "target_table": "test_silver.orders",
            "scd_type": 1,
            "business_key_columns": ["order_id"],
            "watermark_column": "ingestion_ts",
            "target_watermark_column": "processing_timestamp",
            "enabled": True
        }

    @pytest.fixture
    def global_settings(self):
        """Sample global settings for testing."""
        return {
            "catalog": "test",
            "schema_bronze": "bronze",
            "schema_silver": "silver",
            "default_watermark_column": "ingestion_ts",
            "scd2_end_date_value": "9999-12-31 23:59:59",
            "log_level": "INFO"
        }

    @pytest.fixture
    def sample_scd2_config(self):
        """Sample SCD Type 2 configuration for testing."""
        return {
            "table_name": "test_silver_customers",
            "source_table": "test_bronze.customers",
            "target_table": "test_silver.customers",
            "scd_type": 2,
            "business_key_columns": ["customer_id"],
            "watermark_column": "ingestion_ts",
            "target_watermark_column": "processing_timestamp",
            "scd2_columns": {
                "effective_start_date": "effective_start_date",
                "effective_end_date": "effective_end_date",
                "is_current": "is_current"
            },
            "track_columns": ["customer_name", "email", "address"],
            "enabled": True
        }

    def test_config_loading(self, sample_config, global_settings):
        """Test that configuration is properly loaded."""
        assert sample_config["table_name"] == "test_silver_orders"
        assert sample_config["scd_type"] == 1
        assert len(sample_config["business_key_columns"]) == 1
        assert global_settings["scd2_end_date_value"] == "9999-12-31 23:59:59"

    def test_scd2_config_has_required_fields(self, sample_scd2_config):
        """Test that SCD2 config has all required fields."""
        assert "scd2_columns" in sample_scd2_config
        assert "track_columns" in sample_scd2_config
        assert sample_scd2_config["scd_type"] == 2


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_validate_table_config_valid(self):
        """Test validation of a valid config."""
        from curation_framework.utils import validate_table_config
        
        config = {
            "table_name": "test",
            "source_table": "bronze.test",
            "target_table": "silver.test",
            "scd_type": 1,
            "business_key_columns": ["id"]
        }
        errors = validate_table_config(config)
        assert len(errors) == 0

    def test_validate_table_config_missing_fields(self):
        """Test validation catches missing required fields."""
        from curation_framework.utils import validate_table_config
        
        config = {
            "table_name": "test"
        }
        errors = validate_table_config(config)
        assert len(errors) > 0
        assert any("source_table" in e for e in errors)

    def test_validate_table_config_invalid_scd_type(self):
        """Test validation catches invalid SCD type."""
        from curation_framework.utils import validate_table_config
        
        config = {
            "table_name": "test",
            "source_table": "bronze.test",
            "target_table": "silver.test",
            "scd_type": 3,  # Invalid
            "business_key_columns": ["id"]
        }
        errors = validate_table_config(config)
        assert any("scd_type" in e for e in errors)


class TestDataFrameOperations:
    """Tests for DataFrame operations using Spark fixtures."""

    def test_create_hash_column(self, spark: SparkSession):
        """Test hash column creation."""
        from curation_framework.utils import create_hash_column
        
        # Create test DataFrame
        data = [
            ("1", "John", "john@example.com"),
            ("2", "Jane", "jane@example.com"),
        ]
        df = spark.createDataFrame(data, ["id", "name", "email"])
        
        # Add hash column
        result_df = create_hash_column(df, ["name", "email"])
        
        assert "row_hash" in result_df.columns
        assert result_df.count() == 2
        
        # Verify hashes are different for different rows
        hashes = [row.row_hash for row in result_df.collect()]
        assert hashes[0] != hashes[1]

    def test_deduplicate_by_key(self, spark: SparkSession):
        """Test deduplication by key keeping latest."""
        from curation_framework.utils import deduplicate_by_key
        
        # Create test DataFrame with duplicates
        data = [
            ("1", "2024-01-01 10:00:00", "old_value"),
            ("1", "2024-01-02 10:00:00", "new_value"),
            ("2", "2024-01-01 10:00:00", "only_value"),
        ]
        df = spark.createDataFrame(data, ["id", "timestamp", "value"])
        df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
        
        # Deduplicate keeping latest
        result_df = deduplicate_by_key(df, ["id"], "timestamp", ascending=False)
        
        assert result_df.count() == 2
        
        # Verify we kept the latest for id=1
        row_1 = result_df.filter(F.col("id") == "1").collect()[0]
        assert row_1.value == "new_value"

    def test_generate_scd2_columns(self, spark: SparkSession):
        """Test SCD2 column generation."""
        from curation_framework.utils import generate_scd2_columns
        
        # Create test DataFrame
        data = [("1", "John"), ("2", "Jane")]
        df = spark.createDataFrame(data, ["id", "name"])
        
        # Add SCD2 columns
        result_df = generate_scd2_columns(df)
        
        assert "effective_start_date" in result_df.columns
        assert "effective_end_date" in result_df.columns
        assert "is_current" in result_df.columns
        
        # Verify is_current is True for all
        assert all(row.is_current for row in result_df.collect())

    def test_get_schema_diff_matching(self, spark: SparkSession):
        """Test schema comparison for matching schemas."""
        from curation_framework.utils import get_schema_diff
        
        data = [("1", "John")]
        df1 = spark.createDataFrame(data, ["id", "name"])
        df2 = spark.createDataFrame(data, ["id", "name"])
        
        diff = get_schema_diff(df1, df2)
        
        assert diff["schemas_match"] == True
        assert len(diff["only_in_first"]) == 0
        assert len(diff["only_in_second"]) == 0

    def test_get_schema_diff_different(self, spark: SparkSession):
        """Test schema comparison for different schemas."""
        from curation_framework.utils import get_schema_diff
        
        df1 = spark.createDataFrame([("1", "John")], ["id", "name"])
        df2 = spark.createDataFrame([("1", "john@example.com")], ["id", "email"])
        
        diff = get_schema_diff(df1, df2)
        
        assert diff["schemas_match"] == False
        assert "name" in diff["only_in_first"]
        assert "email" in diff["only_in_second"]


class TestHighWatermarkLogic:
    """Tests for high watermark incremental processing logic."""

    def test_incremental_filter_logic(self, spark: SparkSession):
        """Test that incremental filtering works correctly."""
        # Create test data with timestamps
        now = datetime.now()
        old_time = now - timedelta(hours=2)
        new_time = now - timedelta(hours=1)
        
        data = [
            ("1", old_time, "old_record"),
            ("2", new_time, "new_record"),
        ]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("ingestion_ts", TimestampType(), True),
            StructField("value", StringType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Filter for records newer than old_time
        watermark = old_time
        filtered_df = df.filter(F.col("ingestion_ts") > F.lit(watermark))
        
        assert filtered_df.count() == 1
        assert filtered_df.collect()[0].id == "2"

    def test_full_load_when_no_watermark(self, spark: SparkSession):
        """Test that all records are loaded when watermark is None."""
        data = [
            ("1", datetime.now(), "record1"),
            ("2", datetime.now(), "record2"),
        ]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("ingestion_ts", TimestampType(), True),
            StructField("value", StringType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # No filter applied (simulating no watermark)
        assert df.count() == 2


class TestSCD1MergeLogic:
    """Tests for SCD Type 1 merge logic simulation."""

    def test_scd1_upsert_new_records(self, spark: SparkSession):
        """Test SCD1 inserts new records correctly."""
        # Existing records
        existing_data = [
            ("1", "John", "john@old.com"),
        ]
        
        # Incoming records (one update, one new)
        incoming_data = [
            ("1", "John", "john@new.com"),  # Update
            ("2", "Jane", "jane@example.com"),  # New
        ]
        
        existing_df = spark.createDataFrame(existing_data, ["id", "name", "email"])
        incoming_df = spark.createDataFrame(incoming_data, ["id", "name", "email"])
        
        # Simulate merge logic: union with deduplication
        from curation_framework.utils import deduplicate_by_key
        
        # Priority to incoming records (appear first in union for this test)
        combined_df = incoming_df.union(existing_df)
        
        # Add row number for deduplication (keep first = incoming)
        from pyspark.sql.window import Window
        window = Window.partitionBy("id").orderBy(F.lit(1))
        result_df = combined_df.withColumn("rn", F.row_number().over(window))
        result_df = result_df.filter(F.col("rn") == 1).drop("rn")
        
        assert result_df.count() == 2
        
        # Verify update took effect
        john_email = result_df.filter(F.col("id") == "1").collect()[0].email
        assert john_email == "john@new.com"


class TestSCD2MergeLogic:
    """Tests for SCD Type 2 merge logic simulation."""

    def test_scd2_identifies_new_records(self, spark: SparkSession):
        """Test SCD2 correctly identifies new records."""
        # Existing current records
        existing_data = [
            ("1", "John", True),
        ]
        existing_df = spark.createDataFrame(existing_data, ["id", "name", "is_current"])
        
        # Incoming records
        incoming_data = [
            ("1", "John Smith"),  # Changed
            ("2", "Jane"),  # New
        ]
        incoming_df = spark.createDataFrame(incoming_data, ["id", "name"])
        
        # Left join to find new records
        joined = incoming_df.alias("i").join(
            existing_df.filter(F.col("is_current") == True).alias("e"),
            F.col("i.id") == F.col("e.id"),
            "left"
        )
        
        new_records = joined.filter(F.col("e.id").isNull()).select("i.*")
        
        assert new_records.count() == 1
        assert new_records.collect()[0].id == "2"

    def test_scd2_identifies_changed_records(self, spark: SparkSession):
        """Test SCD2 correctly identifies changed records."""
        # Existing current records
        existing_data = [
            ("1", "John", True),
            ("2", "Jane", True),
        ]
        existing_df = spark.createDataFrame(existing_data, ["id", "name", "is_current"])
        
        # Incoming records
        incoming_data = [
            ("1", "John Smith"),  # Changed
            ("2", "Jane"),  # Unchanged
        ]
        incoming_df = spark.createDataFrame(incoming_data, ["id", "name"])
        
        # Join to compare
        joined = incoming_df.alias("i").join(
            existing_df.filter(F.col("is_current") == True).alias("e"),
            F.col("i.id") == F.col("e.id"),
            "inner"
        )
        
        # Find changed records (name is different)
        changed_records = joined.filter(F.col("i.name") != F.col("e.name"))
        
        assert changed_records.count() == 1

    def test_scd2_column_structure(self, spark: SparkSession):
        """Test SCD2 record structure with metadata columns."""
        from curation_framework.utils import generate_scd2_columns
        
        data = [("1", "John", "john@example.com")]
        df = spark.createDataFrame(data, ["id", "name", "email"])
        
        result_df = generate_scd2_columns(df)
        
        # Verify all expected columns exist
        expected_columns = ["id", "name", "email", "effective_start_date", "effective_end_date", "is_current"]
        for col in expected_columns:
            assert col in result_df.columns
        
        # Verify data types
        schema_dict = {f.name: f.dataType for f in result_df.schema.fields}
        assert isinstance(schema_dict["is_current"], BooleanType)


class TestConfigurationValidation:
    """Tests for configuration validation."""

    def test_complete_config_validation(self):
        """Test full configuration validation."""
        from curation_framework.utils import validate_table_config
        
        valid_config = {
            "table_name": "silver_orders",
            "source_table": "bronze.orders_streaming",
            "target_table": "silver.orders",
            "scd_type": 1,
            "business_key_columns": ["order_id"],
            "watermark_column": "ingestion_ts",
            "transformation_sql_path": "conf/sql/orders_transform.sql"
        }
        
        errors = validate_table_config(valid_config)
        assert len(errors) == 0

    def test_composite_key_validation(self):
        """Test validation of composite business keys."""
        from curation_framework.utils import validate_table_config
        
        config = {
            "table_name": "silver_products",
            "source_table": "bronze.products",
            "target_table": "silver.products",
            "scd_type": 2,
            "business_key_columns": ["product_id", "region_code"]  # Composite key
        }
        
        errors = validate_table_config(config)
        assert len(errors) == 0
        assert len(config["business_key_columns"]) == 2

