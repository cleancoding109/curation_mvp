"""
Silver Processor Module - Metadata-Driven Spark Batch Framework

This module implements the core batch processing logic for the Silver layer,
including high-watermark incremental processing and SCD Type 1/2 patterns.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable
import logging


class SilverProcessor:
    """
    Batch processor for Silver layer tables with support for:
    - High-watermark incremental processing
    - SCD Type 1 (Upsert)
    - SCD Type 2 (History tracking without surrogate keys)
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], global_settings: Dict[str, Any]):
        """
        Initialize the Silver Processor.

        Args:
            spark: Active SparkSession
            config: Table-specific configuration from tables_config.json
            global_settings: Global settings from tables_config.json
        """
        self.spark = spark
        self.config = config
        self.global_settings = global_settings
        self.logger = self._setup_logger()
        
        # Extract common config values
        self.table_name = config["table_name"]
        self.source_table = config["source_table"]
        self.target_table = config["target_table"]
        self.scd_type = config["scd_type"]
        self.business_keys = config["business_key_columns"]
        self.watermark_column = config.get("watermark_column", global_settings.get("default_watermark_column", "ingestion_ts"))
        self.target_watermark_column = config.get("target_watermark_column", "processing_timestamp")
        self.transformation_sql_path = config.get("transformation_sql_path")
        
        # SCD2 specific config
        self.scd2_columns = config.get("scd2_columns", {
            "effective_start_date": "effective_start_date",
            "effective_end_date": "effective_end_date",
            "is_current": "is_current"
        })
        self.track_columns = config.get("track_columns", [])
        self.scd2_end_date_value = global_settings.get("scd2_end_date_value", "9999-12-31 23:59:59")

    def _setup_logger(self) -> logging.Logger:
        """Configure logging for the processor."""
        logger = logging.getLogger(f"SilverProcessor.{self.config.get('table_name', 'unknown')}")
        log_level = self.global_settings.get("log_level", "INFO")
        logger.setLevel(getattr(logging, log_level))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def target_table_exists(self) -> bool:
        """
        Check if the target Delta table exists.

        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.target_table}")
            return True
        except Exception:
            return False

    def get_high_watermark(self) -> Optional[datetime]:
        """
        Get the maximum watermark value from the target table.
        This is used for incremental processing to only read new records.

        Returns:
            Optional[datetime]: The maximum timestamp value, or None if table is empty/doesn't exist
        """
        self.logger.info(f"Fetching high watermark from {self.target_table}")
        
        if not self.target_table_exists():
            self.logger.info(f"Target table {self.target_table} does not exist. Initial load required.")
            return None

        try:
            watermark_df = self.spark.sql(f"""
                SELECT MAX({self.target_watermark_column}) as max_watermark
                FROM {self.target_table}
            """)
            
            result = watermark_df.collect()[0]["max_watermark"]
            
            if result is None:
                self.logger.info(f"Target table {self.target_table} is empty. Initial load required.")
                return None
            
            self.logger.info(f"High watermark for {self.target_table}: {result}")
            return result
            
        except Exception as e:
            self.logger.warning(f"Error fetching high watermark: {str(e)}. Proceeding with full load.")
            return None

    def read_incremental_source(self, watermark_value: Optional[datetime] = None) -> DataFrame:
        """
        Read from the Bronze source table with optional incremental filtering.

        Args:
            watermark_value: The watermark timestamp to filter from. If None, reads all data.

        Returns:
            DataFrame: Filtered source DataFrame
        """
        self.logger.info(f"Reading from source table: {self.source_table}")
        
        if watermark_value is None:
            self.logger.info("No watermark provided - performing full load")
            df = self.spark.read.table(self.source_table)
        else:
            self.logger.info(f"Incremental load: filtering where {self.watermark_column} > '{watermark_value}'")
            df = (
                self.spark.read.table(self.source_table)
                .filter(F.col(self.watermark_column) > F.lit(watermark_value))
            )
        
        record_count = df.count()
        self.logger.info(f"Read {record_count} records from source")
        
        return df

    def apply_transformation(self, source_df: DataFrame) -> DataFrame:
        """
        Apply SQL transformation to the source DataFrame.
        Registers the source as a temp view and executes the transformation SQL.

        Args:
            source_df: The filtered source DataFrame

        Returns:
            DataFrame: Transformed DataFrame
        """
        # Register source as temp view
        source_df.createOrReplaceTempView("source_incremental")
        
        if self.transformation_sql_path:
            self.logger.info(f"Applying transformation from: {self.transformation_sql_path}")
            
            # Read SQL file from workspace
            try:
                # Try reading from DBFS/Workspace
                sql_content = self.spark.read.text(
                    f"file:{self.transformation_sql_path}"
                ).collect()
                sql_query = "\n".join([row.value for row in sql_content])
            except Exception:
                # Fallback: read from local file system (for local development)
                try:
                    with open(self.transformation_sql_path, 'r') as f:
                        sql_query = f.read()
                except Exception as e:
                    self.logger.error(f"Failed to read SQL file: {str(e)}")
                    raise
            
            transformed_df = self.spark.sql(sql_query)
        else:
            self.logger.info("No transformation SQL specified - using source as-is")
            transformed_df = source_df
        
        return transformed_df

    def process_scd_type1(self, transformed_df: DataFrame) -> int:
        """
        Process SCD Type 1 (Upsert) - Standard MERGE operation.
        Inserts new records and updates existing ones based on business keys.

        Args:
            transformed_df: The transformed DataFrame to merge

        Returns:
            int: Number of records processed
        """
        self.logger.info(f"Processing SCD Type 1 for {self.target_table}")
        
        record_count = transformed_df.count()
        if record_count == 0:
            self.logger.info("No records to process")
            return 0

        if not self.target_table_exists():
            self.logger.info(f"Creating new table {self.target_table}")
            transformed_df.write.format("delta").mode("overwrite").saveAsTable(self.target_table)
            self.logger.info(f"Initial load complete: {record_count} records inserted")
            return record_count

        # Perform MERGE for upsert
        target_delta = DeltaTable.forName(self.spark, self.target_table)
        
        # Build merge condition from business keys
        merge_condition = " AND ".join([
            f"target.{key} = source.{key}" for key in self.business_keys
        ])
        
        self.logger.info(f"Merge condition: {merge_condition}")

        # Get all columns except business keys for update
        update_columns = [col for col in transformed_df.columns if col not in self.business_keys]
        update_set = {col: f"source.{col}" for col in update_columns}
        
        # Perform the merge
        (
            target_delta.alias("target")
            .merge(transformed_df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        self.logger.info(f"SCD Type 1 merge complete: {record_count} records processed")
        return record_count

    def process_scd_type2(self, transformed_df: DataFrame) -> int:
        """
        Process SCD Type 2 (History Tracking) - Batch-optimized pattern.
        
        This implements the following logic:
        1. Identify truly new records (not in target)
        2. Identify changed records (in target but with different values)
        3. Close old versions (update end_date, set is_current=False)
        4. Insert new versions for both new and changed records

        Args:
            transformed_df: The transformed DataFrame to process

        Returns:
            int: Number of records processed
        """
        self.logger.info(f"Processing SCD Type 2 for {self.target_table}")
        
        record_count = transformed_df.count()
        if record_count == 0:
            self.logger.info("No records to process")
            return 0

        # Column names for SCD2
        start_date_col = self.scd2_columns.get("effective_start_date", "effective_start_date")
        end_date_col = self.scd2_columns.get("effective_end_date", "effective_end_date")
        is_current_col = self.scd2_columns.get("is_current", "is_current")
        
        current_timestamp = F.current_timestamp()
        end_date_max = F.to_timestamp(F.lit(self.scd2_end_date_value))

        # Add SCD2 metadata columns to incoming data
        incoming_df = (
            transformed_df
            .withColumn(start_date_col, current_timestamp)
            .withColumn(end_date_col, end_date_max)
            .withColumn(is_current_col, F.lit(True))
        )

        # Handle initial load
        if not self.target_table_exists():
            self.logger.info(f"Creating new SCD2 table {self.target_table}")
            incoming_df.write.format("delta").mode("overwrite").saveAsTable(self.target_table)
            self.logger.info(f"Initial load complete: {record_count} records inserted")
            return record_count

        # Read current records from target (only current versions)
        current_target_df = (
            self.spark.read.table(self.target_table)
            .filter(F.col(is_current_col) == True)
        )

        # Build join condition for business keys
        join_condition = [incoming_df[key] == current_target_df[key] for key in self.business_keys]

        # Determine which columns to track for changes
        if self.track_columns:
            columns_to_track = self.track_columns
        else:
            # Default: track all non-key, non-SCD2-metadata columns
            exclude_cols = (
                self.business_keys + 
                [start_date_col, end_date_col, is_current_col, self.target_watermark_column, "source_timestamp"]
            )
            columns_to_track = [col for col in transformed_df.columns if col not in exclude_cols]

        self.logger.info(f"Tracking columns for changes: {columns_to_track}")

        # Join incoming with current target to find matches
        joined_df = incoming_df.alias("incoming").join(
            current_target_df.alias("target"),
            join_condition,
            "left"
        )

        # Identify NEW records (no match in target)
        new_records_df = (
            joined_df
            .filter(F.col(f"target.{self.business_keys[0]}").isNull())
            .select([F.col(f"incoming.{c}") for c in incoming_df.columns])
        )
        new_count = new_records_df.count()
        self.logger.info(f"New records identified: {new_count}")

        # Identify CHANGED records (match exists but values differ)
        # Build change detection condition
        change_conditions = [
            (F.col(f"incoming.{col}") != F.col(f"target.{col}")) | 
            (F.col(f"incoming.{col}").isNull() != F.col(f"target.{col}").isNull())
            for col in columns_to_track
        ]
        
        if change_conditions:
            has_changes = change_conditions[0]
            for cond in change_conditions[1:]:
                has_changes = has_changes | cond
        else:
            # No columns to track - treat all matched records as unchanged
            has_changes = F.lit(False)

        changed_records_df = (
            joined_df
            .filter(F.col(f"target.{self.business_keys[0]}").isNotNull())
            .filter(has_changes)
            .select([F.col(f"incoming.{c}") for c in incoming_df.columns])
        )
        changed_count = changed_records_df.count()
        self.logger.info(f"Changed records identified: {changed_count}")

        # If no changes, exit early
        if new_count == 0 and changed_count == 0:
            self.logger.info("No new or changed records to process")
            return 0

        # Get the target Delta table
        target_delta = DeltaTable.forName(self.spark, self.target_table)

        # Step 1: Close old versions for changed records
        if changed_count > 0:
            # Get business keys of changed records for the update condition
            changed_keys_df = changed_records_df.select(self.business_keys).distinct()
            
            # Build merge condition to close old records
            merge_condition = " AND ".join([
                f"target.{key} = closing.{key}" for key in self.business_keys
            ])
            
            self.logger.info("Closing old versions of changed records")
            
            (
                target_delta.alias("target")
                .merge(changed_keys_df.alias("closing"), merge_condition)
                .whenMatchedUpdate(
                    condition=f"target.{is_current_col} = true",
                    set={
                        end_date_col: "current_timestamp()",
                        is_current_col: "false"
                    }
                )
                .execute()
            )

        # Step 2: Insert all new versions (both new and changed records)
        records_to_insert = new_records_df.union(changed_records_df)
        insert_count = records_to_insert.count()
        
        if insert_count > 0:
            self.logger.info(f"Inserting {insert_count} new record versions")
            records_to_insert.write.format("delta").mode("append").saveAsTable(self.target_table)

        self.logger.info(f"SCD Type 2 complete: {new_count} new, {changed_count} updated")
        return insert_count

    def process(self) -> Dict[str, Any]:
        """
        Main processing method - orchestrates the full batch processing flow.

        Returns:
            Dict containing processing statistics and status
        """
        start_time = datetime.now()
        self.logger.info(f"Starting batch processing for {self.table_name}")
        
        result = {
            "table_name": self.table_name,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "scd_type": self.scd_type,
            "status": "success",
            "records_processed": 0,
            "start_time": start_time.isoformat(),
            "end_time": None,
            "duration_seconds": None,
            "error": None
        }

        try:
            # Step 1: Get high watermark for incremental processing
            watermark = self.get_high_watermark()
            result["watermark_used"] = str(watermark) if watermark else "FULL_LOAD"

            # Step 2: Read incremental source data
            source_df = self.read_incremental_source(watermark)
            
            if source_df.count() == 0:
                self.logger.info("No new records to process")
                result["records_processed"] = 0
            else:
                # Step 3: Apply transformations
                transformed_df = self.apply_transformation(source_df)

                # Step 4: Process based on SCD type
                if self.scd_type == 1:
                    records_processed = self.process_scd_type1(transformed_df)
                elif self.scd_type == 2:
                    records_processed = self.process_scd_type2(transformed_df)
                else:
                    raise ValueError(f"Unsupported SCD type: {self.scd_type}")

                result["records_processed"] = records_processed

        except Exception as e:
            self.logger.error(f"Error processing {self.table_name}: {str(e)}")
            result["status"] = "failed"
            result["error"] = str(e)
            raise

        finally:
            end_time = datetime.now()
            result["end_time"] = end_time.isoformat()
            result["duration_seconds"] = (end_time - start_time).total_seconds()

        self.logger.info(f"Completed processing for {self.table_name}: {result['records_processed']} records in {result['duration_seconds']:.2f}s")
        return result


class BatchFrameworkOrchestrator:
    """
    Orchestrator class to manage batch processing of multiple tables
    based on metadata configuration.
    """

    def __init__(self, spark: SparkSession, config_path: str):
        """
        Initialize the orchestrator with configuration.

        Args:
            spark: Active SparkSession
            config_path: Path to tables_config.json
        """
        self.spark = spark
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Configure logging for the orchestrator."""
        logger = logging.getLogger("BatchFrameworkOrchestrator")
        log_level = self.config.get("global_settings", {}).get("log_level", "INFO")
        logger.setLevel(getattr(logging, log_level))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        import json
        
        try:
            # Try reading from DBFS/Workspace
            config_df = self.spark.read.text(f"file:{self.config_path}")
            json_str = "\n".join([row.value for row in config_df.collect()])
            return json.loads(json_str)
        except Exception:
            # Fallback: read from local file system
            try:
                with open(self.config_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                raise RuntimeError(f"Failed to load config from {self.config_path}: {str(e)}")

    def process_table(self, table_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single table.

        Args:
            table_config: Configuration for the table

        Returns:
            Dict containing processing results
        """
        global_settings = self.config.get("global_settings", {})
        processor = SilverProcessor(self.spark, table_config, global_settings)
        return processor.process()

    def process_all_tables(self, table_filter: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Process all enabled tables in the configuration.

        Args:
            table_filter: Optional list of table names to process. If None, processes all enabled tables.

        Returns:
            List of processing results for each table
        """
        results = []
        tables = self.config.get("tables", [])
        
        for table_config in tables:
            table_name = table_config.get("table_name")
            is_enabled = table_config.get("enabled", True)
            
            # Skip disabled tables
            if not is_enabled:
                self.logger.info(f"Skipping disabled table: {table_name}")
                continue
            
            # Apply filter if specified
            if table_filter and table_name not in table_filter:
                self.logger.info(f"Skipping filtered table: {table_name}")
                continue
            
            self.logger.info(f"Processing table: {table_name}")
            
            try:
                result = self.process_table(table_config)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Failed to process {table_name}: {str(e)}")
                results.append({
                    "table_name": table_name,
                    "status": "failed",
                    "error": str(e)
                })
        
        return results

    def get_processing_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate a summary of the processing run.

        Args:
            results: List of processing results

        Returns:
            Summary dictionary
        """
        total_records = sum(r.get("records_processed", 0) for r in results)
        successful = len([r for r in results if r.get("status") == "success"])
        failed = len([r for r in results if r.get("status") == "failed"])
        
        return {
            "total_tables_processed": len(results),
            "successful": successful,
            "failed": failed,
            "total_records_processed": total_records,
            "failed_tables": [r.get("table_name") for r in results if r.get("status") == "failed"]
        }
