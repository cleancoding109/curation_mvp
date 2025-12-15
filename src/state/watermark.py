"""
Watermark Management

Manages watermark state for incremental processing.
Tracks last processed timestamp/version per table.
"""

from typing import Dict, Any, Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.environment import EnvironmentConfig
from utils.logging import get_logger

logger = get_logger(__name__)

# Default control table for watermarks
DEFAULT_CONTROL_TABLE = "standardized_data_layer.curation_watermarks"


def parse_control_table(control_table: str, env_config: EnvironmentConfig) -> str:
    """
    Parse and fully qualify control table name.
    
    Args:
        control_table: Control table name (e.g. "schema.table" or "table")
        env_config: Environment configuration
        
    Returns:
        Fully qualified table name
    """
    parts = control_table.split(".")
    
    if len(parts) == 1:
        # Just table name, use default schema
        return env_config.get_fully_qualified_table("standardized_data_layer", parts[0])
    elif len(parts) == 2:
        # schema.table
        return env_config.get_fully_qualified_table(parts[0], parts[1])
    elif len(parts) == 3:
        # catalog.schema.table - already fully qualified
        return control_table
    else:
        raise ValueError(f"Invalid control table name: {control_table}")


def get_watermark(
    spark: SparkSession,
    table_name: str,
    env_config: EnvironmentConfig,
    control_table: str = None,
    default_watermark: str = "1970-01-01 00:00:00"
) -> str:
    """
    Get the current watermark value for a table.
    
    Args:
        spark: SparkSession instance
        table_name: Target table name
        env_config: Environment configuration
        control_table: Control table name (default: curation_watermarks)
        default_watermark: Default value if no watermark exists
        
    Returns:
        Watermark value or default if not found
    """
    control = control_table or DEFAULT_CONTROL_TABLE
    fq_control = parse_control_table(control, env_config)
    
    try:
        df = spark.table(fq_control).filter(
            F.col("table_name") == table_name
        )
        
        # Issue #1: Remove eager count() - use first() instead
        result = df.select("watermark_value").first()
        
        if result is None:
            logger.info(
                f"No watermark found for table: {table_name}, "
                f"using default: {default_watermark}"
            )
            return default_watermark
        
        watermark = result[0]
        logger.info(f"Current watermark for {table_name}: {watermark}")
        return watermark
        
    except Exception as e:
        logger.warning(
            f"Could not read watermark: {e}, "
            f"using default: {default_watermark}"
        )
        return default_watermark


def update_watermark(
    spark: SparkSession,
    table_name: str,
    watermark_value: str,
    env_config: EnvironmentConfig,
    control_table: str = None
):
    """
    Update the watermark value for a table.
    
    IMPORTANT: This should only be called AFTER the target table MERGE succeeds.
    Calling this before MERGE violates the failure-safe ordering principle.
    
    Args:
        spark: SparkSession instance
        table_name: Target table name
        watermark_value: New watermark value
        env_config: Environment configuration
        control_table: Control table name (default: curation_watermarks)
    """
    control = control_table or DEFAULT_CONTROL_TABLE
    fq_control = parse_control_table(control, env_config)
    
    # Issue #3: Monotonic Rule - Check if new value > current value
    current = get_watermark(spark, table_name, env_config, control_table, default_watermark="")
    if current and watermark_value <= current:
        logger.warning(
            f"Skipping watermark update for {table_name}: "
            f"new value ({watermark_value}) <= current ({current})"
        )
        return
    
    update_time = datetime.utcnow().isoformat()
    
    # Issue #2: Fix SQL Injection Vulnerability
    # Create a DataFrame for the source to avoid string interpolation of values
    source_data = [(table_name, watermark_value, update_time)]
    source_df = spark.createDataFrame(
        source_data, 
        ["table_name", "watermark_value", "updated_at"]
    )
    source_df.createOrReplaceTempView("watermark_source")
    
    # Use MERGE to upsert watermark using temp view
    merge_sql = f"""
    MERGE INTO {fq_control} AS target
    USING watermark_source AS source
    ON target.table_name = source.table_name
    WHEN MATCHED THEN UPDATE SET
        watermark_value = source.watermark_value,
        updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_sql)
    spark.catalog.dropTempView("watermark_source")
    logger.info(f"Updated watermark for {table_name}: {watermark_value}")


def get_max_watermark_from_df(
    df: DataFrame,
    watermark_column: str = "_commit_timestamp"
) -> Optional[str]:
    """
    Extract max watermark value from a DataFrame.
    
    Args:
        df: DataFrame to extract watermark from
        watermark_column: Column containing watermark values
        
    Returns:
        Max watermark value as string or None
    """
    # Issue #1: Remove eager count()
    result = df.agg(F.max(watermark_column)).first()
    
    if result is None or result[0] is None:
        return None
    
    return str(result[0])


def create_watermark_table(
    spark: SparkSession,
    env_config: EnvironmentConfig,
    control_table: str = None
):
    """
    Create the watermark control table if it doesn't exist.
    
    Args:
        spark: SparkSession instance
        env_config: Environment configuration
        control_table: Control table name
    """
    control = control_table or DEFAULT_CONTROL_TABLE
    fq_control = parse_control_table(control, env_config)
    
    # Issue #6: Fix CREATE TABLE syntax for Delta Lake
    # Delta Lake doesn't support PRIMARY KEY constraints in CREATE TABLE
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {fq_control} (
        table_name STRING NOT NULL,
        watermark_value STRING,
        updated_at TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    COMMENT 'Watermark tracking for curation framework'
    """
    
    spark.sql(create_sql)
    logger.info(f"Ensured watermark table exists: {fq_control}")


class WatermarkManager:
    """
    Watermark manager for incremental processing.
    
    Provides convenient interface for watermark state management.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        env_config: EnvironmentConfig = None,
        control_table: str = None
    ):
        self.spark = spark
        self.env_config = env_config or EnvironmentConfig()
        self.control_table = control_table or DEFAULT_CONTROL_TABLE
    
    def get(self, table_name: str) -> Optional[str]:
        """
        Get current watermark for a table.
        
        Args:
            table_name: Target table name
            
        Returns:
            Watermark value or None
        """
        return get_watermark(
            self.spark,
            table_name,
            self.env_config,
            self.control_table
        )
    
    def update(self, table_name: str, watermark_value: str):
        """
        Update watermark for a table.
        
        Args:
            table_name: Target table name
            watermark_value: New watermark value
        """
        update_watermark(
            self.spark,
            table_name,
            watermark_value,
            self.env_config,
            self.control_table
        )
    
    def update_from_df(self, table_name: str, df: DataFrame, watermark_column: str = None):
        """
        Update watermark from max value in DataFrame.
        
        Args:
            table_name: Target table name
            df: DataFrame to extract max watermark from
            watermark_column: Watermark column name
        """
        col = watermark_column or "_commit_timestamp"
        max_val = get_max_watermark_from_df(df, col)
        
        if max_val:
            self.update(table_name, max_val)
    
    def ensure_table_exists(self):
        """Create watermark control table if needed."""
        create_watermark_table(
            self.spark,
            self.env_config,
            self.control_table
        )
