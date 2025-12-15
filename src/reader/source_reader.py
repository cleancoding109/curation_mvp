"""
Source Reader

Reads from source tables with optional watermark filtering for incremental loads.
"""

from typing import Dict, Any, Optional

from pyspark.sql import DataFrame, SparkSession

from config.environment import EnvironmentConfig
from utils.logging import get_logger

logger = get_logger(__name__)


def read_source_table(
    spark: SparkSession,
    metadata: Dict[str, Any],
    env_config: EnvironmentConfig
) -> DataFrame:
    """
    Read source table as DataFrame.
    
    Args:
        spark: SparkSession instance
        metadata: Table metadata dictionary
        env_config: Environment configuration
        
    Returns:
        Source table DataFrame
    """
    source_schema = metadata.get("source_schema", "unified_dev")
    source_table = metadata.get("source_table", metadata.get("table_name"))
    
    fq_table = env_config.get_fully_qualified_table(source_schema, source_table)
    
    logger.info(f"Reading source table: {fq_table}")
    
    # Issue #8: Add error handling
    if not spark.catalog.tableExists(fq_table):
        raise ValueError(f"Source table does not exist: {fq_table}")
        
    df = spark.table(fq_table)
    
    # Issue #2: Remove eager count
    # row_count = df.count()
    # logger.info(f"Source table has {row_count:,} rows")
    
    return df


def read_source_incremental(
    spark: SparkSession,
    metadata: Dict[str, Any],
    env_config: EnvironmentConfig,
    watermark_value: Any,
    watermark_column: str = None
) -> DataFrame:
    """
    Read source table with watermark filter for incremental processing.
    
    Args:
        spark: SparkSession instance
        metadata: Table metadata dictionary
        env_config: Environment configuration
        watermark_value: Last processed watermark value
        watermark_column: Column to filter on (default from metadata)
        
    Returns:
        Filtered source DataFrame
    """
    from pyspark.sql import functions as F
    
    source_schema = metadata.get("source_schema", "unified_dev")
    source_table = metadata.get("source_table", metadata.get("table_name"))
    
    fq_table = env_config.get_fully_qualified_table(source_schema, source_table)
    
    # Issue #8: Add error handling
    if not spark.catalog.tableExists(fq_table):
        raise ValueError(f"Source table does not exist: {fq_table}")
    
    # Get watermark column from metadata if not specified
    if watermark_column is None:
        watermark_column = metadata.get("watermark_column", "_commit_timestamp")
        
    # Issue #1: Add lookback window
    lookback_hours = metadata.get("lookback_hours", 2)
    
    logger.info(f"Reading source table incrementally: {fq_table}")
    logger.info(f"Watermark: {watermark_column} > {watermark_value} (Lookback: {lookback_hours}h)")
    
    # Issue #4: Fix SQL injection vulnerability using DataFrame API
    # Calculate lookback timestamp
    # We cast watermark_value to timestamp and subtract lookback hours
    
    # Note: watermark_value is typically a string from state store
    
    df = spark.table(fq_table).filter(
        F.col(watermark_column) > 
        (F.lit(watermark_value).cast("timestamp") - F.expr(f"INTERVAL {lookback_hours} HOURS"))
    )
    
    # Issue #3: Add Deduplication
    # Mandatory framework step
    if metadata.get("requires_dedup", True):
        from utils.dedup import deduplicate_from_metadata
        df = deduplicate_from_metadata(df, metadata)
    
    # Issue #2: Remove eager count
    # row_count = df.count()
    # logger.info(f"Incremental read returned {row_count:,} rows")
    
    return df


class SourceReader:
    """
    Source reader with environment and metadata context.
    
    Provides convenient interface for reading source tables.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        env_config: EnvironmentConfig = None
    ):
        self.spark = spark
        self.env_config = env_config or EnvironmentConfig()
    
    def read(self, metadata: Dict[str, Any]) -> DataFrame:
        """
        Read source table for full load.
        
        Args:
            metadata: Table metadata dictionary
            
        Returns:
            Source DataFrame
        """
        return read_source_table(self.spark, metadata, self.env_config)
    
    def read_incremental(
        self,
        metadata: Dict[str, Any],
        watermark_value: Any
    ) -> DataFrame:
        """
        Read source table incrementally from watermark.
        
        Args:
            metadata: Table metadata dictionary
            watermark_value: Last processed watermark value
            
        Returns:
            Filtered source DataFrame
        """
        return read_source_incremental(
            self.spark,
            metadata,
            self.env_config,
            watermark_value
        )
    
    def get_source_schema(self, metadata: Dict[str, Any]) -> str:
        """
        Get source table schema (column info).
        
        Args:
            metadata: Table metadata dictionary
            
        Returns:
            Schema string representation
        """
        df = self.read(metadata)
        return df.schema.simpleString()
