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
    df = spark.table(fq_table)
    
    row_count = df.count()
    logger.info(f"Source table has {row_count:,} rows")
    
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
    source_schema = metadata.get("source_schema", "unified_dev")
    source_table = metadata.get("source_table", metadata.get("table_name"))
    
    fq_table = env_config.get_fully_qualified_table(source_schema, source_table)
    
    # Get watermark column from metadata if not specified
    if watermark_column is None:
        watermark_column = metadata.get("watermark_column", "_commit_timestamp")
    
    logger.info(f"Reading source table incrementally: {fq_table}")
    logger.info(f"Watermark filter: {watermark_column} > {watermark_value}")
    
    df = spark.table(fq_table).filter(f"{watermark_column} > '{watermark_value}'")
    
    row_count = df.count()
    logger.info(f"Incremental read returned {row_count:,} rows")
    
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
