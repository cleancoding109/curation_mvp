"""
Deduplication Utilities

Provides metadata-driven deduplication for source data.
"""

from typing import Dict, Any, List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utils.logging import get_logger

logger = get_logger(__name__)


def deduplicate_by_key(
    df: DataFrame,
    key_columns: List[str],
    order_column: str,
    order_ascending: bool = False
) -> DataFrame:
    """
    Deduplicate DataFrame keeping latest record per key.
    
    Args:
        df: Input DataFrame
        key_columns: Columns forming the unique key
        order_column: Column to order by (e.g., timestamp)
        order_ascending: If True, keep first; if False (default), keep latest
        
    Returns:
        Deduplicated DataFrame
    """
    logger.info(f"Deduplicating by key columns: {key_columns}")
    logger.debug(f"Order by: {order_column} (ascending={order_ascending})")
    
    # Create window partitioned by key, ordered by timestamp
    order_expr = F.col(order_column).asc() if order_ascending else F.col(order_column).desc()
    window = Window.partitionBy(*key_columns).orderBy(order_expr)
    
    # Add row number and filter to first row per partition
    original_count = df.count()
    deduped = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    deduped_count = deduped.count()
    
    logger.info(f"Deduplicated: {original_count:,} -> {deduped_count:,} rows")
    
    return deduped


def deduplicate_from_metadata(
    df: DataFrame,
    metadata: Dict[str, Any]
) -> DataFrame:
    """
    Deduplicate DataFrame using metadata configuration.
    
    Uses hash_keys.primary_key_columns for grouping and
    watermark_column for ordering (keeps latest).
    
    Args:
        df: Input DataFrame
        metadata: Table metadata dictionary
        
    Returns:
        Deduplicated DataFrame
    """
    hash_keys = metadata.get("hash_keys", {})
    key_columns = hash_keys.get("primary_key_columns", [])
    
    if not key_columns:
        logger.warning("No primary_key_columns defined, skipping deduplication")
        return df
    
    order_column = metadata.get("watermark_column", "_commit_timestamp")
    
    return deduplicate_by_key(
        df,
        key_columns=key_columns,
        order_column=order_column,
        order_ascending=False  # Keep latest
    )


def deduplicate_by_hash(
    df: DataFrame,
    pk_hash_column: str = "_pk_hash",
    order_column: str = "_commit_timestamp"
) -> DataFrame:
    """
    Deduplicate DataFrame by pre-computed primary key hash.
    
    Args:
        df: Input DataFrame with _pk_hash column
        pk_hash_column: Name of the primary key hash column
        order_column: Column to order by
        
    Returns:
        Deduplicated DataFrame
    """
    return deduplicate_by_key(
        df,
        key_columns=[pk_hash_column],
        order_column=order_column,
        order_ascending=False
    )


class Deduplicator:
    """
    Deduplicator with metadata context.
    
    Provides convenient deduplication based on table configuration.
    """
    
    def __init__(self, metadata: Dict[str, Any] = None):
        self.metadata = metadata or {}
        self._hash_keys = self.metadata.get("hash_keys", {})
        self._key_columns = self._hash_keys.get("primary_key_columns", [])
        self._order_column = self.metadata.get("watermark_column", "_commit_timestamp")
    
    def deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Deduplicate DataFrame using configured keys.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Deduplicated DataFrame
        """
        if not self._key_columns:
            logger.warning("No key columns configured, returning original DataFrame")
            return df
        
        return deduplicate_by_key(
            df,
            key_columns=self._key_columns,
            order_column=self._order_column
        )
    
    def with_metadata(self, metadata: Dict[str, Any]) -> "Deduplicator":
        """
        Create new Deduplicator with different metadata.
        
        Args:
            metadata: New metadata configuration
            
        Returns:
            New Deduplicator instance
        """
        return Deduplicator(metadata)
