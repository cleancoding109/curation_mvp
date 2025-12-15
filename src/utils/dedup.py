"""
Deduplication Utilities

Provides metadata-driven deduplication for source data.
"""

from typing import Dict, Any, List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utils.logging import get_logger

logger = get_logger(__name__)


def parse_order_columns(dedup_order_config: List[str]) -> List[str]:
    """
    Parse dedup_order_columns configuration.
    
    Args:
        dedup_order_config: List like ["_commit_timestamp DESC", "_kafka_offset DESC"]
    
    Returns:
        List of column names (DESC/ASC suffix removed)
    """
    order_columns = []
    for order_spec in dedup_order_config:
        parts = order_spec.split()
        col_name = parts[0]
        order_columns.append(col_name)
    
    # Issue: Warn if no tiebreaker
    if len(order_columns) == 1:
        logger.warning(
            f"Only one order column configured: {order_columns[0]}. "
            "Without a tiebreaker (e.g., _kafka_offset), duplicate timestamps may "
            "produce non-deterministic results."
        )
    
    return order_columns


def deduplicate_by_key(
    df: DataFrame,
    key_columns: List[str],
    order_columns: List[str],
    order_ascending: bool = False
) -> DataFrame:
    """
    Deduplicate DataFrame keeping latest record per key.
    
    Args:
        df: Input DataFrame
        key_columns: Columns forming the unique key
        order_columns: Columns to order by (e.g., ["_commit_timestamp", "_kafka_offset"])
        order_ascending: If True, keep first; if False (default), keep latest
    
    Returns:
        Deduplicated DataFrame
    """
    logger.info(f"Deduplicating by key columns: {key_columns}")
    logger.debug(f"Order by: {order_columns} (ascending={order_ascending})")
    
    # Issue #4: Support multiple order columns for tiebreaker
    order_exprs = [
        F.col(col).asc() if order_ascending else F.col(col).desc()
        for col in order_columns
    ]
    
    window = Window.partitionBy(*key_columns).orderBy(*order_exprs)
    
    # Issue #1: Remove eager count() calls
    deduped = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    
    logger.info(f"Deduplication transformation applied")
    return deduped


def deduplicate_from_metadata(
    df: DataFrame,
    metadata: Dict[str, Any]
) -> DataFrame:
    """
    Deduplicate DataFrame using metadata configuration.
    
    Uses business_key_columns + source_system for grouping and
    dedup_order_columns for ordering (keeps latest).
    
    Args:
        df: Input DataFrame
        metadata: Table metadata dictionary
    
    Returns:
        Deduplicated DataFrame
    """
    # Issue #2/#3: Get business keys + source_system
    business_keys = metadata.get("business_key_columns", [])
    source_system_col = metadata.get("source_system_column", "source_system")
    
    if not business_keys:
        logger.warning("No business_key_columns defined, skipping deduplication")
        return df
    
    # Construct dedup keys: business_key + source_system
    dedup_keys = business_keys + [source_system_col]
    
    # Issue #5: Parse dedup_order_columns configuration
    dedup_order_config = metadata.get("dedup_order_columns", ["_commit_timestamp DESC"])
    order_columns = parse_order_columns(dedup_order_config)
    
    return deduplicate_by_key(
        df,
        key_columns=dedup_keys,
        order_columns=order_columns,
        order_ascending=False  # DESC order (keep latest)
    )


def deduplicate_by_hash(
    df: DataFrame,
    pk_hash_column: str = "_pk_hash",
    order_columns: List[str] = ["_commit_timestamp"]
) -> DataFrame:
    """
    Deduplicate DataFrame by pre-computed primary key hash.
    
    NOTE: Only use this if _pk_hash already exists in DataFrame.
    Typically, deduplication happens BEFORE hash generation.
    
    Args:
        df: Input DataFrame with _pk_hash column
        pk_hash_column: Name of the primary key hash column
        order_columns: Columns to order by
        
    Returns:
        Deduplicated DataFrame
    """
    return deduplicate_by_key(
        df,
        key_columns=[pk_hash_column],
        order_columns=order_columns,
        order_ascending=False
    )


class Deduplicator:
    """
    Deduplicator with metadata context.
    
    Provides convenient deduplication based on table configuration.
    """
    
    def __init__(self, metadata: Dict[str, Any] = None):
        self.metadata = metadata or {}
        
        # Issue #2: Fix metadata structure
        self._business_keys = self.metadata.get("business_key_columns", [])
        self._source_system_col = self.metadata.get("source_system_column", "source_system")
        self._dedup_keys = self._business_keys + [self._source_system_col] if self._business_keys else []
        
        # Issue #5: Parse dedup_order_columns
        dedup_order_config = self.metadata.get("dedup_order_columns", ["_commit_timestamp DESC"])
        self._order_columns = parse_order_columns(dedup_order_config)
    
    def deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Deduplicate DataFrame using configured keys.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Deduplicated DataFrame
        """
        if not self._dedup_keys:
            logger.warning("No dedup keys configured, returning original DataFrame")
            return df
        
        return deduplicate_by_key(
            df,
            key_columns=self._dedup_keys,
            order_columns=self._order_columns,
            order_ascending=False
        )
    
    def with_metadata(self, metadata: Dict[str, Any]) -> "Deduplicator":
        """Create new Deduplicator with different metadata."""
        return Deduplicator(metadata)
