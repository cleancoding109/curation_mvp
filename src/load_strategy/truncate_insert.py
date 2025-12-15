"""
Truncate-Insert Load Strategy

Truncates the entire target table and inserts new data.
Suitable for full table refreshes.
"""

from typing import Dict, Any

from pyspark.sql import DataFrame

from load_strategy.base import LoadStrategy
from load_strategy.factory import register_strategy
from writer.delta_writer import write_truncate_insert


@register_strategy("truncate_insert")
class TruncateInsertStrategy(LoadStrategy):
    """
    Truncate-Insert load strategy.
    
    Overwrites the entire target table with new data.
    All existing records are deleted and replaced.
    
    Use cases:
    - Small dimension tables
    - Snapshot tables
    - Reference/lookup tables
    - Full reloads
    """
    
    name = "truncate_insert"
    
    def execute(self, df: DataFrame) -> Dict[str, Any]:
        """
        Execute truncate-insert load.
        
        Args:
            df: DataFrame with complete data
            
        Returns:
            Execution metrics
        """
        row_count = df.count()
        
        write_truncate_insert(
            df=df,
            target_table=self.target_table
        )
        
        return {
            "rows_processed": row_count,
            "rows_inserted": row_count,
            "rows_updated": 0,
            "rows_deleted": -1,  # Full truncate
            "full_refresh": True
        }
