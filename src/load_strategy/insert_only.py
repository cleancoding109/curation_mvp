"""
Insert Only Load Strategy

Appends all records to target table without any merge or deduplication.
Suitable for event/log tables where all records should be preserved.
"""

from typing import Dict, Any

from pyspark.sql import DataFrame

from load_strategy.base import LoadStrategy
from writer.delta_writer import write_insert


class InsertOnlyStrategy(LoadStrategy):
    """
    Insert Only load strategy.
    
    Simply appends all records to the target table.
    No deduplication or merge logic is applied.
    
    Use cases:
    - Event/audit log tables
    - Append-only fact tables
    - Staging tables
    """
    
    name = "insert_only"
    
    def execute(self, df: DataFrame) -> Dict[str, Any]:
        """
        Execute insert-only load.
        
        Args:
            df: DataFrame to append
            
        Returns:
            Execution metrics
        """
        row_count = df.count()
        
        write_insert(
            df=df,
            target_table=self.target_table,
            mode="append"
        )
        
        return {
            "rows_processed": row_count,
            "rows_inserted": row_count,
            "rows_updated": 0,
            "rows_deleted": 0
        }
