"""
Delete-Insert Load Strategy

Deletes existing records matching partition keys, then inserts new data.
Suitable for partition-level refreshes.
"""

from typing import Dict, Any, List

from pyspark.sql import DataFrame

from load_strategy.base import LoadStrategy
from writer.delta_writer import write_delete_insert


class DeleteInsertStrategy(LoadStrategy):
    """
    Delete-Insert load strategy.
    
    Deletes all records matching the partition keys in the incoming data,
    then inserts the new records.
    
    Use cases:
    - Daily partition refreshes
    - Reprocessing specific date ranges
    - Regional/segment data updates
    """
    
    name = "delete_insert"
    
    @property
    def partition_columns(self) -> List[str]:
        """Get partition columns from metadata."""
        return self.metadata.get("partition_columns", [])
    
    def validate_input(self, df: DataFrame) -> bool:
        """Validate that partition columns exist."""
        super().validate_input(df)
        
        if not self.partition_columns:
            raise ValueError(
                "partition_columns must be defined in metadata for delete_insert strategy"
            )
        
        missing = [c for c in self.partition_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Missing partition columns in DataFrame: {missing}")
        
        return True
    
    def execute(self, df: DataFrame) -> Dict[str, Any]:
        """
        Execute delete-insert load.
        
        Args:
            df: DataFrame with new data
            
        Returns:
            Execution metrics
        """
        row_count = df.count()
        
        # Get distinct partition values for logging
        partitions = df.select(*self.partition_columns).distinct().count()
        self._logger.info(f"Processing {partitions} distinct partition(s)")
        
        write_delete_insert(
            spark=self.spark,
            df=df,
            target_table=self.target_table,
            partition_columns=self.partition_columns
        )
        
        return {
            "rows_processed": row_count,
            "rows_inserted": row_count,
            "rows_updated": 0,
            "rows_deleted": -1,  # Unknown count
            "partitions_refreshed": partitions
        }
