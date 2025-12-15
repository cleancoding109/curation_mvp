"""
SCD Type 1 Load Strategy

Implements Slowly Changing Dimension Type 1 (Upsert).
Updates existing records and inserts new ones. No history is preserved.
"""

from typing import Dict, Any, List
from pyspark.sql import DataFrame, functions as F

from load_strategy.base import LoadStrategy
from writer.delta_writer import DeltaWriter
from utils.logging import get_logger

logger = get_logger(__name__)


class SCD1Strategy(LoadStrategy):
    """
    SCD Type 1 Load Strategy (Upsert).
    
    Merges source data into target table:
    - Matches on Primary Key
    - Updates record if content changed (detected via diff_hash)
    - Inserts new records
    """
    
    name: str = "scd1"
    
    def __init__(
        self,
        spark,
        env_config,
        metadata: Dict[str, Any]
    ):
        super().__init__(spark, env_config, metadata)
        self.writer = DeltaWriter(spark, env_config)
        
        # Extract keys from metadata
        hash_keys = metadata.get("hash_keys", {})
        pk_config = hash_keys.get("_pk_hash", {})
        self.pk_columns = pk_config.get("columns", [])
        
        if not self.pk_columns:
            # Fallback to business keys if no PK hash defined
            self.pk_columns = metadata.get("business_key_columns", [])
            
        if not self.pk_columns:
            raise ValueError(f"Primary key columns required for SCD1 strategy in {self.target_table}")

    def execute(self, df: DataFrame):
        """
        Execute SCD1 load (Merge/Upsert).
        """
        logger.info(f"Executing SCD1 strategy for {self.target_table}")
        
        target_table = self.writer.get_target_table(self.metadata)
        
        # Check if target exists
        if not self.spark.catalog.tableExists(target_table):
            logger.info(f"Target table {target_table} does not exist. Creating initial table.")
            self.writer.write_initial(df, self.metadata)
            return

        # Perform Merge
        self._execute_merge(df, target_table)
        logger.info(f"SCD1 strategy completed for {self.target_table}")

    def _execute_merge(self, source_df: DataFrame, target_table: str):
        """
        Perform Delta Merge operation.
        """
        # Construct merge condition
        # If _pk_hash exists, use it for faster join
        if "_pk_hash" in source_df.columns:
            merge_condition = "target._pk_hash = source._pk_hash"
        else:
            # Fallback to column-based join
            conditions = [f"target.{col} = source.{col}" for col in self.pk_columns]
            merge_condition = " AND ".join(conditions)
            
        # Construct update condition
        # If _diff_hash exists, use it to detect changes
        if "_diff_hash" in source_df.columns:
            update_condition = "target._diff_hash != source._diff_hash"
        else:
            # Update if any non-key column changed (simplified, usually we rely on diff_hash)
            update_condition = "true" 

        logger.debug(f"Merge condition: {merge_condition}")
        logger.debug(f"Update condition: {update_condition}")

        # Use DeltaWriter's merge capability or direct SQL
        # Since DeltaWriter might be generic, let's use Spark SQL MERGE here for control
        
        source_df.createOrReplaceTempView("source_scd1")
        
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING source_scd1 AS source
        ON {merge_condition}
        WHEN MATCHED AND {update_condition} THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        self.spark.sql(merge_sql)
        self.spark.catalog.dropTempView("source_scd1")
