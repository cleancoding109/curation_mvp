"""
SCD Type 2 Load Strategy

Implements Slowly Changing Dimension Type 2 (History Tracking).
"""

from typing import Dict, Any, List
from pyspark.sql import DataFrame, functions as F

from load_strategy.base import LoadStrategy
from writer.delta_writer import DeltaWriter
from utils.logging import get_logger

logger = get_logger(__name__)


class SCD2Strategy(LoadStrategy):
    """
    SCD Type 2 Load Strategy.
    
    Maintains history by creating new versions of records when changes occur.
    Uses effective_start_date, effective_end_date, and is_current flags.
    """
    
    name: str = "scd2"
    
    def __init__(
        self,
        spark,
        env_config,
        metadata: Dict[str, Any]
    ):
        super().__init__(spark, env_config, metadata)
        self.writer = DeltaWriter(spark, env_config)
        
        # Extract SCD2 config
        strategy_config = metadata.get("load_strategy", {})
        self.business_keys = strategy_config.get("business_keys", [])
        self.scd2_columns = strategy_config.get("scd2_columns", {
            "effective_start_date": "effective_start_date",
            "effective_end_date": "effective_end_date",
            "is_current": "is_current"
        })
        
        if not self.business_keys:
            raise ValueError(f"business_keys required for SCD2 strategy in {self.target_table}")

    def execute(self, df: DataFrame):
        """
        Execute SCD2 load.
        
        1. Prepare source data (add SCD2 columns)
        2. Perform Merge-on-Read write
        """
        logger.info(f"Executing SCD2 strategy for {self.target_table}")
        
        # Prepare source DataFrame
        # We assume the input df contains the new/changed records
        # We need to add the SCD2 control columns
        
        start_col = self.scd2_columns.get("effective_start_date", "effective_start_date")
        end_col = self.scd2_columns.get("effective_end_date", "effective_end_date")
        curr_col = self.scd2_columns.get("is_current", "is_current")
        
        # Use current timestamp as start date for new versions
        # In a real scenario, this might come from a source timestamp
        current_ts = F.current_timestamp()
        
        prepared_df = df.withColumn(start_col, current_ts) \
                        .withColumn(end_col, F.lit(None).cast("timestamp")) \
                        .withColumn(curr_col, F.lit(True))
                        
        # Execute write
        self.writer.scd2(
            prepared_df,
            self.metadata,
            self.scd2_columns
        )
        
        logger.info(f"SCD2 strategy completed for {self.target_table}")
