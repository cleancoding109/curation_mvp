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
        
        Handles two modes based on metadata 'scd2_mode':
        1. Passthrough: Source already has SCD2 columns (historical load)
        2. Managed: Framework calculates SCD2 columns (incremental load)
        """
        logger.info(f"Executing SCD2 strategy for {self.target_table}")
        
        # Determine mode
        scd2_mode = self.metadata.get("scd2_mode", {})
        # Default to managed if not specified or if we can't determine run type
        # In a real framework, run_type would be passed in env_config or args
        # Here we infer: if target doesn't exist, it's likely historical/init
        
        target_table = self.writer.get_target_table(self.metadata)
        table_exists = self.spark.catalog.tableExists(target_table)
        
        if not table_exists:
            logger.info(f"Target table {target_table} does not exist. Running Initial Load.")
            self._execute_initial(df)
        else:
            logger.info(f"Target table {target_table} exists. Running Incremental Load.")
            self._execute_managed(df, target_table)
            
        logger.info(f"SCD2 strategy completed for {self.target_table}")

    def _execute_initial(self, df: DataFrame):
        """
        Initial load - treats all records as new versions.
        """
        start_col = self.scd2_columns.get("effective_start_date", "effective_start_date")
        end_col = self.scd2_columns.get("effective_end_date", "effective_end_date")
        curr_col = self.scd2_columns.get("is_current", "is_current")
        
        # Issue #3 & #4: Fix source timestamp column extraction
        source_ts_col = None
        for col in self.metadata.get("columns", []):
            if col.get("transform") == "to_utc":
                source_ts_col = col.get("target_col")
                break
        
        if not source_ts_col:
            source_ts_col = "event_at_utc" # Default fallback
        
        # Use source timestamp if available, else current_timestamp
        if source_ts_col in df.columns:
            start_ts = F.col(source_ts_col)
        else:
            start_ts = F.current_timestamp()
            
        prepared_df = df.withColumn(start_col, start_ts) \
                        .withColumn(end_col, F.lit(None).cast("timestamp")) \
                        .withColumn(curr_col, F.lit(True))
        
        # For initial load, we can just overwrite or append
        self.writer.insert(prepared_df, self.metadata, mode="overwrite")

    def _execute_managed(self, df: DataFrame, target_table: str):
        """
        Managed SCD2 - Change detection and Merge.
        """
        # Config
        # Issue #2: Fix hash column extraction (using defaults as keys are standard)
        pk_hash_col = "_pk_hash"
        diff_hash_col = "_diff_hash"
        
        # Issue #3: Fix source timestamp column extraction
        source_ts_col = None
        for col in self.metadata.get("columns", []):
            if col.get("transform") == "to_utc":
                source_ts_col = col.get("target_col")
                break
        
        if not source_ts_col:
            source_ts_col = "event_at_utc" # Default fallback
        
        start_col = self.scd2_columns.get("effective_start_date", "effective_start_date")
        end_col = self.scd2_columns.get("effective_end_date", "effective_end_date")
        curr_col = self.scd2_columns.get("is_current", "is_current")
        
        # Issue #5: Use SQL for better optimization (avoid full table scan if possible)
        # and Issue #1: Avoid redundant join by selecting target key
        
        df.createOrReplaceTempView("source_view")
        
        changes_sql = f"""
        SELECT 
            s.*,
            t.{pk_hash_col} as target_pk_hash
        FROM source_view s
        LEFT JOIN {target_table} t 
          ON s.{pk_hash_col} = t.{pk_hash_col} 
          AND t.{curr_col} = true
        WHERE t.{pk_hash_col} IS NULL 
           OR s.{diff_hash_col} != t.{diff_hash_col}
        """
        
        changes = self.spark.sql(changes_sql)
        
        # 3. Prepare Staged Data for MERGE
        # We need to construct a dataset that has:
        # - Rows to CLOSE (Update): merge_key = pk_hash
        # - Rows to INSERT (New): merge_key = NULL
        
        # Updates (Close old) - where target_pk_hash is NOT NULL (meaning it exists in target)
        updates_df = changes.filter(F.col("target_pk_hash").isNotNull()) \
            .withColumn("merge_key", F.col("target_pk_hash")) \
            .drop("target_pk_hash")
        
        # Inserts (New versions)
        # Filter out soft-deletes from being inserted as new versions
        inserts_source = changes
        if "deleted_ind" in df.columns:
            # Handle boolean or string 'true'
            inserts_source = changes.filter(
                (F.col("deleted_ind") != True) & 
                (F.lower(F.col("deleted_ind").cast("string")) != "true")
            )
            
        inserts_df = inserts_source.withColumn("merge_key", F.lit(None).cast("string")) \
            .drop("target_pk_hash")
        
        # Union
        # allowMissingColumns=True helps if types need alignment, though schemas should match
        staged_df = updates_df.unionByName(inserts_df, allowMissingColumns=True)
        
        # 4. Add SCD2 timestamps
        # Use source timestamp for start_date
        if source_ts_col in df.columns:
            eff_start = F.col(source_ts_col)
        else:
            eff_start = F.current_timestamp()
            
        staged_df = staged_df.withColumn(start_col, eff_start) \
                             .withColumn(end_col, F.lit(None).cast("timestamp")) \
                             .withColumn(curr_col, F.lit(True))
                             
        # Execute write
        self.writer.scd2(
            staged_df,
            self.metadata,
            self.scd2_columns,
            merge_key_col="merge_key"
        )
