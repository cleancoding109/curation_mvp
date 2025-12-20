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
        self.scd2_columns = strategy_config.get("scd2_columns")
        if not self.scd2_columns:
            raise ValueError(f"scd2_columns configuration required for SCD2 strategy in {self.target_table}")
        
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
        start_col = self.scd2_columns.get("effective_start_date")
        end_col = self.scd2_columns.get("effective_end_date")
        curr_col = self.scd2_columns.get("is_current")

        if not all([start_col, end_col, curr_col]):
             raise ValueError("SCD2 columns (effective_start_date, effective_end_date, is_current) must be defined in metadata")
        
        # Issue #3 & #4: Fix source timestamp column extraction
        source_ts_col = self.metadata.get("source_timestamp_column")
        
        if not source_ts_col:
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

    def _ensure_scd2_columns(self, target_table: str, target_columns: List[str]) -> List[str]:
        """
        Ensure SCD2 columns exist in target table.
        """
        missing_cols = []
        
        curr_col = self.scd2_columns.get("is_current")
        if curr_col and curr_col not in target_columns:
            missing_cols.append(f"{curr_col} BOOLEAN")
            
        end_col = self.scd2_columns.get("effective_end_date")
        if end_col and end_col not in target_columns:
            missing_cols.append(f"{end_col} TIMESTAMP")
            
        start_col = self.scd2_columns.get("effective_start_date")
        if start_col and start_col not in target_columns:
            missing_cols.append(f"{start_col} TIMESTAMP")
            
        if missing_cols:
            logger.info(f"Adding missing SCD2 columns to {target_table}: {missing_cols}")
            alter_sql = f"ALTER TABLE {target_table} ADD COLUMNS ({', '.join(missing_cols)})"
            self.spark.sql(alter_sql)
            
            # Initialize is_current to true for existing records if we just added it
            if f"{curr_col} BOOLEAN" in missing_cols:
                logger.info(f"Initializing {curr_col} to true for existing records")
                update_sql = f"UPDATE {target_table} SET {curr_col} = true WHERE {curr_col} IS NULL"
                self.spark.sql(update_sql)
                
            # Refresh columns
            return self.spark.table(target_table).columns
            
        return target_columns

    def _execute_managed(self, df: DataFrame, target_table: str):
        """
        Managed SCD2 - Change detection and Merge.
        """
        # Config
        hash_keys = self.metadata.get("hash_keys", {})
        pk_config = hash_keys.get("_pk_hash", {})
        diff_config = hash_keys.get("_diff_hash", {})
        
        source_pk_hash_col = pk_config.get("target_col", "_pk_hash")
        source_diff_hash_col = diff_config.get("target_col", "_diff_hash")
        
        # Detect target columns to handle legacy schema (row_hash vs _pk_hash)
        target_columns = self.spark.table(target_table).columns
        
        # Ensure SCD2 columns exist (Self-healing)
        target_columns = self._ensure_scd2_columns(target_table, target_columns)
        
        target_pk_hash_col = source_pk_hash_col
        if target_pk_hash_col not in target_columns and "row_hash" in target_columns:
            target_pk_hash_col = "row_hash"
            logger.info(f"Using legacy target PK hash column: {target_pk_hash_col}")
            
        target_diff_hash_col = source_diff_hash_col
        if target_diff_hash_col not in target_columns:
            if "diff_hash" in target_columns:
                target_diff_hash_col = "diff_hash"
            elif "change_hash" in target_columns:
                target_diff_hash_col = "change_hash"
            elif "business_diff_hash" in target_columns:
                target_diff_hash_col = "business_diff_hash"
            logger.info(f"Using legacy target Diff hash column: {target_diff_hash_col}")
        
        # Issue #3: Fix source timestamp column extraction
        source_ts_col = self.metadata.get("source_timestamp_column")
        
        if not source_ts_col:
            for col in self.metadata.get("columns", []):
                if col.get("transform") == "to_utc":
                    source_ts_col = col.get("target_col")
                    break
        
        if not source_ts_col:
            source_ts_col = "event_at_utc" # Default fallback
        
        start_col = self.scd2_columns.get("effective_start_date")
        end_col = self.scd2_columns.get("effective_end_date")
        curr_col = self.scd2_columns.get("is_current")

        if not all([start_col, end_col, curr_col]):
             raise ValueError("SCD2 columns (effective_start_date, effective_end_date, is_current) must be defined in metadata")
        
        # Update scd2_columns with resolved name so writer uses it
        self.scd2_columns["is_current"] = curr_col

        # Issue #5: Use SQL for better optimization (avoid full table scan if possible)
        # and Issue #1: Avoid redundant join by selecting target key
        
        df.createOrReplaceTempView("source_view")
        
        # Construct active record condition
        if curr_col in target_columns:
            active_cond = f"t.{curr_col} = true"
        elif end_col in target_columns:
            active_cond = f"t.{end_col} IS NULL"
            logger.info(f"Column {curr_col} not found. Using {end_col} IS NULL for active check.")
            # We set curr_col to None to signal writer not to use it
            self.scd2_columns["is_current"] = None
            curr_col = None
        else:
            raise ValueError(f"Target table {target_table} missing both {curr_col} and {end_col}. Cannot determine active records.")

        changes_sql = f"""
        SELECT
            s.*,
            t.{target_pk_hash_col} as target_pk_hash
        FROM source_view s
        LEFT JOIN {target_table} t
          ON s.{source_pk_hash_col} = t.{target_pk_hash_col}
          AND {active_cond}
        WHERE t.{target_pk_hash_col} IS NULL
           OR s.{source_diff_hash_col} != t.{target_diff_hash_col}
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
        # We insert a new version for ALL changes (New or Updated), unless it's a delete.
        inserts_source = changes
        
        if "deleted_ind" in df.columns:
            # Handle boolean or string 'true'
            inserts_source = inserts_source.filter(
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
            
        # Issue #11: Only add SCD2 columns if they don't exist
        if start_col not in staged_df.columns:
            staged_df = staged_df.withColumn(start_col, eff_start)
        if end_col not in staged_df.columns:
            staged_df = staged_df.withColumn(end_col, F.lit(None).cast("timestamp"))
        if curr_col and curr_col not in staged_df.columns:
            staged_df = staged_df.withColumn(curr_col, F.lit(True))
                             
        # Execute write
        # Pass the detected target PK hash column via metadata override
        write_metadata = self.metadata.copy()
        write_metadata["_pk_hash"] = target_pk_hash_col
        
        # Rename legacy hash columns in staged_df to match target
        if target_pk_hash_col != source_pk_hash_col:
            logger.info(f"Renaming {source_pk_hash_col} to {target_pk_hash_col} for write")
            staged_df = staged_df.withColumnRenamed(source_pk_hash_col, target_pk_hash_col)
            
        if target_diff_hash_col != source_diff_hash_col:
            logger.info(f"Renaming {source_diff_hash_col} to {target_diff_hash_col} for write")
            staged_df = staged_df.withColumnRenamed(source_diff_hash_col, target_diff_hash_col)
        
        self.writer.scd2(
            staged_df,
            write_metadata,
            self.scd2_columns,
            merge_key_col="merge_key"
        )
