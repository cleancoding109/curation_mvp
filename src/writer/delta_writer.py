"""
Delta Writer

Writes DataFrames to Delta tables using various strategies.
Supports INSERT, MERGE, DELETE-INSERT operations.
"""

from typing import Dict, Any, List, Optional

from pyspark.sql import DataFrame, SparkSession

from config.environment import EnvironmentConfig
from utils.logging import get_logger

logger = get_logger(__name__)


def write_insert(
    df: DataFrame,
    target_table: str,
    mode: str = "append"
):
    """
    Write DataFrame to target table using INSERT.
    
    Args:
        df: DataFrame to write
        target_table: Fully qualified target table name
        mode: Write mode ("append" or "overwrite")
    """
    logger.info(f"Writing {df.count():,} rows to {target_table} (mode={mode})")
    
    df.write.format("delta").mode(mode).saveAsTable(target_table)
    
    logger.info(f"Write completed to {target_table}")


def write_merge(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
    merge_keys: List[str],
    update_columns: List[str] = None
):
    """
    Write DataFrame to target table using MERGE.
    
    Args:
        spark: SparkSession instance
        df: DataFrame to write
        target_table: Fully qualified target table name
        merge_keys: Columns to join on
        update_columns: Columns to update (default: all non-key columns)
    """
    # Register source as temp view
    source_view = "_merge_source"
    df.createOrReplaceTempView(source_view)
    
    # Build merge condition
    merge_condition = " AND ".join([
        f"target.{col} = source.{col}" for col in merge_keys
    ])
    
    # Get columns for update
    all_columns = df.columns
    if update_columns is None:
        update_columns = [c for c in all_columns if c not in merge_keys]
    
    # Build update set clause
    update_set = ", ".join([
        f"target.{col} = source.{col}" for col in update_columns
    ])
    
    # Build insert columns
    insert_cols = ", ".join(all_columns)
    insert_values = ", ".join([f"source.{col}" for col in all_columns])
    
    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING {source_view} AS source
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
    """
    
    logger.info(f"Executing MERGE into {target_table}")
    logger.debug(f"Merge keys: {merge_keys}")
    
    spark.sql(merge_sql)
    
    # Cleanup
    spark.catalog.dropTempView(source_view)
    
    logger.info(f"MERGE completed to {target_table}")


def write_delete_insert(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
    partition_columns: List[str]
):
    """
    Delete matching partitions and insert new data.
    
    Args:
        spark: SparkSession instance
        df: DataFrame to write
        target_table: Fully qualified target table name
        partition_columns: Partition columns to match for delete
    """
    # Get distinct partition values from source
    partitions = df.select(*partition_columns).distinct().collect()
    
    for partition in partitions:
        conditions = " AND ".join([
            f"{col} = '{partition[col]}'" for col in partition_columns
        ])
        delete_sql = f"DELETE FROM {target_table} WHERE {conditions}"
        
        logger.debug(f"Deleting: {conditions}")
        spark.sql(delete_sql)
    
    # Insert new data
    write_insert(df, target_table, mode="append")
    
    logger.info(f"DELETE-INSERT completed to {target_table}")


def write_truncate_insert(
    df: DataFrame,
    target_table: str
):
    """
    Truncate target table and insert new data.
    
    Args:
        df: DataFrame to write
        target_table: Fully qualified target table name
    """
    logger.info(f"Truncating and inserting to {target_table}")
    write_insert(df, target_table, mode="overwrite")


def write_scd2(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
    merge_keys: List[str],
    scd2_columns: Dict[str, str]
):
    """
    Write DataFrame to target table using SCD Type 2 (Merge-on-Read).
    
    Args:
        spark: SparkSession instance
        df: DataFrame to write (Source)
        target_table: Fully qualified target table name
        merge_keys: Business keys to join on
        scd2_columns: Dictionary mapping logical names to physical columns
                      {'effective_start_date': 'start_col', 
                       'effective_end_date': 'end_col', 
                       'is_current': 'curr_col'}
    """
    from pyspark.sql import functions as F
    
    # Extract column names
    start_col = scd2_columns.get("effective_start_date", "effective_start_date")
    end_col = scd2_columns.get("effective_end_date", "effective_end_date")
    curr_col = scd2_columns.get("is_current", "is_current")
    
    # Register source as temp view
    source_view = "_scd2_source"
    df.createOrReplaceTempView(source_view)
    
    # 1. Identify rows to UPDATE (close old records)
    # We need to join source with target to find records that exist and are current
    # but have changed (different hash or just new version)
    # For simplicity in this framework, we assume the source contains ONLY changed/new records
    # or that we want to update ANY match found in source.
    
    # Construct the merge key condition
    join_cond = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    
    # Prepare the staged update DataFrame
    # We use a UNION approach:
    # 1. Rows to INSERT (New versions) -> mergeKey = NULL
    # 2. Rows to UPDATE (Close old versions) -> mergeKey = business_key
    
    # We need to handle this carefully. The standard pattern is:
    # MERGE INTO target
    # USING (
    #   SELECT key, ... as mergeKey, ... FROM source  -- For closing old
    #   UNION ALL
    #   SELECT NULL as mergeKey, ... FROM source      -- For inserting new
    # ) staged_updates
    # ON target.key = staged_updates.mergeKey
    
    # However, we only want to close the OLD record if it is currently active (is_current=true)
    
    # Let's build the SQL dynamically
    
    # Columns to select from source
    source_cols = df.columns
    select_cols = ", ".join([f"source.{c}" for c in source_cols])
    
    # Merge Key Selection
    # If we are closing a record, we match on keys.
    # If we are inserting, we force a non-match (NULL).
    
    # We need a unique key for the merge condition to avoid ambiguous matches if source has dupes
    # (Source shouldn't have dupes on business key for one batch)
    
    merge_key_select = ", ".join([f"source.{k} as merge_{k}" for k in merge_keys])
    null_key_select = ", ".join([f"NULL as merge_{k}" for k in merge_keys])
    
    staged_view = "_scd2_staged"
    
    staged_sql = f"""
    CREATE OR REPLACE TEMP VIEW {staged_view} AS
    -- 1. Rows to UPDATE (Close existing current records)
    SELECT 
        {merge_key_select},
        {select_cols}
    FROM {source_view} source
    
    UNION ALL
    
    -- 2. Rows to INSERT (New versions)
    SELECT 
        {null_key_select},
        {select_cols}
    FROM {source_view} source
    """
    
    spark.sql(staged_sql)
    
    # Build the MERGE statement
    
    # ON condition: Match on generated merge keys AND target is current
    on_clause = " AND ".join([f"target.{k} = staged.merge_{k}" for k in merge_keys])
    on_clause += f" AND target.{curr_col} = true"
    
    # UPDATE clause (Close record)
    # Set end_date to source's start_date (which is the new version's start)
    # Set is_current to false
    update_clause = f"""
        UPDATE SET 
            target.{curr_col} = false,
            target.{end_col} = staged.{start_col}
    """
    
    # INSERT clause (New record)
    insert_cols_str = ", ".join(source_cols)
    insert_vals_str = ", ".join([f"staged.{c}" for c in source_cols])
    insert_clause = f"""
        INSERT ({insert_cols_str}) VALUES ({insert_vals_str})
    """
    
    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING {staged_view} AS staged
    ON {on_clause}
    WHEN MATCHED THEN {update_clause}
    WHEN NOT MATCHED THEN {insert_clause}
    """
    
    logger.info(f"Executing SCD2 MERGE into {target_table}")
    spark.sql(merge_sql)
    
    # Cleanup
    spark.catalog.dropTempView(source_view)
    spark.catalog.dropTempView(staged_view)
    
    logger.info(f"SCD2 MERGE completed to {target_table}")


class DeltaWriter:
    """
    Delta writer with environment context.
    
    Provides convenient interface for writing to Delta tables.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        env_config: EnvironmentConfig = None
    ):
        self.spark = spark
        self.env_config = env_config or EnvironmentConfig()
    
    def get_target_table(self, metadata: Dict[str, Any]) -> str:
        """
        Get fully qualified target table name from metadata.
        
        Args:
            metadata: Table metadata dictionary
            
        Returns:
            Fully qualified table name
        """
        target_schema = metadata.get("target_schema", "standardized_data_layer")
        target_table = metadata.get("target_table", metadata.get("table_name"))
        return self.env_config.get_fully_qualified_table(target_schema, target_table)
    
    def insert(self, df: DataFrame, metadata: Dict[str, Any], mode: str = "append"):
        """
        Write using INSERT.
        
        Args:
            df: DataFrame to write
            metadata: Table metadata dictionary
            mode: Write mode
        """
        target = self.get_target_table(metadata)
        write_insert(df, target, mode)
    
    def merge(
        self,
        df: DataFrame,
        metadata: Dict[str, Any],
        merge_keys: List[str] = None
    ):
        """
        Write using MERGE.
        
        Args:
            df: DataFrame to write
            metadata: Table metadata dictionary
            merge_keys: Merge keys (default: primary_key_columns from metadata)
        """
        target = self.get_target_table(metadata)
        
        if merge_keys is None:
            hash_keys = metadata.get("hash_keys", {})
            merge_keys = hash_keys.get("primary_key_columns", [])
        
        if not merge_keys:
            raise ValueError("merge_keys required for MERGE operation")
        
        write_merge(self.spark, df, target, merge_keys)
    
    def delete_insert(
        self,
        df: DataFrame,
        metadata: Dict[str, Any],
        partition_columns: List[str] = None
    ):
        """
        Write using DELETE-INSERT.
        
        Args:
            df: DataFrame to write
            metadata: Table metadata dictionary
            partition_columns: Partition columns for delete
        """
        target = self.get_target_table(metadata)
        
        if partition_columns is None:
            partition_columns = metadata.get("partition_columns", [])
        
        if not partition_columns:
            raise ValueError("partition_columns required for DELETE-INSERT")
        
        write_delete_insert(self.spark, df, target, partition_columns)
    
    def truncate_insert(self, df: DataFrame, metadata: Dict[str, Any]):
        """
        Write using TRUNCATE-INSERT.
        
        Args:
            df: DataFrame to write
            metadata: Table metadata dictionary
        """
        target = self.get_target_table(metadata)
        write_truncate_insert(df, target)

    def scd2(
        self,
        df: DataFrame,
        metadata: Dict[str, Any],
        scd2_columns: Dict[str, str] = None
    ):
        """
        Write using SCD Type 2.
        
        Args:
            df: DataFrame to write
            metadata: Table metadata dictionary
            scd2_columns: SCD2 column mapping
        """
        target = self.get_target_table(metadata)
        
        load_strategy = metadata.get("load_strategy", {})
        merge_keys = load_strategy.get("business_keys", [])
        
        if not merge_keys:
            raise ValueError("business_keys required for SCD2 operation")
            
        if scd2_columns is None:
            scd2_columns = load_strategy.get("scd2_columns", {})
            
        write_scd2(self.spark, df, target, merge_keys, scd2_columns)
