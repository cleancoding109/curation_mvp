"""
Utility Functions for the Curation Framework

Common helper functions for configuration loading, validation,
and Delta Lake operations.
"""

import json
import os
from typing import Dict, Any, List, Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def load_config(spark: SparkSession, config_path: str) -> Dict[str, Any]:
    """
    Load JSON configuration from a file path.
    Supports both local filesystem and DBFS paths.
    
    Args:
        spark: Active SparkSession
        config_path: Path to the configuration file
        
    Returns:
        Parsed configuration dictionary
    """
    try:
        # Try reading from DBFS/Workspace first
        config_df = spark.read.text(f"file:{config_path}")
        json_str = "\n".join([row.value for row in config_df.collect()])
        return json.loads(json_str)
    except Exception:
        # Fallback to local file system
        with open(config_path, 'r') as f:
            return json.load(f)


def load_sql_file(spark: SparkSession, sql_path: str) -> str:
    """
    Load SQL content from a file path.
    
    Args:
        spark: Active SparkSession
        sql_path: Path to the SQL file
        
    Returns:
        SQL content as string
    """
    try:
        # Try DBFS/Workspace first
        sql_df = spark.read.text(f"file:{sql_path}")
        return "\n".join([row.value for row in sql_df.collect()])
    except Exception:
        # Fallback to local file system
        with open(sql_path, 'r') as f:
            return f.read()


def validate_table_config(config: Dict[str, Any]) -> List[str]:
    """
    Validate a table configuration and return any errors.
    
    Args:
        config: Table configuration dictionary
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    required_fields = [
        "table_name",
        "source_table", 
        "target_table",
        "scd_type",
        "business_key_columns"
    ]
    
    for field in required_fields:
        if field not in config:
            errors.append(f"Missing required field: {field}")
    
    if "scd_type" in config:
        if config["scd_type"] not in [1, 2]:
            errors.append(f"Invalid scd_type: {config['scd_type']}. Must be 1 or 2.")
    
    if "business_key_columns" in config:
        if not isinstance(config["business_key_columns"], list) or len(config["business_key_columns"]) == 0:
            errors.append("business_key_columns must be a non-empty list")
    
    return errors


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """
    Check if a Delta table exists.
    
    Args:
        spark: Active SparkSession
        table_name: Fully qualified table name
        
    Returns:
        True if table exists, False otherwise
    """
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False


def get_table_row_count(spark: SparkSession, table_name: str) -> int:
    """
    Get the row count of a table.
    
    Args:
        spark: Active SparkSession
        table_name: Fully qualified table name
        
    Returns:
        Number of rows in the table
    """
    return spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]["cnt"]


def get_delta_table_history(spark: SparkSession, table_name: str, limit: int = 10) -> DataFrame:
    """
    Get the history of a Delta table.
    
    Args:
        spark: Active SparkSession
        table_name: Fully qualified table name
        limit: Maximum number of history records to return
        
    Returns:
        DataFrame with table history
    """
    return spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT {limit}")


def optimize_delta_table(spark: SparkSession, table_name: str, zorder_columns: Optional[List[str]] = None):
    """
    Optimize a Delta table with optional Z-ORDER.
    
    Args:
        spark: Active SparkSession
        table_name: Fully qualified table name
        zorder_columns: Optional list of columns to Z-ORDER by
    """
    if zorder_columns:
        zorder_clause = ", ".join(zorder_columns)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_clause})")
    else:
        spark.sql(f"OPTIMIZE {table_name}")


def vacuum_delta_table(spark: SparkSession, table_name: str, retention_hours: int = 168):
    """
    Vacuum a Delta table to remove old files.
    
    Args:
        spark: Active SparkSession
        table_name: Fully qualified table name
        retention_hours: Number of hours to retain files (default 7 days)
    """
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")


def create_hash_column(df: DataFrame, columns: List[str], hash_column_name: str = "row_hash") -> DataFrame:
    """
    Create a hash column from multiple columns for change detection.
    
    Args:
        df: Input DataFrame
        columns: List of column names to include in hash
        hash_column_name: Name of the resulting hash column
        
    Returns:
        DataFrame with added hash column
    """
    concat_cols = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in columns])
    return df.withColumn(hash_column_name, F.sha2(concat_cols, 256))


def deduplicate_by_key(
    df: DataFrame, 
    key_columns: List[str], 
    order_column: str, 
    ascending: bool = False
) -> DataFrame:
    """
    Deduplicate DataFrame by keeping the latest record per key.
    
    Args:
        df: Input DataFrame
        key_columns: Columns that form the unique key
        order_column: Column to order by for selecting the record to keep
        ascending: If True, keep first; if False, keep last
        
    Returns:
        Deduplicated DataFrame
    """
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy(key_columns).orderBy(
        F.col(order_column).asc() if ascending else F.col(order_column).desc()
    )
    
    return (
        df
        .withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def generate_scd2_columns(
    df: DataFrame,
    start_date_col: str = "effective_start_date",
    end_date_col: str = "effective_end_date", 
    is_current_col: str = "is_current",
    end_date_value: str = "9999-12-31 23:59:59"
) -> DataFrame:
    """
    Add SCD Type 2 metadata columns to a DataFrame.
    
    Args:
        df: Input DataFrame
        start_date_col: Name of the effective start date column
        end_date_col: Name of the effective end date column
        is_current_col: Name of the is_current flag column
        end_date_value: Default value for end date (representing current/active)
        
    Returns:
        DataFrame with SCD2 columns added
    """
    return (
        df
        .withColumn(start_date_col, F.current_timestamp())
        .withColumn(end_date_col, F.to_timestamp(F.lit(end_date_value)))
        .withColumn(is_current_col, F.lit(True))
    )


def get_schema_diff(df1: DataFrame, df2: DataFrame) -> Dict[str, Any]:
    """
    Compare schemas of two DataFrames.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        
    Returns:
        Dictionary with schema differences
    """
    fields1 = {f.name: str(f.dataType) for f in df1.schema.fields}
    fields2 = {f.name: str(f.dataType) for f in df2.schema.fields}
    
    only_in_df1 = set(fields1.keys()) - set(fields2.keys())
    only_in_df2 = set(fields2.keys()) - set(fields1.keys())
    
    type_mismatches = {
        col: {"df1": fields1[col], "df2": fields2[col]}
        for col in set(fields1.keys()) & set(fields2.keys())
        if fields1[col] != fields2[col]
    }
    
    return {
        "only_in_first": list(only_in_df1),
        "only_in_second": list(only_in_df2),
        "type_mismatches": type_mismatches,
        "schemas_match": len(only_in_df1) == 0 and len(only_in_df2) == 0 and len(type_mismatches) == 0
    }
