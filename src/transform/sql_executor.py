"""
SQL Executor

Executes resolved SQL queries via Spark and returns DataFrames.
"""

from pyspark.sql import DataFrame, SparkSession

from utils.logging import get_logger

logger = get_logger(__name__)


def execute_sql(
    spark: SparkSession,
    sql: str,
    description: str = "SQL query"
) -> DataFrame:
    """
    Execute a SQL query and return the result DataFrame.
    
    Args:
        spark: SparkSession instance
        sql: SQL query to execute
        description: Description for logging
        
    Returns:
        Result DataFrame
    """
    logger.info(f"Executing {description}")
    logger.debug(f"SQL:\n{sql[:500]}..." if len(sql) > 500 else f"SQL:\n{sql}")
    
    # Set job description for Spark UI observability
    spark.sparkContext.setJobDescription(description)
    
    try:
        df = spark.sql(sql)
        logger.info(f"Query executed successfully")
        return df
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        raise


def execute_sql_with_temp_view(
    spark: SparkSession,
    df: DataFrame,
    view_name: str,
    sql: str,
    description: str = "SQL query"
) -> DataFrame:
    """
    Register a DataFrame as temp view and execute SQL against it.
    
    Args:
        spark: SparkSession instance
        df: DataFrame to register as temp view
        view_name: Name for the temp view
        sql: SQL query referencing the view
        description: Description for logging
        
    Returns:
        Result DataFrame
    """
    df.createOrReplaceTempView(view_name)
    logger.debug(f"Created temp view: {view_name}")
    
    return execute_sql(spark, sql, description)


def explain_sql(spark: SparkSession, sql: str) -> str:
    """
    Get the execution plan for a SQL query.
    
    Useful for debugging and optimization.
    
    Args:
        spark: SparkSession instance
        sql: SQL query to explain
        
    Returns:
        Execution plan string
    """
    df = spark.sql(sql)
    return df._jdf.queryExecution().toString()


class SqlExecutor:
    """
    SQL Executor with SparkSession context.
    
    Provides convenient interface for executing SQL queries.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._temp_views = []
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup_temp_views()
        # Return False to propagate exceptions (or True to suppress them)
        return False
    
    def execute(self, sql: str, description: str = "SQL query") -> DataFrame:
        """
        Execute SQL query.
        
        Args:
            sql: SQL query
            description: Description for logging
            
        Returns:
            Result DataFrame
        """
        return execute_sql(self.spark, sql, description)
    
    def register_temp_view(self, df: DataFrame, view_name: str):
        """
        Register DataFrame as temp view.
        
        Args:
            df: DataFrame to register
            view_name: Name for the temp view
        """
        df.createOrReplaceTempView(view_name)
        self._temp_views.append(view_name)
        logger.debug(f"Registered temp view: {view_name}")
    
    def execute_with_view(
        self,
        df: DataFrame,
        view_name: str,
        sql: str,
        description: str = "SQL query"
    ) -> DataFrame:
        """
        Register DataFrame as view and execute SQL.
        
        Args:
            df: DataFrame to register
            view_name: View name
            sql: SQL query
            description: Description for logging
            
        Returns:
            Result DataFrame
        """
        self.register_temp_view(df, view_name)
        return self.execute(sql, description)
    
    def table(self, table_name: str) -> DataFrame:
        """
        Read a table as DataFrame.
        
        Args:
            table_name: Fully qualified table name
            
        Returns:
            Table DataFrame
        """
        logger.debug(f"Reading table: {table_name}")
        return self.spark.table(table_name)
    
    def cleanup_temp_views(self):
        """Remove all temp views created by this executor."""
        for view_name in self._temp_views:
            try:
                self.spark.catalog.dropTempView(view_name)
                logger.debug(f"Dropped temp view: {view_name}")
            except:
                pass
        self._temp_views.clear()
