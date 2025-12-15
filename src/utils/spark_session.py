"""
Spark Session Utilities

Provides SparkSession management for local development and Databricks environments.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "CurationFramework") -> SparkSession:
    """
    Get or create a SparkSession.
    
    In Databricks, uses the existing session.
    In local development, creates a new session with Delta Lake support.
    
    Args:
        app_name: Application name for the Spark session
        
    Returns:
        SparkSession instance
    """
    try:
        # Try to get existing Databricks session
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        # Fall back to local Spark session with Delta
        return (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )


def get_dbutils(spark: SparkSession):
    """
    Get dbutils for Databricks operations.
    
    Args:
        spark: SparkSession instance
        
    Returns:
        dbutils instance or None if not in Databricks
    """
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        try:
            # Alternative method for Databricks notebooks
            import IPython
            return IPython.get_ipython().user_ns.get("dbutils")
        except:
            return None
