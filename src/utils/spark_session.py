"""
Spark Session Utilities

Provides SparkSession management for local development and Databricks environments.
"""

import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "CurationFramework", cluster_id: str = None, use_databricks_connect: bool = None) -> SparkSession:
    """
    Get or create a SparkSession.
    
    In Databricks, uses Databricks Connect.
    In local development, creates a new session.
    
    Args:
        app_name: Application name for the Spark session
        cluster_id: Optional Databricks cluster ID for Databricks Connect
        use_databricks_connect: Force Databricks Connect (True) or local (False). 
                               If None, attempts Databricks Connect first, falls back to local.
        
    Returns:
        SparkSession instance
    """
    # Check environment variable for explicit mode
    if use_databricks_connect is None:
        use_databricks_connect = os.environ.get("USE_DATABRICKS_CONNECT", "true").lower() == "true"
    
    if use_databricks_connect:
        try:
            from databricks.connect import DatabricksSession
            
            builder = DatabricksSession.builder
            
            # Check for cluster_id in argument, env var, or use serverless
            cluster = cluster_id or os.environ.get("DATABRICKS_CLUSTER_ID")
            
            if cluster:
                builder = builder.clusterId(cluster)
            else:
                # Try serverless compute
                builder = builder.serverless(True)
            
            # Set profile if specified
            profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEV")
            builder = builder.profile(profile)
            
            return builder.getOrCreate()
        except ImportError:
            print("Databricks Connect not installed, using local Spark")
        except Exception as e:
            print(f"Databricks Connect failed: {e}, falling back to local Spark")
    
    # Fall back to local Spark session
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
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
