"""
Job Executor

Entry point for Databricks jobs.
Loads metadata, initializes environment, and triggers the pipeline.
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Dict, Any

from pyspark.sql import SparkSession

from pipeline.lakeflow_curation_pipeline import LakeflowCurationPipeline
from config.environment import EnvironmentConfig
from config.metadata_loader import load_table_metadata as load_metadata_file
from utils.logging import get_logger

logger = get_logger(__name__)


import inspect


def _get_script_dir() -> str:
    """Get the directory where this script is located, handling Databricks exec() context."""
    # Try __file__ first (works in normal Python execution)
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        pass
    
    # Fallback for Databricks exec() context: use inspect to get the frame's file
    try:
        frame = inspect.currentframe()
        if frame and frame.f_code.co_filename:
            return os.path.dirname(os.path.abspath(frame.f_code.co_filename))
    except Exception:
        pass
    
    # Last resort: use sys.argv[0] or current working directory
    if sys.argv and sys.argv[0]:
        return os.path.dirname(os.path.abspath(sys.argv[0]))
    
    return os.getcwd()


def _resolve_metadata_path(path: str) -> str:
    """Resolve metadata path relative to project root if not absolute."""
    if os.path.isabs(path):
        return path
    script_dir = _get_script_dir()
    project_root = os.path.dirname(os.path.dirname(script_dir))
    return os.path.join(project_root, path)


def validate_metadata(metadata: Dict[str, Any]) -> None:
    """Validate metadata has required fields and correct structure."""
    required_fields = ["table_name", "load_strategy"]
    
    # Check for required top-level fields
    missing = [f for f in required_fields if f not in metadata]
    if missing:
        raise ValueError(f"Missing required metadata fields: {missing}")
    
    # Check for source config (nested or flat structure)
    if "source" in metadata:
        source = metadata["source"]
        if "schema" not in source or "table" not in source:
            raise ValueError("source.schema and source.table are required")
    elif "source_schema" not in metadata or "source_table" not in metadata:
        raise ValueError("source configuration (source.schema/table or source_schema/source_table) is required")
    
    # Check for target config (nested or flat structure)
    if "target" in metadata:
        target = metadata["target"]
        if "schema" not in target or "table" not in target:
            raise ValueError("target.schema and target.table are required")
    elif "target_schema" not in metadata or "target_table" not in metadata:
        raise ValueError("target configuration (target.schema/table or target_schema/target_table) is required")
    
    load_strategy = metadata.get("load_strategy", {})
    if "type" not in load_strategy:
        raise ValueError("load_strategy.type is required")
    
    if load_strategy["type"] == "scd2":
        if "business_keys" not in load_strategy:
            raise ValueError("business_keys required for SCD2 tables")
        if "scd2_columns" not in load_strategy:
            raise ValueError("scd2_columns required for SCD2 tables")
    
    logger.info(f"Metadata validation passed for {metadata['table_name']}")


def write_audit_log(
    spark: SparkSession,
    catalog: str,
    table_name: str,
    status: str,
    start_time: datetime,
    end_time: datetime,
    records_processed: int = 0,
    error_message: str = None
):
    """Write execution audit log to audit table."""
    try:
        audit_data = [{
            "table_name": table_name,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": (end_time - start_time).total_seconds(),
            "records_processed": records_processed,
            "error_message": error_message,
            "execution_timestamp": datetime.now()
        }]
        
        audit_df = spark.createDataFrame(audit_data)
        audit_table = f"{catalog}.standardized_data_layer.curation_audit_log"
        
        if spark.catalog.tableExists(audit_table):
            audit_df.write.mode("append").saveAsTable(audit_table)
            logger.info(f"Audit log written for {table_name}: {status}")
        else:
            audit_df.write.mode("overwrite").saveAsTable(audit_table)
            logger.info(f"Created audit log table and written log for {table_name}: {status}")
        
    except Exception as e:
        logger.warning(f"Failed to write audit log: {e}")


def main():
    """Main entry point for job executor."""
    parser = argparse.ArgumentParser(
        description="Lakeflow Curation Pipeline Executor",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("--metadata_path", required=True, help="Path to metadata JSON file")
    parser.add_argument("--catalog", required=False, help="Unity Catalog name")
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info(f"Starting Lakeflow Curation Job")
    logger.info(f"Metadata path: {args.metadata_path}")
    logger.info("=" * 80)
    
    start_time = datetime.now()
    records_processed = 0
    spark = None
    catalog = None
    table_name = None
    
    try:
        # Initialize Spark
        spark = SparkSession.builder.appName("Lakeflow_Curation").getOrCreate()
        logger.info(f"Spark version: {spark.version}")
        
        # Initialize Catalog - Priority: CLI arg > Spark config > Env var > default
        if args.catalog:
            catalog = args.catalog
        else:
            try:
                catalog = spark.conf.get("spark.databricks.catalog")
            except Exception:
                logger.warning("Could not retrieve spark.databricks.catalog from Spark config.")
                catalog = None
                
        if not catalog:
            catalog = os.getenv("DATABRICKS_BUNDLE_CATALOG", "default_catalog")
            
        environment = os.getenv("DATABRICKS_BUNDLE_ENV", "dev")
        
        env_config = EnvironmentConfig()
        env_config.catalog = catalog
        env_config.environment = environment
        
        logger.info(f"Environment: {environment}")
        logger.info(f"Catalog: {catalog}")
        
        # Load table metadata
        metadata_path = _resolve_metadata_path(args.metadata_path)
        metadata = load_metadata_file(metadata_path)
        table_name = metadata.get("table_name")
        if not table_name:
            raise ValueError("table_name is required in metadata")
        logger.info(f"Table: {table_name}")
        
        # Validate metadata
        validate_metadata(metadata)
        
        # Determine Pipeline Type
        pipeline_type = metadata.get("pipeline_type", "standardization")
        logger.info(f"Pipeline type: {pipeline_type}")
        
        if pipeline_type == "standardization":
            pipeline = LakeflowCurationPipeline(spark, env_config)
        else:
            raise ValueError(f"Unknown pipeline type: {pipeline_type}")
        
        # Run Pipeline
        logger.info(f"Executing pipeline for {table_name}...")
        pipeline.run([metadata])
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info(f"Job completed successfully")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("=" * 80)
        
        # Write audit log
        write_audit_log(spark, catalog, table_name, "SUCCESS", start_time, end_time, records_processed)
        
        sys.exit(0)
        
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.error("=" * 80)
        logger.error(f"Job failed: {e}")
        logger.error(f"Duration: {duration:.2f} seconds")
        logger.error("=" * 80, exc_info=True)
        
        if spark and table_name:
            write_audit_log(spark, catalog, table_name, "FAILED", start_time, end_time, error_message=str(e))
        
        sys.exit(1)


if __name__ == "__main__":
    main()
