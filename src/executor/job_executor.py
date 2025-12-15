"""
Job Executor

Entry point for Databricks jobs.
Parses arguments, loads configuration, and triggers the pipeline.
"""

import argparse
import yaml
import sys
from typing import Dict, Any

from pyspark.sql import SparkSession

from pipeline.lakeflow_curation_pipeline import LakeflowCurationPipeline
from config.environment import EnvironmentConfig
from utils.logging import get_logger

logger = get_logger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load YAML configuration file.
    """
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config file {config_path}: {e}")
        raise


def get_table_metadata(config: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """
    Extract metadata for a specific table from the configuration.
    
    Expects configuration structure:
    pipeline_config:
      tables:
        table_name:
          ... metadata ...
    """
    # Try to find 'pipeline_config' or use root
    pipeline_config = config.get("pipeline_config", config)
    tables = pipeline_config.get("tables", {})
    
    if table_name not in tables:
        # Check if the config itself is the metadata (single table config)
        if pipeline_config.get("table_name") == table_name:
            return pipeline_config
            
        raise ValueError(f"Configuration for table '{table_name}' not found.")
        
    metadata = tables[table_name]
    metadata["table_name"] = table_name
    return metadata


def main():
    """
    Main entry point.
    """
    parser = argparse.ArgumentParser(description="Lakeflow Curation Pipeline Executor")
    parser.add_argument("--job_config", required=True, help="Path to YAML configuration file")
    parser.add_argument("--table_name", required=True, help="Target table name to process")
    parser.add_argument("--domain", required=False, help="Domain name (optional)")
    
    args = parser.parse_args()
    
    logger.info(f"Starting job for table: {args.table_name}")
    logger.info(f"Config file: {args.job_config}")
    
    try:
        # Load configuration
        config = load_config(args.job_config)
        
        # Extract table metadata
        metadata = get_table_metadata(config, args.table_name)
        
        # Inject domain if provided and not in metadata
        if args.domain and "domain" not in metadata:
            metadata["domain"] = args.domain
            
        # Initialize Spark
        spark = SparkSession.builder.getOrCreate()
        
        # Initialize Environment
        env_config = EnvironmentConfig()
        
        # Initialize and Run Pipeline
        pipeline = LakeflowCurationPipeline(spark, env_config)
        pipeline.run([metadata])
        
        logger.info("Job completed successfully")
        
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
