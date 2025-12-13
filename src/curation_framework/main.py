"""
Main Entry Point - Metadata-Driven Spark Batch Framework

This module serves as the entry point for the Silver layer batch processing job.
It orchestrates the processing of all configured tables using high-watermark
incremental patterns and SCD Type 1/2 merge strategies.
"""

import sys
import json
from typing import Optional, List
from datetime import datetime

from databricks.sdk.runtime import spark
from pyspark.sql import SparkSession

from curation_framework.silver_processor import BatchFrameworkOrchestrator, SilverProcessor


# Default configuration path (relative to bundle root)
DEFAULT_CONFIG_PATH = "conf/tables_config.json"


def get_dbutils():
    """Get dbutils for Databricks-specific operations."""
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        try:
            import IPython
            return IPython.get_ipython().user_ns.get("dbutils")
        except Exception:
            return None


def get_config_path() -> str:
    """
    Get the configuration file path.
    Checks for widget parameter first, then falls back to default.
    """
    dbutils = get_dbutils()
    
    if dbutils:
        try:
            config_path = dbutils.widgets.get("config_path")
            if config_path:
                return config_path
        except Exception:
            pass
    
    return DEFAULT_CONFIG_PATH


def get_table_filter() -> Optional[List[str]]:
    """
    Get optional table filter from job parameters.
    Allows running specific tables instead of all configured tables.
    """
    dbutils = get_dbutils()
    
    if dbutils:
        try:
            tables_param = dbutils.widgets.get("tables")
            if tables_param:
                return [t.strip() for t in tables_param.split(",")]
        except Exception:
            pass
    
    return None


def process_single_table(table_name: str, config_path: str = DEFAULT_CONFIG_PATH) -> dict:
    """
    Process a single table by name.
    
    Args:
        table_name: Name of the table to process
        config_path: Path to the configuration file
        
    Returns:
        Processing result dictionary
    """
    orchestrator = BatchFrameworkOrchestrator(spark, config_path)
    
    # Find the table config
    tables = orchestrator.config.get("tables", [])
    table_config = next((t for t in tables if t["table_name"] == table_name), None)
    
    if not table_config:
        raise ValueError(f"Table '{table_name}' not found in configuration")
    
    return orchestrator.process_table(table_config)


def process_all_tables(
    config_path: str = DEFAULT_CONFIG_PATH,
    table_filter: Optional[List[str]] = None
) -> dict:
    """
    Process all configured tables.
    
    Args:
        config_path: Path to the configuration file
        table_filter: Optional list of table names to process
        
    Returns:
        Summary dictionary with processing results
    """
    print(f"=" * 60)
    print(f"Metadata-Driven Spark Batch Framework")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"Config: {config_path}")
    print(f"=" * 60)
    
    orchestrator = BatchFrameworkOrchestrator(spark, config_path)
    results = orchestrator.process_all_tables(table_filter)
    summary = orchestrator.get_processing_summary(results)
    
    # Print summary
    print(f"\n{'=' * 60}")
    print("PROCESSING SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total Tables Processed: {summary['total_tables_processed']}")
    print(f"Successful: {summary['successful']}")
    print(f"Failed: {summary['failed']}")
    print(f"Total Records Processed: {summary['total_records_processed']}")
    
    if summary['failed_tables']:
        print(f"\nFailed Tables: {', '.join(summary['failed_tables'])}")
    
    print(f"\nDetailed Results:")
    print("-" * 60)
    
    for result in results:
        status_icon = "✓" if result.get("status") == "success" else "✗"
        print(f"{status_icon} {result.get('table_name')}: "
              f"{result.get('records_processed', 0)} records "
              f"({result.get('duration_seconds', 0):.2f}s)")
        if result.get("error"):
            print(f"  Error: {result.get('error')}")
    
    print(f"{'=' * 60}")
    print(f"Completed: {datetime.now().isoformat()}")
    print(f"{'=' * 60}")
    
    # Return summary for programmatic access
    return {
        "summary": summary,
        "results": results
    }


def main():
    """
    Main entry point for the batch processing job.
    This is called when running as a Python wheel task.
    """
    config_path = get_config_path()
    table_filter = get_table_filter()
    
    result = process_all_tables(config_path, table_filter)
    
    # Exit with error code if any tables failed
    if result["summary"]["failed"] > 0:
        print(f"\n⚠ WARNING: {result['summary']['failed']} table(s) failed processing")
        sys.exit(1)
    
    print("\n✓ All tables processed successfully")
    sys.exit(0)


# Expose key functions for notebook usage
__all__ = [
    "main",
    "process_all_tables",
    "process_single_table",
    "SilverProcessor",
    "BatchFrameworkOrchestrator"
]


if __name__ == "__main__":
    main()
