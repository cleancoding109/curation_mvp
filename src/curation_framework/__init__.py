"""
Curation Framework - Metadata-Driven Spark Batch Framework for Databricks

This framework provides:
- High-watermark incremental batch processing
- SCD Type 1 (Upsert) and Type 2 (History tracking) patterns
- Metadata-driven table processing from JSON configuration
- SQL-based transformations

Usage:
    from curation_framework import BatchFrameworkOrchestrator, SilverProcessor
    from curation_framework import process_all_tables, process_single_table
"""

from curation_framework.silver_processor import (
    SilverProcessor,
    BatchFrameworkOrchestrator
)

from curation_framework.main import (
    main,
    process_all_tables,
    process_single_table
)

from curation_framework.utils import (
    validate_table_config,
    table_exists,
    get_table_row_count,
    create_hash_column,
    deduplicate_by_key,
    generate_scd2_columns,
    optimize_delta_table,
    vacuum_delta_table
)

__version__ = "0.0.1"

__all__ = [
    # Core classes
    "SilverProcessor",
    "BatchFrameworkOrchestrator",
    # Main functions
    "main",
    "process_all_tables",
    "process_single_table",
    # Utility functions
    "validate_table_config",
    "table_exists",
    "get_table_row_count",
    "create_hash_column",
    "deduplicate_by_key",
    "generate_scd2_columns",
    "optimize_delta_table",
    "vacuum_delta_table",
    # Version
    "__version__"
]