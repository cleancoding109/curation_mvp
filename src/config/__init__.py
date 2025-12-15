"""
Config Package

Configuration loading and environment management.
"""

from config.environment import (
    get_catalog,
    get_environment,
    get_bundle_variable,
    EnvironmentConfig,
)
from config.metadata_loader import (
    load_table_metadata,
    load_job_config,
    load_sql_template,
    discover_domain_tables,
    get_query_path,
    MetadataRegistry,
)

__all__ = [
    "get_catalog",
    "get_environment",
    "get_bundle_variable",
    "EnvironmentConfig",
    "load_table_metadata",
    "load_job_config",
    "load_sql_template",
    "discover_domain_tables",
    "get_query_path",
    "MetadataRegistry",
]
