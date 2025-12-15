"""
Environment Configuration

Resolves environment-specific values like catalog from Databricks Asset Bundle
or environment variables.
"""

import os
from typing import Optional

from utils.logging import get_logger

logger = get_logger(__name__)


def get_catalog(default: str = "ltc_insurance") -> str:
    """
    Get the catalog name for the current environment.
    
    Resolution order:
    1. CATALOG environment variable (set by DAB bundle)
    2. Default value
    
    Args:
        default: Default catalog name if not found in environment
        
    Returns:
        Catalog name string
    """
    catalog = os.environ.get("CATALOG", default)
    logger.debug(f"Resolved catalog: {catalog}")
    return catalog


def get_environment() -> str:
    """
    Get the current environment name (dev, staging, prod).
    
    Resolution order:
    1. ENVIRONMENT environment variable
    2. Default to "dev"
    
    Returns:
        Environment name string
    """
    return os.environ.get("ENVIRONMENT", "dev")


def get_bundle_variable(name: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get a variable from Databricks Asset Bundle configuration.
    
    Variables are passed as environment variables by DAB.
    
    Args:
        name: Variable name
        default: Default value if not found
        
    Returns:
        Variable value or default
    """
    # DAB passes bundle variables as environment variables
    # Convention: bundle variable "catalog" becomes env var "CATALOG"
    env_name = name.upper()
    return os.environ.get(env_name, default)


class EnvironmentConfig:
    """
    Environment configuration container.
    
    Resolves and caches all environment-specific settings.
    """
    
    def __init__(self, catalog: str = None, environment: str = None):
        self.catalog = catalog or get_catalog()
        self.environment = environment or get_environment()
    
    def get_fully_qualified_table(self, schema: str, table: str) -> str:
        """
        Build fully qualified table name with catalog.
        
        Args:
            schema: Schema/database name
            table: Table name
            
        Returns:
            Fully qualified table name (catalog.schema.table)
        """
        return f"{self.catalog}.{schema}.{table}"
    
    def __repr__(self) -> str:
        return f"EnvironmentConfig(catalog={self.catalog}, environment={self.environment})"
