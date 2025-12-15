"""
Metadata Loader

Loads table metadata JSON files and job YAML configurations.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional

import yaml

from utils.logging import get_logger

logger = get_logger(__name__)


def load_table_metadata(metadata_path: str) -> Dict[str, Any]:
    """
    Load a single table metadata JSON file.
    
    Args:
        metadata_path: Path to the metadata JSON file
        
    Returns:
        Dictionary containing table metadata
        
    Raises:
        FileNotFoundError: If metadata file doesn't exist
        json.JSONDecodeError: If JSON is invalid
    """
    path = Path(metadata_path)
    if not path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
    
    with open(path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    
    logger.info(f"Loaded metadata for table: {metadata.get('table_name', 'unknown')}")
    return metadata


def load_job_config(job_config_path: str) -> Dict[str, Any]:
    """
    Load a job YAML configuration file.
    
    Args:
        job_config_path: Path to the job YAML file
        
    Returns:
        Dictionary containing job configuration
        
    Raises:
        FileNotFoundError: If config file doesn't exist
    """
    path = Path(job_config_path)
    if not path.exists():
        raise FileNotFoundError(f"Job config file not found: {job_config_path}")
    
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    logger.info(f"Loaded job config: {job_config_path}")
    return config


def discover_domain_tables(domain: str, base_path: str = "metadata/sdl") -> List[str]:
    """
    Discover all table metadata files in a domain folder.
    
    Args:
        domain: Domain name (e.g., "claims")
        base_path: Base path for metadata files
        
    Returns:
        List of table metadata file paths
    """
    domain_path = Path(base_path) / domain
    if not domain_path.exists():
        logger.warning(f"Domain path not found: {domain_path}")
        return []
    
    metadata_files = list(domain_path.glob("*.json"))
    logger.info(f"Discovered {len(metadata_files)} tables in domain '{domain}'")
    return [str(f) for f in metadata_files]


def get_query_path(table_name: str, base_path: str = "query") -> str:
    """
    Get the SQL query file path for a table (convention-based).
    
    Args:
        table_name: Name of the table
        base_path: Base path for query files
        
    Returns:
        Path to the SQL query file
    """
    return f"{base_path}/{table_name}.sql"


def load_sql_template(sql_path: str) -> str:
    """
    Load a SQL template file.
    
    Args:
        sql_path: Path to the SQL file
        
    Returns:
        SQL template string
        
    Raises:
        FileNotFoundError: If SQL file doesn't exist
    """
    path = Path(sql_path)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    
    logger.debug(f"Loaded SQL template: {sql_path}")
    return sql


class MetadataRegistry:
    """
    Registry for table metadata with caching.
    
    Provides a central point to load and access table configurations.
    """
    
    def __init__(self, base_metadata_path: str = "metadata/sdl", base_query_path: str = "query"):
        self.base_metadata_path = base_metadata_path
        self.base_query_path = base_query_path
        self._cache: Dict[str, Dict[str, Any]] = {}
    
    def get_table_metadata(self, domain: str, table_name: str) -> Dict[str, Any]:
        """
        Get metadata for a table, loading from file if not cached.
        
        Args:
            domain: Domain name
            table_name: Table name
            
        Returns:
            Table metadata dictionary
        """
        cache_key = f"{domain}/{table_name}"
        
        if cache_key not in self._cache:
            metadata_path = f"{self.base_metadata_path}/{domain}/{table_name}.json"
            self._cache[cache_key] = load_table_metadata(metadata_path)
        
        return self._cache[cache_key]
    
    def get_sql_template(self, table_name: str) -> str:
        """
        Get SQL template for a table.
        
        Args:
            table_name: Table name
            
        Returns:
            SQL template string
        """
        sql_path = get_query_path(table_name, self.base_query_path)
        return load_sql_template(sql_path)
    
    def clear_cache(self):
        """Clear the metadata cache."""
        self._cache.clear()
