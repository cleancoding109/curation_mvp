"""
Reference Reader

Loads reference tables for joins (lookup tables, dimension tables).
"""

from typing import Dict, Any, List, Optional

from pyspark.sql import DataFrame, SparkSession

from config.environment import EnvironmentConfig
from utils.logging import get_logger

logger = get_logger(__name__)


def read_reference_table(
    spark: SparkSession,
    ref_name: str,
    ref_config: Dict[str, Any],
    env_config: EnvironmentConfig
) -> DataFrame:
    """
    Read a single reference table.
    
    Args:
        spark: SparkSession instance
        ref_name: Reference table name
        ref_config: Reference table configuration from metadata
        env_config: Environment configuration
        
    Returns:
        Reference table DataFrame
    """
    ref_schema = ref_config.get("schema", "standardized_data_layer")
    fq_table = env_config.get_fully_qualified_table(ref_schema, ref_name)
    
    logger.info(f"Reading reference table: {fq_table}")
    df = spark.table(fq_table)
    
    # Optionally select only needed columns
    columns = ref_config.get("columns")
    if columns:
        df = df.select(*columns)
        logger.debug(f"Selected columns: {columns}")
    
    return df


def load_reference_tables(
    spark: SparkSession,
    metadata: Dict[str, Any],
    env_config: EnvironmentConfig
) -> Dict[str, DataFrame]:
    """
    Load all reference tables defined in metadata.
    
    Args:
        spark: SparkSession instance
        metadata: Table metadata dictionary
        env_config: Environment configuration
        
    Returns:
        Dictionary mapping reference table names to DataFrames
    """
    reference_joins = metadata.get("reference_joins", {})
    
    if not reference_joins:
        logger.debug("No reference tables defined in metadata")
        return {}
    
    result = {}
    for ref_name, ref_config in reference_joins.items():
        result[ref_name] = read_reference_table(
            spark,
            ref_name,
            ref_config,
            env_config
        )
    
    logger.info(f"Loaded {len(result)} reference tables")
    return result


def register_reference_views(
    spark: SparkSession,
    ref_dataframes: Dict[str, DataFrame]
) -> List[str]:
    """
    Register reference DataFrames as temp views.
    
    Args:
        spark: SparkSession instance
        ref_dataframes: Dictionary of reference DataFrames
        
    Returns:
        List of created view names
    """
    view_names = []
    for ref_name, df in ref_dataframes.items():
        view_name = f"ref_{ref_name}"
        df.createOrReplaceTempView(view_name)
        view_names.append(view_name)
        logger.debug(f"Registered reference view: {view_name}")
    
    return view_names


class ReferenceReader:
    """
    Reference table reader with environment context.
    
    Manages loading and registering reference tables for joins.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        env_config: EnvironmentConfig = None
    ):
        self.spark = spark
        self.env_config = env_config or EnvironmentConfig()
        self._loaded: Dict[str, DataFrame] = {}
        self._view_names: List[str] = []
    
    def load(self, metadata: Dict[str, Any]) -> Dict[str, DataFrame]:
        """
        Load all reference tables from metadata.
        
        Args:
            metadata: Table metadata dictionary
            
        Returns:
            Dictionary of reference DataFrames
        """
        self._loaded = load_reference_tables(
            self.spark,
            metadata,
            self.env_config
        )
        return self._loaded
    
    def register_views(self) -> List[str]:
        """
        Register loaded reference tables as temp views.
        
        Returns:
            List of view names
        """
        self._view_names = register_reference_views(self.spark, self._loaded)
        return self._view_names
    
    def get(self, ref_name: str) -> Optional[DataFrame]:
        """
        Get a specific reference DataFrame.
        
        Args:
            ref_name: Reference table name
            
        Returns:
            Reference DataFrame or None
        """
        return self._loaded.get(ref_name)
    
    def cleanup_views(self):
        """Remove all registered temp views."""
        for view_name in self._view_names:
            try:
                self.spark.catalog.dropTempView(view_name)
                logger.debug(f"Dropped reference view: {view_name}")
            except:
                pass
        self._view_names.clear()
