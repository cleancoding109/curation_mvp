"""
Load Strategy Factory

Selects and instantiates the appropriate load strategy based on metadata.
"""

from typing import Dict, Any, Type

from pyspark.sql import SparkSession

from config.environment import EnvironmentConfig
from load_strategy.base import LoadStrategy
from utils.logging import get_logger

logger = get_logger(__name__)


# Registry of available load strategies
_STRATEGY_REGISTRY: Dict[str, Type[LoadStrategy]] = {}

from load_strategy.insert_only import InsertOnlyStrategy
from load_strategy.delete_insert import DeleteInsertStrategy
from load_strategy.truncate_insert import TruncateInsertStrategy
from load_strategy.scd2 import SCD2Strategy

# Register strategies
# Note: In a larger framework, this might be done via dynamic import or decorators
_STRATEGY_REGISTRY["insert_only"] = InsertOnlyStrategy
_STRATEGY_REGISTRY["delete_insert"] = DeleteInsertStrategy
_STRATEGY_REGISTRY["truncate_insert"] = TruncateInsertStrategy
_STRATEGY_REGISTRY["scd2"] = SCD2Strategy



def get_strategy(
    strategy_name: str,
    spark: SparkSession,
    env_config: EnvironmentConfig,
    metadata: Dict[str, Any]
) -> LoadStrategy:
    """
    Get a load strategy instance by name.
    
    Args:
        strategy_name: Name of the strategy (e.g., "scd2", "insert_only")
        spark: SparkSession instance
        env_config: Environment configuration
        metadata: Table metadata dictionary
        
    Returns:
        LoadStrategy instance
        
    Raises:
        ValueError: If strategy name is not registered
    """
    name = strategy_name.lower()
    
    if name not in _STRATEGY_REGISTRY:
        available = ", ".join(_STRATEGY_REGISTRY.keys())
        raise ValueError(
            f"Unknown load strategy: '{name}'. Available: {available}"
        )
    
    strategy_class = _STRATEGY_REGISTRY[name]
    logger.info(f"Creating load strategy: {name}")
    
    return strategy_class(spark, env_config, metadata)


def get_strategy_from_metadata(
    spark: SparkSession,
    env_config: EnvironmentConfig,
    metadata: Dict[str, Any]
) -> LoadStrategy:
    """
    Get load strategy based on metadata configuration.
    
    Uses the 'load_strategy' field in metadata.
    Defaults to 'scd2' if not specified.
    
    Args:
        spark: SparkSession instance
        env_config: Environment configuration
        metadata: Table metadata dictionary
        
    Returns:
        LoadStrategy instance
    """
    # Handle both string and dict formats for load_strategy
    strategy_config = metadata.get("load_strategy", "scd2")
    if isinstance(strategy_config, dict):
        strategy_name = strategy_config.get("type", "scd2")
    else:
        strategy_name = strategy_config
        
    return get_strategy(strategy_name, spark, env_config, metadata)


def list_strategies() -> list:
    """
    Get list of registered strategy names.
    
    Returns:
        List of strategy names
    """
    return list(_STRATEGY_REGISTRY.keys())


class LoadStrategyFactory:
    """
    Factory for creating load strategy instances.
    
    Provides convenient interface with cached environment config.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        env_config: EnvironmentConfig = None
    ):
        self.spark = spark
        self.env_config = env_config or EnvironmentConfig()
    
    def create(self, metadata: Dict[str, Any]) -> LoadStrategy:
        """
        Create load strategy from metadata.
        
        Args:
            metadata: Table metadata dictionary
            
        Returns:
            LoadStrategy instance
        """
        return get_strategy_from_metadata(
            self.spark,
            self.env_config,
            metadata
        )
    
    def create_by_name(
        self,
        strategy_name: str,
        metadata: Dict[str, Any]
    ) -> LoadStrategy:
        """
        Create load strategy by explicit name.
        
        Args:
            strategy_name: Strategy name
            metadata: Table metadata dictionary
            
        Returns:
            LoadStrategy instance
        """
        return get_strategy(
            strategy_name,
            self.spark,
            self.env_config,
            metadata
        )
    
    @staticmethod
    def available_strategies() -> list:
        """Get list of available strategies."""
        return list_strategies()
