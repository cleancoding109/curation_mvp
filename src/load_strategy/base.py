"""
Base Load Strategy

Abstract base class for all load strategies.
Defines the interface that all strategies must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any

from pyspark.sql import DataFrame, SparkSession

from config.environment import EnvironmentConfig
from utils.logging import get_logger

logger = get_logger(__name__)


class LoadStrategy(ABC):
    """
    Abstract base class for load strategies.
    
    All load strategies (SCD1, SCD2, INSERT_ONLY, etc.) must inherit
    from this class and implement the execute method.
    """
    
    # Strategy name for logging and identification
    name: str = "base"
    
    def __init__(
        self,
        spark: SparkSession,
        env_config: EnvironmentConfig,
        metadata: Dict[str, Any]
    ):
        """
        Initialize the load strategy.
        
        Args:
            spark: SparkSession instance
            env_config: Environment configuration
            metadata: Table metadata dictionary
        """
        self.spark = spark
        self.env_config = env_config
        self.metadata = metadata
        self._logger = get_logger(f"load_strategy.{self.name}")
    
    @property
    def target_table(self) -> str:
        """Get fully qualified target table name."""
        target_schema = self.metadata.get("target_schema", "standardized_data_layer")
        target_table_name = self.metadata.get("target_table", self.metadata.get("table_name"))
        return self.env_config.get_fully_qualified_table(target_schema, target_table_name)
    
    @property
    def primary_key_columns(self) -> list:
        """Get primary key columns from metadata."""
        hash_keys = self.metadata.get("hash_keys", {})
        return hash_keys.get("primary_key_columns", [])
    
    @abstractmethod
    def execute(self, df: DataFrame) -> Dict[str, Any]:
        """
        Execute the load strategy.
        
        Args:
            df: DataFrame to load into target
            
        Returns:
            Dictionary with execution metrics:
            - rows_processed: Number of rows processed
            - rows_inserted: Number of rows inserted
            - rows_updated: Number of rows updated (if applicable)
            - rows_deleted: Number of rows deleted (if applicable)
        """
        pass
    
    def validate_input(self, df: DataFrame) -> bool:
        """
        Validate input DataFrame before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if valid, raises exception otherwise
        """
        if df is None:
            raise ValueError("Input DataFrame cannot be None")
        
        # Check for required primary key columns
        pk_columns = self.primary_key_columns
        if pk_columns:
            missing = [c for c in pk_columns if c not in df.columns]
            if missing:
                raise ValueError(f"Missing primary key columns: {missing}")
        
        return True
    
    def pre_execute(self, df: DataFrame):
        """
        Hook called before execute. Can be overridden by subclasses.
        
        Args:
            df: DataFrame being loaded
        """
        self._logger.info(f"Starting {self.name} load to {self.target_table}")
        self._logger.info(f"Input rows: {df.count():,}")
    
    def post_execute(self, metrics: Dict[str, Any]):
        """
        Hook called after execute. Can be overridden by subclasses.
        
        Args:
            metrics: Execution metrics from execute
        """
        self._logger.info(f"Completed {self.name} load")
        for key, value in metrics.items():
            self._logger.info(f"  {key}: {value:,}" if isinstance(value, int) else f"  {key}: {value}")
    
    def run(self, df: DataFrame) -> Dict[str, Any]:
        """
        Main entry point: validate, pre-execute, execute, post-execute.
        
        Args:
            df: DataFrame to load
            
        Returns:
            Execution metrics
        """
        self.validate_input(df)
        self.pre_execute(df)
        metrics = self.execute(df)
        self.post_execute(metrics)
        return metrics
