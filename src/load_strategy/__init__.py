"""
Load Strategy Package

Load strategy implementations for different target table patterns.
"""

from load_strategy.base import LoadStrategy
from load_strategy.factory import (
    register_strategy,
    get_strategy,
    get_strategy_from_metadata,
    list_strategies,
    LoadStrategyFactory,
)

# Import strategies to register them
from load_strategy.insert_only import InsertOnlyStrategy
from load_strategy.delete_insert import DeleteInsertStrategy
from load_strategy.truncate_insert import TruncateInsertStrategy
# SCD1 and SCD2 will be imported when implemented

__all__ = [
    "LoadStrategy",
    "register_strategy",
    "get_strategy",
    "get_strategy_from_metadata",
    "list_strategies",
    "LoadStrategyFactory",
    "InsertOnlyStrategy",
    "DeleteInsertStrategy",
    "TruncateInsertStrategy",
]
