"""
Hash Generator

Generates SHA2-256 hash expressions for primary key and diff hashing.
Uses canonicalization for consistent hash values across records.
"""

from typing import List, Dict, Any

from utils.logging import get_logger

logger = get_logger(__name__)


def canonicalize_column(column: str, alias: str = None) -> str:
    """
    Generate canonicalized column expression.
    
    Handles NULL values by replacing with empty string,
    trims whitespace, and converts to uppercase for consistency.
    
    Args:
        column: Column name
        alias: Optional alias prefix (e.g., "src")
        
    Returns:
        Canonicalized SQL expression
    """
    col_ref = f"{alias}.{column}" if alias else column
    return f"UPPER(TRIM(COALESCE(CAST({col_ref} AS STRING), '')))"


def generate_pk_hash_expression(
    primary_key_columns: List[str],
    alias: str = None
) -> str:
    """
    Generate SHA2-256 hash expression for primary key columns.
    
    Args:
        primary_key_columns: List of column names forming the primary key
        alias: Optional table alias prefix
        
    Returns:
        SQL expression for _pk_hash column
    """
    if not primary_key_columns:
        raise ValueError("primary_key_columns cannot be empty")
    
    canonicalized = [canonicalize_column(col, alias) for col in primary_key_columns]
    concat_expr = " || '|' || ".join(canonicalized)
    
    return f"SHA2({concat_expr}, 256)"


def generate_diff_hash_expression(
    diff_columns: List[str],
    alias: str = None
) -> str:
    """
    Generate SHA2-256 hash expression for diff columns.
    
    Used to detect changes in non-key columns.
    
    Args:
        diff_columns: List of column names to include in diff hash
        alias: Optional table alias prefix
        
    Returns:
        SQL expression for _diff_hash column
    """
    if not diff_columns:
        raise ValueError("diff_columns cannot be empty")
    
    canonicalized = [canonicalize_column(col, alias) for col in diff_columns]
    concat_expr = " || '|' || ".join(canonicalized)
    
    return f"SHA2({concat_expr}, 256)"


def generate_hash_expressions_from_metadata(
    metadata: Dict[str, Any],
    alias: str = None
) -> Dict[str, str]:
    """
    Generate both pk_hash and diff_hash expressions from table metadata.
    
    Args:
        metadata: Table metadata dictionary containing hash_keys configuration
        alias: Optional table alias prefix
        
    Returns:
        Dictionary with '_pk_hash' and '_diff_hash' SQL expressions
    """
    hash_keys = metadata.get("hash_keys", {})
    
    pk_columns = hash_keys.get("primary_key_columns", [])
    diff_columns = hash_keys.get("diff_columns", [])
    
    result = {}
    
    if pk_columns:
        result["_pk_hash"] = generate_pk_hash_expression(pk_columns, alias)
        logger.debug(f"Generated _pk_hash from columns: {pk_columns}")
    
    if diff_columns:
        result["_diff_hash"] = generate_diff_hash_expression(diff_columns, alias)
        logger.debug(f"Generated _diff_hash from columns: {diff_columns}")
    
    return result


class HashGenerator:
    """
    Hash generator with metadata context.
    
    Provides hash expressions based on table metadata configuration.
    """
    
    def __init__(self, metadata: Dict[str, Any]):
        self.metadata = metadata
        self.hash_keys = metadata.get("hash_keys", {})
        self._pk_columns = self.hash_keys.get("primary_key_columns", [])
        self._diff_columns = self.hash_keys.get("diff_columns", [])
    
    @property
    def pk_hash_expression(self) -> str:
        """Get the primary key hash expression."""
        if not self._pk_columns:
            raise ValueError("No primary_key_columns defined in metadata")
        return generate_pk_hash_expression(self._pk_columns)
    
    @property
    def diff_hash_expression(self) -> str:
        """Get the diff hash expression."""
        if not self._diff_columns:
            raise ValueError("No diff_columns defined in metadata")
        return generate_diff_hash_expression(self._diff_columns)
    
    def get_hash_select_expressions(self, alias: str = None) -> str:
        """
        Get SELECT clause expressions for both hash columns.
        
        Args:
            alias: Optional table alias prefix
            
        Returns:
            SQL SELECT clause fragment with hash columns
        """
        expressions = []
        
        if self._pk_columns:
            pk_expr = generate_pk_hash_expression(self._pk_columns, alias)
            expressions.append(f"{pk_expr} AS _pk_hash")
        
        if self._diff_columns:
            diff_expr = generate_diff_hash_expression(self._diff_columns, alias)
            expressions.append(f"{diff_expr} AS _diff_hash")
        
        return ",\n    ".join(expressions)
