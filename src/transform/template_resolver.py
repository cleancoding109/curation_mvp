"""
Template Resolver

Loads SQL template files and resolves {{...}} placeholders using metadata.
Supports source references, reference table joins, column aliases, and hash expressions.

NOTE: {{source}} placeholders should be replaced BEFORE calling this resolver
when using temp views in the pipeline.
"""

import re
from typing import Dict, Any
from config.environment import EnvironmentConfig
from transform.hash_generator import generate_hash_expressions_from_metadata
from utils.logging import get_logger

logger = get_logger(__name__)

# Regex pattern for placeholder extraction
PLACEHOLDER_PATTERN = re.compile(r"\{\{([^}]+)\}\}")


def resolve_source_placeholder(
    metadata: Dict[str, Any],
    env_config: EnvironmentConfig
) -> str:
    """
    Resolve {{source}} to fully qualified source table name.
    
    NOTE: If using temp views in pipeline, replace {{source}} manually
    BEFORE calling this resolver.
    
    Args:
        metadata: Table metadata dictionary
        env_config: Environment configuration with catalog
    
    Returns:
        Fully qualified table name (catalog.schema.table)
    """
    source_schema = metadata.get("source_schema")
    source_table = metadata.get("source_table", metadata.get("table_name"))
    
    if not source_schema:
        raise ValueError("source_schema must be specified in metadata")
    
    return env_config.get_fully_qualified_table(source_schema, source_table)


def resolve_target_placeholder(
    metadata: Dict[str, Any],
    env_config: EnvironmentConfig
) -> str:
    """
    Resolve {{target}} to fully qualified target table name.
    
    Args:
        metadata: Table metadata dictionary
        env_config: Environment configuration with catalog
    
    Returns:
        Fully qualified table name (catalog.schema.table)
    """
    target_schema = metadata.get("target_schema")
    target_table = metadata.get("target_table", metadata.get("table_name"))
    
    if not target_schema:
        raise ValueError("target_schema must be specified in metadata")
    
    return env_config.get_fully_qualified_table(target_schema, target_table)


def resolve_ref_placeholder(
    ref_name: str,
    metadata: Dict[str, Any],
    env_config: EnvironmentConfig
) -> str:
    """
    Resolve {{ref:table_name}} to fully qualified reference table.
    
    Args:
        ref_name: Reference table name
        metadata: Table metadata dictionary
        env_config: Environment configuration with catalog
    
    Returns:
        Fully qualified reference table name
    """
    reference_joins = metadata.get("reference_joins", [])
    
    # Default to target_schema for reference tables
    default_ref_schema = metadata.get("target_schema")
    
    # Handle reference_joins as list of dicts
    if isinstance(reference_joins, list):
        for join in reference_joins:
            if join.get("table") == ref_name:
                # Use schema from join config, or fallback to default
                ref_schema = join.get("schema", default_ref_schema)
                return env_config.get_fully_qualified_table(ref_schema, ref_name)
    
    elif isinstance(reference_joins, dict):
        # Legacy support for dict format
        if ref_name in reference_joins:
            ref_config = reference_joins[ref_name]
            ref_schema = ref_config.get("schema", default_ref_schema)
            return env_config.get_fully_qualified_table(ref_schema, ref_name)
    
    # Fallback: use target_schema
    if not default_ref_schema:
        raise ValueError(
            f"Cannot resolve reference table '{ref_name}': "
            f"no schema specified in join config or metadata"
        )
    
    logger.warning(
        f"Reference table '{ref_name}' not found in reference_joins, "
        f"using default schema: {default_ref_schema}"
    )
    return env_config.get_fully_qualified_table(default_ref_schema, ref_name)


def resolve_alias_placeholder(
    alias_name: str,
    metadata: Dict[str, Any]
) -> str:
    """
    Resolve {{alias:column}} to target column name from column_mapping.
    
    Args:
        alias_name: Source column name
        metadata: Table metadata dictionary
    
    Returns:
        Target column name
    """
    column_mapping = metadata.get("column_mapping", {})
    return column_mapping.get(alias_name, alias_name)


def build_placeholder_context(
    metadata: Dict[str, Any],
    env_config: EnvironmentConfig
) -> Dict[str, str]:
    """
    Build a dictionary of all placeholder resolutions.
    
    Args:
        metadata: Table metadata dictionary
        env_config: Environment configuration
    
    Returns:
        Dictionary mapping placeholder keys to resolved values
    """
    context = {}
    
    # Basic placeholders
    context["source"] = resolve_source_placeholder(metadata, env_config)
    context["target"] = resolve_target_placeholder(metadata, env_config)
    context["table_name"] = metadata.get("table_name", "")
    
    # Metadata-driven placeholders
    context["source_timezone"] = metadata.get("source_timezone", "UTC")
    
    # Hash expressions with 'src' alias to avoid ambiguous column references
    # When queries have JOINs, columns must be qualified with table alias
    hash_exprs = generate_hash_expressions_from_metadata(metadata, alias="src")
    context["_pk_hash"] = hash_exprs.get("_pk_hash", "NULL")
    context["_diff_hash"] = hash_exprs.get("_diff_hash", "NULL")
    
    # SCD2 columns if passthrough mode
    scd2_mode = metadata.get("scd2_mode", "framework_managed")
    if scd2_mode == "passthrough":
        context["__START_AT"] = "__START_AT"
        context["__END_AT"] = "__END_AT"
    
    return context


def resolve_template(
    sql_template: str,
    metadata: Dict[str, Any],
    env_config: EnvironmentConfig
) -> str:
    """
    Resolve all placeholders in a SQL template.
    
    Supports:
    - {{source}} - Source table (replace manually for temp views!)
    - {{target}} - Target table
    - {{ref:table_name}} - Reference tables
    - {{alias:column}} - Column aliases
    - {{adj:column_name}} - Reference table column (adj.column_name)
    - {{_pk_hash}} - Primary key hash expression (with src. qualification)
    - {{_diff_hash}} - Diff hash expression (with src. qualification)
    - {{source_timezone}} - Source timezone
    
    Args:
        sql_template: SQL template string with placeholders
        metadata: Table metadata dictionary
        env_config: Environment configuration
    
    Returns:
        Resolved SQL string
    """
    context = build_placeholder_context(metadata, env_config)
    
    # Build alias lookup for reference table columns
    alias_lookup = set()
    reference_joins = metadata.get("reference_joins", [])
    if isinstance(reference_joins, list):
        for join in reference_joins:
            alias = join.get("alias")
            if alias:
                alias_lookup.add(alias)
    
    def replace_placeholder(match):
        placeholder = match.group(1).strip()
        
        # Check for ref: prefix ({{ref:table_name}})
        if placeholder.startswith("ref:"):
            ref_name = placeholder.split(":", 1)[1]
            return resolve_ref_placeholder(ref_name, metadata, env_config)
        
        # Check for colon pattern
        if ":" in placeholder:
            prefix, suffix = placeholder.split(":", 1)
            
            # Check if prefix is a known alias from reference_joins
            # Example: {{adj:adjuster_name}} -> adj.adjuster_name
            if prefix in alias_lookup:
                return f"{prefix}.{suffix}"
            
            # Check if it's the literal "alias" keyword for column mapping
            if prefix == "alias":
                return resolve_alias_placeholder(suffix, metadata)
        
        # Direct lookup in context
        if placeholder in context:
            return context[placeholder]
        
        # Unknown placeholder - log warning and leave as-is
        logger.warning(f"Unknown placeholder: {{{{{placeholder}}}}}")
        return match.group(0)
    
    resolved = PLACEHOLDER_PATTERN.sub(replace_placeholder, sql_template)
    logger.debug(f"Resolved SQL template ({len(sql_template)} -> {len(resolved)} chars)")
    return resolved


class TemplateResolver:
    """
    Template resolver with environment and metadata context.
    
    Provides convenient interface for resolving SQL templates.
    """
    
    def __init__(
        self,
        env_config: EnvironmentConfig = None,
        metadata: Dict[str, Any] = None
    ):
        self.env_config = env_config or EnvironmentConfig()
        self.metadata = metadata or {}
    
    def resolve(self, sql_template: str, metadata: Dict[str, Any] = None) -> str:
        """
        Resolve placeholders in SQL template.
        
        Args:
            sql_template: SQL template string
            metadata: Optional metadata override
        
        Returns:
            Resolved SQL string
        """
        meta = metadata or self.metadata
        return resolve_template(sql_template, meta, self.env_config)
    
    def load_and_resolve(
        self,
        sql_path: str,
        metadata: Dict[str, Any] = None
    ) -> str:
        """
        Load SQL file and resolve placeholders.
        
        Args:
            sql_path: Path to SQL template file
            metadata: Optional metadata override
        
        Returns:
            Resolved SQL string
        """
        from config.metadata_loader import load_sql_template
        sql_template = load_sql_template(sql_path)
        return self.resolve(sql_template, metadata)
    
    def get_placeholder_context(self, metadata: Dict[str, Any] = None) -> Dict[str, str]:
        """
        Get the placeholder context dictionary for inspection.
        
        Args:
            metadata: Optional metadata override
        
        Returns:
            Dictionary of placeholder resolutions
        """
        meta = metadata or self.metadata
        return build_placeholder_context(meta, self.env_config)
