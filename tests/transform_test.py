import pytest
from src.transform.hash_generator import HashGenerator, canonicalize_column
from src.transform.template_resolver import TemplateResolver

class TestHashGenerator:
    def test_canonicalize_column(self):
        assert canonicalize_column("col1") == "UPPER(TRIM(COALESCE(CAST(col1 AS STRING), '__NULL__')))"
        assert canonicalize_column("col1", "src") == "UPPER(TRIM(COALESCE(CAST(src.col1 AS STRING), '__NULL__')))"

    def test_pk_hash_generation(self):
        metadata = {
            "hash_keys": {
                "_pk_hash": {"columns": ["id", "code"]},
                "_diff_hash": {"track_columns": ["name", "value"]}
            }
        }
        generator = HashGenerator(metadata)
        
        expected_pk = "SHA2(CONCAT_WS('|', UPPER(TRIM(COALESCE(CAST(id AS STRING), '__NULL__'))), UPPER(TRIM(COALESCE(CAST(code AS STRING), '__NULL__')))), 256)"
        assert generator.pk_hash_expression == expected_pk

    def test_diff_hash_generation(self):
        metadata = {
            "hash_keys": {
                "_pk_hash": {"columns": ["id"]},
                "_diff_hash": {"track_columns": ["name"]}
            }
        }
        generator = HashGenerator(metadata)
        
        expected_diff = "SHA2(CONCAT_WS('|', UPPER(TRIM(COALESCE(CAST(name AS STRING), '__NULL__')))), 256)"
        assert generator.diff_hash_expression == expected_diff

class TestTemplateResolver:
    def test_resolve_ref_list(self):
        from src.transform.template_resolver import resolve_ref_placeholder
        from src.config.environment import EnvironmentConfig
        
        metadata = {
            "reference_joins": [
                {"table": "ref_table", "schema": "ref_schema"}
            ]
        }
        env_config = EnvironmentConfig()
        # Assuming default catalog is 'hive_metastore' or similar, or we can check suffix
        resolved = resolve_ref_placeholder("ref_table", metadata, env_config)
        assert "ref_schema.ref_table" in resolved

    def test_regex_patterns(self):
        from src.transform.template_resolver import PLACEHOLDER_PATTERN
        
        assert PLACEHOLDER_PATTERN.match("{{alias:col1}}").group(1) == "alias:col1"

    def test_resolve_generic_alias(self):
        from src.transform.template_resolver import resolve_template
        from src.config.environment import EnvironmentConfig
        
        metadata = {
            "reference_joins": [
                {"table": "adjuster_lookup", "alias": "adj"}
            ]
        }
        template = "SELECT {{adj:name}} FROM table"
        resolved = resolve_template(template, metadata, EnvironmentConfig())
        assert resolved == "SELECT adj.name FROM table"
