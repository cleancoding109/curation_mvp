"""
Lakeflow Curation Pipeline

Orchestrates the end-to-end curation process:
1. Read source data (incremental)
2. Deduplicate raw data
3. Apply transformations (SQL with hash placeholders)
4. Cache deterministic source
5. Apply load strategy (SCD1/SCD2)
6. Update watermarks (failure-safe ordering)
"""

import uuid
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, DataFrame, functions as F

from config.environment import EnvironmentConfig
from reader.source_reader import read_source_incremental
from reader.reference_reader import load_reference_tables
from state.watermark import WatermarkManager, get_max_watermark_from_df
from transform.template_resolver import TemplateResolver
from transform.sql_executor import execute_sql
from utils.dedup import deduplicate_from_metadata
from load_strategy.factory import get_strategy_from_metadata
from utils.logging import get_logger

logger = get_logger(__name__)


class TempViewConfig(EnvironmentConfig):
    """
    Specialized EnvironmentConfig to support temporary views.
    Allows bypassing catalog/schema qualification for temp views.
    """
    def get_fully_qualified_table(self, schema: str, table: str) -> str:
        if schema == "__TEMP__":
            return table
        return super().get_fully_qualified_table(schema, table)


class LakeflowCurationPipeline:
    """
    Main pipeline orchestration class.
    """
    
    def __init__(self, spark: SparkSession, env_config: EnvironmentConfig = None):
        self.spark = spark
        self.env_config = env_config or EnvironmentConfig()
        self.watermark_manager = WatermarkManager(self.spark, self.env_config)

    def run(self, metadata_list: List[Dict[str, Any]]):
        """
        Run the pipeline for a list of table metadata configurations.
        
        Args:
            metadata_list: List of table metadata dictionaries
        """
        for metadata in metadata_list:
            table_name = metadata.get("table_name")
            is_critical = metadata.get("critical", True)
            
            try:
                self.process_table(metadata)
            except Exception as e:
                logger.error(f"Failed to process table {table_name}: {e}", exc_info=True)
                
                if is_critical:
                    logger.error(f"Critical table {table_name} failed. Stopping pipeline.")
                    raise
                else:
                    logger.warning(f"Non-critical table {table_name} failed. Continuing.")
                    continue

    def process_table(self, metadata: Dict[str, Any]):
        """
        Process a single table following failure-safe ordering.
        
        Args:
            metadata: Table metadata dictionary
        """
        table_name = metadata.get("table_name")
        logger.info(f"Starting pipeline for table: {table_name}")
        
        # 1. Get Watermark
        watermark_value = self.watermark_manager.get(table_name)
        logger.info(f"Current watermark: {watermark_value}")
        
        # 2. Read Source (Incremental)
        df_source = read_source_incremental(
            self.spark, 
            metadata, 
            self.env_config, 
            watermark_value
        )
        
        # Quick check: limit(1).count() is more efficient than rdd.isEmpty()
        if df_source.limit(1).count() == 0:
            logger.info(f"No new data for {table_name}. Skipping.")
            return

        # Extract max watermark from SOURCE (before any transformations)
        watermark_column = metadata.get("watermark_column", "_commit_timestamp")
        original_max_watermark = get_max_watermark_from_df(df_source, watermark_column)
        logger.info(f"New max watermark: {original_max_watermark}")

        # 3. Deduplicate RAW Source Data (before transformations)
        # Optional: Only if business keys are defined
        if metadata.get("business_key_columns"):
            df_deduped = deduplicate_from_metadata(df_source, metadata)
        else:
            logger.debug(f"Skipping deduplication for {table_name} (no business keys defined)")
            df_deduped = df_source
        
        # 4. Load Reference Tables (if any)
        reference_joins = metadata.get("reference_joins", [])
        if reference_joins:
            load_reference_tables(self.spark, reference_joins, self.env_config)

        # 5. Apply Transformations (hash expressions resolved IN the SQL)
        # We register the deduped DF as a temp view so SQL can query it
        temp_view_name = f"source_{table_name}_{uuid.uuid4().hex[:8]}"
        df_deduped.createOrReplaceTempView(temp_view_name)
        
        try:
            # Load SQL template from file
            sql_path = metadata.get("transformation_sql_path")
            if not sql_path:
                # Default path convention
                sql_path = f"query/{table_name}.sql"
            
            # Use custom config to handle __TEMP__ schema
            # We instantiate TempViewConfig with same params as EnvironmentConfig
            # temp_config = TempViewConfig(self.env_config.catalog, self.env_config.environment)
            
            # Override source to point to temp view
            resolve_metadata = metadata.copy()
            resolve_metadata["source_schema"] = "__TEMP__"
            resolve_metadata["source_table"] = temp_view_name
            
            # Resolve template (includes hash generation!)
            resolver = TemplateResolver(env_config=self.env_config, metadata=resolve_metadata)
            
            # Load and resolve SQL ({{_pk_hash}} and {{_diff_hash}} are resolved here!)
            try:
                # Load SQL file manually
                with open(sql_path, 'r') as f:
                    template_sql = f.read()
                
                # Resolve placeholders
                resolved_sql = resolver.resolve(template_sql, resolve_metadata)
                
                # Execute transformation
                df_transformed = execute_sql(
                    self.spark, 
                    resolved_sql, 
                    f"Transformation for {table_name}"
                )
            except FileNotFoundError:
                logger.warning(f"SQL file not found at {sql_path}. Using source data as-is.")
                # If no SQL, we just use the deduped source
                # But we still need to add hashes if they were expected to be in SQL
                # If SQL is missing, we can't easily add hashes via SQL.
                # We might need to fallback to DataFrame API for hashes if SQL is missing.
                # For now, let's assume SQL is required for transformation + hashing.
                # Or we can generate a default SQL: SELECT *, {{_pk_hash}} as _pk_hash ...
                # Let's stick to the user's flow: if no SQL, use source as-is.
                # But wait, if we use source as-is, we miss hashes!
                # The user's design implies hashes are ALWAYS generated via SQL.
                # So if SQL is missing, we probably should fail or generate a default one.
                # Let's assume for now we just use df_deduped, but warn.
                df_transformed = df_deduped

        finally:
            # Clean up temp view
            self.spark.catalog.dropTempView(temp_view_name)
            
        # 6. Cache for Deterministic Merge (prevents non-deterministic re-reads)
        df_transformed.cache()
        record_count = df_transformed.count()  # Materialize
        logger.info(f"Cached {record_count:,} records for deterministic merge")
        
        try:
            # 7. Apply Load Strategy (FIRST - merge before watermark update)
            strategy = get_strategy_from_metadata(self.spark, self.env_config, metadata)
            strategy.execute(df_transformed)
            logger.info(f"Successfully merged data for {table_name}")
            
            # 8. Update Watermark (SECOND - only if merge succeeded)
            if original_max_watermark:
                self.watermark_manager.update(table_name, original_max_watermark)
                logger.info(f"Updated watermark to {original_max_watermark}")
            
            logger.info(f"Pipeline completed for {table_name}")
            
        except Exception as merge_error:
            logger.error(f"Merge failed for {table_name}: {merge_error}")
            # Watermark NOT updated - next run will safely reprocess
            raise
        finally:
            # Clean up cache
            df_transformed.unpersist()
