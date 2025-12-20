"""
Lakeflow Curation Pipeline

Orchestrates the end-to-end curation process:
1. Read source data (incremental)
2. Deduplicate raw data
3. Apply transformations (SQL with hash placeholders)
4. Persist via temp tables in target schema (with temp_ prefix)
5. Apply load strategy (SCD1/SCD2)
6. Update watermarks (failure-safe ordering)
"""

import os
import uuid
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from config.environment import EnvironmentConfig
from reader.source_reader import read_source_incremental
from reader.reference_reader import load_reference_tables
from state.watermark import WatermarkManager, get_max_watermark_from_df
from transform.template_resolver import TemplateResolver
from config.metadata_loader import load_sql_template
from transform.sql_executor import execute_sql
from utils.dedup import deduplicate_from_metadata
from load_strategy.factory import get_strategy_from_metadata
from utils.logging import get_logger

logger = get_logger(__name__)


def _ensure_schema_exists(spark, catalog: str, schema: str):
    """Create schema if missing to avoid SCHEMA_NOT_FOUND."""
    fq_schema = f"{catalog}.{schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq_schema}")
    logger.info(f"Ensured schema exists: {fq_schema}")


class LakeflowCurationPipeline:
    """
    Main pipeline orchestration class.
    
    Handles incremental data processing with:
    - Watermark-based incremental reads
    - Optional deduplication
    - SQL-based transformations with hash generation
    - Deterministic temp table caching
    - SCD1/SCD2 merge strategies
    - Failure-safe watermark updates
    """
    
    def __init__(self, spark: SparkSession, env_config: EnvironmentConfig = None):
        """
        Initialize pipeline.
        
        Args:
            spark: SparkSession instance
            env_config: Environment configuration (catalog, environment)
        """
        self.spark = spark
        self.env_config = env_config or EnvironmentConfig()
        self.watermark_manager = WatermarkManager(self.spark, self.env_config)
        self.temp_tables_created: List[str] = []
    
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
    
    def _cleanup_temp_table(self, temp_table: Optional[str]):
        """
        Clean up specific temporary staging table.
        
        Args:
            temp_table: Fully qualified temp table name to drop
        """
        if not temp_table:
            return
        try:
            self.spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
            if temp_table in self.temp_tables_created:
                self.temp_tables_created.remove(temp_table)
            logger.info(f"Dropped temp table: {temp_table}")
        except Exception as e:
            logger.warning(f"Cleanup failed for temp table {temp_table}: {e}")
    
    def _cleanup_temp_view(self, temp_view: str):
        """
        Clean up temporary view.
        
        Args:
            temp_view: Temp view name to drop
        """
        try:
            self.spark.catalog.dropTempView(temp_view)
            logger.debug(f"Dropped temp view: {temp_view}")
        except Exception as e:
            logger.warning(f"Failed to drop temp view {temp_view}: {e}")
    
    def process_table(self, metadata: Dict[str, Any]):
        """
        Process a single table following failure-safe ordering.
        
        Pipeline Steps:
        1. Get current watermark
        2. Read source data incrementally
        3. Deduplicate (optional)
        4. Load reference tables (optional)
        5. Apply SQL transformations with hash generation
        6. Materialize to temp table for deterministic processing
        7. Execute load strategy (SCD1/SCD2)
        8. Update watermark (only after successful merge)
        
        Args:
            metadata: Table metadata dictionary
        """
        table_name = metadata.get("table_name")
        logger.info(f"Starting pipeline for table: {table_name}")
        
        target_schema = metadata.get("target_schema", "standardized_data_layer")
        
        # Ensure both target schema and watermark schema exist before use
        _ensure_schema_exists(self.spark, self.env_config.catalog, target_schema)
        _ensure_schema_exists(self.spark, self.env_config.catalog, "standardized_data_layer")
        
        # Ensure watermark control table exists before reads/writes
        self.watermark_manager.ensure_table_exists()
        
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
        enable_deduplication = metadata.get("enable_deduplication", False)
        business_keys = metadata.get("business_key_columns")
        
        if enable_deduplication and business_keys:
            df_deduped = deduplicate_from_metadata(df_source, metadata)
        elif enable_deduplication and not business_keys:
            logger.warning(f"Deduplication enabled for {table_name} but no business keys defined. Skipping.")
            df_deduped = df_source
        else:
            logger.debug(f"Deduplication skipped for {table_name} (enabled={enable_deduplication})")
            df_deduped = df_source
        
        # 4. Load Reference Tables (if any)
        reference_joins = metadata.get("reference_joins", [])
        if reference_joins:
            load_reference_tables(self.spark, metadata, self.env_config)
        
        temp_table: Optional[str] = None
        temp_view_name = f"source_{table_name}_{uuid.uuid4().hex[:8]}"
        
        try:
            # 5. Apply Transformations
            # Register deduped DataFrame as temp view (no schema qualification needed)
            df_deduped.createOrReplaceTempView(temp_view_name)
            logger.debug(f"Created temp view: {temp_view_name}")
            
            # Load SQL template from file
            sql_path = metadata.get("transformation_sql_path")
            if not sql_path:
                # Default path convention
                sql_path = f"query/{table_name}.sql"
            
            # Resolve sql_path using _base_path if available and path is relative
            base_path = metadata.get("_base_path")
            if base_path and not os.path.isabs(sql_path):
                sql_path = os.path.join(base_path, sql_path)
            
            try:
                # Load SQL template
                template_sql = load_sql_template(sql_path)
                
                # Replace {{source}} placeholder with temp view name BEFORE resolver
                # This bypasses the need for schema qualification
                template_sql = template_sql.replace("{{source}}", temp_view_name)
                logger.debug(f"Replaced {{{{source}}}} with temp view: {temp_view_name}")
                
                # Resolve other placeholders (hashes, refs, aliases, etc.)
                resolver = TemplateResolver(env_config=self.env_config, metadata=metadata)
                resolved_sql = resolver.resolve(template_sql, metadata)
                
                # Execute transformation
                df_transformed = execute_sql(
                    self.spark,
                    resolved_sql,
                    f"Transformation for {table_name}"
                )
                
            except FileNotFoundError as fnf_err:
                logger.error(f"SQL file not found at {sql_path}", exc_info=True)
                raise fnf_err
            except Exception as transform_err:
                logger.error("Failed to resolve or execute transformation SQL", exc_info=True)
                raise transform_err
        
            # 6. Persist deterministically via temp table in target schema
            # Using temp_ prefix instead of separate temp schema
            temp_suffix = uuid.uuid4().hex[:8]
            temp_table = f"{self.env_config.catalog}.{target_schema}.temp_{table_name}_{temp_suffix}"
            self.temp_tables_created.append(temp_table)
            
            logger.debug(f"Creating temp staging table: {temp_table}")
            df_transformed.write.mode("overwrite").saveAsTable(temp_table)
            df_deterministic = self.spark.table(temp_table)
            
            record_count = df_deterministic.count()
            logger.info(f"Materialized {record_count:,} records via temp table {temp_table}")

        finally:
            # Clean up temp view (even if transformation failed)
            self._cleanup_temp_view(temp_view_name)
        
        try:
            # 7. Apply Load Strategy (FIRST - merge before watermark update)
            strategy = get_strategy_from_metadata(self.spark, self.env_config, metadata)
            strategy.execute(df_deterministic)
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
            # Clean up temp staging table (specific table, not regex)
            self._cleanup_temp_table(temp_table)
