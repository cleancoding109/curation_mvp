"""
Lakeflow Curation Pipeline

Orchestrates the end-to-end curation process:
1. Read source data (incremental)
2. Apply transformations (SQL)
3. Generate hash keys
4. Deduplicate
5. Apply load strategy (SCD1/SCD2)
6. Update watermarks
"""

import uuid
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, DataFrame, functions as F

from config.environment import EnvironmentConfig
from reader.source_reader import read_source_incremental
from state.watermark import WatermarkManager, get_max_watermark_from_df
from transform.template_resolver import TemplateResolver
from transform.sql_executor import execute_sql
from transform.hash_generator import HashGenerator
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
            try:
                self.process_table(metadata)
            except Exception as e:
                logger.error(f"Failed to process table {table_name}: {e}", exc_info=True)
                # Continue with next table? Or fail fast?
                # Usually pipelines might want to fail fast or continue based on config.
                # For now, we log and re-raise to fail the job.
                raise e

    def process_table(self, metadata: Dict[str, Any]):
        """
        Process a single table.
        
        Args:
            metadata: Table metadata dictionary
        """
        table_name = metadata.get("table_name")
        logger.info(f"Starting pipeline for table: {table_name}")
        
        # 1. Get Watermark
        watermark_value = self.watermark_manager.get(table_name)
        
        # 2. Read Source (Incremental)
        df = read_source_incremental(
            self.spark, 
            metadata, 
            self.env_config, 
            watermark_value
        )
        
        if df.rdd.isEmpty():
            logger.info(f"No new data for {table_name}. Skipping.")
            return

        # 3. Apply Transformations
        # We register the incremental DF as a temp view so SQL can query it
        temp_view_name = f"source_{table_name}_{uuid.uuid4().hex[:8]}"
        df.createOrReplaceTempView(temp_view_name)
        
        try:
            # Resolve SQL template
            # We override source resolution to point to our temp view
            resolve_metadata = metadata.copy()
            resolve_metadata["source_schema"] = "__TEMP__"
            resolve_metadata["source_table"] = temp_view_name
            
            # Use custom config to handle __TEMP__ schema
            temp_config = TempViewConfig(self.env_config.catalog, self.env_config.environment)
            resolver = TemplateResolver(env_config=temp_config, metadata=resolve_metadata)
            
            transform_sql = metadata.get("transform_sql")
            if transform_sql:
                resolved_sql = resolver.resolve(transform_sql, resolve_metadata)
                df_transformed = execute_sql(self.spark, resolved_sql, f"Transformation for {table_name}")
            else:
                # If no transform SQL, just use the source DF
                logger.info("No transform_sql provided, using source data as-is")
                df_transformed = df
                
        finally:
            # Clean up temp view
            self.spark.catalog.dropTempView(temp_view_name)
            
        # 4. Generate Hash Keys
        hash_gen = HashGenerator(metadata)
        
        # Add _pk_hash if configured
        if "_pk_hash" in hash_gen.hash_keys:
            pk_expr = hash_gen.pk_hash_expression
            df_transformed = df_transformed.withColumn("_pk_hash", F.expr(pk_expr))
            
        # Add _diff_hash if configured
        if "_diff_hash" in hash_gen.hash_keys:
            diff_expr = hash_gen.diff_hash_expression
            df_transformed = df_transformed.withColumn("_diff_hash", F.expr(diff_expr))
            
        # 5. Deduplicate
        df_deduped = deduplicate_from_metadata(df_transformed, metadata)
        
        # 6. Apply Load Strategy
        strategy = get_strategy_from_metadata(self.spark, self.env_config, metadata)
        strategy.execute(df_deduped)
        
        # 7. Update Watermark
        # We calculate max watermark from the *original* read DF (or transformed?)
        # Usually from the source data's commit timestamp.
        # If transformation drops the watermark column, we might have an issue.
        # But usually we preserve metadata columns.
        # Let's use df_deduped to be safe, assuming it has the watermark column.
        
        new_watermark = get_max_watermark_from_df(df_deduped)
        if new_watermark:
            self.watermark_manager.update(table_name, new_watermark)
            
        logger.info(f"Pipeline completed for {table_name}")
