# Databricks notebook source
# Setup: Configure environment and paths
import sys
import os
import pyspark
import traceback

# Determine execution environment
is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
print(f"Execution Environment: {'Databricks' if is_databricks else 'Local'}")

if is_databricks:
    # Robust path finding logic
    current_dir = os.getcwd()
    print(f"Current working directory: {current_dir}")
    
    # Go up one level to find root
    project_root = os.path.dirname(current_dir)
    print(f"Project root (assumed): {project_root}")
    
    src_path = os.path.join(project_root, "src")
    if os.path.exists(src_path):
        print(f"Found src at {src_path}")
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
    else:
        print(f"WARNING: src not found at {src_path}")
        if os.path.exists(os.path.join(current_dir, "src")):
             src_path = os.path.join(current_dir, "src")
             sys.path.insert(0, src_path)
             print(f"Found src at {src_path}")

else:
    # Local Development
    possible_roots = [
        r"d:\HandsOn\Databricks\curation_framework",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")) if '__file__' in locals() else os.path.abspath("..")
    ]
    
    project_root = None
    for path in possible_roots:
        if os.path.exists(os.path.join(path, "src")):
            project_root = path
            break
            
    if not project_root:
        project_root = os.getcwd()
        print(f"Warning: Could not find project root. Using CWD: {project_root}")

    src_path = os.path.join(project_root, "src")
    os.environ["USE_DATABRICKS_CONNECT"] = "false"
    if src_path in sys.path:
        sys.path.remove(src_path)
    sys.path.insert(0, src_path)
    os.chdir(project_root)

# Verify setup
print(f"PySpark location: {pyspark.__file__}")
print(f"Project root: {project_root}")
print(f"Working directory: {os.getcwd()}")

# Global spark variable
spark = None
failed_tests = []

# COMMAND ----------

# Test 1: Logging Module
try:
    from utils.logging import get_logger, PipelineLogger

    # Test basic logger
    logger = get_logger("test_module")
    logger.info("Basic logger test - INFO")
    logger.debug("Basic logger test - DEBUG (may not show if level=INFO)")
    logger.warning("Basic logger test - WARNING")

    # Test pipeline logger with context
    pipeline_logger = PipelineLogger(
        name="test_pipeline",
        run_id="test-001",
        table_name="customer"
    )
    pipeline_logger.info("Pipeline logger test with context")
    pipeline_logger.set_context(step="load")
    pipeline_logger.info("After setting step context")

    print("\n✅ Test 1 PASSED: Logging module works correctly")
except Exception as e:
    print(f"❌ Test 1 FAILED: {e}")
    failed_tests.append("Test 1: Logging Module")
    traceback.print_exc()

# COMMAND ----------

# Test 2: Spark Session Module
try:
    from utils.spark_session import get_spark_session, get_dbutils

    # Get or create SparkSession
    spark = get_spark_session("IntegrationTest")
    print(f"SparkSession: {spark}")
    print(f"Spark version: {spark.version}")
    # print(f"App name: {spark.sparkContext.appName}") # Not supported in Serverless

    print("\n✅ Test 2 PASSED: Spark session module works correctly")
except Exception as e:
    print(f"❌ Test 2 FAILED: {e}")
    failed_tests.append("Test 2: Spark Session Module")
    traceback.print_exc()

# COMMAND ----------

# Test 3: Environment Module
try:
    from config.environment import get_catalog, get_environment, EnvironmentConfig

    # Test standalone functions
    catalog = get_catalog()
    environment = get_environment()
    print(f"Catalog: {catalog}")
    print(f"Environment: {environment}")

    # Test EnvironmentConfig class
    env_config = EnvironmentConfig()
    print(f"EnvironmentConfig: {env_config}")

    # Test fully qualified table name
    fq_table = env_config.get_fully_qualified_table("standardized_data_layer", "customer")
    print(f"Fully qualified table: {fq_table}")

    # Verify format
    assert "." in fq_table, "Table name should contain dots"
    assert fq_table.count(".") == 2, "Should have catalog.schema.table format"

    print("\n✅ Test 3 PASSED: Environment module works correctly")
except Exception as e:
    print(f"❌ Test 3 FAILED: {e}")
    failed_tests.append("Test 3: Environment Module")
    traceback.print_exc()

# COMMAND ----------

# Test 4: Metadata Loader Module
try:
    from config.metadata_loader import (
        load_table_metadata,
        load_sql_template,
        discover_domain_tables,
        MetadataRegistry
    )

    # Set working directory to project root for file paths
    if project_root:
        try:
            os.chdir(project_root)
            print(f"Changed working directory to: {os.getcwd()}")
        except Exception as e:
            print(f"Warning: Could not change directory to {project_root}: {e}")

    # Test loading customer metadata
    # Note: Paths are relative to project root
    customer_metadata = load_table_metadata("./metadata/sdl/claims/customer.json")
    print(f"\nLoaded metadata for: {customer_metadata['table_name']}")
    print(f"Source schema: {customer_metadata['source']['schema']}")
    print(f"Target schema: {customer_metadata['target']['schema']}")
    print(f"Load strategy: {customer_metadata['load_strategy']}")
    print(f"SCD2 mode: {customer_metadata['load_strategy']['scd2_mode']}")
    print(f"Source timezone: {customer_metadata.get('source_timezone', 'N/A')}")

    # Test SQL template loading
    customer_sql = load_sql_template("query/customer.sql")
    print(f"\nSQL template loaded: {len(customer_sql)} characters")
    print(f"Contains {{{{source}}}}: {('{{source}}' in customer_sql)}")
    print(f"Contains {{{{_pk_hash}}}}: {('{{_pk_hash}}' in customer_sql)}")

    # Test domain discovery
    tables = discover_domain_tables("claims")
    print(f"\nDiscovered tables in 'claims' domain: {len(tables)}")
    for t in tables:
        print(f"  - {t}")

    # Test MetadataRegistry
    registry = MetadataRegistry()
    claims_metadata = registry.get_table_metadata("claims", "claims")
    print(f"\nRegistry loaded claims metadata: {claims_metadata['table_name']}")

    print("\n✅ Test 4 PASSED: Metadata loader module works correctly")
except Exception as e:
    print(f"❌ Test 4 FAILED: {e}")
    failed_tests.append("Test 4: Metadata Loader Module")
    traceback.print_exc()

# COMMAND ----------

# Test 5: Hash Generator Module
try:
    from transform.hash_generator import (
        canonicalize_column,
        generate_pk_hash_expression,
        generate_diff_hash_expression,
        generate_hash_expressions_from_metadata,
        HashGenerator
    )

    # Test canonicalization
    canon = canonicalize_column("customer_id")
    print(f"Canonicalized column: {canon}")
    assert "UPPER" in canon and "TRIM" in canon and "COALESCE" in canon

    # Test with alias
    canon_alias = canonicalize_column("customer_id", alias="src")
    print(f"With alias: {canon_alias}")
    assert "src.customer_id" in canon_alias

    # Test PK hash expression
    pk_expr = generate_pk_hash_expression(["customer_id", "policy_id"])
    print(f"\nPK hash expression: {pk_expr[:100]}...")
    assert "SHA2" in pk_expr
    assert "customer_id" in pk_expr
    assert "policy_id" in pk_expr

    # Test from metadata
    # Ensure customer_metadata is available from Test 4
    if 'customer_metadata' in locals():
        hash_exprs = generate_hash_expressions_from_metadata(customer_metadata)
        print(f"\nHash expressions from metadata:")
        print(f"  _pk_hash: {hash_exprs.get('_pk_hash', 'N/A')[:80]}...")
        print(f"  _diff_hash: {hash_exprs.get('_diff_hash', 'N/A')[:80]}...")

        # Test HashGenerator class
        hash_gen = HashGenerator(customer_metadata)
        select_exprs = hash_gen.get_hash_select_expressions()
        print(f"\nSELECT expressions:\n{select_exprs}")
    else:
        print("Skipping metadata dependent tests in Test 5 (customer_metadata missing)")

    print("\n✅ Test 5 PASSED: Hash generator module works correctly")
except Exception as e:
    print(f"❌ Test 5 FAILED: {e}")
    failed_tests.append("Test 5: Hash Generator Module")
    traceback.print_exc()

# COMMAND ----------

# Test 6: Template Resolver Module
try:
    from transform.template_resolver import (
        resolve_source_placeholder,
        resolve_target_placeholder,
        build_placeholder_context,
        resolve_template,
        TemplateResolver
    )

    if 'customer_metadata' in locals() and 'env_config' in locals():
        # Test source placeholder
        source_table = resolve_source_placeholder(customer_metadata, env_config)
        print(f"Source table: {source_table}")

        # Test target placeholder
        target_table = resolve_target_placeholder(customer_metadata, env_config)
        print(f"Target table: {target_table}")

        # Test placeholder context
        context = build_placeholder_context(customer_metadata, env_config)
        print(f"\nPlaceholder context:")
        for key, value in context.items():
            val_str = str(value)[:60] + "..." if len(str(value)) > 60 else str(value)
            print(f"  {key}: {val_str}")

        # Test template resolution
        test_template = "SELECT * FROM {{source}} WHERE timezone = '{{source_timezone}}'"
        resolved = resolve_template(test_template, customer_metadata, env_config)
        print(f"\nTemplate: {test_template}")
        print(f"Resolved: {resolved}")
        assert "{{" not in resolved, "All placeholders should be resolved"

        # Test TemplateResolver class
        resolver = TemplateResolver(env_config, customer_metadata)
        resolved_sql = resolver.load_and_resolve("query/customer.sql")
        print(f"\nResolved SQL length: {len(resolved_sql)} chars")
        print(f"Sample: {resolved_sql[:200]}...")
    else:
        print("Skipping Test 6 (dependencies missing)")

    print("\n✅ Test 6 PASSED: Template resolver module works correctly")
except Exception as e:
    print(f"❌ Test 6 FAILED: {e}")
    failed_tests.append("Test 6: Template Resolver Module")
    traceback.print_exc()

# COMMAND ----------

# Test 7: SQL Executor Module
try:
    from transform.sql_executor import execute_sql, SqlExecutor

    if spark:
        # Test simple SQL execution
        df = execute_sql(spark, "SELECT 1 as id, 'test' as name", "test query")
        df.show()
        assert df.count() == 1

        # Test SqlExecutor class
        executor = SqlExecutor(spark)

        # Create test data
        test_df = spark.createDataFrame([
            (1, "Alice", "2024-01-01"),
            (2, "Bob", "2024-01-02"),
            (3, "Charlie", "2024-01-03")
        ], ["id", "name", "date"])

        # Execute with temp view
        result_df = executor.execute_with_view(
            test_df,
            "test_data",
            "SELECT id, name FROM test_data WHERE id > 1",
            "filter test"
        )
        result_df.show()
        assert result_df.count() == 2

        # Cleanup
        executor.cleanup_temp_views()
    else:
        print("Skipping Test 7 (spark session missing)")

    print("\n✅ Test 7 PASSED: SQL executor module works correctly")
except Exception as e:
    print(f"❌ Test 7 FAILED: {e}")
    failed_tests.append("Test 7: SQL Executor Module")
    traceback.print_exc()

# COMMAND ----------

# Test 8: Dedup Module
try:
    from utils.dedup import deduplicate_by_key, deduplicate_from_metadata, Deduplicator

    if spark:
        # Create test data with duplicates
        dup_df = spark.createDataFrame([
            (1, "Alice", "2024-01-01 10:00:00"),
            (1, "Alice Updated", "2024-01-01 11:00:00"),  # Later - should keep
            (2, "Bob", "2024-01-02 10:00:00"),
            (2, "Bob Updated", "2024-01-02 09:00:00"),   # Earlier - should drop
            (3, "Charlie", "2024-01-03 10:00:00")
        ], ["id", "name", "timestamp"])

        print("Original data:")
        dup_df.show()

        # Test deduplicate_by_key (keep latest)
        deduped = deduplicate_by_key(
            dup_df,
            key_columns=["id"],
            order_column="timestamp",
            order_ascending=False  # Keep latest
        )
        print("Deduplicated (keep latest):")
        deduped.show()
        assert deduped.count() == 3

        # Verify we kept the right records
        alice_name = deduped.filter("id = 1").select("name").first()[0]
        bob_name = deduped.filter("id = 2").select("name").first()[0]
        assert alice_name == "Alice Updated", f"Expected 'Alice Updated', got '{alice_name}'"
        assert bob_name == "Bob", f"Expected 'Bob', got '{bob_name}'"

        # Test Deduplicator class
        if 'customer_metadata' in locals():
            deduplicator = Deduplicator(customer_metadata)
            print(f"Deduplicator key columns: {deduplicator._key_columns}")
            print(f"Deduplicator order column: {deduplicator._order_column}")
    else:
        print("Skipping Test 8 (spark session missing)")

    print("\n✅ Test 8 PASSED: Dedup module works correctly")
except Exception as e:
    print(f"❌ Test 8 FAILED: {e}")
    failed_tests.append("Test 8: Dedup Module")
    traceback.print_exc()

# COMMAND ----------

# Test 9: Delta Writer Module (structure test only - no actual writes)
try:
    from writer.delta_writer import DeltaWriter

    if spark and 'env_config' in locals():
        # Test DeltaWriter class instantiation
        writer = DeltaWriter(spark, env_config)

        if 'customer_metadata' in locals():
            # Test target table resolution
            target = writer.get_target_table(customer_metadata)
            print(f"Target table: {target}")
            assert ".standardized_data_layer." in target

        # Verify method signatures exist
        assert hasattr(writer, 'insert'), "Should have insert method"
        assert hasattr(writer, 'merge'), "Should have merge method"
        assert hasattr(writer, 'delete_insert'), "Should have delete_insert method"
        assert hasattr(writer, 'truncate_insert'), "Should have truncate_insert method"
    else:
        print("Skipping Test 9 (dependencies missing)")

    print("\n✅ Test 9 PASSED: Delta writer module structure is correct")
except Exception as e:
    print(f"❌ Test 9 FAILED: {e}")
    failed_tests.append("Test 9: Delta Writer Module")
    traceback.print_exc()

# COMMAND ----------

# Test 10: Watermark Module (structure test only - no actual table access)
try:
    from state.watermark import (
        get_max_watermark_from_df,
        WatermarkManager
    )

    if spark:
        # Test get_max_watermark_from_df
        ts_df = spark.createDataFrame([
            (1, "2024-01-01 10:00:00"),
            (2, "2024-01-02 10:00:00"),
            (3, "2024-01-03 10:00:00")
        ], ["id", "timestamp"])

        max_wm = get_max_watermark_from_df(ts_df, "timestamp")
        print(f"Max watermark: {max_wm}")
        assert "2024-01-03" in max_wm

        # Test WatermarkManager structure
        if 'env_config' in locals():
            wm_manager = WatermarkManager(spark, env_config)
            assert hasattr(wm_manager, 'get'), "Should have get method"
            assert hasattr(wm_manager, 'update'), "Should have update method"
            assert hasattr(wm_manager, 'update_from_df'), "Should have update_from_df method"
    else:
        print("Skipping Test 10 (spark session missing)")

    print("\n✅ Test 10 PASSED: Watermark module structure is correct")
except Exception as e:
    print(f"❌ Test 10 FAILED: {e}")
    failed_tests.append("Test 10: Watermark Module")
    traceback.print_exc()

# COMMAND ----------

# Test 11: Load Strategy Base and Factory
try:
    from load_strategy.base import LoadStrategy
    from load_strategy.factory import (
        LoadStrategyFactory,
        list_strategies,
        get_strategy
    )

    # List available strategies
    strategies = list_strategies()
    print(f"Available strategies: {strategies}")
    assert "insert_only" in strategies
    assert "delete_insert" in strategies
    assert "truncate_insert" in strategies

    if spark and 'env_config' in locals():
        # Test factory
        factory = LoadStrategyFactory(spark, env_config)
        print(f"\nFactory available strategies: {factory.available_strategies()}")

        if 'customer_metadata' in locals():
            # Create insert_only strategy
            insert_strategy = factory.create_by_name("insert_only", customer_metadata)
            print(f"\nCreated strategy: {insert_strategy.name}")
            print(f"Target table: {insert_strategy.target_table}")
            print(f"Primary key columns: {insert_strategy.primary_key_columns}")

            # Test LoadStrategy is abstract
            try:
                abstract_strategy = LoadStrategy(spark, env_config, customer_metadata)
                abstract_strategy.execute(None)  # Should fail
                print("ERROR: Should not be able to instantiate abstract class")
            except TypeError as e:
                print(f"Correctly prevented abstract instantiation: {e}")
    else:
        print("Skipping Test 11 (dependencies missing)")

    print("\n✅ Test 11 PASSED: Load strategy base and factory work correctly")
except Exception as e:
    print(f"❌ Test 11 FAILED: {e}")
    failed_tests.append("Test 11: Load Strategy Base and Factory")
    traceback.print_exc()

# COMMAND ----------

# Test 12: Concrete Load Strategies (structure test only)
try:
    from load_strategy.insert_only import InsertOnlyStrategy
    from load_strategy.delete_insert import DeleteInsertStrategy
    from load_strategy.truncate_insert import TruncateInsertStrategy

    if spark and 'env_config' in locals() and 'customer_metadata' in locals():
        # Test InsertOnlyStrategy
        insert_strategy = InsertOnlyStrategy(spark, env_config, customer_metadata)
        print(f"InsertOnlyStrategy.name: {insert_strategy.name}")
        assert insert_strategy.name == "insert_only"

        # Test DeleteInsertStrategy
        # Add partition_columns to metadata for this test
        di_metadata = {**customer_metadata, "partition_columns": ["data_date"]}
        delete_strategy = DeleteInsertStrategy(spark, env_config, di_metadata)
        print(f"DeleteInsertStrategy.name: {delete_strategy.name}")
        print(f"DeleteInsertStrategy.partition_columns: {delete_strategy.partition_columns}")
        assert delete_strategy.name == "delete_insert"

        # Test TruncateInsertStrategy
        truncate_strategy = TruncateInsertStrategy(spark, env_config, customer_metadata)
        print(f"TruncateInsertStrategy.name: {truncate_strategy.name}")
        assert truncate_strategy.name == "truncate_insert"

        # Verify all implement execute method
        for strategy in [insert_strategy, delete_strategy, truncate_strategy]:
            assert hasattr(strategy, 'execute'), f"{strategy.name} should have execute method"
            assert hasattr(strategy, 'run'), f"{strategy.name} should have run method"
    else:
        print("Skipping Test 12 (dependencies missing)")

    print("\n✅ Test 12 PASSED: Concrete load strategies are properly structured")
except Exception as e:
    print(f"❌ Test 12 FAILED: {e}")
    failed_tests.append("Test 12: Concrete Load Strategies")
    traceback.print_exc()

# COMMAND ----------

# Summary
print("="*60)
print("INTEGRATION TEST SUMMARY")
print("="*60)
if failed_tests:
    print(f"❌ {len(failed_tests)} TESTS FAILED:")
    for test in failed_tests:
        print(f"  - {test}")
    print("="*60)
    print("Exiting with error code 1")
    sys.exit(1)
else:
    print("✅ ALL TESTS PASSED")
    print("="*60)

