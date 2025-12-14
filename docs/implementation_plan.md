# LTC Claims Management - Curation Framework Implementation Plan

## 1. Project Overview

| Attribute | Value |
|-----------|-------|
| Project Name | LTC Claims Curation Framework |
| Domain | Long-Term Care (LTC) Claims Management |
| Purpose | Bronze to Silver batch processing with SCD support |
| Technology | PySpark, Delta Lake, Databricks Asset Bundles |
| Deployment | Databricks Workflows (Lakeflow Jobs) |

### 1.1 Domain Entities

| Entity | SCD Type | Description |
|--------|----------|-------------|
| Claimants | SCD2 | Policyholders who file LTC claims |
| Policies | SCD2 | Insurance contracts with coverage details |
| Claims | SCD1 | Claim submissions and status updates |
| Claim Lines | SCD1 | Individual service lines within claims |
| Providers | SCD2 | Care facilities and service providers |
| Payments | SCD1 | Benefit disbursements |
| Assessments | SCD2 | Care needs evaluations |

## 2. Implementation Phases

### Phase 1: Core Framework Development ✅

| Task | Description | Status |
|------|-------------|--------|
| 1.1 | Project structure setup (DAB template) | ✅ Complete |
| 1.2 | Configuration schema design (JSON) | ✅ Complete |
| 1.3 | SilverProcessor class implementation | ✅ Complete |
| 1.4 | High-watermark incremental logic | ✅ Complete |
| 1.5 | SCD Type 1 merge implementation | ✅ Complete |
| 1.6 | SCD Type 2 merge implementation | ✅ Complete |
| 1.7 | SQL transformation engine | ✅ Complete |
| 1.8 | BatchFrameworkOrchestrator class | ✅ Complete |

### Phase 2: LTC Domain Configuration ✅

| Task | Description | Status |
|------|-------------|--------|
| 2.1 | tables_config.json with LTC entities | ✅ Complete |
| 2.2 | Claimants transformation SQL (SCD2) | ✅ Complete |
| 2.3 | Policies transformation SQL (SCD2) | ✅ Complete |
| 2.4 | Claims transformation SQL (SCD1) | ✅ Complete |
| 2.5 | Providers transformation SQL (SCD2) | ✅ Complete |
| 2.6 | Claim Lines transformation SQL (SCD1) | ✅ Complete |
| 2.7 | Payments transformation SQL (SCD1) | ✅ Complete |
| 2.8 | Assessments transformation SQL (SCD2) | ✅ Complete |

### Phase 3: DAB Deployment Configuration ✅

| Task | Description | Status |
|------|-------------|--------|
| 3.1 | databricks.yml bundle configuration | ✅ Complete |
| 3.2 | Job definition (hourly batch) | ✅ Complete |
| 3.3 | Single table job (on-demand) | ✅ Complete |
| 3.4 | Environment specifications | ✅ Complete |

### Phase 4: Testing & Validation ✅

| Task | Description | Status |
|------|-------------|--------|
| 4.1 | Unit tests for utility functions | ✅ Complete |
| 4.2 | Unit tests for SCD logic | ✅ Complete |
| 4.3 | Configuration validation tests | ✅ Complete |
| 4.4 | DataFrame operation tests | ✅ Complete |

### Phase 5: Documentation ✅

| Task | Description | Status |
|------|-------------|--------|
| 5.1 | Design document | ✅ Complete |
| 5.2 | Implementation plan | ✅ Complete |
| 5.3 | Interactive notebook examples | ✅ Complete |

### Phase 6: Production Deployment (Pending)

| Task | Description | Status |
|------|-------------|--------|
| 6.1 | Create Bronze source tables | ⏳ Pending |
| 6.2 | Deploy to dev environment | ⏳ Pending |
| 6.3 | Integration testing | ⏳ Pending |
| 6.4 | Deploy to prod environment | ⏳ Pending |
| 6.5 | Enable scheduling | ⏳ Pending |

## 3. File Structure

```
curation_framework/
├── databricks.yml                    # DAB bundle configuration
├── pyproject.toml                    # Python project config
├── README.md                         # Project readme
│
├── conf/                             # Configuration files
│   ├── tables_config.json            # LTC table metadata definitions
│   └── sql/                          # SQL transformation files
│       ├── claimants_transform.sql   # Claimants (SCD2)
│       ├── policies_transform.sql    # Policies (SCD2)
│       ├── claims_transform.sql      # Claims (SCD1)
│       ├── providers_transform.sql   # Providers (SCD2)
│       ├── claim_lines_transform.sql # Claim Lines (SCD1)
│       ├── payments_transform.sql    # Payments (SCD1)
│       └── assessments_transform.sql # Assessments (SCD2)
│
├── docs/                             # Documentation
│   ├── design_document.md            # Architecture & design
│   └── implementation_plan.md        # This file
│
├── resources/                        # DAB resource definitions
│   ├── curation_framework.job.yml    # Job configurations
│   └── curation_framework.pipeline.yml
│
├── src/                              # Source code
│   ├── notebook.ipynb                # Interactive notebook
│   └── curation_framework/           # Python package
│       ├── __init__.py               # Package exports
│       ├── main.py                   # Entry point
│       ├── silver_processor.py       # Core processing logic
│       └── utils.py                  # Utility functions
│
└── tests/                            # Test files
    ├── conftest.py                   # pytest configuration
    └── main_test.py                  # Unit tests
```

## 4. Key Components

### 4.1 SilverProcessor Class

**Location:** `src/curation_framework/silver_processor.py`

**Methods:**
| Method | Description |
|--------|-------------|
| `__init__()` | Initialize with config and settings |
| `get_high_watermark()` | Query MAX timestamp from target |
| `read_incremental_source()` | Read source with watermark filter |
| `apply_transformation()` | Execute SQL transformation |
| `process_scd_type1()` | MERGE upsert pattern |
| `process_scd_type2()` | History tracking pattern |
| `process()` | Main orchestration method |

### 4.2 BatchFrameworkOrchestrator Class

**Location:** `src/curation_framework/silver_processor.py`

**Methods:**
| Method | Description |
|--------|-------------|
| `__init__()` | Load configuration |
| `process_table()` | Process single table |
| `process_all_tables()` | Process all enabled tables |
| `get_processing_summary()` | Generate execution report |

### 4.3 Utility Functions

**Location:** `src/curation_framework/utils.py`

| Function | Description |
|----------|-------------|
| `validate_table_config()` | Validate config structure |
| `table_exists()` | Check if Delta table exists |
| `create_hash_column()` | Generate row hash for change detection |
| `deduplicate_by_key()` | Remove duplicates keeping latest |
| `generate_scd2_columns()` | Add SCD2 metadata columns |
| `optimize_delta_table()` | Run OPTIMIZE command |
| `vacuum_delta_table()` | Run VACUUM command |

## 5. Configuration Reference

### 5.1 Table Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `table_name` | string | Yes | Unique identifier for the table |
| `source_table` | string | Yes | Fully qualified source table name |
| `target_table` | string | Yes | Fully qualified target table name |
| `scd_type` | int | Yes | 1 (Upsert) or 2 (History) |
| `business_key_columns` | array | Yes | Natural key column(s) |
| `watermark_column` | string | No | Source timestamp column |
| `target_watermark_column` | string | No | Target timestamp column |
| `transformation_sql_path` | string | No | Path to SQL file |
| `scd2_columns` | object | No | SCD2 column names |
| `track_columns` | array | No | Columns to track for SCD2 changes |
| `enabled` | boolean | No | Enable/disable processing |

### 5.2 SCD2 Column Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `effective_start_date` | effective_start_date | Start date column name |
| `effective_end_date` | effective_end_date | End date column name |
| `is_current` | is_current | Current flag column name |

## 6. Deployment Commands

### Development Deployment

```bash
# Authenticate to Databricks
databricks configure

# Deploy to dev environment
databricks bundle deploy --target dev

# Run the job manually
databricks bundle run --target dev curation_framework_job

# View job status
databricks jobs list
```

### Production Deployment

```bash
# Deploy to production
databricks bundle deploy --target prod

# Validate deployment
databricks bundle validate --target prod
```

### Local Development

```bash
# Install dependencies
uv sync --dev

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=curation_framework
```

## 7. Job Scheduling

### Batch Processing Job

| Setting | Value |
|---------|-------|
| Name | ltc_claims_silver_batch_job |
| Schedule | Every 1 hour |
| Timeout | 2 hours |
| Retries | 2 |
| Compute | Serverless |

### On-Demand Single Table Job

| Setting | Value |
|---------|-------|
| Name | ltc_claims_single_table_job |
| Schedule | Manual trigger only |
| Timeout | 1 hour |
| Parameters | config_path, table_name |

## 8. Adding New Tables

### Step 1: Add Table Configuration

Edit `conf/tables_config.json`:

```json
{
  "table_name": "silver_new_table",
  "source_table": "bronze.new_table_streaming",
  "target_table": "silver.new_table",
  "scd_type": 1,
  "business_key_columns": ["id"],
  "watermark_column": "ingestion_ts",
  "transformation_sql_path": "conf/sql/new_table_transform.sql",
  "enabled": true
}
```

### Step 2: Create SQL Transformation

Create `conf/sql/new_table_transform.sql`:

```sql
SELECT
    id,
    TRIM(column1) AS column1,
    CAST(column2 AS INT) AS column2,
    ingestion_ts AS source_timestamp,
    current_timestamp() AS processing_timestamp
FROM source_incremental
WHERE id IS NOT NULL
```

### Step 3: Deploy

```bash
databricks bundle deploy --target dev
```

## 9. Monitoring & Observability

### Logging

- All processing steps are logged with timestamps
- Log level configurable via `global_settings.log_level`
- Logs available in Databricks job runs

### Metrics Captured

| Metric | Description |
|--------|-------------|
| `records_processed` | Number of records processed |
| `duration_seconds` | Processing time |
| `watermark_used` | Timestamp filter applied |
| `status` | success/failed |

### Alerting

- Email notifications on job failure
- Email notifications on job success (optional)
- Configure in `resources/curation_framework.job.yml`

## 10. Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| No records processed | Watermark too recent | Check source data arrival |
| Table not found | Missing catalog/schema | Verify table path in config |
| SQL error | Invalid transformation | Test SQL in notebook first |
| Merge conflict | Duplicate keys | Add deduplication logic |

### Debug Mode

Run with verbose logging:

```python
# In notebook
import logging
logging.getLogger("SilverProcessor").setLevel(logging.DEBUG)
```

## 11. Future Enhancements

| Enhancement | Priority | Description |
|-------------|----------|-------------|
| Data quality checks | High | Claim validation rules (e.g., service dates, amounts) |
| Claims analytics Gold layer | High | Aggregated metrics for claims processing |
| Schema evolution | Medium | Auto-detect and handle schema changes |
| Parallel processing | Medium | Process multiple LTC tables concurrently |
| Provider network validation | Medium | Verify provider eligibility and network status |
| Metrics dashboard | Medium | Claims processing KPIs in Databricks SQL |
| Audit trail | Medium | Track all claim status changes |
| CDC support | Low | Support for Change Data Capture patterns |

## 12. LTC Domain-Specific Considerations

### Business Rules

| Rule | Description |
|------|-------------|
| Claim-Policy Link | Every claim must reference a valid policy |
| Elimination Period | Benefits start after elimination period expires |
| Daily Benefit Cap | Payments cannot exceed daily benefit amount |
| Benefit Period | Track remaining benefit days per claimant |
| Provider Eligibility | Verify provider is in-network and licensed |

### Key Metrics (Gold Layer - Future)

| Metric | Description |
|--------|-------------|
| Claims Submitted | Count of new claims per period |
| Average Days to Decision | Time from submission to approval/denial |
| Approval Rate | Percentage of claims approved |
| Denial Reasons | Distribution of denial reason codes |
| Average Claim Amount | Mean claim value by care setting |
| Provider Utilization | Claims volume by provider type |

## 13. Version History

| Version | Date | Changes |
|---------|------|---------|
| 0.0.1 | 2024-12-14 | Initial implementation for LTC Claims domain |
