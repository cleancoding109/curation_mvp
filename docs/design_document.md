# Metadata-Driven Spark Batch Framework - Design Document

## 1. Overview

This document describes the architecture and design of a metadata-driven Spark Batch Framework for Databricks, built for the **Long-Term Care (LTC) Claims Management** domain. The framework performs **Curation** - processing data from Bronze layer (Lakeflow Streaming Tables) to **Silver layer** (Delta Tables) using efficient incremental batch patterns.

### 1.1 Domain Context: LTC Claims Management

Long-Term Care (LTC) insurance covers services for individuals who need extended assistance with daily living activities. The claims management process involves:
- **Claimants**: Policyholders who file claims for LTC services
- **Claims**: Requests for reimbursement of covered LTC expenses
- **Providers**: Facilities and caregivers providing LTC services (nursing homes, home health aides, assisted living)
- **Policies**: Insurance contracts defining coverage, benefits, and eligibility
- **Assessments**: Evaluations of claimant care needs and eligibility
- **Payments**: Benefit disbursements to claimants or providers

**Curation = Silver Layer Processing** - This framework is responsible for:
- Cleansing and standardizing raw Bronze data from claims systems
- Applying business transformations for LTC domain entities
- Implementing SCD (Slowly Changing Dimension) patterns for claim/policy history
- Producing curated, query-ready Silver tables for claims analytics

**Silver Layer = Relational Model (ODS Pattern)**
- Follows a normalized/relational data model similar to an Operational Data Store (ODS)
- Entity-centric tables (Claimants, Claims, Providers, Policies, Payments, etc.)
- **Multi-Source Grain**: Tables retain `source_system` to distinguish data origin.
- **Logical Relationships**: Models relationships between LTC entities (Referential integrity is not enforced at load time).
- Optimized for operational reporting and downstream Gold layer aggregations

### 1.2 Upstream Integration: Unified Bronze Layer

The Curation Framework consumes data from a **Unified Bronze Layer** which acts as the raw ingestion point for all source systems.

- **Source Architecture**: The Bronze layer uses a **Unified SCD Type 2** pattern.
- **Composite Key**: Records are uniquely identified by `entity_id` + `source_system`.
- **Duplicate Preservation**: The Bronze layer intentionally preserves duplicates from different source systems.
- **Role of Silver**: The Silver layer cleanses and standardizes this data but **preserves the multi-source nature**. It does *not* conform or merge different sources into a single "Golden Record" at this stage.

### 1.3 Multi-Source Strategy

The Silver layer maintains the distinction between data sources, effectively operating as a **Partitioned ODS**.

1.  **Source Preservation**:
    - The `source_system` column is propagated from Bronze to Silver.
    - No "winner-takes-all" logic is applied. Data from `AdminSystem` and `CRM` for the same Claimant co-exists as separate records.

2.  **Composite Business Keys**:
    - The effective Business Key for all Silver operations is `(Business Key, Source System)`.
    - *Example*: A Claimant is identified by `(claimant_id, source_system)`.

3.  **Union & Aggregation**:
    - All records are unioned into the target tables.
    - Downstream Gold layers will be responsible for any cross-source merging or "Golden Record" creation if required.

## 2. Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Databricks Workspace                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────────────┐    ┌──────────────────────┐   │
│  │    Kafka     │───▶│   Lakeflow Streaming │───▶│    Bronze Layer      │   │
│  │   Sources    │    │       Ingestion      │    │   (Streaming Tables) │   │
│  └──────────────┘    └──────────────────────┘    └──────────┬───────────┘   │
│                                                              │               │
│                                                              ▼               │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │          CURATION FRAMEWORK = SILVER LAYER PROCESSING                 │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │  │
│  │  │  1. High-Watermark Incremental Reader (Efficient Batch Reads)   │  │  │
│  │  │  2. SQL Transformation Engine (Cleansing & Business Logic)      │  │  │
│  │  │  3. SCD Type 1/2 Merge Processor (Upsert & History Tracking)    │  │  │
│  │  └─────────────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                              │               │
│                                                              ▼               │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │           SILVER LAYER = RELATIONAL MODEL (ODS Pattern)               │  │
│  │                    Curated, Normalized Delta Tables                   │  │
│  │  ┌─────────────────────────────┐  ┌─────────────────────────────────┐ │  │
│  │  │  SCD Type 1 Tables          │  │  SCD Type 2 Tables              │ │  │
│  │  │  - Current state only       │  │  - Full history tracking        │ │  │
│  │  │  - Upsert pattern           │  │  - Effective dates              │ │  │
│  │  │  - Fast lookups             │  │  - Point-in-time queries        │ │  │
│  │  └─────────────────────────────┘  └─────────────────────────────────┘ │  │
│  │                                                                        │  │
│  │  LTC Entity Tables: Claimants, Claims, Providers, Policies, Payments  │  │
│  │  Referential Integrity: Claim→Claimant→Policy relationships           │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                              │               │
│                                                              ▼               │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         GOLD LAYER (Future)                           │  │
│  │                    Aggregations, Metrics, Reports                     │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Component Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Curation Framework Components                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────┐     ┌─────────────────────────────────────────┐   │
│  │   Configuration     │     │           BatchFrameworkOrchestrator     │   │
│  │   (JSON Metadata)   │────▶│   - Loads configuration                  │   │
│  │                     │     │   - Iterates through tables              │   │
│  │ - tables_config.json│     │   - Manages execution flow               │   │
│  │ - SQL transforms    │     │   - Generates summary reports            │   │
│  └─────────────────────┘     └────────────────┬────────────────────────┘   │
│                                               │                             │
│                                               ▼                             │
│                              ┌─────────────────────────────────────────┐   │
│                              │           SilverProcessor               │   │
│                              │   - get_high_watermark()                │   │
│                              │   - read_incremental_source()           │   │
│                              │   - apply_transformation()              │   │
│                              │   - process_scd_type1()                 │   │
│                              │   - process_scd_type2()                 │   │
│                              └─────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 3. Core Design Patterns

### 3.1 High-Watermark Incremental Processing

The framework uses a **Control Table** pattern with a **Lookback Window** to handle late-arriving data and ensure robustness.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    High-Watermark Processing Flow                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Step 1: Get Last Processed Timestamp                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  SELECT watermark_value FROM silver.control_table                    │    │
│  │  WHERE table_name = 'silver.customers'                               │    │
│  │  Result: 2024-01-15 10:30:00 (Source Ingestion TS)                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  Step 2: Filter Source Table with Lookback                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  SELECT * FROM bronze.streaming_table                                │    │
│  │  WHERE ingestion_ts > ('2024-01-15 10:30:00' - INTERVAL 2 HOURS)     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  Step 3: Process, Deduplicate & Atomic Update                                │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  1. Deduplicate: Filter out records already processed (by Key + TS)  │    │
│  │  2. Apply transformations and merge into target                      │    │
│  │  3. COMMIT Transaction                                               │    │
│  │  4. UPDATE silver.control_table SET watermark = new_max_ts           │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key Features:**
- **Lookback Window**: Mitigates risk of late-arriving data by re-scanning a safety buffer (configurable, default 2 hours).
- **Failure-Safe Ordering**: Merge executes **first**; watermark update executes **second**. If merge succeeds but watermark update fails, the next run reprocesses the lookback window safely.
- **Idempotency Guarantee**: Because of the lookback + dedup, reprocessing is always safe and produces the same result.

**Deduplication Specification:**
- **Dedup Key**: `(business_key_columns, source_system)` — One row per entity per batch.
- **Ordering**: `ORDER BY ingestion_ts DESC, _kafka_offset DESC` (or configured tiebreaker)
- **Rule**: Keep the **first** record per dedup key after ordering (latest by time, stable by offset).
- **Mandatory Step**: This reduction to one row per entity **must** occur before SCD merge.

### 3.2 SCD Type 1 (Upsert Pattern)

SCD Type 1 maintains only the current state of records by overwriting changes.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SCD Type 1 - Upsert Pattern                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Incoming Data:                    Target Table (Before):                    │
│  ┌────────────────────┐           ┌────────────────────────────────┐        │
│  │ id │ name  │ email │           │ id │ name │ email              │        │
│  ├────┼───────┼───────┤           ├────┼──────┼────────────────────┤        │
│  │ 1  │ John  │ new@  │           │ 1  │ John │ old@example.com    │        │
│  │ 3  │ Alice │ ali@  │           │ 2  │ Jane │ jane@example.com   │        │
│  └────────────────────┘           └────────────────────────────────┘        │
│           │                                    │                             │
│           └──────────────┬─────────────────────┘                             │
│                          ▼                                                   │
│                   MERGE Operation                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  MERGE INTO target USING source ON target.id = source.id            │    │
│  │  WHEN MATCHED THEN UPDATE SET *                                      │    │
│  │  WHEN NOT MATCHED THEN INSERT *                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                          │                                                   │
│                          ▼                                                   │
│  Target Table (After):                                                       │
│  ┌────────────────────────────────┐                                         │
│  │ id │ name  │ email             │                                         │
│  ├────┼───────┼───────────────────┤                                         │
│  │ 1  │ John  │ new@example.com   │  ← Updated                              │
│  │ 2  │ Jane  │ jane@example.com  │  ← Unchanged                            │
│  │ 3  │ Alice │ ali@example.com   │  ← Inserted                             │
│  └────────────────────────────────┘                                         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 SCD Type 2 Strategy (Hash-Based)

This framework uses a **Hash-Based SCD Type 2** approach for performance and determinism.

#### 3.3.1 Hash Key Concepts

Two internal hash columns are generated to simplify logic:

1.  **Entity Hash (`_pk_hash`)**:
    - **Purpose**: Uniquely identifies the entity across time. Used as the **Merge Key**.
    - **Formula**: `MD5(CONCAT_WS('|', business_key_1, business_key_n, source_system))`
    - **Benefit**: Replaces complex multi-column join conditions with a single string comparison.

2.  **Change Hash (`_diff_hash`)**:
    - **Purpose**: Detects if any business value has changed.
    - **Formula**: `MD5(CONCAT_WS('|', col1, col2, col3...))` (all `track_columns`)
    - **Benefit**: Avoids expensive column-by-column comparisons; handles nulls consistently.

#### 3.3.4 Hash Canonicalization Rules

To ensure deterministic hashing across runs and Spark versions:

| Data Type | Canonicalization Rule |
|-----------|-----------------------|
| `NULL` | Coalesce to literal string `"__NULL__"` |
| `STRING` | `TRIM()` to remove leading/trailing whitespace |
| `TIMESTAMP` | Cast to `STRING` using ISO format `yyyy-MM-dd HH:mm:ss.SSSSSS` |
| `DECIMAL` | Cast to `STRING` with fixed scale (e.g., `CAST(col AS DECIMAL(18,6))`) |
| `BOOLEAN` | Cast to `STRING` (`"true"` / `"false"`) |

#### 3.3.2 Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `business_key_columns` | Columns defining the entity identity. | `["claim_id"]` |
| `track_columns` | Columns to monitor for history. | `["status", "amount", "diagnosis_code"]` |
| `scd2_columns` | Metadata columns for versioning. | `{"start": "eff_start", "end": "eff_end", "curr": "is_current"}` |
| `source_system_col` | Column identifying data origin. | `"source_system"` |

#### 3.3.3 Logic Flow Diagram

**Pre-Merge Steps (Mandatory):**
1. **Batch Reduction**: Deduplicate incoming data to one row per `(business_key, source_system)` using configured ordering.
2. **Target Filtering (Semi-Join)**: Filter target's current rows to only keys present in incoming batch to avoid full table scan.

```
┌──────────────────────┐          ┌──────────────────────┐
│    Incoming Batch    │          │    Target Table      │
│ (Bronze + Lookback)  │          │   (Silver Delta)     │
└──────────┬───────────┘          └──────────┬───────────┘
           │                                 │
           ▼                                 ▼
   ┌────────────────┐               ┌────────────────┐
   │ Generate Hashes│               │ Filter Current │
   │ - _pk_hash     │               │ WHERE is_curr  │
   │ - _diff_hash   │               └────────┬───────┘
   └───────┬────────┘                        │
           │                                 │
           ▼                                 ▼
   ┌─────────────────────────────────────────────────┐
   │              FULL OUTER JOIN                    │
   │              ON a._pk_hash = b._pk_hash         │
   └───────────────────────┬─────────────────────────┘
                           │
           ┌───────────────┴────────────────┐
           ▼                                ▼
    [ Match Found ]                  [ No Match ]
           │                                │
    Check _diff_hash                        │
           │                                ▼
    ┌──────┴───────┐                 ┌──────────────┐
    ▼              ▼                 │  New Entity  │
 [Different]    [Same]               │   (INSERT)   │
    │              │                 └──────────────┘
    ▼              ▼
 ┌──────┐      ┌──────┐
 │Update│      │Ignore│
 └──────┘      └──────┘
    │
    ▼
 1. Close Old (UPDATE target SET end = now, curr = false)
 2. Insert New (INSERT values ..., start = now, curr = true)
```

**Key Design Decisions:**
- **Hash-Based Merging**: The merge condition uses `_pk_hash` for performance.
- **No Surrogate Keys in Merge**: Surrogate keys are strictly forbidden in merge conditions. They are derived columns for downstream use only.
- **Batch Granularity**: The framework captures the **latest state per batch interval** (e.g., hourly). Intermediate changes within the same hour are collapsed.
- **Deterministic Change Detection**: Uses `_diff_hash` to reliably detect changes.

## 4. Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           End-to-End Data Flow                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐                                                             │
│  │   Start     │                                                             │
│  └──────┬──────┘                                                             │
│         │                                                                    │
│         ▼                                                                    │
│  ┌─────────────────────────────────┐                                        │
│  │  Load Configuration (JSON)      │                                        │
│  │  - Table definitions            │                                        │
│  │  - SCD type, keys, columns      │                                        │
│  └──────────────┬──────────────────┘                                        │
│                 │                                                            │
│                 ▼                                                            │
│  ┌─────────────────────────────────┐     ┌─────────────────────────────┐   │
│  │  Get High Watermark from        │────▶│  Initial Load?              │   │
│  │  Target Table                   │     │  (watermark = NULL)         │   │
│  └──────────────┬──────────────────┘     └─────────────┬───────────────┘   │
│                 │                                       │                    │
│                 ▼                                       ▼                    │
│  ┌─────────────────────────────────┐     ┌─────────────────────────────┐   │
│  │  Read Source with Filter        │     │  Read Full Source           │   │
│  │  WHERE ts > watermark           │     │  (No filter)                │   │
│  └──────────────┬──────────────────┘     └─────────────┬───────────────┘   │
│                 │                                       │                    │
│                 └───────────────┬───────────────────────┘                    │
│                                 │                                            │
│                                 ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Register as Temp View: source_incremental                          │    │
│  └──────────────────────────────────┬──────────────────────────────────┘    │
│                                     │                                        │
│                                     ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Apply SQL Transformation (from configured SQL file)                 │    │
│  │  - Data cleansing                                                    │    │
│  │  - Type casting                                                      │    │
│  │  - Business logic                                                    │    │
│  └──────────────────────────────────┬──────────────────────────────────┘    │
│                                     │                                        │
│                                     ▼                                        │
│                    ┌────────────────┴────────────────┐                      │
│                    │         SCD Type?               │                      │
│                    └────────────────┬────────────────┘                      │
│                          │                   │                               │
│                     SCD 1                  SCD 2                            │
│                          │                   │                               │
│                          ▼                   ▼                               │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────────┐  │
│  │  MERGE (Upsert)             │  │  1. Identify New Records            │  │
│  │  - Match on business keys   │  │  2. Identify Changed Records        │  │
│  │  - Update if matched        │  │  3. Close Old Versions (UPDATE)     │  │
│  │  - Insert if not matched    │  │  4. Insert New Versions (APPEND)    │  │
│  └──────────────┬──────────────┘  └──────────────────┬──────────────────┘  │
│                 │                                     │                      │
│                 └─────────────────┬───────────────────┘                      │
│                                   │                                          │
│                                   ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Return Processing Results                                           │    │
│  │  - Records processed                                                 │    │
│  │  - Duration                                                          │    │
│  │  - Status                                                            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 5. Configuration Schema

### 5.1 Table Configuration

```json
{
  "table_name": "silver_customers",
  "source_table": "bronze.customers_streaming",
  "target_table": "silver.customers",
  "scd_type": 2,
  "business_key_columns": ["customer_id"],
  "source_system_column": "source_system",
  "watermark_column": "ingestion_ts",
  "lookback_interval": "2 HOURS",
  "dedup_order_columns": ["ingestion_ts DESC", "_kafka_offset DESC"],
  "scd2_columns": {
    "effective_start_date": "effective_start_date",
    "effective_end_date": "effective_end_date",
    "is_current": "is_current"
  },
  "track_columns": ["name", "email", "address"],
  "transformation_sql_path": "${workspace.file_path}/conf/sql/customers_transform.sql",
  "enabled": true
}
```

### 5.2 Global Settings

```json
{
  "catalog": "main",
  "schema_bronze": "bronze",
  "schema_silver": "silver",
  "control_table": "silver.curation_control",
  "audit_table": "silver.curation_audit_log",
  "default_watermark_column": "ingestion_ts",
  "default_lookback_interval": "2 HOURS",
  "default_dedup_order_columns": ["ingestion_ts DESC"],
  "scd2_end_date_value": "9999-12-31 23:59:59",
  "workspace_file_path": "${workspace.root_path}/files",
  "log_level": "INFO"
}
```

### 5.3 Control Table Schema

```sql
CREATE TABLE IF NOT EXISTS silver.curation_control (
  table_name        STRING NOT NULL,
  watermark_value   TIMESTAMP,
  last_run_id       STRING,
  last_run_status   STRING,
  records_processed BIGINT,
  updated_at        TIMESTAMP,
  CONSTRAINT pk_control PRIMARY KEY (table_name)
);
```

### 5.4 Audit Log Table Schema

```sql
CREATE TABLE IF NOT EXISTS silver.curation_audit_log (
  run_id            STRING NOT NULL,
  table_name        STRING NOT NULL,
  run_start_ts      TIMESTAMP,
  run_end_ts        TIMESTAMP,
  status            STRING,
  records_read      BIGINT,
  records_inserted  BIGINT,
  records_updated   BIGINT,
  watermark_before  TIMESTAMP,
  watermark_after   TIMESTAMP,
  error_message     STRING,
  CONSTRAINT pk_audit PRIMARY KEY (run_id, table_name)
);
```
## 6. Deployment Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Databricks Asset Bundles Deployment                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Local Development                    Databricks Workspace                   │
│  ┌─────────────────────┐             ┌─────────────────────────────────┐    │
│  │  Source Code        │             │  Jobs                           │    │
│  │  ├── src/           │   deploy    │  ├── curation_framework_job     │    │
│  │  ├── conf/          │ ──────────▶ │  │   (Scheduled: Hourly)        │    │
│  │  ├── resources/     │             │  └── single_table_job           │    │
│  │  └── tests/         │             │      (On-demand)                │    │
│  └─────────────────────┘             └─────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────┐             ┌─────────────────────────────────┐    │
│  │  databricks.yml     │             │  Artifacts                      │    │
│  │  - Bundle config    │   deploy    │  ├── Python Wheel (.whl)        │    │
│  │  - Target envs      │ ──────────▶ │  └── Workspace Files            │    │
│  └─────────────────────┘             │      ├── conf/tables_config.json│    │
│                                      │      └── conf/sql/*.sql         │    │
│                                      │                                 │    │
│                                      │  Path: ${workspace.file_path}   │    │
│                                      │  Access: file:/Workspace/...    │    │
│                                      └─────────────────────────────────┘    │
│                                                                              │
│  Targets:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  dev:  [dev username] prefixed, paused schedules                    │    │
│  │  prod: Production deployment, active schedules                       │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```───────────────────────────────────────────────────────────────────────────┘
```

## 7. Error Handling Strategy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Error Handling Flow                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        Table Processing                              │    │
│  │                              │                                       │    │
│  │              ┌───────────────┴───────────────┐                      │    │
│  │              │         Try/Except            │                      │    │
│  │              └───────────────┬───────────────┘                      │    │
│  │                    │                   │                             │    │
│  │               Success              Exception                         │    │
│  │                    │                   │                             │    │
│  │  ┌─────────────────────┐  ┌─────────────────────────────────────┐   │    │
│  │  │ Log success         │  │ Log error with details              │   │    │
│  │  │ Update Control Tbl  │  │ Mark table as failed                │   │    │
│  │  │ Insert Audit Log    │  │ Insert Audit Log (Failed)           │   │    │
│  │  └─────────────────────┘  │ (Isolation - don't fail entire job) │   │    │
│  │                           └─────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         Job Completion                               │    │
│  │  - Generate summary report                                           │    │
│  │  - List failed tables                                                │    │
│  │  - Exit with appropriate code (0 = all success, 1 = any failure)    │    │
│  │  - Send email notification if configured                             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 8. Technology Stack

| Component | Technology |
|-----------|------------|
| Compute | Databricks Serverless / Job Clusters |
| Processing | Apache Spark (PySpark) |
| Storage | Delta Lake |
| Configuration | JSON + SQL files |
| Deployment | Databricks Asset Bundles (DAB) |
| Scheduling | Databricks Workflows |
| Package Management | UV (Python) |
| Testing | pytest + Databricks Connect |

## 9. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Batch over Streaming | Predictable costs, easier debugging, no checkpoint management |
| High-Watermark | Efficient incremental processing without scanning full tables |
| No Surrogate Keys | Simpler schema, rely on natural business keys |
| SQL Transformations | Familiar syntax, easy to modify without code changes |
| JSON Configuration | Human-readable, version-controllable, easy to extend |
| Per-Table Isolation | One table failure doesn't affect others |
| Delta Lake | ACID transactions, time travel, schema evolution |
