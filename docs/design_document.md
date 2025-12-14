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
│  Step 3: Process, Deduplicate & Failure-Safe Update                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  1. Deduplicate: Reduce to one row per (business_key, source_system) │    │
│  │  2. Cache/Persist prepared DataFrame (deterministic source)         │    │
│  │  3. Apply transformations and merge into target                      │    │
│  │  4. COMMIT Merge                                                     │    │
│  │  5. UPDATE silver.control_table SET watermark = new_max_ts           │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key Features:**
- **Lookback Window**: Mitigates risk of late-arriving data by re-scanning a safety buffer (configurable, default 2 hours).
- **Failure-Safe Ordering**: Merge executes **first**; watermark update **second**. If merge succeeds but watermark update fails, next run reprocesses safely.
- **Idempotency Guarantee**: Lookback + entity-level dedup ensures reprocessing produces identical results.
- **Deterministic Source**: The prepared DataFrame is cached/persisted before MERGE to prevent non-deterministic re-reads.

**Watermark Definition:**
- `watermark_after` = `MAX(ingestion_ts)` among rows successfully merged in the current batch.
- **Monotonic Rule**: Watermark never decreases. If a batch produces a lower max, watermark is not updated.

**Deduplication Specification:**
- **Dedup Key**: `(business_key_columns, source_system)` — One row per entity per batch.
- **Ordering**: `ORDER BY ingestion_ts DESC, <tiebreaker> DESC` (configured via `dedup_order_columns`)
- **Rule**: Keep the **first** record per dedup key after ordering (latest by time, stable by tiebreaker).
- **Mandatory Step**: This reduction to one row per entity **must** occur before SCD merge.

**Tiebreaker Fallback:**
- If `_kafka_offset` is available in Bronze, use it as the primary tiebreaker.
- If not available, configure `dedup_order_columns: ["ingestion_ts DESC"]` only.
- When no tiebreaker exists and timestamps collide, behavior is non-deterministic but stable across reruns (same row selected).

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
    - **Formula**: `SHA2(CONCAT_WS('|', business_key_1, business_key_n, source_system), 256)`
    - **Benefit**: Replaces complex multi-column join conditions with a single string comparison.

2.  **Change Hash (`_diff_hash`)**:
    - **Purpose**: Detects if any business value has changed.
    - **Formula**: `SHA2(CONCAT_WS('|', col1, col2, col3...), 256)` (all `track_columns`)
    - **Benefit**: Avoids expensive column-by-column comparisons; handles nulls consistently.
    - **Algorithm**: SHA-256 chosen over MD5 for collision resistance in regulated domains.

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
│  │  - Timezone conversion (parameterized)                               │    │
│  │  - Reference data lookups (JOIN to ref tables)                       │    │
│  │  - Data cleansing & type casting                                     │    │
│  │  - Business logic                                                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
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

### 5.0 Parameterized SQL Transformations

All transformations are implemented in **pure Spark SQL** (no Python UDFs). Parameters are injected into SQL at runtime using variable substitution.

#### 5.0.1 Timezone Conversion

Source data may arrive in UTC or source-system-local time. The framework supports parameterized timezone conversion.

**Configuration:**
```json
{
  "timezone_config": {
    "source_timezone": "UTC",
    "target_timezone": "America/New_York",
    "timestamp_columns": ["event_ts", "created_at", "updated_at"]
  }
}
```

**SQL Pattern:**
```sql
SELECT
  claim_id,
  -- Convert from source timezone to target timezone
  FROM_UTC_TIMESTAMP(
    TO_UTC_TIMESTAMP(event_ts, '${source_timezone}'),
    '${target_timezone}'
  ) AS event_ts,
  -- Or if source is already UTC:
  FROM_UTC_TIMESTAMP(event_ts, '${target_timezone}') AS event_ts_local
FROM source_incremental
```

**Supported Functions (Native Spark SQL):**
| Function | Purpose |
|----------|---------||
| `TO_UTC_TIMESTAMP(ts, tz)` | Convert local time to UTC |
| `FROM_UTC_TIMESTAMP(ts, tz)` | Convert UTC to local time |
| `CONVERT_TIMEZONE(from_tz, to_tz, ts)` | Direct conversion (Databricks SQL) |

#### 5.0.2 Reference Data Lookups

Enrich source data by joining to reference/dimension tables. Reference tables are registered as temp views before transformation SQL executes.

**Configuration:**
```json
{
  "reference_tables": [
    {
      "alias": "ref_providers",
      "table": "silver.dim_providers",
      "filter": "is_current = true"
    },
    {
      "alias": "ref_icd_codes",
      "table": "silver.dim_icd_codes",
      "filter": null
    }
  ]
}
```

**SQL Pattern:**
```sql
SELECT
  s.claim_id,
  s.provider_id,
  p.provider_name,
  p.provider_npi,
  s.diagnosis_code,
  icd.diagnosis_description,
  icd.diagnosis_category
FROM source_incremental s
LEFT JOIN ref_providers p
  ON s.provider_id = p.provider_id
LEFT JOIN ref_icd_codes icd
  ON s.diagnosis_code = icd.icd_code
```

**Design Principles:**
- **No Python UDFs**: All logic in native Spark SQL for performance and maintainability.
- **LEFT JOIN Default**: Reference lookups use LEFT JOIN to avoid dropping records when reference data is missing.
- **Current Records Only**: Dimension tables filtered to `is_current = true` for SCD2 dimensions.
- **Broadcast Hint**: Small reference tables can use `/*+ BROADCAST(ref_providers) */` for performance.

#### 5.0.3 SQL Variable Substitution

The framework replaces `${variable_name}` placeholders in SQL files with values from configuration at runtime.

**Available Variables:**
| Variable | Source | Example |
|----------|--------|---------||
| `${source_timezone}` | Table config | `"UTC"` |
| `${target_timezone}` | Table config | `"America/New_York"` |
| `${catalog}` | Global settings | `"main"` |
| `${schema_silver}` | Global settings | `"silver"` |
| `${processing_date}` | Runtime | `"2024-01-15"` |

#### 5.0.4 Complete Transformation Example

This section demonstrates a complete end-to-end transformation for the `Claims` entity.

**Step 1: Base Source Query (Before Enrichment)**

```sql
-- conf/sql/claims_transform_base.sql
-- Raw query without parameterization
SELECT
  claim_id,
  claimant_id,
  provider_id,
  policy_id,
  diagnosis_code,
  claim_amount,
  claim_status,
  service_date,
  submitted_at,
  source_system,
  ingestion_ts
FROM source_incremental
```

**Step 2: Enriched Query with All Transformation Parameters**

```sql
-- conf/sql/claims_transform.sql
-- Full transformation with timezone, reference data, and SCD2 preparation

WITH deduplicated AS (
  -- Step 1: Intra-batch deduplication (one row per entity per batch)
  SELECT *
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (
        PARTITION BY claim_id, source_system 
        ORDER BY ingestion_ts DESC
      ) AS _row_num
    FROM source_incremental
  )
  WHERE _row_num = 1
),

transformed AS (
  SELECT
    -- Business Keys
    s.claim_id,
    s.source_system,
    
    -- Foreign Keys
    s.claimant_id,
    s.provider_id,
    s.policy_id,
    
    -- Timezone Conversion: UTC -> Target Timezone
    FROM_UTC_TIMESTAMP(s.service_date, '${target_timezone}') AS service_date,
    FROM_UTC_TIMESTAMP(s.submitted_at, '${target_timezone}') AS submitted_at,
    
    -- Reference Data Enrichment: Provider
    p.provider_name,
    p.provider_npi,
    p.provider_type,
    
    -- Reference Data Enrichment: ICD Codes
    s.diagnosis_code,
    icd.diagnosis_description,
    icd.diagnosis_category,
    icd.is_chronic,
    
    -- Reference Data Enrichment: Claim Status
    s.claim_status AS claim_status_code,
    cs.status_description AS claim_status_desc,
    
    -- Business Logic: Computed Fields
    s.claim_amount,
    CASE 
      WHEN s.claim_amount > 10000 THEN 'HIGH'
      WHEN s.claim_amount > 1000 THEN 'MEDIUM'
      ELSE 'LOW'
    END AS claim_tier,
    
    -- Data Quality: Cleansing
    TRIM(UPPER(s.claim_status)) AS claim_status_clean,
    COALESCE(s.claim_amount, 0) AS claim_amount_safe,
    
    -- Hash Keys for SCD2
    SHA2(CONCAT_WS('|', 
      COALESCE(CAST(s.claim_id AS STRING), '__NULL__'),
      COALESCE(s.source_system, '__NULL__')
    ), 256) AS _pk_hash,
    
    SHA2(CONCAT_WS('|',
      COALESCE(s.claim_status, '__NULL__'),
      COALESCE(CAST(s.claim_amount AS STRING), '__NULL__'),
      COALESCE(s.diagnosis_code, '__NULL__'),
      COALESCE(CAST(s.provider_id AS STRING), '__NULL__')
    ), 256) AS _diff_hash,
    
    -- Metadata
    s.ingestion_ts,
    CURRENT_TIMESTAMP() AS processing_timestamp
    
  FROM deduplicated s
  
  -- Reference Data JOINs
  LEFT JOIN /*+ BROADCAST(ref_providers) */ ref_providers p
    ON s.provider_id = p.provider_id
    AND p.is_current = true
    
  LEFT JOIN /*+ BROADCAST(ref_icd_codes) */ ref_icd_codes icd
    ON s.diagnosis_code = icd.icd_code
    
  LEFT JOIN /*+ BROADCAST(ref_claim_status) */ ref_claim_status cs
    ON TRIM(UPPER(s.claim_status)) = cs.status_code
)

SELECT * FROM transformed
```

**Step 3: How SCD Type 2 Uses This Query**

The framework takes the output of the transformation SQL and applies SCD2 logic:

```sql
-- Framework-generated SCD2 MERGE (not user-written)
-- This shows conceptually how the transformed data feeds into SCD2

-- 1. Get current records from target (semi-join optimized)
CREATE OR REPLACE TEMP VIEW target_current AS
SELECT * FROM ${target_table}
WHERE is_current = true
  AND _pk_hash IN (SELECT _pk_hash FROM transformed_cached);

-- 2. Identify changes by comparing _diff_hash
CREATE OR REPLACE TEMP VIEW changes AS
SELECT 
  src.*,
  CASE 
    WHEN tgt._pk_hash IS NULL THEN 'INSERT'           -- New entity
    WHEN src._diff_hash != tgt._diff_hash THEN 'UPDATE' -- Changed
    ELSE 'NO_CHANGE'
  END AS _action
FROM transformed_cached src
LEFT JOIN target_current tgt
  ON src._pk_hash = tgt._pk_hash;

-- 3. Close old versions (UPDATE)
MERGE INTO ${target_table} AS tgt
USING (SELECT * FROM changes WHERE _action = 'UPDATE') AS src
ON tgt._pk_hash = src._pk_hash AND tgt.is_current = true
WHEN MATCHED THEN UPDATE SET
  tgt.effective_end_date = CURRENT_TIMESTAMP(),
  tgt.is_current = false;

-- 4. Insert new versions
INSERT INTO ${target_table}
SELECT 
  *,
  CURRENT_TIMESTAMP() AS effective_start_date,
  CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS effective_end_date,
  true AS is_current
FROM changes
WHERE _action IN ('INSERT', 'UPDATE');
```

**Transformation Parameter Summary:**

| Parameter Type | Example in Query | Config Source |
|----------------|------------------|---------------|
| Timezone | `FROM_UTC_TIMESTAMP(..., '${target_timezone}')` | `timezone_config` |
| Reference Table | `LEFT JOIN ref_providers` | `reference_tables` |
| Hash Algorithm | `SHA2(..., 256)` | Framework default |
| Dedup Logic | `ROW_NUMBER() OVER (PARTITION BY ...)` | `dedup_order_columns` |
| Track Columns | Included in `_diff_hash` | `track_columns` |

### 5.1 Table Configuration

```json
{
  "table_name": "silver_customers",
  "source_table": "bronze.customers_streaming",
  "target_table": "silver.customers",
  "sub_domain": "party",
  "sequence": 1,
  "critical": true,
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
  "timezone_config": {
    "source_timezone": "UTC",
    "target_timezone": "America/New_York",
    "timestamp_columns": ["created_at", "updated_at"]
  },
  "reference_tables": [
    {
      "alias": "ref_states",
      "table": "silver.dim_states",
      "filter": null,
      "broadcast": true,
      "max_rows": 10000
    }
  ],
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
  "default_source_timezone": "UTC",
  "default_target_timezone": "America/New_York",
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

**Constraint Enforcement Note:**
- Delta Lake treats PRIMARY KEY as informational (not strictly enforced like RDBMS).
- Application-level guarantee: Use `MERGE INTO control_table` keyed by `table_name` to ensure upsert semantics.
- Periodic validation: Schedule a check for duplicate `table_name` entries.

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
```

## 7. Orchestration Strategy

### 7.1 Hierarchical Execution Model

The framework uses a **Sub-Domain Ordered** orchestration strategy where tables are loaded in a sequence that respects data model dependencies.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Orchestration Hierarchy                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  WORKFLOW (Databricks Job)                                                   │
│  └── Scheduled: Hourly                                                      │
│  └── Scope: All Sub-Domains                                                 │
│                                                                              │
│       ┌─────────────────────┐                                               │
│       │   SUB-DOMAIN 1    │   (e.g., Party: Claimants, Providers)            │
│       │   load_order: 1   │                                                  │
│       └─────────────────────┘                                               │
│               │                                                              │
│       ┌───────┴───────┬───────────────┐                                     │
│       │               │               │  Tables within sub-domain           │
│       ▼               ▼               ▼  run in data model sequence          │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐                               │
│  │ Pipeline │   │ Pipeline │   │ Pipeline │  Each pipeline = 1 table       │
│  │ Claimant │──▶│ Provider │──▶│ Policy   │                               │
│  │ seq: 1   │   │ seq: 2   │   │ seq: 3   │                               │
│  └──────────┘   └──────────┘   └──────────┘                               │
│               │                                                              │
│               ▼  (After Sub-Domain 1 completes)                              │
│       ┌─────────────────────┐                                               │
│       │   SUB-DOMAIN 2    │   (e.g., Claims: Claims, Claim Lines)            │
│       │   load_order: 2   │                                                  │
│       └─────────────────────┘                                               │
│               │                                                              │
│       ┌───────┴───────┬───────────────┐                                     │
│       │               │               │                                      │
│       ▼               ▼               ▼                                      │
│  ┌──────────┐   ┌──────────┐   ┌────────────┐                             │
│  │ Pipeline │   │ Pipeline │   │ Pipeline   │                             │
│  │ Claims   │──▶│ ClaimLine│──▶│ ClaimDiag  │                             │
│  │ seq: 1   │   │ seq: 2   │   │ seq: 3     │                             │
│  └──────────┘   └──────────┘   └────────────┘                             │
│               │                                                              │
│               ▼  (After Sub-Domain 2 completes)                              │
│       ┌─────────────────────┐                                               │
│       │   SUB-DOMAIN 3    │   (e.g., Financials: Payments, Adjustments)      │
│       │   load_order: 3   │                                                  │
│       └─────────────────────┘                                               │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Configuration Structure

**Sub-Domain Configuration:**
```json
{
  "sub_domains": [
    {
      "name": "party",
      "description": "Party entities (Claimants, Providers)",
      "load_order": 1,
      "tables": [
        { "table_name": "silver_claimants", "sequence": 1 },
        { "table_name": "silver_providers", "sequence": 2 },
        { "table_name": "silver_policies", "sequence": 3 }
      ]
    },
    {
      "name": "claims",
      "description": "Claims processing entities",
      "load_order": 2,
      "tables": [
        { "table_name": "silver_claims", "sequence": 1 },
        { "table_name": "silver_claim_lines", "sequence": 2 },
        { "table_name": "silver_claim_diagnoses", "sequence": 3 }
      ]
    },
    {
      "name": "financials",
      "description": "Financial transactions",
      "load_order": 3,
      "tables": [
        { "table_name": "silver_payments", "sequence": 1 },
        { "table_name": "silver_adjustments", "sequence": 2 }
      ]
    }
  ]
}
```

### 7.3 Execution Rules

| Rule | Description |
|------|-------------|
| **Sub-Domain Ordering** | Sub-domains execute in `load_order` sequence (1, 2, 3...) |
| **Table Sequencing** | Tables within a sub-domain execute in `sequence` order |
| **Pipeline Isolation** | Each pipeline loads exactly **one target table** |
| **Dependency Wait** | Sub-domain N+1 starts only after Sub-domain N completes successfully |
| **Parallel Within Domain** | Tables with same `sequence` can run in parallel (optional) |
| **Critical Table Failure** | If `critical: true` table fails, sub-domain execution halts |
| **Non-Critical Failure** | If `critical: false` table fails, log error and continue |

### 7.4 DAB Job Definition

```yaml
# resources/curation_workflow.job.yml
resources:
  jobs:
    ltc_curation_workflow:
      name: "LTC Claims Curation Workflow"
      schedule:
        quartz_cron_expression: "0 0 * * * ?"  # Hourly
        timezone_id: "America/New_York"
      
      tasks:
        # Sub-Domain 1: Party
        - task_key: "party_claimants"
          spark_python_task:
            python_file: "${workspace.file_path}/src/pipeline.py"
            parameters:
              - "--table_name=silver_claimants"
        
        - task_key: "party_providers"
          depends_on:
            - task_key: "party_claimants"
          spark_python_task:
            python_file: "${workspace.file_path}/src/pipeline.py"
            parameters:
              - "--table_name=silver_providers"
        
        - task_key: "party_policies"
          depends_on:
            - task_key: "party_providers"
          spark_python_task:
            python_file: "${workspace.file_path}/src/pipeline.py"
            parameters:
              - "--table_name=silver_policies"
        
        # Sub-Domain 2: Claims (depends on Party completion)
        - task_key: "claims_claims"
          depends_on:
            - task_key: "party_policies"
          spark_python_task:
            python_file: "${workspace.file_path}/src/pipeline.py"
            parameters:
              - "--table_name=silver_claims"
        
        - task_key: "claims_claim_lines"
          depends_on:
            - task_key: "claims_claims"
          spark_python_task:
            python_file: "${workspace.file_path}/src/pipeline.py"
            parameters:
              - "--table_name=silver_claim_lines"
        
        # Sub-Domain 3: Financials (depends on Claims completion)
        - task_key: "financials_payments"
          depends_on:
            - task_key: "claims_claim_lines"
          spark_python_task:
            python_file: "${workspace.file_path}/src/pipeline.py"
            parameters:
              - "--table_name=silver_payments"
```

### 7.5 LTC Claims Data Model Sequence

| Sub-Domain | Sequence | Entity | Depends On | SCD Type | Critical |
|------------|----------|--------|------------|----------|----------|
| Party | 1 | Claimants | - | SCD2 | Yes |
| Party | 2 | Providers | - | SCD2 | Yes |
| Party | 3 | Policies | Claimants | SCD2 | Yes |
| Claims | 1 | Claims | Claimants, Policies, Providers | SCD1 | Yes |
| Claims | 2 | Claim Lines | Claims | SCD1 | No |
| Claims | 3 | Claim Diagnoses | Claims | SCD1 | No |
| Claims | 4 | Assessments | Claimants | SCD2 | No |
| Financials | 1 | Payments | Claims | SCD1 | Yes |
| Financials | 2 | Adjustments | Payments | SCD1 | No |

## 8. Error Handling Strategy

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

## 9. Technology Stack

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

## 10. Code Hierarchy (Modular Architecture)

The framework follows a **layered modular architecture** with clear separation of concerns. Each module has a single responsibility and well-defined interfaces.

### 10.1 Package Structure

```
src/curation_framework/
├── __init__.py                    # Package exports & public API
├── main.py                        # Entry point (CLI & Job execution)
│
├── core/                          # Core Processing Modules
│   ├── __init__.py
│   ├── processor.py               # Abstract base processor
│   ├── scd1_processor.py          # SCD Type 1 merge logic
│   ├── scd2_processor.py          # SCD Type 2 hash-based merge
│   └── dedup.py                   # Entity-level deduplication
│
├── io/                            # Input/Output Modules
│   ├── __init__.py
│   ├── reader.py                  # Incremental source reader
│   ├── writer.py                  # Delta table writer
│   └── control_table.py           # Watermark control table manager
│
├── transform/                     # Transformation Modules
│   ├── __init__.py
│   ├── sql_engine.py              # SQL file executor with variable substitution
│   ├── hash_generator.py          # SHA-256 _pk_hash & _diff_hash
│   └── reference_loader.py        # Reference table registration
│
├── orchestration/                 # Orchestration Modules
│   ├── __init__.py
│   ├── orchestrator.py            # Batch orchestration logic
│   ├── dependency_resolver.py     # Sub-domain ordering
│   └── parallel_executor.py       # Parallel table processing
│
├── config/                        # Configuration Modules
│   ├── __init__.py
│   ├── loader.py                  # JSON config loading
│   ├── validator.py               # Schema validation
│   └── schema.py                  # Pydantic/dataclass schemas
│
├── observability/                 # Monitoring & Logging Modules
│   ├── __init__.py
│   ├── logger.py                  # Structured logging
│   ├── metrics.py                 # Processing metrics
│   └── audit.py                   # Audit log writer
│
└── utils/                         # Shared Utilities
    ├── __init__.py
    ├── spark_utils.py             # SparkSession helpers
    ├── delta_utils.py             # Delta Lake operations
    └── datetime_utils.py          # Timezone & timestamp helpers
```

### 10.2 Module Responsibilities

#### 10.2.1 Core Layer (`core/`)

| Module | Responsibility |
|--------|---------------|
| `processor.py` | Abstract `BaseProcessor` class defining the processing contract |
| `scd1_processor.py` | Implements SCD Type 1 MERGE (upsert) pattern |
| `scd2_processor.py` | Implements Hash-Based SCD Type 2 with `_pk_hash` and `_diff_hash` |
| `dedup.py` | Entity-level deduplication on `(business_key, source_system)` |

**Key Design**: Processors are stateless and receive all dependencies via constructor injection.

```python
# core/processor.py
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseProcessor(ABC):
    """Abstract base class for SCD processors."""
    
    @abstractmethod
    def process(self, source_df: DataFrame, target_table: str) -> ProcessingResult:
        """Process source DataFrame and merge into target table."""
        pass
```

#### 10.2.2 I/O Layer (`io/`)

| Module | Responsibility |
|--------|---------------|
| `reader.py` | Reads from Bronze with watermark filter + lookback window |
| `writer.py` | Writes to Silver Delta tables with optimized settings |
| `control_table.py` | Manages `curation_control.watermarks` table atomically |

**Key Design**: I/O operations are isolated for testability and potential source abstraction.

```python
# io/reader.py
class IncrementalReader:
    """Reads source data with watermark filtering."""
    
    def read(self, table: str, watermark: datetime, lookback: timedelta) -> DataFrame:
        """Read records where timestamp > (watermark - lookback)."""
        pass
```

#### 10.2.3 Transform Layer (`transform/`)

| Module | Responsibility |
|--------|---------------|
| `sql_engine.py` | Loads SQL files, substitutes `${variables}`, executes against temp views |
| `hash_generator.py` | Generates `_pk_hash` and `_diff_hash` columns using SHA-256 |
| `reference_loader.py` | Registers reference tables as temp views with optional filtering |

**Key Design**: Transformations are pure SQL - no Python UDFs.

```python
# transform/sql_engine.py
class SQLEngine:
    """Executes parameterized SQL transformations."""
    
    def execute(self, sql_path: str, variables: dict, source_view: str) -> DataFrame:
        """Load SQL, substitute variables, execute and return result."""
        pass
```

#### 10.2.4 Orchestration Layer (`orchestration/`)

| Module | Responsibility |
|--------|---------------|
| `orchestrator.py` | Main `BatchOrchestrator` class coordinating all processing |
| `dependency_resolver.py` | Resolves table processing order based on `depends_on` config |
| `parallel_executor.py` | Executes independent tables in parallel using ThreadPoolExecutor |

**Key Design**: Orchestrator is the only module with knowledge of the full pipeline.

```python
# orchestration/orchestrator.py
class BatchOrchestrator:
    """Coordinates batch processing for all configured tables."""
    
    def __init__(self, spark: SparkSession, config_path: str):
        self.reader = IncrementalReader(spark)
        self.sql_engine = SQLEngine(spark)
        self.control_table = ControlTableManager(spark)
        # ... inject other dependencies
    
    def run(self, table_filter: list[str] = None) -> list[ProcessingResult]:
        """Process all tables in dependency order."""
        pass
```

#### 10.2.5 Config Layer (`config/`)

| Module | Responsibility |
|--------|---------------|
| `loader.py` | Loads `tables_config.json` from workspace or local filesystem |
| `validator.py` | Validates config against schema, checks required fields |
| `schema.py` | Defines `TableConfig`, `GlobalSettings` dataclasses/Pydantic models |

**Key Design**: Configuration is validated at load time, failing fast on invalid config.

```python
# config/schema.py
from dataclasses import dataclass
from typing import Optional

@dataclass
class TableConfig:
    table_name: str
    source_table: str
    target_table: str
    scd_type: int
    business_key_columns: list[str]
    watermark_column: str = "ingestion_ts"
    critical: bool = False
    enabled: bool = True
```

#### 10.2.6 Observability Layer (`observability/`)

| Module | Responsibility |
|--------|---------------|
| `logger.py` | Structured logging with JSON format for log analytics |
| `metrics.py` | Collects processing metrics (records, duration, watermarks) |
| `audit.py` | Writes to `curation_control.audit_log` table |

**Key Design**: Observability is cross-cutting but injected, not hard-coded.

```python
# observability/audit.py
class AuditLogger:
    """Writes processing results to audit log table."""
    
    def log_run(self, run_id: str, table_name: str, result: ProcessingResult):
        """Insert audit record for processing run."""
        pass
```

#### 10.2.7 Utils Layer (`utils/`)

| Module | Responsibility |
|--------|---------------|
| `spark_utils.py` | SparkSession creation, dbutils access |
| `delta_utils.py` | OPTIMIZE, VACUUM, table existence checks |
| `datetime_utils.py` | Timezone conversion, timestamp parsing |

### 10.3 Module Dependency Graph

```
                    ┌─────────────────┐
                    │     main.py     │
                    │  (Entry Point)  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  orchestration/ │
                    │   orchestrator  │
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│     config/     │ │       io/       │ │   transform/    │
│  loader, schema │ │ reader, writer  │ │   sql_engine    │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘
         │                   │                   │
         │                   ▼                   │
         │          ┌─────────────────┐          │
         │          │      core/      │◀─────────┘
         │          │ scd1, scd2, dedup│
         │          └────────┬────────┘
         │                   │
         ▼                   ▼
┌─────────────────────────────────────────────────────────┐
│                      utils/                             │
│     spark_utils, delta_utils, datetime_utils            │
└─────────────────────────────────────────────────────────┘
         │                   │
         ▼                   ▼
┌─────────────────────────────────────────────────────────┐
│                   observability/                        │
│           logger, metrics, audit                        │
└─────────────────────────────────────────────────────────┘
```

**Dependency Rules:**
1. **Downward Only**: Higher layers depend on lower layers, never the reverse.
2. **No Circular**: Modules within the same layer do not depend on each other.
3. **Utils at Bottom**: `utils/` and `observability/` are foundational, used everywhere.
4. **Config Isolated**: `config/` only loads data, has no processing logic.

### 10.4 Key Interfaces

```python
# Public API exported from __init__.py
from curation_framework.orchestration import BatchOrchestrator
from curation_framework.core import SCD1Processor, SCD2Processor
from curation_framework.config import TableConfig, load_config
from curation_framework.io import IncrementalReader, ControlTableManager

__all__ = [
    "BatchOrchestrator",
    "SCD1Processor", 
    "SCD2Processor",
    "TableConfig",
    "load_config",
    "IncrementalReader",
    "ControlTableManager",
]
```

### 10.5 Testing Strategy per Module

| Layer | Test Type | Mock Dependencies |
|-------|-----------|-------------------|
| `core/` | Unit tests | Mock DataFrame, DeltaTable |
| `io/` | Integration tests | Spark local, temp Delta tables |
| `transform/` | Unit tests | Mock SQL files, temp views |
| `orchestration/` | Integration tests | Mock all processors |
| `config/` | Unit tests | Test JSON files |
| `observability/` | Unit tests | Mock Spark writes |
| `utils/` | Unit tests | Minimal mocking |

## 11. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Batch over Streaming | Predictable costs, easier debugging, no checkpoint management |
| High-Watermark | Efficient incremental processing without scanning full tables |
| No Surrogate Keys | Simpler schema, rely on natural business keys |
| SQL Transformations | Familiar syntax, easy to modify without code changes |
| JSON Configuration | Human-readable, version-controllable, easy to extend |
| Per-Table Isolation | One table failure doesn't affect others |
| Delta Lake | ACID transactions, time travel, schema evolution |
