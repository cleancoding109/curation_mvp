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
- **Ownership**: Dedup is **framework-driven**, not implemented in developer SQL.

**Tiebreaker Configuration (Required for Determinism):**
- **Recommended**: If `_kafka_offset` is available in Bronze, use it as the primary tiebreaker: `["ingestion_ts DESC", "_kafka_offset DESC"]`
- **Alternative**: Use a unique monotonic column from the source system (e.g., `source_sequence_id`).
- **Fallback**: If no tiebreaker column exists, configure `dedup_order_columns: ["ingestion_ts DESC"]` only.

**⚠️ OPERATIONAL RISK - Timestamp Collision Without Tiebreaker:**
> When no tiebreaker is configured and multiple records share the same `ingestion_ts`, the selected row is **undefined**. Spark's `ROW_NUMBER()` may return different results across:
> - Different cluster configurations
> - Spark version upgrades
> - Partition changes
>
> **Mitigation**: Always configure a tiebreaker column. If unavailable, document this as a known limitation and monitor for duplicate keys in source data.

### 3.1.1 Ownership Model: Framework vs Developer Responsibilities

The framework enforces a clear separation of concerns between **infrastructure** (framework) and **business logic** (developer).

```
┌───────────────────────────────────────────────────────────────────────────┐
│                    FRAMEWORK RESPONSIBILITIES                             │
│                     (Infrastructure Layer)                                │
├───────────────────────────────────────────────────────────────────────────┤
│  1. Read Bronze with watermark + lookback filter                          │
│  2. Apply metadata-driven deduplication (ROW_NUMBER)                      │
│  3. Resolve {{source}} and {{ref:*}} placeholders to qualified tables     │
│  4. Generate hash keys from metadata ({{_pk_hash}}, {{_diff_hash}})       │
│  5. Substitute placeholders and execute developer SQL                     │
│  6. Validate output schema (required columns present)                     │
│  7. Cache/persist prepared DataFrame                                      │
│  8. Execute SCD1 or SCD2 merge (passthrough or framework-managed)         │
│  9. Update watermark in control table                                     │
│  10. Write audit log                                                      │
└───────────────────────────────────────────────────────────────────────────┘
                                │
                                │ Provides: placeholder resolution, hash generation
                                ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                    DEVELOPER RESPONSIBILITIES                             │
│                   (SQL Template per Table)                                │
├───────────────────────────────────────────────────────────────────────────┤
│  1. Use {{source}} placeholder for source table (NOT hardcoded names)     │
│  2. Use {{ref:table}} for reference table joins                           │
│  3. Use {{alias:column}} for columns from reference tables                │
│  4. Include {{_pk_hash}} and {{_diff_hash}} placeholders                  │
│  5. Apply business logic, cleansing, type casting                         │
│  6. Apply timezone conversions                                            │
│  7. Include deleted_ind and SCD2 columns for passthrough mode             │
└───────────────────────────────────────────────────────────────────────────┘
```

**Placeholder Syntax Reference:**

| Placeholder | Purpose | Resolution |
|-------------|---------|------------|
| `{{source}}` | Source table reference | `{catalog}.{source.schema}.{source.table}` |
| `{{ref:table_name}}` | Reference table join | `{catalog}.{ref.schema}.{ref.table}` |
| `{{alias:column}}` | Column from reference | `{alias}.{column}` |
| `{{_pk_hash}}` | Entity hash (merge key) | Generated SHA2 expression from `hash_keys._pk_hash.columns` |
| `{{_diff_hash}}` | Change hash (SCD2) | Generated SHA2 expression from `hash_keys._diff_hash.track_columns` |
| `{{source_timezone}}` | Source data timezone | Value from `source_timezone` in metadata (e.g., `America/New_York`) |

**Contract: Developer SQL Template Must:**
- Use `{{source}}` placeholder (NOT hardcoded table names)
- Include `{{_pk_hash}}` and `{{_diff_hash}}` placeholders
- Include `deleted_ind` column with appropriate default
- Include SCD2 columns when using passthrough mode
- Use `src` alias for source table

**Contract: Developer SQL Template Must NOT:**
- Hardcode catalog or schema names
- Implement hash key generation logic
- Implement deduplication (no `ROW_NUMBER()` partitioned by business keys)
- Implement SCD merge logic

**Example SQL Template:**
```sql
SELECT
  {{_pk_hash}},
  {{_diff_hash}},
  src.claim_id,
  src.claimant_id AS customer_id,
  src.adjuster_id,
  {{adj:adjuster_name}} AS adjuster_name,
  {{adj:adjuster_region}} AS adjuster_region,
  -- Timezone: Convert from source timezone (from metadata) to UTC
  to_utc_timestamp(src.event_timestamp, '{{source_timezone}}') AS event_timestamp_utc,
  src.source_system,
  coalesce(src.deleted_ind, false) AS deleted_ind,
  src.effective_start_date,
  src.effective_end_date,
  src.is_current
FROM {{source}} src
LEFT JOIN {{ref:adjuster_lookup}} adj ON src.adjuster_id = adj.adjuster_id
```

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

Two internal hash columns are generated by the **framework at runtime** based on metadata configuration:

1.  **Entity Hash (`_pk_hash`)**:
    - **Purpose**: Uniquely identifies the entity across time. Used as the **Merge Key**.
    - **Formula**: `SHA2(CONCAT_WS('|', business_key_1, business_key_n, source_system), 256)`
    - **Configuration**: Defined in `hash_keys._pk_hash.columns` in metadata JSON
    - **SQL Placeholder**: `{{_pk_hash}}`

2.  **Change Hash (`_diff_hash`)**:
    - **Purpose**: Detects if any business value has changed.
    - **Formula**: `SHA2(CONCAT_WS('|', col1, col2, col3...), 256)` (all `track_columns`)
    - **Configuration**: Defined in `hash_keys._diff_hash.track_columns` in metadata JSON
    - **SQL Placeholder**: `{{_diff_hash}}`
    - **Algorithm**: SHA-256 chosen over MD5 for collision resistance in regulated domains.

**Metadata Configuration Example:**
```json
{
  "hash_keys": {
    "_pk_hash": {
      "description": "Entity Hash (Merge Key)",
      "algorithm": "SHA2-256",
      "columns": ["claim_id", "source_system"]
    },
    "_diff_hash": {
      "description": "Change Hash (SCD2 Change Detection)",
      "algorithm": "SHA2-256",
      "track_columns": ["claimant_id", "policy_number", "claim_type", "claim_amount"]
    }
  },
  "source_system_column": "source_system"
}
```

**SQL Template Usage:**
```sql
SELECT
  {{_pk_hash}},
  {{_diff_hash}},
  src.claim_id,
  ...
FROM {{source}} src
```

**Framework Runtime Resolution:**
The framework generates the complete hash expressions with proper canonicalization:
```sql
SHA2(CONCAT_WS('|',
  COALESCE(CAST(src.claim_id AS STRING), '__NULL__'),
  COALESCE(src.source_system, '__NULL__')
), 256) AS _pk_hash
```

**Benefits of Framework-Managed Hash Generation:**
- **Consistency**: All tables use identical canonicalization rules
- **Error Reduction**: Developers cannot forget NULL handling, TRIM, or type casting
- **Maintainability**: Hash algorithm changes require only framework code updates
- **Cleaner SQL**: Business logic is not obscured by ~20 lines of hash boilerplate

#### 3.3.2 Hash Canonicalization Rules

To ensure deterministic hashing across runs and Spark versions:

| Data Type | Canonicalization Rule |
|-----------|-----------------------|
| `NULL` | Coalesce to literal string `"__NULL__"` |
| `STRING` | `TRIM()` to remove leading/trailing whitespace |
| `TIMESTAMP` | Cast to `STRING` using ISO format `yyyy-MM-dd HH:mm:ss.SSSSSS` |
| `DECIMAL` | Cast to `STRING` with fixed scale (e.g., `CAST(col AS DECIMAL(18,6))`) |
| `BOOLEAN` | Cast to `STRING` (`"true"` / `"false"`) |

#### 3.3.3 Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `business_key_columns` | Columns defining the entity identity. | `["claim_id"]` |
| `hash_keys._pk_hash.columns` | Columns for entity hash (usually business_key + source_system). | `["claim_id", "source_system"]` |
| `hash_keys._diff_hash.track_columns` | Columns to monitor for history. | `["status", "amount", "diagnosis_code"]` |
| `scd2_columns` | Metadata columns for versioning. | `{"effective_start_date": "...", "effective_end_date": "...", "is_current": "..."}` |
| `source_system_column` | Column identifying data origin. | `"source_system"` |
| `scd2_mode` | Historical vs incremental behavior. | `{"historical": "passthrough", "incremental": "framework_managed"}` |

#### 3.3.4 Dual-Mode SCD2 Processing

The framework supports two SCD2 modes to handle different data source scenarios:

| Mode | Configuration | Behavior | Use Case |
|------|---------------|----------|----------|
| **Passthrough** | `scd2_mode.historical: "passthrough"` | Preserve SCD2 columns from source | Historical/batch loads from Bronze with existing SCD2 history |
| **Framework-Managed** | `scd2_mode.incremental: "framework_managed"` | Framework detects changes and manages SCD2 columns | Real-time Kafka events without SCD2 metadata |

**Metadata Configuration:**
```json
{
  "load_strategy": {
    "type": "scd2",
    "business_keys": ["claim_id"],
    "scd2_mode": {
      "historical": "passthrough",
      "incremental": "framework_managed"
    },
    "scd2_columns": {
      "effective_start_date": "effective_start_date",
      "effective_end_date": "effective_end_date",
      "is_current": "is_current"
    }
  }
}
```

**SQL Template for Passthrough Mode:**
```sql
SELECT
  {{_pk_hash}},
  {{_diff_hash}},
  src.claim_id,
  ...
  src.source_system,
  coalesce(src.deleted_ind, false) AS deleted_ind,
  src.effective_start_date,
  src.effective_end_date,
  src.is_current
FROM {{source}} src
```

**Soft Delete Indicator:**
All SCD2 tables include a `deleted_ind` column for audit compliance:
- If source provides `deleted_ind`, it's passed through
- If source doesn't have it, defaults to `false`
- Enables tracking of logically deleted records without physical deletion

#### 3.3.5 Logic Flow Diagram

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

### 5.1 Project Directory Structure

The framework uses a clear separation of concerns with dedicated folders:

```
curation_framework/
├── databricks.yml              # DAB config with catalog per environment
├── metadata/
│   └── sdl/                    # Standardized Data Layer
│       └── claims/             # Domain-organized metadata
│           ├── customer.json
│           └── claims.json
├── query/                      # SQL transformation templates
│   ├── customer.sql
│   └── claims.sql
├── fixtures/                   # Reference table DDL & data
│   ├── adjuster_lookup.sql
│   └── state_lookup.sql
├── src/                        # Framework Python code
│   └── curation_framework/
└── resources/                  # Databricks job/pipeline definitions
    └── *.yml
```

**Directory Responsibilities:**

| Directory | Purpose |
|-----------|---------|
| `metadata/` | Table configuration JSON files organized by domain |
| `query/` | SQL transformation templates with placeholder syntax |
| `fixtures/` | Reference table DDL and seed data scripts |
| `src/` | Framework Python implementation |
| `resources/` | Databricks Asset Bundle resource definitions |

### 5.2 Environment Configuration (databricks.yml)

Catalog and other environment-specific values are configured in `databricks.yml`, NOT in metadata files:

```yaml
bundle:
  name: curation_framework

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://example.cloud.databricks.com
    variables:
      catalog: ltc_insurance_dev
      
  prod:
    mode: production
    workspace:
      host: https://example.cloud.databricks.com
    variables:
      catalog: ltc_insurance_prod
```

**Rationale:**
- Same metadata files work across dev/staging/prod without modification
- Eliminates risk of deploying dev catalog references to production
- Follows Databricks Asset Bundle best practices

### 5.3 Table Metadata Configuration

Each table has a JSON configuration file in `metadata/sdl/{domain}/`:

```json
{
  "table_name": "claims",
  "description": "Standardized Claims Data",
  
  "source": {
    "schema": "unified_dev",
    "table": "unified_claims_scd2"
  },
  
  "target": {
    "schema": "standardized_data_layer",
    "table": "claims"
  },
  
  "load_strategy": {
    "type": "scd2",
    "business_keys": ["claim_id"],
    "scd2_mode": {
      "historical": "passthrough",
      "incremental": "framework_managed"
    },
    "scd2_columns": {
      "effective_start_date": "effective_start_date",
      "effective_end_date": "effective_end_date",
      "is_current": "is_current"
    }
  },
  
  "hash_keys": {
    "_pk_hash": {
      "description": "Entity Hash (Merge Key)",
      "algorithm": "SHA2-256",
      "columns": ["claim_id", "source_system"]
    },
    "_diff_hash": {
      "description": "Change Hash (SCD2 Change Detection)",
      "algorithm": "SHA2-256",
      "track_columns": ["claimant_id", "policy_number", "claim_type", "claim_amount"]
    }
  },
  
  "source_system_column": "source_system",
  
  "reference_joins": [
    {
      "table": "adjuster_lookup",
      "schema": "standardized_data_layer",
      "alias": "adj",
      "join_type": "LEFT",
      "join_condition": "src.adjuster_id = adj.adjuster_id"
    }
  ],
  
  "columns": [
    { "source_col": "claim_id", "target_col": "claim_id", "type": "string" },
    { "source_col": "claimant_id", "target_col": "customer_id", "type": "string" },
    { "source_col": "adj.adjuster_name", "target_col": "adjuster_name", "type": "string", "from_reference": true }
  ]
}
```

**Note:** The `columns` section serves as documentation and schema contract. The SQL template is the source of truth for transformation logic.

### 5.4 SQL Template Syntax

SQL templates use placeholder syntax for framework-managed elements. All transformations are implemented in **pure Spark SQL** (no Python UDFs).

#### 5.4.1 Placeholder Reference

| Placeholder | Purpose | Example Resolution |
|-------------|---------|-------------------|
| `{{source}}` | Source table | `ltc_insurance.unified_dev.unified_claims_scd2` |
| `{{ref:table_name}}` | Reference table | `ltc_insurance.standardized_data_layer.adjuster_lookup` |
| `{{alias:column}}` | Reference column | `adj.adjuster_name` |
| `{{_pk_hash}}` | Entity hash expression | `SHA2(CONCAT_WS('|', ...), 256) AS _pk_hash` |
| `{{_diff_hash}}` | Change hash expression | `SHA2(CONCAT_WS('|', ...), 256) AS _diff_hash` |
| `{{source_timezone}}` | Source data timezone | `America/New_York` |

#### 5.4.2 Complete SQL Template Example

```sql
-- query/claims.sql
SELECT
  {{_pk_hash}},
  {{_diff_hash}},
  src.claim_id,
  src.claimant_id AS customer_id,
  src.policy_number,
  src.claim_type,
  src.step_name AS workflow_step,
  src.step_status AS workflow_status,
  src.adjuster_id,
  {{adj:adjuster_name}} AS adjuster_name,
  {{adj:adjuster_region}} AS adjuster_region,
  src.claim_decision AS decision,
  src.denial_reason,
  src.claim_amount AS requested_amount,
  src.approved_amount,
  -- Timezone: Convert from source timezone (from metadata) to UTC
  to_utc_timestamp(src.event_timestamp, '{{source_timezone}}') AS event_timestamp_utc,
  src.source_system,
  coalesce(src.deleted_ind, false) AS deleted_ind,
  src.effective_start_date,
  src.effective_end_date,
  src.is_current
FROM {{source}} src
LEFT JOIN {{ref:adjuster_lookup}} adj ON src.adjuster_id = adj.adjuster_id
```

#### 5.4.3 Timezone Conversion

Source data arrives in a configurable timezone (specified in metadata). The target SDL layer stores all timestamps in UTC for consistency.

**Timezone Convention:**
- **Source Timezone**: Configured per table in metadata JSON (e.g., `America/New_York`)
- **Target Timezone**: UTC - standard for data lake storage
- **Placeholder**: `{{source_timezone}}` - resolved at runtime from metadata

**Metadata Configuration:**
```json
{
  "source_timezone": "America/New_York"
}
```

**SQL Pattern:**
```sql
SELECT
  claim_id,
  -- Timezone: Convert from source timezone (from metadata) to UTC
  to_utc_timestamp(event_ts, '{{source_timezone}}') AS event_ts_utc
FROM {{source}} src
```

**Supported Functions (Native Spark SQL):**
| Function | Purpose |
|----------|---------|
| `TO_UTC_TIMESTAMP(ts, tz)` | Convert local time to UTC |
| `FROM_UTC_TIMESTAMP(ts, tz)` | Convert UTC to local time |
| `CONVERT_TIMEZONE(from_tz, to_tz, ts)` | Direct conversion (Databricks SQL) |

#### 5.1.2 Reference Data Lookups

Enrich source data by joining to persistent reference tables in SDL. Reference tables are created via fixture scripts and configured in metadata.

**Fixture Script (`fixtures/state_lookup.sql`):**
```sql
CREATE TABLE IF NOT EXISTS ${catalog}.standardized_data_layer.state_lookup (
  state_code STRING,
  state_name STRING,
  region STRING
);

INSERT OVERWRITE ${catalog}.standardized_data_layer.state_lookup VALUES
  ('AL', 'Alabama', 'Southeast'),
  ('AK', 'Alaska', 'West'),
  ...
```

**Metadata Configuration:**
```json
{
  "reference_joins": [
    {
      "table": "state_lookup",
      "schema": "standardized_data_layer",
      "alias": "st",
      "join_type": "LEFT",
      "join_condition": "src.state_code = st.state_code"
    }
  ]
}
```

**SQL Template Pattern:**
```sql
SELECT
  src.customer_id,
  src.state_code,
  {{st:state_name}} AS state_name,
  {{st:region}} AS region
FROM {{source}} src
LEFT JOIN {{ref:state_lookup}} st ON src.state_code = st.state_code
```

**Design Principles:**
- **Persistent Tables**: Reference tables exist as standard Delta tables in SDL, not temp views.
- **Placeholder Resolution**: `{{ref:table}}` resolves to fully-qualified name using catalog from environment.
- **Column Placeholders**: `{{alias:column}}` provides clean syntax for reference columns.
- **LEFT JOIN Default**: Avoid dropping records when reference data is missing.
- **Broadcast Hint**: Small reference tables can use `/*+ BROADCAST(st) */` for performance.

#### 5.1.3 Complete Transformation Example

This section demonstrates a complete end-to-end transformation for the `Claims` entity using the placeholder-based approach.

**Metadata Configuration (`metadata/sdl/claims/claims.json`):**
```json
{
  "table_name": "claims",
  "source": {
    "schema": "unified_dev",
    "table": "claims",
    "watermark_column": "ingestion_ts"
  },
  "target": {
    "schema": "standardized_data_layer",
    "table": "claims"
  },
  "hash_keys": {
    "_pk_hash": {
      "columns": ["claim_id", "source_system"]
    },
    "_diff_hash": {
      "track_columns": ["claimant_id", "policy_number", "claim_type", "claim_amount"]
    }
  },
  "reference_joins": [
    {
      "table": "adjuster_lookup",
      "schema": "standardized_data_layer",
      "alias": "adj",
      "join_type": "LEFT",
      "join_condition": "src.adjuster_id = adj.adjuster_id"
    }
  ],
  "load_strategy": {
    "type": "scd2",
    "scd2_mode": "passthrough"
  }
}
```

**SQL Template (`query/claims.sql`):**
```sql
-- query/claims.sql
-- Developer writes business logic; framework handles hash generation

SELECT
  -- Framework-generated hash keys (from metadata)
  {{_pk_hash}},
  {{_diff_hash}},
  
  -- Business Keys
  src.claim_id,
  src.source_system,
  
  -- Foreign Keys  
  src.claimant_id AS customer_id,
  src.policy_number,
  
  -- Reference Data Enrichment
  src.adjuster_id,
  {{adj:adjuster_name}} AS adjuster_name,
  {{adj:adjuster_region}} AS adjuster_region,
  
  -- Business Logic: Computed Fields
  src.claim_amount,
  CASE 
    WHEN src.claim_amount > 10000 THEN 'HIGH'
    WHEN src.claim_amount > 1000 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS claim_tier,
  
  -- Data Quality: Cleansing
  TRIM(UPPER(src.claim_type)) AS claim_type,
  
  -- Soft Delete Indicator
  coalesce(src.deleted_ind, false) AS deleted_ind,
  
  -- SCD2 Columns (passthrough from source)
  src.effective_start_date,
  src.effective_end_date,
  src.is_current

FROM {{source}} src
LEFT JOIN {{ref:adjuster_lookup}} adj ON src.adjuster_id = adj.adjuster_id
```

**Framework Placeholder Resolution:**

At runtime, the framework transforms the SQL template:

| Placeholder | Resolved Value |
|-------------|----------------|
| `{{source}}` | `ltc_insurance.unified_dev.claims` |
| `{{ref:adjuster_lookup}}` | `ltc_insurance.standardized_data_layer.adjuster_lookup` |
| `{{adj:adjuster_name}}` | `adj.adjuster_name` |
| `{{_pk_hash}}` | `SHA2(CONCAT_WS('\|', COALESCE(CAST(src.claim_id AS STRING), '__NULL__'), COALESCE(src.source_system, '__NULL__')), 256) AS _pk_hash` |
| `{{_diff_hash}}` | `SHA2(CONCAT_WS('\|', COALESCE(...), ...), 256) AS _diff_hash` |

**Key Contract Enforcement:**
- ✅ SQL uses `{{source}}` placeholder (not hardcoded table names)
- ✅ SQL uses `{{_pk_hash}}` and `{{_diff_hash}}` placeholders (framework generates hash logic)
- ✅ SQL uses `{{ref:*}}` for reference tables (catalog resolved from environment)
- ✅ SQL includes `deleted_ind` for soft delete support
- ✅ SQL includes SCD2 columns for passthrough mode

**Framework Post-Processing (After SQL Execution):**

For `scd2_mode: "framework_managed"`, the framework applies SCD2 logic:

```sql
-- Framework-generated SCD2 MERGE (not user-written)
-- Used only when scd2_mode = "framework_managed"

-- 1. Identify changes by comparing _diff_hash
CREATE OR REPLACE TEMP VIEW changes AS
SELECT 
  src.*,
  CASE 
    WHEN tgt._pk_hash IS NULL THEN 'INSERT'
    WHEN src._diff_hash != tgt._diff_hash THEN 'UPDATE'
    ELSE 'NO_CHANGE'
  END AS _action
FROM transformed_cached src
LEFT JOIN target_current tgt ON src._pk_hash = tgt._pk_hash;

-- 2. Close old versions
MERGE INTO target_table AS tgt
USING (SELECT * FROM changes WHERE _action = 'UPDATE') AS src
ON tgt._pk_hash = src._pk_hash AND tgt.is_current = true
WHEN MATCHED THEN UPDATE SET
  tgt.effective_end_date = CURRENT_TIMESTAMP(),
  tgt.is_current = false;

-- 3. Insert new versions
INSERT INTO target_table
SELECT *, CURRENT_TIMESTAMP() AS effective_start_date,
  CAST('9999-12-31' AS TIMESTAMP) AS effective_end_date, true AS is_current
FROM changes WHERE _action IN ('INSERT', 'UPDATE');
```

For `scd2_mode: "passthrough"`, the framework directly inserts records preserving source SCD2 columns.

**Responsibility Summary:**

| Concern | Owner | Location |
|---------|-------|----------|
| Placeholder Resolution | Framework | `transform/placeholder_engine.py` |
| Hash Key Generation | Framework | `transform/hash_generator.py` (from metadata) |
| Reference Table Resolution | Framework | `transform/reference_resolver.py` |
| SCD2 Processing | Framework | `core/scd2_processor.py` (mode-aware) |
| Business Logic & JOINs | Developer | `query/<table>.sql` |
| Column Mapping | Developer | `query/<table>.sql` |
| Watermark Update | Framework | `reader/control_table.py` |
| Audit Logging | Framework | `audit/audit_logger.py` |

### 5.2 Validation Rules

The framework enforces these rules at startup and runtime to prevent configuration drift and ensure contract compliance.

**Startup Validation (Config Load):**

| Rule | Severity | Description |
|------|----------|-------------|
| `dedup_order_columns` has tiebreaker | WARN | Warn if only timestamp column configured (collision risk) |
| `business_key_columns` non-empty | ERROR | Fail if dedup partition key is empty |
| `source_system_column` defined | ERROR | Required for composite dedup key |
| `track_columns` defined for SCD2 | WARN | Warn if SCD2 table has no track columns |
| Unique `(sub_domain, sequence)` | ERROR | No duplicate sequences within sub-domain |

**Runtime Validation (After SQL Execution):**

| Rule | Severity | Description |
|------|----------|-------------|
| Output has `_pk_hash` column | ERROR | Required for all tables |
| Output has `_diff_hash` column | ERROR | Required for SCD2 tables |
| Output has business key columns | ERROR | Must match config `business_key_columns` |
| Output has `source_system` column | ERROR | Must match config `source_system_column` |
| SQL does not contain dedup pattern | WARN | Warn if `ROW_NUMBER.*PARTITION BY.*business_key` detected |

**Validation Implementation:**

```python
# config/validator.py
def validate_table_config(config: TableConfig) -> list[ValidationError]:
    errors = []
    warnings = []
    
    # Startup validations
    if not config.business_key_columns:
        errors.append(ValidationError("business_key_columns cannot be empty"))
    
    if not config.source_system_column:
        errors.append(ValidationError("source_system_column is required"))
    
    if len(config.dedup_order_columns) == 1:
        warnings.append(ValidationWarning(
            f"Table {config.table_name}: Only one dedup_order_column configured. "
            "Timestamp collisions may cause non-deterministic selection. "
            "Consider adding a tiebreaker column like _kafka_offset."
        ))
    
    if config.scd_type == 2 and not config.track_columns:
        warnings.append(ValidationWarning(
            f"Table {config.table_name}: SCD2 table has no track_columns. "
            "All columns will be tracked for changes."
        ))
    
    return errors, warnings


def validate_sql_output(df: DataFrame, config: TableConfig) -> list[ValidationError]:
    errors = []
    
    # Check required columns
    columns = set(df.columns)
    
    if "_pk_hash" not in columns:
        errors.append(ValidationError(
            f"Table {config.table_name}: SQL output missing required column '_pk_hash'"
        ))
    
    if config.scd_type == 2 and "_diff_hash" not in columns:
        errors.append(ValidationError(
            f"Table {config.table_name}: SCD2 table SQL output missing required column '_diff_hash'"
        ))
    
    for key_col in config.business_key_columns:
        if key_col not in columns:
            errors.append(ValidationError(
                f"Table {config.table_name}: SQL output missing business key column '{key_col}'"
            ))
    
    return errors
```

### 5.3 Table Configuration

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

### 5.4 Global Settings

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

### 5.5 Control Table Schema

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

### 5.6 Audit Log Table Schema

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

### 6.1 Wheel Entry Point Configuration

The `pyproject.toml` defines the entry point that `python_wheel_task` invokes:

```toml
# pyproject.toml
[project]
name = "curation_framework"
version = "1.0.0"

[project.scripts]
# This creates the entry point for python_wheel_task
job_executor = "curation_framework.job_executor:main"

[project.entry-points."databricks"]
# Databricks-specific entry point (same as scripts)
job_executor = "curation_framework.job_executor:main"
```

**DAB Wheel Build:**
```yaml
# databricks.yml
bundle:
  name: curation_framework

artifacts:
  default:
    type: whl
    build: uv build
    path: .
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

### 7.2 Configuration Strategy: Single Source of Truth

**Principle**: Orchestration metadata (`sub_domain`, `sequence`, `critical`) is defined **only** in each `TableConfig`. There is no separate `sub_domains` configuration block.

**Why Single Source?**
- Eliminates risk of config drift between table definitions and orchestration blocks
- Each table is self-describing: all metadata in one place
- Validator can check completeness without cross-referencing

**TableConfig Orchestration Fields:**
```json
{
  "table_name": "silver_claimants",
  "sub_domain": "party",
  "sequence": 1,
  "critical": true,
  // ... other fields
}
```

**Orchestration Derived at Runtime:**
```python
# orchestration/dependency_resolver.py
def resolve_execution_order(tables: list[TableConfig]) -> list[list[TableConfig]]:
    """
    Group tables by sub_domain, sort by sequence within each group.
    Returns: [[sub_domain_1_tables], [sub_domain_2_tables], ...]
    """
    # Group by sub_domain
    by_domain = defaultdict(list)
    for t in tables:
        by_domain[t.sub_domain].append(t)
    
    # Sort each group by sequence
    for domain in by_domain:
        by_domain[domain].sort(key=lambda x: x.sequence)
    
    # Order domains by their lowest sequence number (or alphabetically)
    domain_order = sorted(by_domain.keys())
    return [by_domain[d] for d in domain_order]
```

**Validation Rules:**
| Rule | Description |
|------|-------------|
| **Unique (sub_domain, sequence)** | No two tables in same sub_domain can have same sequence |
| **sub_domain Required** | All enabled tables must have a sub_domain |
| **sequence Required** | All enabled tables must have a sequence number |
| **critical Default** | Defaults to `false` if not specified |

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

**Deployment Strategy: Python Wheel Task**

The framework uses `python_wheel_task` as the single standard for both dev and prod environments. This provides:
- **Versioning**: Wheel packages are versioned and immutable once published.
- **Consistency**: Same package runs identically in all environments.
- **Dependencies**: All Python dependencies are bundled in the wheel.
- **No Workspace Files**: No need to sync `.py` files to workspace; wheel is uploaded to cluster.

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
          python_wheel_task:
            package_name: "curation_framework"
            entry_point: "job_executor"  # Calls job_executor:main()
            parameters:
              - "--pipeline_type=lakeflow_curation"
              - "--table_name=silver_claimants"
        
        - task_key: "party_providers"
          depends_on:
            - task_key: "party_claimants"
          python_wheel_task:
            package_name: "curation_framework"
            entry_point: "job_executor"
            parameters:
              - "--pipeline_type=lakeflow_curation"
              - "--table_name=silver_providers"
        
        - task_key: "party_policies"
          depends_on:
            - task_key: "party_providers"
          python_wheel_task:
            package_name: "curation_framework"
            entry_point: "job_executor"
            parameters:
              - "--pipeline_type=lakeflow_curation"
              - "--table_name=silver_policies"
        
        # Sub-Domain 2: Claims (depends on Party completion)
        - task_key: "claims_claims"
          depends_on:
            - task_key: "party_policies"
          python_wheel_task:
            package_name: "curation_framework"
            entry_point: "job_executor"
            parameters:
              - "--pipeline_type=lakeflow_curation"
              - "--table_name=silver_claims"
        
        - task_key: "claims_claim_lines"
          depends_on:
            - task_key: "claims_claims"
          python_wheel_task:
            package_name: "curation_framework"
            entry_point: "job_executor"
            parameters:
              - "--pipeline_type=lakeflow_curation"
              - "--table_name=silver_claim_lines"
        
        # Sub-Domain 3: Financials (depends on Claims completion)
        - task_key: "financials_payments"
          depends_on:
            - task_key: "claims_claim_lines"
          python_wheel_task:
            package_name: "curation_framework"
            entry_point: "job_executor"
            parameters:
              - "--pipeline_type=lakeflow_curation"
              - "--table_name=silver_payments"
```

**Configuration Files Deployment:**
Configuration files (`tables_config.json`, SQL files) are deployed as workspace files via DAB `sync` section and accessed at runtime via `${workspace.file_path}`.
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

**Entry Point**: `curation_framework.job_executor:main()` - invoked by `python_wheel_task` in DAB jobs.

```
src/curation_framework/
├── __init__.py                    # Package exports & public API
├── job_executor.py                # ⭐ Main entry point (dispatches to pipeline by type)
│
├── pipelines/                     # Pipeline Implementations
│   ├── __init__.py
│   ├── base_pipeline.py           # Abstract base pipeline class
│   ├── lakeflow_curation_pipeline.py  # ⭐ Bronze-to-Silver curation pipeline
│   └── (future: other pipeline types)
│
├── core/                          # Core Processing Modules
│   ├── __init__.py
│   ├── processor.py               # Abstract base processor
│   ├── scd1_processor.py          # SCD Type 1 merge logic
│   ├── scd2_processor.py          # SCD Type 2 hash-based merge
│   └── dedup.py                   # Entity-level deduplication
│
├── reader/                        # Source Reading Modules
│   ├── __init__.py
│   ├── incremental_reader.py      # Watermark + lookback incremental read
│   ├── control_table.py           # Watermark control table manager
│   └── bronze_reader.py           # Bronze-specific read utilities
│
├── writer/                        # Target Writing Modules
│   ├── __init__.py
│   ├── delta_writer.py            # Delta table write operations
│   ├── merge_executor.py          # SCD merge execution
│   └── table_manager.py           # Table creation, schema evolution
│
├── audit/                         # Audit & Observability Modules
│   ├── __init__.py
│   ├── audit_logger.py            # Audit log table writer
│   ├── metrics_collector.py       # Processing metrics collection
│   └── run_tracker.py             # Run ID and status tracking
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
└── utils/                         # Shared Utilities
    ├── __init__.py
    ├── spark_utils.py             # SparkSession helpers
    ├── delta_utils.py             # Delta Lake operations
    └── datetime_utils.py          # Timezone & timestamp helpers
```

### 10.2 Entry Point Architecture

The framework uses a **dispatcher pattern** where `job_executor.py` routes execution to the appropriate pipeline based on `pipeline_type`.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Entry Point Architecture                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  DAB Job Task                                                                │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  python_wheel_task:                                                  │    │
│  │    entry_point: job_executor                                         │    │
│  │    parameters:                                                       │    │
│  │      - "--pipeline_type=lakeflow_curation"                           │    │
│  │      - "--table_name=silver_claimants"                               │    │
│  │      - "--config_path=conf/tables_config.json"                       │    │
│  └──────────────────────────────┬──────────────────────────────────────┘    │
│                                 │                                            │
│                                 ▼                                            │
│  job_executor.py                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  1. Parse CLI arguments (--pipeline_type, --table_name, etc.)       │    │
│  │  2. Load configuration                                               │    │
│  │  3. Dispatch to pipeline based on pipeline_type                      │    │
│  │  4. Return exit code                                                 │    │
│  └──────────────────────────────┬──────────────────────────────────────┘    │
│                                 │                                            │
│                   ┌─────────────┴─────────────┐                             │
│                   │    pipeline_type?         │                             │
│                   └─────────────┬─────────────┘                             │
│                                 │                                            │
│         ┌───────────────────────┼───────────────────────┐                   │
│         │                       │                       │                   │
│         ▼                       ▼                       ▼                   │
│  ┌──────────────┐       ┌──────────────┐       ┌──────────────┐            │
│  │ lakeflow_    │       │ (future)     │       │ (future)     │            │
│  │ curation     │       │ gold_agg     │       │ data_quality │            │
│  └──────┬───────┘       └──────────────┘       └──────────────┘            │
│         │                                                                    │
│         ▼                                                                    │
│  lakeflow_curation_pipeline.py                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Bronze → Silver curation with:                                      │    │
│  │  - Watermark + Lookback incremental read                            │    │
│  │  - Metadata-driven deduplication                                     │    │
│  │  - SQL transformation execution                                      │    │
│  │  - SCD1/SCD2 merge                                                   │    │
│  │  - Control table update                                              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 10.2.1 job_executor.py

**Role**: Main entry point invoked by Databricks `python_wheel_task`. Parses arguments and dispatches to the appropriate pipeline.

**Responsibilities:**
- Parse CLI arguments (`--pipeline_type`, `--table_name`, `--config_path`, etc.)
- Initialize SparkSession and logging
- Load and validate configuration
- Dispatch to registered pipeline based on `pipeline_type`
- Handle top-level exceptions and return appropriate exit codes
- Write final audit log entry

**Supported Arguments:**

| Argument | Required | Description |
|----------|----------|-------------|
| `--pipeline_type` | Yes | Pipeline to execute (e.g., `lakeflow_curation`) |
| `--table_name` | Yes | Target table to process |
| `--config_path` | No | Path to config file (default: `conf/tables_config.json`) |
| `--run_id` | No | Unique run identifier (auto-generated if not provided) |

**Pipeline Registry:**

| `pipeline_type` Value | Pipeline Class | Description |
|-----------------------|----------------|-------------|
| `lakeflow_curation` | `LakeflowCurationPipeline` | Bronze → Silver batch curation |
| _(future)_ `gold_aggregation` | `GoldAggregationPipeline` | Silver → Gold aggregations |
| _(future)_ `data_quality` | `DataQualityPipeline` | Data quality checks |

#### 10.2.2 pipelines/base_pipeline.py

**Role**: Abstract base class defining the pipeline contract.

**Interface:**
```
┌─────────────────────────────────────────────────────────────────┐
│  class BasePipeline(ABC):                                       │
│                                                                  │
│    @abstractmethod                                              │
│    def run(self, table_config: TableConfig) -> PipelineResult   │
│                                                                  │
│    @property                                                     │
│    @abstractmethod                                              │
│    def pipeline_type(self) -> str                               │
│                                                                  │
│    def validate_config(self, config: TableConfig) -> bool       │
│    def get_metrics(self) -> dict                                │
└─────────────────────────────────────────────────────────────────┘
```

#### 10.2.3 pipelines/lakeflow_curation_pipeline.py

**Role**: Implements the Bronze → Silver curation pipeline. This is the primary pipeline for LTC Claims processing.

**Execution Flow:**
1. Read from Bronze with watermark + lookback
2. Apply metadata-driven deduplication
3. Register `source_deduped` and reference temp views
4. Execute developer transformation SQL
5. Validate output schema (`_pk_hash`, `_diff_hash`)
6. Execute SCD1 or SCD2 merge
7. Update control table watermark
8. Write audit log

**Dependencies:**
- `core/dedup.py` - Deduplication logic
- `core/scd1_processor.py` or `core/scd2_processor.py` - Merge logic
- `reader/incremental_reader.py` - Incremental source reading
- `reader/control_table.py` - Watermark management
- `writer/merge_executor.py` - SCD merge execution
- `audit/audit_logger.py` - Audit log writing
- `transform/sql_engine.py` - SQL execution
- `transform/reference_loader.py` - Reference table registration

### 10.3 Module Responsibilities

#### 10.3.1 Core Layer (`core/`)

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

#### 10.3.2 Reader Layer (`reader/`)

| Module | Responsibility |
|--------|---------------|
| `incremental_reader.py` | Reads from Bronze with watermark filter + lookback window |
| `control_table.py` | Manages `silver.curation_control` watermark table atomically |
| `bronze_reader.py` | Bronze-specific read utilities and schema validation |

**Key Design**: Reader operations are isolated for testability and source abstraction.

```python
# reader/incremental_reader.py
class IncrementalReader:
    """Reads source data with watermark filtering."""
    
    def read(self, table: str, watermark: datetime, lookback: timedelta) -> DataFrame:
        """Read records where timestamp > (watermark - lookback)."""
        pass
```

#### 10.3.3 Writer Layer (`writer/`)

| Module | Responsibility |
|--------|---------------|
| `delta_writer.py` | Writes to Silver Delta tables with optimized settings |
| `merge_executor.py` | Executes SCD MERGE operations |
| `table_manager.py` | Table creation, schema evolution, OPTIMIZE/VACUUM |

**Key Design**: Writer operations handle all Delta-specific optimizations.

```python
# writer/merge_executor.py
class MergeExecutor:
    """Executes SCD merge operations."""
    
    def execute_scd1_merge(self, source_df: DataFrame, target_table: str, keys: list) -> MergeResult:
        """Execute SCD Type 1 upsert merge."""
        pass
    
    def execute_scd2_merge(self, source_df: DataFrame, target_table: str, pk_hash: str) -> MergeResult:
        """Execute SCD Type 2 hash-based merge."""
        pass
```

#### 10.3.4 Audit Layer (`audit/`)

| Module | Responsibility |
|--------|---------------|
| `audit_logger.py` | Writes processing results to `silver.curation_audit_log` |
| `metrics_collector.py` | Collects processing metrics (records, duration, watermarks) |
| `run_tracker.py` | Manages run IDs, status tracking, and run metadata |

**Key Design**: Audit is cross-cutting but injected, not hard-coded.

```python
# audit/audit_logger.py
class AuditLogger:
    """Writes processing results to audit log table."""
    
    def log_run(self, run_id: str, table_name: str, result: ProcessingResult):
        """Insert audit record for processing run."""
        pass
    
    def log_error(self, run_id: str, table_name: str, error: Exception):
        """Insert error audit record."""
        pass
```

#### 10.3.5 Transform Layer (`transform/`)

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

#### 10.3.6 Orchestration Layer (`orchestration/`)

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

#### 10.3.7 Config Layer (`config/`)

| Module | Responsibility |
|--------|---------------|
| `loader.py` | Loads `tables_config.json` from workspace or local filesystem |
| `validator.py` | Validates config against schema, checks required fields |
| `schema.py` | Defines `TableConfig`, `GlobalSettings` dataclasses/Pydantic models |

**Key Design**: Configuration is validated at load time, failing fast on invalid config.

```python
# config/schema.py
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class TimezoneConfig:
    """Timezone conversion configuration."""
    source_timezone: str = "UTC"
    target_timezone: str = "America/New_York"
    timestamp_columns: list[str] = field(default_factory=list)

@dataclass
class ReferenceTable:
    """Reference table configuration for lookups."""
    alias: str                          # Temp view name for SQL
    table: str                          # Fully qualified table name
    filter: Optional[str] = None        # Optional WHERE clause
    broadcast: bool = False             # Use broadcast hint
    max_rows: int = 50000               # Safety limit

@dataclass
class SCD2Columns:
    """SCD Type 2 metadata column names."""
    effective_start_date: str = "effective_start_date"
    effective_end_date: str = "effective_end_date"
    is_current: str = "is_current"

@dataclass
class TableConfig:
    """Complete table configuration - single source of truth."""
    # Identity
    table_name: str                     # Unique identifier
    source_table: str                   # Fully qualified Bronze table
    target_table: str                   # Fully qualified Silver table
    
    # SCD Configuration
    scd_type: int                       # 1 = Upsert, 2 = History
    business_key_columns: list[str]     # Natural key columns
    source_system_column: str = "source_system"
    
    # Orchestration (Single Source of Truth)
    sub_domain: str = "default"         # Execution group (party, claims, etc.)
    sequence: int = 1                   # Order within sub_domain
    critical: bool = False              # Halt on failure if True
    
    # Watermark & Dedup
    watermark_column: str = "ingestion_ts"
    lookback_interval: str = "2 HOURS"
    dedup_order_columns: list[str] = field(
        default_factory=lambda: ["ingestion_ts DESC"]
    )
    
    # SCD2-Specific
    scd2_columns: Optional[SCD2Columns] = None
    track_columns: list[str] = field(default_factory=list)
    
    # Transformations
    transformation_sql_path: Optional[str] = None
    timezone_config: Optional[TimezoneConfig] = None
    reference_tables: list[ReferenceTable] = field(default_factory=list)
    
    # Control
    enabled: bool = True

@dataclass
class GlobalSettings:
    """Global framework settings."""
    catalog: str = "main"
    schema_bronze: str = "bronze"
    schema_silver: str = "silver"
    control_table: str = "silver.curation_control"
    audit_table: str = "silver.curation_audit_log"
    default_watermark_column: str = "ingestion_ts"
    default_lookback_interval: str = "2 HOURS"
    default_dedup_order_columns: list[str] = field(
        default_factory=lambda: ["ingestion_ts DESC"]
    )
    scd2_end_date_value: str = "9999-12-31 23:59:59"
    default_source_timezone: str = "UTC"
    default_target_timezone: str = "America/New_York"
    log_level: str = "INFO"
    allowed_variables: list[str] = field(default_factory=lambda: [
        "source_timezone", "target_timezone", "catalog",
        "schema_silver", "processing_date"
    ])
```

#### 10.3.8 Utils Layer (`utils/`)

| Module | Responsibility |
|--------|---------------|
| `spark_utils.py` | SparkSession creation, dbutils access |
| `delta_utils.py` | OPTIMIZE, VACUUM, table existence checks |
| `datetime_utils.py` | Timezone conversion, timestamp parsing |

### 10.4 Module Dependency Graph

```
                    ┌─────────────────┐
                    │ job_executor.py │
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
│     config/     │ │ reader/ writer/ │ │   transform/    │
│  loader, schema │ │     audit/      │ │   sql_engine    │
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
```

**Dependency Rules:**
1. **Downward Only**: Higher layers depend on lower layers, never the reverse.
2. **No Circular**: Modules within the same layer do not depend on each other.
3. **Utils at Bottom**: `utils/` is foundational, used everywhere.
4. **Config Isolated**: `config/` only loads data, has no processing logic.

### 10.5 Key Interfaces

```python
# Public API exported from __init__.py
from curation_framework.orchestration import BatchOrchestrator
from curation_framework.core import SCD1Processor, SCD2Processor
from curation_framework.config import TableConfig, load_config
from curation_framework.reader import IncrementalReader, ControlTableManager
from curation_framework.writer import MergeExecutor
from curation_framework.audit import AuditLogger

__all__ = [
    "BatchOrchestrator",
    "SCD1Processor", 
    "SCD2Processor",
    "TableConfig",
    "load_config",
    "IncrementalReader",
    "ControlTableManager",
    "MergeExecutor",
    "AuditLogger",
]
```

### 10.6 Testing Strategy per Module

| Layer | Test Type | Mock Dependencies |
|-------|-----------|-------------------|
| `core/` | Unit tests | Mock DataFrame, DeltaTable |
| `reader/` | Integration tests | Spark local, temp Delta tables |
| `writer/` | Integration tests | Spark local, temp Delta tables |
| `audit/` | Unit tests | Mock Spark writes |
| `transform/` | Unit tests | Mock SQL files, temp views |
| `orchestration/` | Integration tests | Mock all processors |
| `config/` | Unit tests | Test JSON files |
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
| Placeholder-Based SQL | Clean separation between business logic and infrastructure concerns |
| Framework-Managed Hashes | Consistent canonicalization, error-free hash generation |
| Dual SCD2 Mode | Flexibility for historical loads vs. incremental processing |
| Environment in Bundle | Single source of truth for catalog configuration |