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
- Maintains referential integrity between LTC entities
- Serves as the "single source of truth" for cleansed, conformed claims data
- Optimized for operational reporting and downstream Gold layer aggregations

### 1.2 Upstream Integration: Unified Bronze Layer

The Curation Framework consumes data from a **Unified Bronze Layer** which acts as the raw ingestion point for all source systems.

- **Source Architecture**: The Bronze layer uses a **Unified SCD Type 2** pattern.
- **Composite Key**: Records are uniquely identified by `entity_id` + `source_system`.
- **Duplicate Preservation**: The Bronze layer intentionally preserves duplicates from different source systems (e.g., the same Claimant from "AdminSystem" and "CRM").
- **Role of Silver**: The Silver layer is responsible for **Entity Resolution**—collapsing these multi-source Bronze records into a single "Golden Record" for the Silver ODS.

### 1.3 Entity Resolution Strategy

To handle the multi-source nature of the Bronze layer, the Silver layer implements a robust Entity Resolution strategy:

1.  **Priority-Based Selection**:
    - When multiple sources provide data for the same entity (e.g., Claimant), a configured **priority list** determines the winner.
    - *Example*: `[AdminSystem, CRM, Sales]`. If `AdminSystem` has data, it wins. If not, `CRM` is used.
    - This logic is implemented in the SQL Transformation step before the Silver SCD merge.

2.  **Union & Aggregation**:
    - For transactional entities (like Claims or Payments) that are distinct across systems, records are **unioned**.
    - A `source_system` column is maintained in Silver to track lineage.

3.  **Business Key Management**:
    - The Silver layer generates its own surrogate keys (e.g., `claimant_sk`) or uses a standardized Business Key that abstracts away the source-specific IDs.

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

The framework uses a high-watermark pattern to efficiently process only new records from streaming tables without scanning the full history.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    High-Watermark Processing Flow                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Step 1: Query Target Table                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  SELECT MAX(processing_timestamp) FROM silver.table                  │    │
│  │  Result: 2024-01-15 10:30:00 (High Watermark)                        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  Step 2: Filter Source Table                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  SELECT * FROM bronze.streaming_table                                │    │
│  │  WHERE ingestion_ts > '2024-01-15 10:30:00'                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  Step 3: Process Only New Records                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Apply transformations and merge into target                         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Benefits:**
- Efficient processing of real-time streaming sources using batch semantics
- No need for Structured Streaming checkpoints
- Predictable resource usage and costs
- Easy recovery and reprocessing

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

### 3.3 SCD Type 2 (History Tracking Pattern)

SCD Type 2 maintains full history of changes without surrogate keys.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SCD Type 2 - History Tracking Pattern                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Target Table (Before):                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ customer_id │ name │ email    │ effective_start │ effective_end │ is_current │
│  ├─────────────┼──────┼──────────┼─────────────────┼───────────────┼────────────┤
│  │ 1           │ John │ old@mail │ 2024-01-01      │ 9999-12-31    │ true       │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Incoming Change: customer_id=1, name="John Smith"                           │
│                                                                              │
│  Processing Steps:                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  1. Detect change: name "John" → "John Smith"                        │    │
│  │  2. Close old record: UPDATE effective_end, is_current=false         │    │
│  │  3. Insert new version with is_current=true                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Target Table (After):                                                       │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ customer_id │ name       │ email    │ eff_start  │ eff_end    │ is_current │
│  ├─────────────┼────────────┼──────────┼────────────┼────────────┼────────────┤
│  │ 1           │ John       │ old@mail │ 2024-01-01 │ 2024-01-15 │ false      │
│  │ 1           │ John Smith │ old@mail │ 2024-01-15 │ 9999-12-31 │ true       │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key Design Decisions:**
- No surrogate keys - uses natural business keys
- Batch-optimized staging/union approach (not row-by-row)
- Configurable columns to track for changes
- Separate merge operations for closing and inserting

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
  "watermark_column": "ingestion_ts",
  "target_watermark_column": "processing_timestamp",
  "scd2_columns": {
    "effective_start_date": "effective_start_date",
    "effective_end_date": "effective_end_date",
    "is_current": "is_current"
  },
  "track_columns": ["name", "email", "address"],
  "transformation_sql_path": "conf/sql/customers_transform.sql",
  "enabled": true
}
```

### 5.2 Global Settings

```json
{
  "catalog": "main",
  "schema_bronze": "bronze",
  "schema_silver": "silver",
  "default_watermark_column": "ingestion_ts",
  "scd2_end_date_value": "9999-12-31 23:59:59",
  "log_level": "INFO"
}
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
│  │  - Target envs      │ ──────────▶ │  └── Configuration files        │    │
│  └─────────────────────┘             └─────────────────────────────────┘    │
│                                                                              │
│  Targets:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  dev:  [dev username] prefixed, paused schedules                    │    │
│  │  prod: Production deployment, active schedules                       │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
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
│  │                    ▼                   ▼                             │    │
│  │  ┌─────────────────────┐  ┌─────────────────────────────────────┐   │    │
│  │  │ Log success         │  │ Log error with details              │   │    │
│  │  │ Record metrics      │  │ Mark table as failed                │   │    │
│  │  │ Continue to next    │  │ Continue to next table              │   │    │
│  │  └─────────────────────┘  │ (Isolation - don't fail entire job) │   │    │
│  │                           └─────────────────────────────────────┘   │    │
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
