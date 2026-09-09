# Spotify Azure Lakehouse — End-to-End Data Engineering

## 📖 Overview

This project implements an **industry-style Azure Lakehouse architecture** for Spotify streaming analytics.

The platform ingests relational source data using **Azure Data Factory**, stores data in **Azure Data Lake Storage Gen2**, and processes the data using **Azure Databricks, Apache Spark, PySpark and Delta Lake**.

The data moves through a **Medallion Architecture**:

```text
SOURCE
   ↓
ADF
   ↓
ADLS GEN2
   ↓
BRONZE
   ↓
SILVER
   ↓
GOLD
   ↓
ANALYTICS
```

The implementation focuses on the engineering problems commonly encountered in production data platforms:

```text
• Metadata-driven ingestion
• Initial data loading
• Incremental data loading
• CDC / watermark processing
• Data cleansing
• Deduplication
• Data validation
• Delta MERGE
• SCD Type 1
• SCD Type 2
• Dimensional modeling
• Pipeline monitoring
• Fault recovery
```

---

# 🏛️ High-Level Architecture

```text
                         SOURCE DATABASE
                               │
                               │
             ┌─────────────────┴─────────────────┐
             │                                   │
             ▼                                   ▼
       Dimension Tables                    Fact Tables
       ───────────────                    ───────────
       DimUser                            FactStream
       DimTrack
       DimArtist
       DimDate
             │
             └─────────────────┬─────────────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Azure Data Factory │
                    │                     │
                    │ Metadata Lookup     │
                    │ ForEach             │
                    │ Dynamic SQL         │
                    │ Copy Activity       │
                    │ Watermark / CDC     │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     ADLS Gen2       │
                    │                     │
                    │ Landing / Raw Data  │
                    └──────────┬──────────┘
                               │
                               ▼
        ╔════════════════════════════════════════════╗
        ║              DATABRICKS                    ║
        ║                                            ║
        ║  ┌────────────────────────────────────┐    ║
        ║  │ BRONZE                             │    ║
        ║  │                                    │    ║
        ║  │ Raw source-aligned Delta tables   │    ║
        ║  └────────────────┬───────────────────┘    ║
        ║                   │                         ║
        ║                   ▼                         ║
        ║  ┌────────────────────────────────────┐    ║
        ║  │ SILVER                             │    ║
        ║  │                                    │    ║
        ║  │ Cleaned / Validated / CDC / SCD   │    ║
        ║  └────────────────┬───────────────────┘    ║
        ║                   │                         ║
        ║                   ▼                         ║
        ║  ┌────────────────────────────────────┐    ║
        ║  │ GOLD                               │    ║
        ║  │                                    │    ║
        ║  │ Business / Dimensional / KPI data │    ║
        ║  └────────────────┬───────────────────┘    ║
        ╚═══════════════════╪════════════════════════╝
                            │
                            ▼
                   BI / Analytics / SQL
```

---

# 🔹 1. Source Data

The source system contains:

```text
dbo.DimUser
dbo.DimTrack
dbo.DimArtist
dbo.DimDate
dbo.FactStream
```

The tables represent:

| Table      | Responsibility           |
| ---------- | ------------------------ |
| DimUser    | User information         |
| DimTrack   | Track/song information   |
| DimArtist  | Artist information       |
| DimDate    | Calendar/date attributes |
| FactStream | Streaming events         |

`FactStream` acts as the central event/fact table, while the dimensions provide descriptive context.

---

# 🔹 2. Metadata-Driven ADF

The pipeline avoids hardcoding individual ingestion pipelines.

Instead, metadata defines the ingestion behavior.

Example:

```json
{
  "schema": "dbo",
  "table": "DimUser",
  "cdc_col": "updated_at",
  "from_date": ""
}
```

The same structure is used for other tables.

```text
Metadata
   │
   ▼
Lookup
   │
   ▼
ForEach
   │
   ├── DimUser
   ├── DimTrack
   ├── DimDate
   ├── DimArtist
   └── FactStream
```

### Why metadata-driven?

Without metadata:

```text
Pipeline_DimUser
Pipeline_DimTrack
Pipeline_DimArtist
Pipeline_DimDate
Pipeline_FactStream
```

With metadata:

```text
ONE REUSABLE PIPELINE
        +
CONFIGURATION
```

Adding another table becomes primarily a metadata/configuration change instead of creating another pipeline.

---

# 🔹 3. CDC / Watermark Strategy

The project uses a CDC column defined per source table.

Examples:

```text
DimUser    → updated_at
DimTrack   → updated_at
DimArtist  → updated_at
DimDate    → date
FactStream → stream_timestamp
```

The CDC column determines which records should be extracted.

---

# 🔹 4. Initial Load

For the first execution:

```text
Previous Watermark = 1900-01-01
```

Example:

```sql
SELECT *
FROM dbo.DimUser
WHERE updated_at > '1900-01-01'
```

This effectively retrieves historical records.

```text
INITIAL LOAD

SOURCE
  │
  │ ALL AVAILABLE RECORDS
  ▼
ADF
  │
  ▼
ADLS
```

---

# 🔹 5. Incremental Load

After the first successful execution:

```text
Previous Watermark
        │
        ▼
2026-09-05 00:00:00
```

The next query becomes:

```sql
SELECT *
FROM dbo.DimUser
WHERE updated_at > '2026-09-05 00:00:00'
```

Only changed/new data is transferred.

```text
SOURCE
  │
  │ CHANGED RECORDS
  ▼
ADF
  │
  ▼
ADLS
```

This prevents unnecessary full extraction.

---

# 🥉 Bronze — Raw Data Layer

Bronze stores source-aligned data in Delta format.

Example:

```text
bronze.dim_user
bronze.dim_track
bronze.dim_artist
bronze.dim_date
bronze.fact_stream
```

Bronze transformation should be intentionally minimal.

Typical columns:

```text
user_id
name
country
updated_at
_ingestion_timestamp
_batch_id
_source_file
```

### Bronze responsibilities

```text
✓ Preserve source data
✓ Capture ingestion metadata
✓ Support replay/reprocessing
✓ Maintain raw history
✓ Provide recovery point
```

Bronze should generally not contain complex business transformations.

---

# 🥈 Silver — Trusted Data Layer

Silver converts raw data into reliable datasets.

```text
BRONZE
   │
   ├── Schema validation
   ├── Data type casting
   ├── Null handling
   ├── Deduplication
   ├── Standardization
   ├── Referential checks
   ├── CDC processing
   └── SCD processing
   │
   ▼
SILVER
```

Example:

### Bronze

```text
101 | Anujit | india
101 | Anujit | india
102 | Rahul  | India
```

### Silver

```text
101 | Anujit | India
102 | Rahul  | India
```

Silver becomes the trusted detailed dataset for downstream processing.

---

# 🥇 Gold — Business Data Layer

Gold applies business rules and produces analytics-ready datasets.

Example:

```text
silver.fact_stream
       +
silver.dim_user
       +
silver.dim_track
       +
silver.dim_artist
       +
silver.dim_date
       │
       ▼
      GOLD
```

Possible outputs:

```text
gold.user_stream_summary
gold.track_stream_summary
gold.artist_stream_summary
gold.daily_stream_summary
gold.country_stream_summary
```

Example:

```text
Artist      Total Streams
-------------------------
Artist A    125,000
Artist B     98,000
Artist C     72,000
```

---

# 🧱 Delta Lake

The Bronze, Silver and Gold tables can be implemented using **Delta Lake**.

Conceptually:

```text
                 DELTA TABLE
                      │
             ┌────────┴────────┐
             │                 │
       Data Files          _delta_log
       (Parquet)           Transactions
```

Delta provides:

```text
ACID Transactions
Schema Enforcement
Schema Evolution
MERGE / UPSERT
Time Travel
Reliable Batch Processing
Reliable Streaming Processing
```

---

# 🔀 Incremental Upsert

When Silver receives an incoming record:

```text
Incoming Record
      │
      ▼
Match Business Key
      │
   ┌──┴──┐
   │     │
Match   No Match
   │     │
 UPDATE INSERT
```

Example:

```sql
MERGE INTO silver.dim_user target
USING bronze.dim_user source
ON target.user_id = source.user_id

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *
```

This avoids blindly appending duplicate current-state records.

---

# 🔢 SCD Type 1

SCD1 maintains only the latest state.

```text
BEFORE

101 | India
```

After change:

```text
101 | UK
```

The old value is replaced.

Use SCD1 when historical versions are not required.

---

# 📚 SCD Type 2

SCD2 preserves history.

```text
user_id | country | valid_from | valid_to   | is_current
---------------------------------------------------------
101     | India   | 2026-01-01 | 2026-09-05 | false
101     | UK      | 2026-09-06 | NULL       | true
```

When a tracked attribute changes:

```text
CURRENT RECORD
      │
      ▼
Expire old version
      │
      ▼
Insert new version
```

This allows historical questions such as:

```text
"What was the user's country in March?"
```

rather than only:

```text
"What is the user's country now?"
```

---

# 🔄 Complete Data Lifecycle

```text
             SOURCE
                │
                ▼
       ┌────────────────┐
       │      ADF       │
       │                │
       │ Metadata       │
       │ Lookup         │
       │ ForEach        │
       │ CDC/Watermark  │
       │ Copy           │
       └───────┬────────┘
               │
               ▼
            ADLS GEN2
               │
               ▼
        ┌──────────────┐
        │    BRONZE    │
        │ Raw Delta    │
        └──────┬───────┘
               │
       Clean / Validate
       Deduplicate / CDC
               │
               ▼
        ┌──────────────┐
        │    SILVER    │
        │ Trusted Data │
        └──────┬───────┘
               │
        Join / Aggregate
        Business Logic
               │
               ▼
        ┌──────────────┐
        │     GOLD     │
        │ Business Data│
        └──────┬───────┘
               │
               ▼
       BI / Analytics
```

---

# 📊 Operational Monitoring

## ADF Monitoring

ADF provides visibility into:

```text
Pipeline execution
Activity execution
Rows read
Rows written
Duration
Failures
Retries
```

## Databricks Monitoring

Databricks provides:

```text
Job execution
Spark stages
Task execution
Cluster metrics
Notebook logs
Exceptions
```

## Delta Transaction Log

```text
_delta_log
```

is used for Delta table transaction/state management.

## Pipeline Event Logs

Lakeflow/Databricks pipeline event logs can be used for:

```text
Execution monitoring
Data quality
Lineage
Pipeline events
Error analysis
```

---

# ⭐ Engineering Design Principles

### Separation of Responsibilities

```text
ADF
→ Ingest + Orchestrate

ADLS
→ Store

Databricks
→ Process

Bronze
→ Preserve

Silver
→ Trust

Gold
→ Serve
```

### Reusability

Metadata-driven ingestion avoids creating separate pipelines for each table.

### Incremental Processing

CDC/watermarks prevent repeated full extraction.

### Reliability

Delta transactions and MERGE support reliable updates.

### Recoverability

Bronze retains source-aligned data so downstream layers can be rebuilt.

### Scalability

Spark distributes processing across the Databricks cluster.

### Maintainability

The separation between ingestion, refinement and business logic makes individual components easier to modify and troubleshoot.

---

# 🛠️ Technology Stack

```text
Azure Data Factory
Azure Data Lake Storage Gen2
Azure Databricks
Apache Spark
PySpark
Spark SQL
Delta Lake
Lakeflow Declarative Pipelines / DLT
SQL
Git
GitHub
```

---

# 📁 Conceptual Repository Structure

```text
spotify_azure_project/
│
├── adf/
│   ├── pipelines/
│   ├── datasets/
│   └── linked_services/
│
├── databricks/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── jobs/
│
├── config/
│   ├── loop_input
│   └── cdc.json
│
├── bundle/
│   └── databricks.yml
│
└── README.md
```

---

# 🎯 What This Project Demonstrates

```text
Azure Cloud Data Engineering
        +
Metadata-Driven Architecture
        +
Incremental Data Processing
        +
CDC / Watermark
        +
Medallion Architecture
        +
Apache Spark
        +
Delta Lake
        +
SCD Type 1 / Type 2
        +
Dimensional Modeling
        +
Data Quality
        +
Monitoring
        +
Git / CI-CD Ready Architecture
```

---

# 💼 Interview Summary

> **This project implements an end-to-end Azure Lakehouse pipeline for Spotify streaming data. Azure Data Factory provides metadata-driven orchestration and incremental ingestion using table-specific CDC/watermark columns. Data is landed in ADLS Gen2 and processed in Azure Databricks following the Medallion Architecture. Bronze preserves source-aligned Delta data, Silver performs cleansing, validation, deduplication and CDC/SCD processing, while Gold provides business-ready fact, dimension and aggregated datasets. Delta Lake provides ACID transactions and reliable MERGE/upsert capabilities. The architecture is designed to be reusable, scalable, auditable and suitable for production-style incremental data processing.**
