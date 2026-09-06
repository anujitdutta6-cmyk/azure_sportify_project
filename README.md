# 🎵 Spotify Azure Data Engineering Platform

An end-to-end **Azure Data Engineering project** designed using an industry-style **Medallion Architecture** to ingest, process, transform, and serve Spotify streaming data for analytics.

The solution uses **Azure Data Factory (ADF)** for metadata-driven ingestion and orchestration, **Azure Data Lake Storage Gen2 (ADLS Gen2)** as the data lake, and **Azure Databricks with Delta Lake** for scalable data processing and transformation.

---

## 🏗️ Architecture

```text
                         ┌──────────────────────┐
                         │    SOURCE SYSTEM     │
                         │                      │
                         │  Spotify SQL Tables  │
                         │                      │
                         │ DimUser              │
                         │ DimTrack             │
                         │ DimArtist            │
                         │ DimDate              │
                         │ FactStream            │
                         └──────────┬───────────┘
                                    │
                                    │ Initial / Incremental
                                    │ CDC / Watermark
                                    ▼
                         ┌──────────────────────┐
                         │   AZURE DATA FACTORY │
                         │                      │
                         │ Lookup Metadata      │
                         │ ForEach              │
                         │ Dynamic Queries      │
                         │ Copy Activity        │
                         │ Pipeline Orchestration│
                         └──────────┬───────────┘
                                    │
                                    │ Data Ingestion
                                    ▼
                         ┌──────────────────────┐
                         │      ADLS GEN2       │
                         │                      │
                         │ Landing / Raw Data   │
                         └──────────┬───────────┘
                                    │
                                    ▼
                ╔══════════════════════════════════════╗
                ║          AZURE DATABRICKS            ║
                ║                                      ║
                ║       MEDALLION ARCHITECTURE         ║
                ║                                      ║
                ║  ┌───────────────┐                   ║
                ║  │    BRONZE     │                   ║
                ║  │ Raw Delta     │                   ║
                ║  └───────┬───────┘                   ║
                ║          │                            ║
                ║          ▼                            ║
                ║  ┌───────────────┐                   ║
                ║  │    SILVER     │                   ║
                ║  │ Clean +       │                   ║
                ║  │ Validated     │                   ║
                ║  └───────┬───────┘                   ║
                ║          │                            ║
                ║          ▼                            ║
                ║  ┌───────────────┐                   ║
                ║  │     GOLD      │                   ║
                ║  │ Business /    │                   ║
                ║  │ Analytics     │                   ║
                ║  └───────────────┘                   ║
                ╚══════════════════╤═══════════════════╝
                                   │
                                   ▼
                         ┌──────────────────────┐
                         │  ANALYTICS / BI      │
                         │                      │
                         │ SQL / Power BI / BI  │
                         └──────────────────────┘
```

---

# 📌 Project Overview

This project demonstrates how a modern cloud data platform can process Spotify streaming data using an **ELT-oriented architecture**.

The pipeline supports:

* Metadata-driven ingestion
* Initial/full data load
* Incremental data load
* CDC/watermark-based extraction
* Bronze/Silver/Gold processing
* Delta Lake
* SCD Type 1
* SCD Type 2
* Data cleansing and validation
* Deduplication
* Dimensional modeling
* Fact and dimension processing
* Incremental transformations
* Pipeline monitoring and logging

---

# 🔄 End-to-End Data Flow

```text
Source Database
      │
      ▼
ADF Metadata Configuration
      │
      ▼
Lookup
      │
      ▼
ForEach Table
      │
      ▼
Dynamic Copy Activity
      │
      ▼
ADLS Gen2
      │
      ▼
Databricks Bronze
      │
      ▼
Databricks Silver
      │
      ▼
Databricks Gold
      │
      ▼
Analytics / BI
```

---

# 1️⃣ Source Layer

The source contains Spotify-related relational data.

### Dimension Tables

```text
DimUser
DimTrack
DimArtist
DimDate
```

### Fact Table

```text
FactStream
```

`FactStream` represents streaming events, while dimension tables provide descriptive information about users, tracks, artists, and dates.

---

# 2️⃣ Azure Data Factory

ADF is responsible for **orchestration and data movement**.

Instead of creating a separate pipeline for every table, the project follows a **metadata-driven ingestion pattern**.

Example metadata:

```json
{
  "schema": "dbo",
  "table": "DimUser",
  "cdc_col": "updated_at",
  "from_date": ""
}
```

The metadata defines:

| Field       | Purpose                                |
| ----------- | -------------------------------------- |
| `schema`    | Source schema                          |
| `table`     | Source table                           |
| `cdc_col`   | Column used for incremental processing |
| `from_date` | Initial watermark/start date           |

---

# 🔁 Metadata-Driven Processing

ADF performs:

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
   ├── DimArtist
   ├── DimDate
   └── FactStream
```

The Copy Activity dynamically builds the source query based on the current table.

This makes the pipeline reusable and scalable.

---

# 🔄 Initial Load

During the first execution, there may be no previous watermark.

Example:

```text
CDC / Watermark = 1900-01-01
```

The pipeline effectively performs:

```sql
SELECT *
FROM dbo.DimUser
WHERE updated_at > '1900-01-01'
```

Therefore, existing historical data is loaded.

```text
SOURCE
   │
   │ FULL LOAD
   ▼
ADLS
```

---

# ⚡ Incremental Load

After the initial load, the pipeline only extracts newly inserted or updated records.

Example:

```text
Last Run Time:
2026-09-05 00:00:00
```

ADF can generate:

```sql
SELECT *
FROM dbo.DimUser
WHERE updated_at > '2026-09-05 00:00:00'
```

Instead of processing millions of rows, only changed records are transferred.

### Benefits

* Reduced source-system load
* Reduced network traffic
* Faster execution
* Lower compute/storage cost
* Scalable ingestion

---

# 3️⃣ ADLS Gen2

ADLS Gen2 acts as the centralized data lake storage layer.

Conceptually:

```text
ADLS
│
├── landing/
│
├── bronze/
│
├── silver/
│
└── gold/
```

ADF writes incoming source data into the lake.

Databricks then processes the data.

---

# 4️⃣ Databricks — Medallion Architecture

The Databricks processing layer follows:

```text
BRONZE
   │
   ▼
SILVER
   │
   ▼
GOLD
```

Each layer has a separate responsibility.

---

# 🥉 Bronze Layer

### Purpose

Bronze represents the **raw/source-aligned layer**.

Main principle:

> Preserve what was received from the source with minimal transformation.

Example:

```text
bronze.dim_user

user_id | name   | country | updated_at
-----------------------------------------
101     | Anujit | India   | 2026-09-05
102     | Rahul  | India   | 2026-09-05
```

Typical Bronze operations:

* Raw ingestion
* Schema handling
* Ingestion metadata
* Source tracking
* Append/incremental ingestion

Example technical metadata:

```text
_ingestion_timestamp
_source_file
_batch_id
```

### Why Bronze?

Bronze acts as a **recovery and historical source layer**.

If Silver transformation logic changes:

```text
Bronze
   │
   ├── Silver Version 1
   │
   ├── Silver Version 2
   │
   └── Silver Version 3
```

Silver can be rebuilt without requesting the data again from the source.

---

# 🥈 Silver Layer

Silver is the **trusted and refined data layer**.

Typical operations:

```text
Bronze
  │
  ├── Data cleansing
  ├── Data validation
  ├── Deduplication
  ├── Data type conversion
  ├── Null handling
  ├── Standardization
  ├── CDC processing
  ├── SCD processing
  └── Business joins
  │
  ▼
Silver
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

Duplicates are removed and values are standardized.

---

# 🥇 Gold Layer

Gold contains **business-ready datasets**.

The Gold layer performs:

* Business transformations
* Aggregations
* Dimensional modeling
* Fact/dimension relationships
* KPI calculations
* Reporting-oriented transformations

Example:

```text
gold.artist_stream_summary

artist_id | artist_name | total_streams
-----------------------------------------
A001      | Artist A    | 125000
A002      | Artist B    | 98000
```

Possible Gold datasets:

```text
gold.user_stream_summary
gold.artist_stream_summary
gold.track_stream_summary
gold.daily_stream_summary
gold.country_stream_summary
```

---

# ⭐ Why Three Layers?

| Layer  | Main Question                |
| ------ | ---------------------------- |
| Bronze | What did the source send?    |
| Silver | Can we trust/use the data?   |
| Gold   | What does the business need? |

```text
Bronze = RAW
Silver = TRUSTED
Gold   = BUSINESS
```

---

# 🧱 Delta Lake

Databricks uses **Delta Lake** as the storage format for reliable data processing.

Conceptually:

```text
Delta Table
│
├── Parquet Data Files
│
└── _delta_log
```

Delta Lake provides capabilities such as:

* ACID transactions
* Schema enforcement
* Schema evolution
* MERGE/upsert
* Time travel
* Reliable batch processing
* Reliable streaming processing

---

# 📝 Delta Transaction Log

Every Delta table contains:

```text
_delta_log/
```

The transaction log records table changes and helps Delta determine the current state of the table.

Therefore:

```text
Delta Table
     │
     ├── Data
     │
     └── Transaction History
```

This is different from application/pipeline logs.

---

# 🔀 MERGE / UPSERT

For incremental processing, Delta `MERGE` can identify whether an incoming record already exists.

Conceptually:

```text
Incoming Record
      │
      ▼
Does Key Exist?
    /       \
  YES       NO
   │         │
UPDATE     INSERT
```

Example:

```sql
MERGE INTO silver.dim_user AS target
USING bronze.dim_user AS source
ON target.user_id = source.user_id

WHEN MATCHED THEN
  UPDATE SET *

WHEN NOT MATCHED THEN
  INSERT *
```

---

# 🔢 SCD Type 1

SCD1 keeps only the latest value.

Before:

```text
101 | Anujit | India
```

After:

```text
101 | Anujit | UK
```

The previous value is overwritten.

### Suitable for:

* Current attributes
* Non-historical data
* Attributes where previous values are not required

---

# 📚 SCD Type 2

SCD2 maintains historical versions.

Example:

```text
user_id | country | start_date | end_date   | current
------------------------------------------------------
101     | India   | 2026-01-01 | 2026-09-05 | false
101     | UK      | 2026-09-06 | NULL       | true
```

When the attribute changes:

```text
Old Record
    │
    ▼
Expire old version
    │
    ▼
Insert new version
```

SCD2 is useful when historical analysis is required.

---

# 🔄 Complete Incremental Flow

```text
SOURCE
  │
  │ updated_at > last_watermark
  ▼
ADF
  │
  ▼
ADLS
  │
  ▼
BRONZE
  │
  │ Clean / Validate / Deduplicate
  ▼
SILVER
  │
  │ MERGE / SCD / Business Transformation
  ▼
GOLD
  │
  ▼
BI / Analytics
```

---

# 🚦 Initial vs Incremental

### Initial

```text
Source
  │
  │ FULL
  ▼
Bronze
  │
  ▼
Silver
  │
  ▼
Gold
```

### Incremental

```text
Source
  │
  │ ONLY CHANGES
  ▼
Bronze
  │
  │ PROCESS CHANGES
  ▼
Silver
  │
  │ UPDATE AFFECTED DATA
  ▼
Gold
```

---

# 🔍 Data Quality

Data quality checks are applied during refinement.

Examples:

```text
user_id IS NOT NULL
track_id IS NOT NULL
artist_id IS NOT NULL
stream_timestamp IS NOT NULL
```

Invalid records can be:

```text
Rejected
Quarantined
Logged
```

while valid records continue through the pipeline.

---

# 📊 Logging & Monitoring

The architecture provides monitoring at multiple levels.

### ADF

Tracks:

```text
Pipeline status
Activity status
Rows read
Rows written
Execution duration
Errors
```

### Databricks

Tracks:

```text
Job execution
Spark stages
Task failures
Cluster information
Notebook errors
```

### Delta

```text
_delta_log
```

Tracks Delta table transactions.

### Pipeline Event Logs

Lakeflow/Databricks pipeline event logs can provide:

```text
Pipeline events
Data quality results
Lineage
Execution information
Errors
```

---

# 🗂️ Data Model

The project follows a dimensional/star-schema-oriented model.

```text
                  DimUser
                     │
                     │
DimDate ──────── FactStream ──────── DimTrack
                     │
                     │
                 DimArtist
```

### Fact

```text
FactStream
```

Contains streaming events/measures.

### Dimensions

```text
DimUser
DimTrack
DimArtist
DimDate
```

Provide descriptive attributes used for analysis.

---

# 🛠️ Technology Stack

| Technology         | Purpose                    |
| ------------------ | -------------------------- |
| Azure Data Factory | Orchestration & ingestion  |
| ADLS Gen2          | Data lake storage          |
| Azure Databricks   | Data processing            |
| Apache Spark       | Distributed processing     |
| PySpark            | Transformation             |
| Spark SQL          | SQL-based transformation   |
| Delta Lake         | Reliable lakehouse storage |
| Lakeflow / DLT     | Declarative data pipelines |
| Git/GitHub         | Source control & CI/CD     |
| Power BI / SQL     | Analytics layer            |

---

# 🎯 Key Engineering Techniques

This project demonstrates:

```text
✓ Metadata-driven pipelines
✓ Dynamic ingestion
✓ Initial/full load
✓ Incremental load
✓ CDC / watermark processing
✓ Medallion Architecture
✓ Delta Lake
✓ Delta MERGE
✓ SCD Type 1
✓ SCD Type 2
✓ Deduplication
✓ Data quality
✓ Dimensional modeling
✓ Fact & dimension design
✓ Pipeline monitoring
✓ Git-based development
```

---

# 💡 Why This Architecture?

The architecture separates responsibilities:

```text
ADF
 ↓
INGEST + ORCHESTRATE

ADLS
 ↓
STORE

DATABRICKS
 ↓
PROCESS + TRANSFORM

BRONZE
 ↓
RAW

SILVER
 ↓
TRUSTED

GOLD
 ↓
BUSINESS

BI
 ↓
CONSUME
```

This separation makes the platform:

* Scalable
* Maintainable
* Reusable
* Auditable
* Fault-tolerant
* Suitable for incremental processing
* Easier to troubleshoot
* Easier to extend with additional source tables

---

# 🚀 Production-Style Extension

For a production implementation, this architecture can be extended with:

```text
Azure Key Vault
       │
       ▼
Managed Identity
       │
       ▼
ADF ── ADLS ── Databricks
                  │
                  ├── Unity Catalog
                  ├── Delta Lake
                  ├── Lakeflow
                  ├── Data Quality
                  ├── Monitoring
                  └── CI/CD
```

Additional production capabilities can include:

* Unity Catalog governance
* RBAC
* Azure Key Vault secrets
* Managed identities
* Schema evolution
* Data lineage
* CI/CD
* Automated testing
* Retry/recovery
* Alerting
* Cost optimization

---

# 👨‍💻 Author

**Anujit Dutta**

Azure Data Engineering | Databricks | PySpark | ADF | SQL | Informatica

---

## 📌 Project Objective

The primary objective of this project is to demonstrate an end-to-end **cloud data engineering solution** using Azure services and Databricks, with emphasis on reusable ingestion, incremental processing, Medallion Architecture, Delta Lake, CDC/SCD processing, and analytics-ready data modeling.
