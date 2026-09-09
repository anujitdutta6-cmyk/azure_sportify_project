# 🎵 Spotify Azure Data Engineering Project

## 📌 Overview

This project demonstrates an end-to-end Azure Data Engineering solution for processing Spotify-style streaming data using **Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Delta Lake, Structured Streaming, Unity Catalog, and Lakeflow Declarative Pipelines (formerly Delta Live Tables / DLT).**

The project follows the **Medallion Architecture**:

```text
Source
  ↓
Azure Data Factory
  ↓
Bronze
  ↓
Silver
  ↓
Lakeflow / DLT Pipeline
  ↓
AUTO CDC
  ↓
Curated Fact & Dimension Tables
```

The main purpose of this project is to demonstrate:

* Cloud data ingestion
* Medallion architecture
* Incremental data processing
* Structured Streaming
* Delta Lake
* Unity Catalog
* Change Data Capture (CDC)
* SCD Type 1
* SCD Type 2
* Fact and Dimension modelling
* Metadata-driven processing
* Azure Databricks pipeline orchestration

---

# 🏗️ Architecture

## High-Level Architecture

```text
                 ┌──────────────────────┐
                 │      Source Data     │
                 │  Spotify-style data  │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │ Azure Data Factory   │
                 │ Ingestion/Orchestration
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │       BRONZE         │
                 │ Raw source data      │
                 │ ADLS / Delta         │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │    Databricks/Spark  │
                 │ Structured Streaming │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │       SILVER         │
                 │ Cleaned & transformed│
                 │ Streaming Delta      │
                 │ + _delta_log         │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │    Unity Catalog     │
                 │   Silver Tables      │
                 └──────────┬───────────┘
                            │
                            ▼
              ┌──────────────────────────────┐
              │ Lakeflow / DLT Pipeline      │
              │                              │
              │ Read Silver Catalog Tables   │
              │              ↓               │
              │           AUTO CDC           │
              │              ↓               │
              │       SCD Type 1 / SCD2      │
              └──────────────┬───────────────┘
                             │
                             ▼
                 ┌──────────────────────┐
                 │ Curated Data Layer   │
                 │                      │
                 │ FactStream → SCD1    │
                 │ Dimensions → SCD2    │
                 └──────────────────────┘
```

---

# 🥉 Bronze Layer

The Bronze layer contains the raw or source-aligned data.

### Responsibilities

* Receive data from the ingestion process
* Preserve source information
* Perform minimal transformation
* Provide a reliable input for downstream processing

The Bronze layer acts as the recoverable/raw starting point of the lakehouse.

```text
Source
  ↓
ADF
  ↓
Bronze
```

---

# 🥈 Silver Layer

The Silver layer is where the major cleansing and transformation takes place.

In this project, Silver is maintained using **Spark Structured Streaming**.

### Typical processing

```text
Bronze
  ↓
readStream
  ↓
Cleansing
  ↓
Filtering
  ↓
Schema / datatype handling
  ↓
Transformation
  ↓
writeStream
  ↓
Silver Delta Table
```

The Silver data is written using Delta format.

Therefore, the Silver storage contains:

```text
Delta data files
+
_delta_log
```

The `_delta_log` maintains Delta Lake transaction and table-state information.

### Important

`writeStream` does **not** mean SCD Type 1 or SCD Type 2.

It means that the data is being processed incrementally using Structured Streaming.

SCD logic is handled separately in the downstream CDC process.

---

# 🧠 Important Concept: Streaming ≠ SCD

This project uses two different concepts.

### Structured Streaming

Answers:

> "How do I process new data incrementally?"

Example:

```text
New Bronze records
       ↓
Spark detects new records
       ↓
Transformation
       ↓
Silver
```

### SCD

Answers:

> "What should happen when an existing business record changes?"

Example:

```text
User 101
India → USA
```

SCD Type 1:

```text
USA
```

SCD Type 2:

```text
India → USA
```

with historical records preserved.

---

# 🏅 Unity Catalog

The processed Silver data is registered and consumed through **Unity Catalog**.

Conceptually:

```text
Silver Delta Storage
       ↓
Unity Catalog
       ↓
silver.fact_stream
silver.dim_user
silver.dim_track
...
```

This allows downstream pipelines to work with tables rather than manually navigating storage folders.

---

# 🏆 Lakeflow / DLT Pipeline

The second major part of the architecture is the Lakeflow Declarative Pipeline.

Historically this technology was known as **Delta Live Tables (DLT)**.

The pipeline consumes the Silver catalog tables.

```text
Silver Catalog Table
        ↓
Lakeflow Pipeline
        ↓
Streaming Target
        ↓
AUTO CDC
```

The important point is:

> The Lakeflow pipeline is not simply "another Silver transformation."

Its purpose in this project is to apply change-data-processing logic and maintain the curated fact/dimension datasets.

---

# 🔄 AUTO CDC

AUTO CDC is used to apply source changes to the target tables.

Conceptually:

```text
Silver Change
     ↓
AUTO CDC
     ↓
Find business key
     ↓
Determine INSERT / UPDATE / DELETE
     ↓
Apply SCD strategy
```

AUTO CDC supports SCD Type 1 and SCD Type 2.

---

# 📊 FactStream — SCD Type 1

`FactStream` uses SCD Type 1.

Example:

### Before

```text
user_id | track_id | status
---------------------------
101     | T001     | active
```

Suppose the incoming change is:

```text
101 | T001 | inactive
```

The target becomes:

```text
user_id | track_id | status
---------------------------
101     | T001     | inactive
```

The previous value is overwritten.

### Why SCD Type 1?

For this fact dataset, the requirement is to maintain the latest/current value rather than preserve historical versions of the same record.

---

# 📚 Dimension Tables — SCD Type 2

Dimension tables use SCD Type 2 when historical changes need to be preserved.

Example:

### Initial record

```text
user_id | country | start_date | end_date
------------------------------------------
101     | India   | 2026-01-01 | NULL
```

Later:

```text
country = USA
```

The SCD2 result becomes:

```text
user_id | country | start_date | end_date
------------------------------------------
101     | India   | 2026-01-01 | 2026-09-05
101     | USA     | 2026-09-05 | NULL
```

Now the system knows both:

* What the value was
* What the current value is

This is useful for historical analytics.

---

# ❓ Where Is the Gold Layer?

This is an important architectural detail of this project.

A traditional Medallion implementation may look like:

```text
ADLS
 ├── bronze/
 ├── silver/
 └── gold/
```

However, this project does **not** rely on a manually managed `gold/` folder in ADLS.

Instead, the final curated datasets are maintained through the Lakeflow pipeline and exposed through the Databricks/Unity Catalog table layer.

Therefore:

```text
No visible ADLS/gold folder
        ≠
No Gold/business layer
```

The word **Gold** describes the logical responsibility of the dataset: business-ready / curated data.

The physical storage location is a separate concern.

With Unity Catalog managed tables, Databricks manages the underlying storage location rather than requiring developers to manually create and manage a traditional `/gold` directory.

---

# 🧩 Physical vs Logical Architecture

This distinction is important when explaining the project.

### Logical architecture

```text
Bronze
  ↓
Silver
  ↓
Gold / Curated
```

### Physical implementation in this project

```text
ADLS / Delta
     ↓
Bronze
     ↓
Silver Delta + _delta_log
     ↓
Unity Catalog
     ↓
Lakeflow Pipeline
     ↓
AUTO CDC
     ↓
Curated Fact & Dimension Tables
```

Therefore, the project demonstrates a **logical Gold layer without requiring a manually visible ADLS Gold folder**.

---

# 🔄 End-to-End Example

Imagine Spotify receives this event:

```text
user_id = 101
track_id = T001
artist = Artist_A
country = India
timestamp = 10:05
```

## Step 1 — ADF

ADF orchestrates ingestion.

```text
Source
 ↓
ADF
 ↓
Bronze
```

---

## Step 2 — Bronze

Raw data is stored.

```text
101 | T001 | Artist_A | India | 10:05
```

---

## Step 3 — Silver Streaming

Spark reads the Bronze data using Structured Streaming.

```text
readStream
    ↓
transform
    ↓
writeStream
```

The cleaned record is written to the Silver Delta table.

```text
Silver Delta
+
_delta_log
```

---

## Step 4 — Unity Catalog

Silver is available as a catalog table.

```text
catalog.silver.fact_stream
```

---

## Step 5 — Lakeflow Pipeline

The pipeline reads the Silver table.

```text
Silver Catalog
      ↓
Lakeflow
```

---

## Step 6 — AUTO CDC

AUTO CDC determines how the incoming change should be applied.

```text
INSERT
UPDATE
DELETE
```

---

## Step 7 — SCD Processing

For `FactStream`:

```text
SCD Type 1
```

For dimensions:

```text
SCD Type 2
```

---

## Step 8 — Final Curated Data

The final datasets are available as curated tables:

```text
FactStream
DimUser
DimTrack
DimArtist
DimDate
```

These are the datasets intended for downstream analytical/business consumption.

---

# 🗂️ Metadata-Driven Processing

The repository also contains configuration files such as:

```text
cdc.json
loop_input
empty.json
```

For example, `loop_input` defines datasets and CDC-related metadata:

```json
[
  {
    "schema": "dbo",
    "table": "DimUser",
    "cdc_col": "updated_at",
    "from_date": ""
  },
  {
    "schema": "dbo",
    "table": "DimTrack",
    "cdc_col": "updated_at",
    "from_date": ""
  },
  {
    "schema": "dbo",
    "table": "DimDate",
    "cdc_col": "date",
    "from_date": ""
  },
  {
    "schema": "dbo",
    "table": "DimArtist",
    "cdc_col": "updated_at",
    "from_date": ""
  },
  {
    "schema": "dbo",
    "table": "FactStream",
    "cdc_col": "stream_timestamp",
    "from_date": ""
  }
]
```

This allows the processing logic to be driven by configuration rather than hardcoding every table independently.

---

# 📂 Repository Structure

```text
spotify_azure_project/
│
├── Databricks Code/
│   └── spotify_dab.dbc
│
├── source_scripts/
│   └── Source/ingestion related scripts
│
├── cdc.json
│
├── empty.json
│
├── loop_input
│
└── README.md
```

---

# 🛠️ Technology Stack

| Technology                   | Purpose                                                     |
| ---------------------------- | ----------------------------------------------------------- |
| Azure Data Factory           | Data ingestion and orchestration                            |
| Azure Data Lake Storage Gen2 | Cloud data lake storage                                     |
| Azure Databricks             | Data engineering and Spark processing                       |
| Apache Spark                 | Distributed data processing                                 |
| Structured Streaming         | Incremental/streaming processing                            |
| Delta Lake                   | ACID tables and transaction management                      |
| Unity Catalog                | Table governance and discovery                              |
| Lakeflow / DLT               | Declarative pipeline processing                             |
| AUTO CDC                     | Change-data application                                     |
| SCD Type 1                   | Current-state dimensions/facts where history isn't required |
| SCD Type 2                   | Historical dimension tracking                               |
| GitHub                       | Source control                                              |

---

# 📌 Important Design Decisions

### Why Streaming in Silver?

Because the Silver layer needs to process incoming Bronze data incrementally.

### Why Delta?

Delta provides reliable table storage, transaction handling and metadata through `_delta_log`.

### Why Unity Catalog?

To provide governed table access and make datasets available through catalog/schema/table names.

### Why AUTO CDC?

To simplify applying inserts, updates and deletes to streaming targets.

### Why SCD Type 1 for FactStream?

The requirement is to maintain the latest state rather than preserve historical versions.

### Why SCD Type 2 for dimensions?

Dimensions such as users/artists/tracks may require historical tracking.

### Why no visible Gold folder?

Because Gold is a logical/business layer in this implementation. The curated datasets are maintained through the Lakeflow/Unity Catalog table architecture rather than a manually managed `ADLS/gold/` directory.

---

# 🎯 Key Interview Explanation

If asked:

**"Explain your Spotify Databricks architecture."**

A professional answer is:

> This project follows a Medallion-style lakehouse architecture. Azure Data Factory handles ingestion into the Bronze layer. Databricks Structured Streaming processes the Bronze data and incrementally maintains Silver Delta tables. The Silver datasets are registered and consumed through Unity Catalog. A downstream Lakeflow Declarative Pipeline consumes these Silver tables and uses AUTO CDC to apply changes to curated fact and dimension datasets. FactStream is maintained using SCD Type 1, while the dimension tables use SCD Type 2 to preserve history. The project does not require a manually visible Gold folder in ADLS because the curated business layer is represented through the governed Unity Catalog/Lakeflow tables.

---

# ⚠️ Important Interview Clarification

Do **not** say:

> "Silver means streaming and Gold means CDC."

Instead say:

> "Streaming and CDC are processing techniques, while Bronze, Silver and Gold describe logical data responsibilities."

This distinction is important.

A different production system could use:

```text
Bronze → Streaming
Silver → Streaming
Gold → Materialized Views
```

or:

```text
Bronze → Batch
Silver → Batch
Gold → Streaming
```

depending on business requirements.

---

# 🏢 Industry Perspective

This architecture is industry-relevant, but it should not be described as the only production architecture.

Databricks recommends Medallion architecture as a logical design pattern, with Bronze representing raw data, Silver refined/validated data and Gold business-ready data.

Current Databricks guidance also recommends Unity Catalog managed tables for lakehouse data, including Bronze, Silver and Gold, rather than requiring every layer to be represented by a manually managed cloud-storage folder.

Production architectures are normally designed around:

* Data latency requirements
* Source-system capabilities
* CDC availability
* Data volume
* Data quality
* Cost
* Governance
* Downstream consumers
* Historical requirements

The architecture should therefore be **requirement-driven rather than color-driven**.

---

# 🚀 Future Improvements

Potential production enhancements:

* Data quality expectations
* Unity Catalog lineage
* Pipeline monitoring
* Error handling and retry mechanisms
* CI/CD with Databricks Asset Bundles
* Automated testing
* Schema evolution handling
* Data observability
* Alerting
* Performance optimization
* Documentation of business keys and SCD rules
* Automated deployment across Dev / QA / Prod

---

# 📚 Key Concepts Learned

This project demonstrates practical understanding of:

* Azure Data Factory
* ADLS Gen2
* Databricks
* Apache Spark
* PySpark
* Structured Streaming
* Delta Lake
* Delta transaction logs
* Unity Catalog
* Lakeflow Declarative Pipelines
* AUTO CDC
* Change Data Capture
* SCD Type 1
* SCD Type 2
* Fact and Dimension modelling
* Medallion Architecture
* Metadata-driven pipelines
* Incremental data processing

---

# 👤 Author

**Anujit Dutta**

Azure / Data Engineering | Databricks | PySpark | SQL | ADF | Delta Lake

This repository is maintained as a hands-on Azure Data Engineering project and as a reference for understanding production-oriented lakehouse architecture.
