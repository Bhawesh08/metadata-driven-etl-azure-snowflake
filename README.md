# 🔷 Metadata-Driven ETL Pipeline — Azure + Snowflake

![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Azure Data Lake](https://img.shields.io/badge/ADLS%20Gen2-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![SQL](https://img.shields.io/badge/Azure%20SQL-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)

> **Author:** Bhawesh Joshi  
> **Stack:** Azure Data Factory · ADLS Gen2 · Azure SQL · Snowflake  
> **Pattern:** Metadata-Driven · CDC Watermarking · ELT · Medallion Architecture

---

##  Project Overview

A production-grade, **metadata-driven ELT pipeline** built on Azure that ingests data 
from multiple source systems into Snowflake using a **fully configuration-based approach** 
— no hardcoded table names, no hardcoded queries, no manual pipeline changes per table.

All ingestion behavior is controlled via a **central metadata control table** in Azure SQL.  
Adding a new table to the pipeline requires **zero code changes** — just a new row in the 
config table.

---

##  Key Features

| Feature | Detail |
|---|---|
| ✅ Metadata-Driven | All pipeline behavior controlled via Azure SQL config table |
| ✅ CDC Watermarking | Incremental loads using high-watermark pattern |
| ✅ Full & Incremental | Supports FULL and CDC load types per table |
| ✅ Parallel Execution | ForEach loop with configurable batch concurrency |
| ✅ Dynamic COPY INTO | Column list auto-discovered from Snowflake INFORMATION_SCHEMA |
| ✅ Audit Columns | Every record stamped with pipeline, batch, run, load type metadata |
| ✅ Error Handling | Full failure logging with error messages back to Azure SQL |
| ✅ Observability | Pipeline run log tracks start, end, duration, rows extracted/loaded |
| ✅ Medallion Architecture | Raw → Staging → (Consumption ready) |

---

##  Architecture
> 📐 See full interactive diagram → [`architecture/architecture_diagram.md`](./architecture/architecture_diagram.md)


---

## 🗂️ Repository Structure

metadata-driven-etl-azure-snowflake/
│
├── README.md
├── architecture/
│   └── architecture_diagram.md
│
├── sql/
│   ├── metadata_tables/
│   │   ├── 01_pipeline_config.sql
│   │   ├── 02_cdc_watermark.sql
│   │   └── 03_pipeline_run_log.sql
│   └── stored_procedures/
│       ├── usp_log_pipeline_start.sql
│       ├── usp_update_watermark.sql
│       └── usp_log_pipeline_end.sql
│
├── adf/
│   ├── datasets/
│   └── pipelines/
│
├── sample_data/
│   └── pipeline_config_sample.csv
│
└── docs/
    ├── design_decisions.md
    └── setup_guide.md

    
## ⚙️ How It Works

### 1️⃣ Metadata Control Table
Every table to be ingested is registered in `dbo.pipeline_config`:
sql: INSERT INTO dbo.pipeline_config (
    pipeline_name, source_schema, source_table,
    load_type, cdc_column, sink_schema, sink_table,
    adls_container, adls_folder_path, is_active
)
VALUES (
    'PL_ORDERS', 'dbo', 'orders',
    'CDC', 'modified_date', 'RAW', 'ORDERS',
    'raw', 'supply_chain/orders', 1
);
> ☝️ That's it. No pipeline changes needed. The table is now ingested automatically.

---

### 2️⃣ CDC Watermarking
Each table maintains its own watermark in `dbo.cdc_watermark`:
First Run  → watermark = '1900-01-01' → FULL extract
Subsequent → watermark = last successful run datetime → INCREMENTAL


---

### 3️⃣ Dynamic Column Discovery
Before loading to Snowflake, the pipeline queries `INFORMATION_SCHEMA.COLUMNS`  
to get the exact column list — making COPY INTO **fully dynamic**:

sql
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'RAW'
  AND TABLE_NAME = 'ORDERS'
ORDER BY ORDINAL_POSITION;

---

### 4️⃣ Audit Columns
Every record loaded into Snowflake is enriched with:

| Column | Description |
|---|---|
| `_PIPELINE_NAME` | Which pipeline loaded this record |
| `_BATCH_ID` | Batch identifier (BATCH-yyyyMMdd-HHmmss) |
| `_RUN_ID` | ADF pipeline run ID |
| `_LOAD_TYPE` | FULL or CDC |
| `_SOURCE_FILE` | ADLS file path via METADATA$FILENAME |

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Azure Data Factory (ADF) |
| Source | Azure SQL Database |
| Landing Zone | Azure Data Lake Storage Gen2 (Parquet) |
| Data Warehouse | Snowflake |
| Metadata Store | Azure SQL Database |
| File Format | Apache Parquet |
| Load Pattern | ELT (Extract → Land → Transform in Snowflake) |
| IaC | (Planned) |

---

## 📊 Pipeline Observability

Every pipeline run is tracked end-to-end:

sql:
SELECT
    pipeline_name,
    status,
    rows_extracted,
    rows_loaded,
    duration_seconds,
    error_message,
    start_time,
    end_time
FROM dbo.pipeline_run_log
ORDER BY start_time DESC;



---

## 🚀 Setup Guide

See → [`docs/setup_guide.md`](./docs/setup_guide.md) for full environment setup instructions.

---

## 📐 Design Decisions

See → [`docs/design_decisions.md`](./docs/design_decisions.md) for architecture  
choices, trade-offs and patterns used.

---

## 📬 Contact

**Bhawesh Joshi**  
LinkedIn-(https://www.linkedin.com/in/bhawesh-joshi/) 

---

