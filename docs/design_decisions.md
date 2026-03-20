
# Design Decisions & Architecture Rationale

> This document explains the key architectural choices made in this project,
> the reasoning behind them, and the trade-offs considered.

---

## 1. Why Metadata-Driven Architecture?

### Decision
All pipeline behavior is controlled via a central metadata table
`dbo.pipeline_config` in Azure SQL — not hardcoded in ADF pipelines.

### Rationale
| Problem with Hardcoded Pipelines | Solution with Metadata-Driven |
|---|---|
| Adding a new table = new pipeline | Adding a new table = new row in config |
| Change in load type = pipeline edit | Change load type = update one column |
| 50 tables = 50 pipelines to maintain | 50 tables = 1 pipeline + 50 config rows |
| Deployments needed for every change | No deployment needed for config changes |

### Trade-offs
- Slightly harder to debug (need to check config table)
- Requires good documentation of config columns
- Initial setup is more complex than simple pipelines

---

## 2. Why ELT Instead of ETL?

### Decision
Data is extracted and loaded as-is into Snowflake RAW schema first.
Transformations happen inside Snowflake via MERGE stored procedures.

### Rationale
- **Snowflake is purpose-built for transformations** — far more efficient
  than transforming in ADF
- **Raw data is preserved** — always possible to reprocess from RAW
- **Separation of concerns** — ingestion pipeline is decoupled from
  business transformation logic
- **Cost efficiency** — ADF compute is expensive for heavy transforms;
  Snowflake virtual warehouses scale independently

### Trade-offs
- Requires more storage (raw + staging layers)
- Business logic lives in Snowflake SPs, not centrally in ADF

---

## 3. Why CDC Watermarking Instead of Full Loads?

### Decision
Each table maintains a high-watermark value in `dbo.cdc_watermark`.
Incremental loads extract only records modified after the last watermark.

### Rationale
- **Performance** — Only changed records extracted, not entire table
- **Cost** — Fewer rows = less ADF pipeline run cost
- **Scalability** — Works for tables with millions of rows
- **Simple to implement** — Just needs a datetime or integer column on source

### How It Works
Run 1 (initial):  WHERE modified_date > '1900-01-01'  → full extract
Run 2 onwards:    WHERE modified_date > '2024-01-15 10:30:00'  → incremental


### Trade-offs
- Source table must have a reliable CDC column
- Late-arriving data (backdated records) can be missed
- FULL load option kept for tables without CDC columns

---

## 4. Why Dynamic Column Discovery?

### Decision
Before every COPY INTO, the pipeline queries Snowflake
`INFORMATION_SCHEMA.COLUMNS` to get the current column list dynamically.

### Rationale
- **Schema evolution safe** — New columns added to source table are
  automatically picked up without pipeline changes
- **No hardcoding** — Works for any table in the metadata config
- **Audit columns** — Business columns + audit columns combined dynamically

### How It Works
sql
-- Pipeline queries this at runtime
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'RAW'
  AND TABLE_NAME = 'ORDERS'
ORDER BY ORDINAL_POSITION;


-- Result used to build dynamic COPY INTO
COPY INTO RAW.ORDERS (
    order_id, customer_id, order_date, ...,  -- from INFORMATION_SCHEMA
    _PIPELINE_NAME, _BATCH_ID, _RUN_ID, _LOAD_TYPE, _SOURCE_FILE  -- audit
)

### Trade-offs
- Extra Snowflake query per pipeline run
- Small latency overhead (negligible)

---

## 5. Why ADLS Gen2 as Landing Zone?

### Decision
Data is first written to ADLS Gen2 as Parquet files before loading
to Snowflake, rather than direct SQL-to-Snowflake copy.

### Rationale
- **Decoupling** — Source system is not directly connected to Snowflake
- **Replayability** — Parquet files in ADLS can be reloaded if Snowflake
  load fails, without re-extracting from source
- **Snowflake COPY INTO** — Snowflake's native COPY INTO from ADLS is
  the fastest and cheapest way to bulk load data
- **Audit trail** — Files in ADLS serve as a raw data archive
- **Format efficiency** — Parquet is columnar, compressed, ideal for
  analytical workloads

### Trade-offs
- Two-hop architecture (SQL → ADLS → Snowflake) vs direct copy
- ADLS storage costs (minimal for Parquet)
- Requires Snowflake external stage setup

---

## 6. Why Azure SQL for Metadata Store?

### Decision
Pipeline config, watermarks and run logs stored in Azure SQL,
not in Snowflake or a flat file.

### Rationale
- **ADF native integration** — ADF Lookup and Stored Procedure activities
  connect natively to Azure SQL
- **ACID transactions** — Watermark updates are transactional
- **Low latency** — Metadata reads/writes happen frequently per run
- **Familiar** — Standard SQL for querying run history and config

### Trade-offs
- Extra Azure SQL database cost
- Another service to maintain
- Could be replaced by Snowflake metadata tables in future

---

## 7. Why Parallel Execution with Batch Count = 3?

### Decision
ForEach loop in PL_MASTER_INGESTION runs with Sequential = FALSE
and Batch Count = 3.

### Rationale
- **Throughput** — 3 tables processed simultaneously vs sequentially
- **Controlled** — Batch count of 3 prevents overwhelming source DB
  with too many concurrent connections
- **ADF limits** — ADF has concurrent activity limits; 3 is safe default

### How to Tune
Conservative sources   → Batch Count = 2
Standard workloads     → Batch Count = 3  (current)
High capacity sources  → Batch Count = 5


### Trade-offs
- Higher batch count = more concurrent DB connections
- Lower batch count = longer total pipeline runtime

---

## 8. Why 5 Audit Columns on Every Record?

### Decision
Every record loaded into Snowflake RAW is stamped with:
`_PIPELINE_NAME`, `_BATCH_ID`, `_RUN_ID`, `_LOAD_TYPE`, `_SOURCE_FILE`

### Rationale
| Audit Column | Purpose |
|---|---|
| _PIPELINE_NAME | Which pipeline loaded this record |
| _BATCH_ID | Group all records from same batch run |
| _RUN_ID | Trace back to exact ADF run in portal |
| _LOAD_TYPE | Know if record came from FULL or CDC load |
| _SOURCE_FILE | Know exact ADLS file record came from |

- **Debugging** — Instantly know where any record came from
- **Reprocessing** — Filter by BATCH_ID to reprocess specific loads
- **Auditing** — Full data lineage at record level

### Trade-offs
- Slight storage overhead per record
- Columns must be added to all RAW tables on creation

---

## 9. Why Full Observability Logging?

### Decision
Every pipeline run is logged in `dbo.pipeline_run_log` with:
start time, end time, duration, rows extracted, rows loaded, errors.

### Rationale
- **Monitoring** — Instantly see which pipelines failed
- **Performance tracking** — Track row counts and duration over time
- **Alerting** — Query log table to build alerts on failures
- **Capacity planning** — Row count trends help predict growth

### Sample Monitoring Query
sql
-- Failed pipelines in last 24 hours
SELECT
    pipeline_name,
    status,
    error_message,
    start_time,
    duration_seconds
FROM dbo.pipeline_run_log
WHERE status = 'FAILED'
AND start_time >= DATEADD(HOUR, -24, GETDATE())
ORDER BY start_time DESC;

---

## 10. Future Enhancements

| Enhancement | Description | Priority |
|---|---|---|
| Data Quality checks | Row count validation, null checks before Snowflake load | High |
| Schema drift detection | Alert when source schema changes unexpectedly | High |
| Email alerting | Send failure emails via ADF Web Activity | Medium |
| dbt integration | Replace Snowflake SPs with dbt models for STAGING layer | Medium |
| Terraform IaC | Infrastructure as code for all Azure resources | Medium |
| Unit testing | pytest based tests for stored procedures | Low |
| Delta Lake format | Replace Parquet with Delta for ADLS layer | Low |

