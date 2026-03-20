# ADF Pipeline & Dataset Definitions

## How to Import These Into Your ADF Instance

### Import Datasets First (always before pipelines)
1. Go to ADF Studio → Author → Datasets
2. Click "..." → Import from JSON
3. Import in this order:
   - DS_AzureSQL_MetadataDB.json
   - DS_AzureSQL_Source.json
   - DS_ADLS_Parquet_Sink.json
   - DS_ADLS_Parquet_Source.json
   - DS_Snowflake_RAW.json

### Import Pipelines
1. Go to ADF Studio → Author → Pipelines
2. Click "..." → Import from JSON
3. Import in this order:
   - PL_CHILD_INGESTION.json  ← always first
   - PL_MASTER_INGESTION.json ← second

### Before Running
- Update all Linked Services with your credentials
- Update pipeline_config table with your source tables
- Ensure Snowflake stages are created
- Ensure ADLS containers exist

## Notes
- All credentials removed from JSONs
- Linked Services must be recreated in your ADF instance
