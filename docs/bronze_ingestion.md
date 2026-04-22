# Bronze Ingestion Framework

This project contains an initial Bronze ingestion framework for a Microsoft Fabric Lakehouse using schema-based medallion layers: `bronze`, `silver`, and `gold`.

## Files

- `config/bronze_sources.json`
- `notebooks/bronze/bronze_ingestion.py`

## What The Notebook Does

The notebook ingests WellnessLiving CSV exports from Lakehouse Files into Delta tables in the `bronze` schema.

It only applies minimal Bronze-standard handling:

- reads raw CSV files
- normalizes column names to snake_case
- adds audit metadata columns
- writes Delta tables to the `bronze` schema

Audit columns added to every Bronze table:

- `ingestion_timestamp`
- `source_file_name`

The notebook does not apply business transformations, deduplication, conforming, or downstream modeling.

## Source Inputs

Expected source files in Lakehouse Files:

- Clients: `/wellnessliving/clients/wl_clients_full_20260420_v1.csv`
- Visits attendance file: `/wellnessliving/visits/wl_attendancewithPurchaseOption_full_20260420_v1.csv`
- Visits booking file: `/wellnessliving/visits/wl_bookingsBySource_full_20260420_v1.csv`
- Timeclock: `/wellnessliving/timeclock/wl_timeclock_full_20260420_v1.csv`
- Purchases: yearly `AllSales` CSV files under `/wellnessliving/purchases/`

## Bronze Outputs

The notebook writes:

- `bronze.clients`
- `bronze.visits`
- `bronze.timeclock`
- `bronze.purchases`

Dataset behavior:

- `clients` and `timeclock` each load from one CSV file
- `visits` merges the two visit files on `client`, `service`, `date`, and `time`
- `purchases` loads all matching yearly `AllSales` CSV files into one table after checking that their normalized schemas match

## Load Modes

Set the `LOAD_MODE` variable near the top of `notebooks/bronze/bronze_ingestion.py`.

Supported values:

- `init`
  - creates each Bronze table only if it does not already exist
  - fails if the target table already exists
  - use this for first-time setup
- `full_refresh`
  - overwrites each target Bronze table
  - use this when you want to rebuild Bronze from the current source files
- `append`
  - appends to existing Bronze tables
  - creates missing tables automatically
  - use this for recurring loads

## How To Run In Fabric

1. Upload or sync this repository into your Fabric workspace.
2. Attach the notebook to the target Lakehouse.
3. Confirm the source files exist in Lakehouse Files at the configured paths.
4. Open `notebooks/bronze/bronze_ingestion.py` in a Fabric PySpark notebook.
5. Set `LOAD_MODE` to one of:
   - `init`
   - `full_refresh`
   - `append`
6. Run the notebook.

## Practical Run Guidance

Recommended first run:

1. Set `LOAD_MODE = "init"`.
2. Run the notebook once to create the Bronze tables.

Recommended recurring run:

1. Set `LOAD_MODE = "append"`.
2. Run the notebook to add newly ingested raw rows.

Recommended rebuild run:

1. Set `LOAD_MODE = "full_refresh"`.
2. Run the notebook to replace Bronze table contents from the current source files.

## Troubleshooting

- Missing source files
  - verify the WellnessLiving exports are present in Lakehouse Files at the configured paths
- Purchase schema mismatch
  - the notebook fails if yearly purchase files do not share the same schema after column normalization
- Visits merge key mismatch
  - confirm both visit files contain columns that normalize to `client`, `service`, `date`, and `time`
- Config path issue
  - if you move the notebook or config file, update `BRONZE_CONFIG_PATH` in the notebook

## Notes

- The notebook expects an active Fabric Spark session named `spark`.
- The notebook creates the `bronze` schema if it does not already exist.
- The purchases loader processes matching files in sorted file-name order for consistent multi-file ingestion behavior.
