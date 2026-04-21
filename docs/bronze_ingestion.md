# Bronze Ingestion Framework

This project includes an initial Bronze ingestion framework for a Microsoft Fabric Lakehouse using schema-based medallion layers (`bronze`, `silver`, `gold`).

## Files

- `config/bronze_sources.json`
  - Maps WellnessLiving source file locations to Bronze target tables.
- `notebooks/bronze/bronze_ingestion.py`
  - PySpark notebook-style script for loading Bronze tables.

## What The Notebook Does

The Bronze notebook:

- reads WellnessLiving CSV exports from Lakehouse Files
- normalizes column names to snake_case
- adds audit metadata columns
- writes Delta tables to the `bronze` schema
- supports both first-time table creation and later append-based loads

Audit columns added to every Bronze table:

- `ingestion_timestamp`
- `source_file_name`

## Source Inputs

Expected source files in Lakehouse Files:

- Clients: `/wellnessliving/clients/wl_clients_full_20260420_v1.csv`
- Visits attendance file: `/wellnessliving/visits/wl_attendancewithPurchaseOption_full_20260420_v1.csv`
- Visits booking file: `/wellnessliving/visits/wl_bookingsBySource_full_20260420_v1.csv`
- Timeclock: `/wellnessliving/timeclock/wl_timeclock_full_20260420_v1.csv`
- Purchases: yearly `AllSales` CSV files under `/wellnessliving/purchases/`

## Bronze Outputs

The notebook writes these Delta tables:

- `bronze.clients`
- `bronze.visits`
- `bronze.timeclock`
- `bronze.purchases`

Behavior by dataset:

- `clients` and `timeclock` are loaded from one source CSV each.
- `purchases` loads all matching yearly `AllSales` CSV files into one `bronze.purchases` table if the normalized schemas match.
- `visits` merges the two visit exports on `client`, `service`, `date`, and `time`.

## How To Run In Fabric

1. Upload or sync this repository into your Fabric workspace.
2. Attach the notebook to the target Lakehouse.
3. Confirm the source CSV files are present in Lakehouse Files at the configured paths.
4. Open `notebooks/bronze/bronze_ingestion.py` as a Fabric notebook or copy the code into a Fabric PySpark notebook.
5. Run the notebook.

## Initial Vs Recurring Loads

- Initial load:
  - if the Bronze table does not exist yet, the notebook creates it in the `bronze` schema.
- Recurring load:
  - if the Bronze table already exists, the notebook appends the newly ingested rows.

This framework intentionally keeps Bronze logic minimal. It does not apply business rules, deduplication, or conformed modeling.

## Practical Notes

- The notebook expects a Fabric Spark session named `spark`.
- The config path is set relative to the notebook source file:
  - `../../config/bronze_sources.json`
- If you move the notebook or config file, update `BRONZE_CONFIG_PATH` in the notebook.
- If purchase file schemas differ after column normalization, the notebook fails fast so the mismatch can be resolved explicitly.
