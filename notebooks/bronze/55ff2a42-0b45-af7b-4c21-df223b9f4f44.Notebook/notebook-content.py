# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6567cb96-1424-4980-8cfb-dc13a43113cc",
# META       "default_lakehouse_name": "saltroom_lakehouse",
# META       "default_lakehouse_workspace_id": "9161a515-b087-41f4-8474-27fcb91ae1c0",
# META       "known_lakehouses": [
# META         {
# META           "id": "6567cb96-1424-4980-8cfb-dc13a43113cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Parameters
load_mode = "init"
batch_id = None
entity_to_load = "all"  # Supported values: all, clients, visits, timeclock, purchases

from datetime import datetime

if not batch_id:
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dependencies

import json
import re
from pathlib import Path
from typing import Iterable

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql import types as T


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Runtime parameters

CONFIG_CANDIDATE_PATHS = [
    "Files/config/bronze_sources.json",
    "/lakehouse/default/Files/config/bronze_sources.json",
    "../../config/bronze_sources.json",
]

CSV_READ_OPTIONS = {
    "header": "true",
    "multiLine": "true",
    "escape": '"',
    "quote": '"',
    "encoding": "UTF-8",
}

VALID_LOAD_MODES = {"init", "refresh", "append"}
VALID_ENTITIES = {"clients", "visits", "timeclock", "purchases"}
DQ_LOAD_WARNINGS_TABLE = "dq.load_warnings"
NOTEBOOK_NAME = "load_bronze"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Helper functions

def resolve_local_config_path(config_path: str) -> Path | None:
    """Resolve a local config path when the notebook is executed from a checked-out repo."""
    candidate_paths = []

    if "__file__" in globals():
        candidate_paths.append((Path(__file__).resolve().parent / config_path).resolve())

    candidate_paths.append((Path.cwd() / config_path).resolve())
    candidate_paths.append((Path.cwd() / "config" / "bronze_sources.json").resolve())

    for candidate in candidate_paths:
        if candidate.exists():
            return candidate

    return None


def read_text_from_spark_path(path: str) -> str:
    lines = spark.read.text(path).collect()
    return "\n".join(row["value"] for row in lines)


def load_config(config_candidate_paths: list[str]) -> dict:
    """Load config from Lakehouse Files first, then fall back to local repo paths."""
    load_errors = []

    for candidate_path in config_candidate_paths:
        if candidate_path.startswith("Files/") or candidate_path.startswith("/lakehouse/"):
            try:
                return json.loads(read_text_from_spark_path(candidate_path))
            except Exception as exc:
                load_errors.append(f"{candidate_path}: {exc}")
                continue

        resolved_path = resolve_local_config_path(candidate_path)
        if resolved_path:
            with resolved_path.open("r", encoding="utf-8") as file_handle:
                return json.load(file_handle)

        load_errors.append(f"{candidate_path}: path not found")

    raise FileNotFoundError(
        "Could not find Bronze config file in Lakehouse Files or local repo paths. "
        f"Tried: {load_errors}"
    )


def validate_load_mode(load_mode: str) -> str:
    if load_mode not in VALID_LOAD_MODES:
        raise ValueError(
            f"Unsupported LOAD_MODE '{load_mode}'. Expected one of: {sorted(VALID_LOAD_MODES)}"
        )
    return load_mode


def validate_entity(entity_to_load: str) -> str:
    if entity_to_load != "all" and entity_to_load not in VALID_ENTITIES:
        raise ValueError(
            f"Unsupported ENTITY_TO_LOAD '{entity_to_load}'. Expected 'all' or one of: {sorted(VALID_ENTITIES)}"
        )
    return entity_to_load


def resolve_selected_entities(entity_to_load: str) -> list[str]:
    if entity_to_load == "all":
        return ["clients", "visits", "timeclock", "purchases"]
    return [entity_to_load]


def read_csv(path: str, **overrides: str) -> DataFrame:
    """Read a CSV file or file pattern from Lakehouse Files."""
    options = {**CSV_READ_OPTIONS, **overrides}
    return spark.read.options(**options).csv(path)


def normalize_column_name(column_name: str) -> str:
    normalized = column_name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def normalize_column_names(dataframe: DataFrame) -> DataFrame:
    """Normalize all column names to snake_case."""
    seen_names: dict[str, int] = {}
    renamed_columns = []

    for original_name in dataframe.columns:
        if original_name.startswith("_"):
            renamed_columns.append(F.col(f"`{original_name}`").alias(original_name))
            continue

        normalized_name = normalize_column_name(original_name)
        normalized_name = normalized_name or "column"

        if normalized_name in seen_names:
            seen_names[normalized_name] += 1
            normalized_name = f"{normalized_name}_{seen_names[normalized_name]}"
        else:
            seen_names[normalized_name] = 0

        renamed_columns.append(F.col(f"`{original_name}`").alias(normalized_name))

    return dataframe.select(*renamed_columns)


def add_audit_columns(dataframe: DataFrame, source_file_column: str = "_source_file_name") -> DataFrame:
    """Add ingestion metadata columns required for Bronze tables."""
    return (
        dataframe.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file_name", F.col(source_file_column))
        .drop(source_file_column)
    )


def read_csv_with_metadata(path: str, **overrides: str) -> DataFrame:
    """Read a CSV and capture the originating source file name."""
    dataframe = read_csv(path, **overrides)
    dataframe = dataframe.withColumn("_source_file_path", F.input_file_name())
    dataframe = dataframe.withColumn(
        "_source_file_name",
        F.regexp_extract(F.col("_source_file_path"), r"([^/\\\\]+)$", 1),
    ).drop("_source_file_path")
    return dataframe


def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def is_dataframe_empty(dataframe: DataFrame) -> bool:
    return dataframe.limit(1).count() == 0


def get_existing_source_file_names(target_table: str) -> set[str]:
    if not table_exists(target_table):
        return set()

    target_dataframe = spark.table(target_table)
    if "source_file_name" not in target_dataframe.columns:
        return set()

    return {
        row["source_file_name"]
        for row in target_dataframe.select("source_file_name").distinct().collect()
        if row["source_file_name"]
    }


def filter_already_ingested_files(
    dataframe: DataFrame,
    target_table: str,
    load_mode: str,
    source_file_column: str = "_source_file_name",
) -> DataFrame:
    """In append mode, keep only rows whose source files have not been loaded before."""
    if load_mode != "append" or not table_exists(target_table):
        return dataframe

    ingested_source_files = sorted(get_existing_source_file_names(target_table))
    if not ingested_source_files:
        return dataframe

    return dataframe.filter(~F.col(source_file_column).isin(ingested_source_files))


def write_delta_table(dataframe: DataFrame, target_table: str, load_mode: str) -> None:
    """Write a Bronze table using the requested load mode."""
    table_already_exists = table_exists(target_table)
    if "batch_id" not in dataframe.columns:
        dataframe = dataframe.withColumn("batch_id", lit(batch_id))

    if load_mode == "init":
        if table_already_exists:
            write_dq_result(
                batch_id,
                "bronze",
                NOTEBOOK_NAME,
                f"load_bronze_{target_table.split('.')[-1]}",
                target_table,
                "target_table_exists_for_init",
                "ERROR",
                "BRONZE_TARGET_TABLE_EXISTS_FOR_INIT",
                f"Target table {target_table} already exists while load_mode is init.",
                1,
                {"target_table": target_table, "load_mode": load_mode},
                "FAIL",
            )
            raise ValueError(
                f"Target table {target_table} already exists. Use LOAD_MODE='refresh' or 'append'."
            )
        dataframe.write.format("delta").mode("errorifexists").saveAsTable(target_table)
        return

    if load_mode == "refresh":
        dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        return

    if table_already_exists:
        dataframe.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
    else:
        dataframe.write.format("delta").mode("errorifexists").saveAsTable(target_table)


def align_to_reference_schema(dataframe: DataFrame, reference_columns: list[str]) -> DataFrame:
    """Reorder columns to a reference schema and fail fast if columns differ."""
    dataframe_columns = dataframe.columns
    missing_columns = sorted(set(reference_columns) - set(dataframe_columns))
    unexpected_columns = sorted(set(dataframe_columns) - set(reference_columns))

    if missing_columns or unexpected_columns:
        write_dq_result(
            batch_id,
            "bronze",
            NOTEBOOK_NAME,
            "load_bronze_purchases",
            "bronze.purchases",
            "purchase_file_schema_mismatch",
            "ERROR",
            "BRONZE_PURCHASE_FILE_SCHEMA_MISMATCH",
            "Purchase files do not share the same normalized schema.",
            1,
            {"missing_columns": missing_columns, "unexpected_columns": unexpected_columns},
            "FAIL",
        )
        raise ValueError(
            "Purchase files do not share the same normalized schema. "
            f"Missing columns: {missing_columns}. Unexpected columns: {unexpected_columns}."
        )

    return dataframe.select(*reference_columns)


def union_dataframes(dataframes: Iterable[DataFrame]) -> DataFrame:
    return reduce(lambda left, right: left.unionByName(right), dataframes)


def build_target_table_name(bronze_schema: str, target_table: str) -> str:
    return f"{bronze_schema}.{target_table}"


def ensure_bronze_schema_exists(bronze_schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")


def ensure_dq_schema_exists() -> None:
    spark.sql("CREATE SCHEMA IF NOT EXISTS dq")


def write_dq_result(
    batch_id: str,
    layer: str,
    notebook_name: str,
    process_name: str,
    target_table: str,
    check_name: str,
    severity: str,
    warning_code: str,
    warning_message: str,
    affected_row_count: int,
    sample_record: dict | None = None,
    status: str = "WARN",
) -> None:
    if not batch_id:
        raise ValueError("batch_id parameter is required before writing DQ results.")

    ensure_dq_schema_exists()
    sample_record_json = json.dumps(sample_record, default=str) if sample_record else None
    schema = T.StructType([
        T.StructField("batch_id", T.StringType(), False),
        T.StructField("layer", T.StringType(), False),
        T.StructField("notebook_name", T.StringType(), False),
        T.StructField("process_name", T.StringType(), False),
        T.StructField("target_table", T.StringType(), False),
        T.StructField("check_name", T.StringType(), False),
        T.StructField("severity", T.StringType(), False),
        T.StructField("warning_code", T.StringType(), False),
        T.StructField("warning_message", T.StringType(), False),
        T.StructField("affected_row_count", T.LongType(), False),
        T.StructField("sample_record", T.StringType(), True),
        T.StructField("status", T.StringType(), False),
    ])
    result_df = spark.createDataFrame(
        [(
            str(batch_id),
            layer,
            notebook_name,
            process_name,
            target_table,
            check_name,
            severity,
            warning_code,
            warning_message,
            int(affected_row_count or 0),
            sample_record_json,
            status,
        )],
        schema,
    ).withColumn("generated_at", F.current_timestamp()).select(
        "batch_id",
        "generated_at",
        "layer",
        "notebook_name",
        "process_name",
        "target_table",
        "check_name",
        "severity",
        "warning_code",
        "warning_message",
        "affected_row_count",
        "sample_record",
        "status",
    )
    result_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(DQ_LOAD_WARNINGS_TABLE)


def write_dq_warning(
    process_name: str,
    target_table: str,
    check_name: str,
    warning_code: str,
    warning_message: str,
    affected_row_count: int,
    sample_record: dict | None = None,
) -> None:
    if affected_row_count > 0:
        write_dq_result(
            batch_id,
            "bronze",
            NOTEBOOK_NAME,
            process_name,
            target_table,
            check_name,
            "WARNING",
            warning_code,
            warning_message,
            affected_row_count,
            sample_record,
            "WARN",
        )


def get_matching_source_files(dataframe: DataFrame, file_name_filter: str | None = None) -> list[str]:
    if file_name_filter:
        dataframe = dataframe.filter(F.col("_source_file_name").contains(file_name_filter))

    matching_files = [
        row["_source_file_name"]
        for row in dataframe.select("_source_file_name").distinct().collect()
    ]
    return sorted(matching_files)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dataset loaders

def load_single_csv_dataset(dataset_config: dict, bronze_schema: str, load_mode: str) -> None:
    target_table = build_target_table_name(bronze_schema, dataset_config["target_table"])
    dataframe = read_csv_with_metadata(dataset_config["path"])
    dataframe = normalize_column_names(dataframe)
    dataframe = filter_already_ingested_files(dataframe, target_table, load_mode)

    if is_dataframe_empty(dataframe):
        print(f"Skipping {target_table}: no new source files to ingest.")
        return

    dataframe = add_audit_columns(dataframe)
    write_delta_table(dataframe, target_table, load_mode)


def load_purchases_dataset(dataset_config: dict, bronze_schema: str, load_mode: str) -> None:
    file_pattern = dataset_config["path_glob"]
    file_name_filter = dataset_config.get("file_name_contains")
    target_table = build_target_table_name(bronze_schema, dataset_config["target_table"])

    raw_dataframe = read_csv_with_metadata(file_pattern)
    matching_files = get_matching_source_files(raw_dataframe, file_name_filter)

    if not matching_files:
        write_dq_result(
            batch_id,
            "bronze",
            NOTEBOOK_NAME,
            "load_bronze_purchases",
            target_table,
            "no_purchase_files_matched",
            "ERROR",
            "BRONZE_PURCHASES_NO_FILES_MATCHED",
            "No purchase files matched the configured path.",
            0,
            {"path": file_pattern, "file_name_filter": file_name_filter},
            "FAIL",
        )
        raise ValueError(f"No purchase files matched the configured path: {file_pattern}")

    if load_mode == "append" and table_exists(target_table):
        ingested_source_files = get_existing_source_file_names(target_table)
        matching_files = [
            source_file_name
            for source_file_name in matching_files
            if source_file_name not in ingested_source_files
        ]

    if not matching_files:
        print(f"Skipping {target_table}: no new purchase files to ingest.")
        return

    purchase_dataframes = []
    reference_columns = None

    for source_file_name in matching_files:
        purchase_df = raw_dataframe.filter(F.col("_source_file_name") == source_file_name)
        purchase_df = normalize_column_names(purchase_df)

        if reference_columns is None:
            reference_columns = purchase_df.columns
        else:
            purchase_df = align_to_reference_schema(purchase_df, reference_columns)

        purchase_df = add_audit_columns(purchase_df)
        purchase_dataframes.append(purchase_df)

    final_dataframe = union_dataframes(purchase_dataframes)
    write_delta_table(final_dataframe, target_table, load_mode)


def load_visits_dataset(dataset_config: dict, bronze_schema: str, load_mode: str) -> None:
    for source_config in dataset_config["sources"]:
        target_table = build_target_table_name(bronze_schema, source_config["target_table"])
        dataframe = read_csv_with_metadata(source_config["path"])
        dataframe = normalize_column_names(dataframe)
        dataframe = filter_already_ingested_files(dataframe, target_table, load_mode)

        if is_dataframe_empty(dataframe):
            print(f"Skipping {target_table}: no new source files to ingest.")
            continue

        dataframe = add_audit_columns(dataframe)
        write_delta_table(dataframe, target_table, load_mode)

def run_bronze_ingestion(config: dict, selected_entities: list[str], active_load_mode: str) -> None:
    bronze_schema = config["bronze_schema"]
    datasets = config["datasets"]

    ensure_bronze_schema_exists(bronze_schema)

    for entity_name in selected_entities:
        print(f"Loading entity '{entity_name}' using LOAD_MODE='{active_load_mode}'.")

        if entity_name == "clients":
            load_single_csv_dataset(datasets["clients"], bronze_schema, active_load_mode)
        elif entity_name == "visits":
            load_visits_dataset(datasets["visits"], bronze_schema, active_load_mode)
        elif entity_name == "timeclock":
            load_single_csv_dataset(datasets["timeclock"], bronze_schema, active_load_mode)
        elif entity_name == "purchases":
            load_purchases_dataset(datasets["purchases"], bronze_schema, active_load_mode)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Main execution

active_entity = validate_entity(entity_to_load)
selected_entities = resolve_selected_entities(active_entity)
active_load_mode = validate_load_mode(load_mode)
config = load_config(CONFIG_CANDIDATE_PATHS)

run_bronze_ingestion(config, selected_entities, active_load_mode)

print(
    "Bronze ingestion completed successfully for "
    f"{selected_entities} using LOAD_MODE='{active_load_mode}'."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
