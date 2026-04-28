# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Bronze data quality checks for Microsoft Fabric Lakehouse.
# This notebook validates existing Bronze tables only. It does not transform data
# and it does not create Silver tables.

from __future__ import annotations

import json
import re
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


# CELL ********************

# Runtime parameters

CONFIG_CANDIDATE_PATHS = [
    "Files/config/bronze_sources.json",
    "/lakehouse/default/Files/config/bronze_sources.json",
    "../../config/bronze_sources.json",
]

ENTITY_TO_CHECK = "all"  # Supported values: all, clients, visits, timeclock, purchases
BRONZE_SCHEMA = "bronze"

VALID_ENTITIES = ["clients", "visits", "timeclock", "purchases"]
DATE_LIKE_NAME_TOKENS = ("date", "time", "timestamp")
LIKELY_KEY_TOKENS = ("id", "key", "client", "service", "visit", "purchase", "invoice", "staff", "employee")
LIKELY_AMOUNT_TOKENS = ("amount", "total", "price", "cost", "paid", "payment", "sale", "revenue")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Helpers

def resolve_local_config_path(config_path: str) -> Path | None:
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


def validate_entity(entity_to_check: str) -> str:
    if entity_to_check != "all" and entity_to_check not in VALID_ENTITIES:
        raise ValueError(
            f"Unsupported ENTITY_TO_CHECK '{entity_to_check}'. "
            f"Expected 'all' or one of: {VALID_ENTITIES}"
        )
    return entity_to_check


def resolve_entities(entity_to_check: str) -> list[str]:
    if entity_to_check == "all":
        return VALID_ENTITIES
    return [entity_to_check]


def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def build_table_name(bronze_schema: str, entity_name: str) -> str:
    return f"{bronze_schema}.{entity_name}"


def discover_source_file_names(path_pattern: str, file_name_contains: str | None = None) -> list[str]:
    try:
        source_df = (
            spark.read.format("binaryFile")
            .option("pathGlobFilter", "*")
            .load(path_pattern)
            .select(F.regexp_extract("path", r"([^/\\\\]+)$", 1).alias("source_file_name"))
        )
    except Exception:
        return []

    if file_name_contains:
        source_df = source_df.filter(F.col("source_file_name").contains(file_name_contains))

    return [row["source_file_name"] for row in source_df.distinct().orderBy("source_file_name").collect()]


def get_expected_source_files(config: dict, entity_name: str) -> list[str]:
    dataset_config = config["datasets"][entity_name]

    if entity_name in {"clients", "timeclock"}:
        return [Path(dataset_config["path"]).name]

    if entity_name == "visits":
        return [
            Path(dataset_config["left_source"]["path"]).name,
            Path(dataset_config["right_source"]["path"]).name,
        ]

    if entity_name == "purchases":
        return discover_source_file_names(
            dataset_config["path_glob"],
            dataset_config.get("file_name_contains"),
        )

    return []


def get_actual_source_files(dataframe: DataFrame) -> list[str]:
    if "source_file_name" not in dataframe.columns:
        return []

    split_files = (
        dataframe.select(F.explode(F.split(F.col("source_file_name"), ";")).alias("source_file_name"))
        .filter(F.col("source_file_name") != "")
        .distinct()
        .orderBy("source_file_name")
    )
    return [row["source_file_name"] for row in split_files.collect()]


def get_date_like_columns(dataframe: DataFrame) -> list[str]:
    date_like_columns = []

    for field in dataframe.schema.fields:
        field_name = field.name.lower()
        field_type = field.dataType

        if isinstance(field_type, (T.DateType, T.TimestampType)):
            date_like_columns.append(field.name)
            continue

        if any(token in field_name for token in DATE_LIKE_NAME_TOKENS):
            date_like_columns.append(field.name)

    return date_like_columns


def get_likely_null_check_columns(dataframe: DataFrame) -> list[str]:
    likely_columns = []

    for field_name in dataframe.columns:
        normalized_name = field_name.lower()
        if any(token in normalized_name for token in LIKELY_KEY_TOKENS + DATE_LIKE_NAME_TOKENS + LIKELY_AMOUNT_TOKENS):
            likely_columns.append(field_name)

    return likely_columns


def is_effectively_null(column_name: str):
    return (
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name).cast("string")) == "")
    )


def get_null_count_metrics(dataframe: DataFrame, entity_name: str) -> list[dict]:
    metrics = []

    for column_name in get_likely_null_check_columns(dataframe):
        null_count = dataframe.filter(is_effectively_null(column_name)).count()
        status = "PASS" if null_count == 0 else "WARN"

        metrics.append(
            {
                "entity": entity_name,
                "check_name": f"null_count:{column_name}",
                "status": status,
                "metric_value": str(null_count),
                "details": f"Rows with null or blank values in {column_name}",
            }
        )

    return metrics


def get_duplicate_check_columns(entity_name: str, dataframe: DataFrame) -> list[str] | None:
    if entity_name == "visits":
        visit_keys = ["client", "service", "date", "time"]
        if all(column_name in dataframe.columns for column_name in visit_keys):
            return visit_keys

    id_columns = [column_name for column_name in dataframe.columns if column_name.lower().endswith("_id")]
    if id_columns:
        return [id_columns[0]]

    return None


def get_duplicate_metrics(dataframe: DataFrame, entity_name: str) -> list[dict]:
    metrics = []

    duplicate_rows_count = dataframe.count() - dataframe.dropDuplicates().count()
    duplicate_rows_status = "PASS" if duplicate_rows_count == 0 else "WARN"
    metrics.append(
        {
            "entity": entity_name,
            "check_name": "duplicate_rows_all_columns",
            "status": duplicate_rows_status,
            "metric_value": str(duplicate_rows_count),
            "details": "Exact duplicate rows across all columns",
        }
    )

    duplicate_check_columns = get_duplicate_check_columns(entity_name, dataframe)
    if duplicate_check_columns:
        duplicate_key_count = (
            dataframe.groupBy(*duplicate_check_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        duplicate_key_status = "PASS" if duplicate_key_count == 0 else "WARN"
        metrics.append(
            {
                "entity": entity_name,
                "check_name": f"duplicate_business_keys:{','.join(duplicate_check_columns)}",
                "status": duplicate_key_status,
                "metric_value": str(duplicate_key_count),
                "details": f"Duplicate groups using columns {duplicate_check_columns}",
            }
        )
    else:
        metrics.append(
            {
                "entity": entity_name,
                "check_name": "duplicate_business_keys",
                "status": "WARN",
                "metric_value": "n/a",
                "details": "No obvious duplicate check columns found",
            }
        )

    return metrics


def get_date_range_metrics(dataframe: DataFrame, entity_name: str) -> list[dict]:
    metrics = []

    for column_name in get_date_like_columns(dataframe):
        range_row = dataframe.select(
            F.min(F.col(column_name).cast("string")).alias("min_value"),
            F.max(F.col(column_name).cast("string")).alias("max_value"),
        ).collect()[0]

        min_value = range_row["min_value"]
        max_value = range_row["max_value"]
        status = "PASS" if min_value is not None or max_value is not None else "WARN"

        metrics.append(
            {
                "entity": entity_name,
                "check_name": f"date_range:{column_name}",
                "status": status,
                "metric_value": f"min={min_value}, max={max_value}",
                "details": f"Raw min and max values for date-like column {column_name}",
            }
        )

    if not metrics:
        metrics.append(
            {
                "entity": entity_name,
                "check_name": "date_range",
                "status": "WARN",
                "metric_value": "n/a",
                "details": "No date-like columns detected",
            }
        )

    return metrics


def get_source_file_coverage_metrics(dataframe: DataFrame, entity_name: str, config: dict) -> list[dict]:
    expected_files = sorted(get_expected_source_files(config, entity_name))
    actual_files = sorted(get_actual_source_files(dataframe))

    if "source_file_name" not in dataframe.columns:
        return [
            {
                "entity": entity_name,
                "check_name": "source_file_coverage",
                "status": "FAIL",
                "metric_value": "0",
                "details": "Required audit column source_file_name is missing",
            }
        ]

    missing_files = sorted(set(expected_files) - set(actual_files))
    extra_files = sorted(set(actual_files) - set(expected_files))

    if missing_files:
        status = "FAIL"
    elif not actual_files:
        status = "FAIL"
    elif extra_files:
        status = "WARN"
    else:
        status = "PASS"

    return [
        {
            "entity": entity_name,
            "check_name": "source_file_coverage",
            "status": status,
            "metric_value": str(len(actual_files)),
            "details": (
                f"expected={expected_files}; actual={actual_files}; "
                f"missing={missing_files}; extra={extra_files}"
            ),
        }
    ]


def summarize_table_metrics(dataframe: DataFrame, entity_name: str) -> list[dict]:
    row_count = dataframe.count()
    column_count = len(dataframe.columns)

    row_count_status = "PASS" if row_count > 0 else "FAIL"
    column_count_status = "PASS" if column_count > 0 else "FAIL"

    return [
        {
            "entity": entity_name,
            "check_name": "row_count",
            "status": row_count_status,
            "metric_value": str(row_count),
            "details": "Total rows in Bronze table",
        },
        {
            "entity": entity_name,
            "check_name": "column_count",
            "status": column_count_status,
            "metric_value": str(column_count),
            "details": "Total columns in Bronze table",
        },
    ]


def evaluate_table(entity_name: str, bronze_schema: str, config: dict) -> list[dict]:
    target_table = build_table_name(bronze_schema, entity_name)

    if not table_exists(target_table):
        return [
            {
                "entity": entity_name,
                "check_name": "table_exists",
                "status": "FAIL",
                "metric_value": "0",
                "details": f"Table {target_table} does not exist",
            }
        ]

    dataframe = spark.table(target_table)
    metrics = []
    metrics.extend(summarize_table_metrics(dataframe, entity_name))
    metrics.extend(get_null_count_metrics(dataframe, entity_name))
    metrics.extend(get_duplicate_metrics(dataframe, entity_name))
    metrics.extend(get_date_range_metrics(dataframe, entity_name))
    metrics.extend(get_source_file_coverage_metrics(dataframe, entity_name, config))

    return metrics


def build_results_dataframe(metrics: list[dict]) -> DataFrame:
    return spark.createDataFrame(metrics)


def build_table_summary(results_df: DataFrame) -> DataFrame:
    return (
        results_df.groupBy("entity")
        .agg(
            F.sum(F.when(F.col("status") == "FAIL", 1).otherwise(0)).alias("fail_count"),
            F.sum(F.when(F.col("status") == "WARN", 1).otherwise(0)).alias("warn_count"),
            F.sum(F.when(F.col("status") == "PASS", 1).otherwise(0)).alias("pass_count"),
        )
        .withColumn(
            "overall_status",
            F.when(F.col("fail_count") > 0, F.lit("FAIL"))
            .when(F.col("warn_count") > 0, F.lit("WARN"))
            .otherwise(F.lit("PASS"))
        )
        .orderBy("entity")
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Main execution

active_entity = validate_entity(ENTITY_TO_CHECK)
selected_entities = resolve_entities(active_entity)
config = load_config(CONFIG_CANDIDATE_PATHS)

all_metrics = []
for entity_name in selected_entities:
    print(f"Running Bronze DQ checks for {BRONZE_SCHEMA}.{entity_name}")
    all_metrics.extend(evaluate_table(entity_name, BRONZE_SCHEMA, config))

results_df = build_results_dataframe(all_metrics).orderBy("entity", "check_name")
summary_df = build_table_summary(results_df)

display(summary_df)
display(results_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
