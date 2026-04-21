# Fabric notebook source

# COMMAND ----------
# Bronze ingestion framework for Microsoft Fabric Lakehouse.
# This notebook reads WellnessLiving CSV exports from Lakehouse Files and lands
# them into Delta tables in the bronze schema with minimal standardization.

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# COMMAND ----------
# Configuration

BRONZE_CONFIG_PATH = "../../config/bronze_sources.json"
CSV_READ_OPTIONS = {
    "header": "true",
    "multiLine": "true",
    "escape": '"',
    "quote": '"',
    "encoding": "UTF-8",
}


# COMMAND ----------
# Helper functions

def resolve_config_path(config_path: str) -> Path:
    """Resolve the config file from the notebook location or current working directory."""
    candidate_paths = []

    if "__file__" in globals():
        candidate_paths.append((Path(__file__).resolve().parent / config_path).resolve())

    candidate_paths.append((Path.cwd() / config_path).resolve())
    candidate_paths.append((Path.cwd() / "config" / "bronze_sources.json").resolve())

    for candidate in candidate_paths:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(f"Could not find Bronze config file: {config_path}")


def load_config(config_path: str) -> dict:
    resolved_path = resolve_config_path(config_path)
    with resolved_path.open("r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


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


def write_delta_table(dataframe: DataFrame, target_table: str) -> None:
    """Create the table on first load, otherwise append on recurring loads."""
    writer = dataframe.write.format("delta").mode("append")

    if not table_exists(target_table):
        writer.option("overwriteSchema", "true")

    writer.saveAsTable(target_table)


def align_to_reference_schema(dataframe: DataFrame, reference_columns: list[str]) -> DataFrame:
    """Reorder columns to a reference schema and fail fast if columns differ."""
    dataframe_columns = dataframe.columns
    missing_columns = sorted(set(reference_columns) - set(dataframe_columns))
    unexpected_columns = sorted(set(dataframe_columns) - set(reference_columns))

    if missing_columns or unexpected_columns:
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


# COMMAND ----------
# Dataset loaders

def load_single_csv_dataset(dataset_config: dict, bronze_schema: str) -> None:
    dataframe = read_csv_with_metadata(dataset_config["path"])
    dataframe = normalize_column_names(dataframe)
    dataframe = add_audit_columns(dataframe)

    target_table = build_target_table_name(bronze_schema, dataset_config["target_table"])
    write_delta_table(dataframe, target_table)


def load_purchases_dataset(dataset_config: dict, bronze_schema: str) -> None:
    file_pattern = dataset_config["path_glob"]
    file_name_filter = dataset_config.get("file_name_contains")

    raw_dataframe = read_csv_with_metadata(file_pattern)

    if file_name_filter:
        raw_dataframe = raw_dataframe.filter(F.col("_source_file_name").contains(file_name_filter))

    distinct_files = [row["_source_file_name"] for row in raw_dataframe.select("_source_file_name").distinct().collect()]
    if not distinct_files:
        raise ValueError(f"No purchase files matched the configured path: {file_pattern}")

    purchase_dataframes = []
    reference_columns = None

    for source_file_name in distinct_files:
        purchase_df = raw_dataframe.filter(F.col("_source_file_name") == source_file_name)
        purchase_df = normalize_column_names(purchase_df)

        if reference_columns is None:
            reference_columns = purchase_df.columns
        else:
            purchase_df = align_to_reference_schema(purchase_df, reference_columns)

        purchase_df = add_audit_columns(purchase_df)
        purchase_dataframes.append(purchase_df)

    final_dataframe = union_dataframes(purchase_dataframes)
    target_table = build_target_table_name(bronze_schema, dataset_config["target_table"])
    write_delta_table(final_dataframe, target_table)


def build_non_key_columns(dataframe: DataFrame, merge_keys: list[str]) -> list[str]:
    return [column_name for column_name in dataframe.columns if column_name not in merge_keys]


def load_visits_dataset(dataset_config: dict, bronze_schema: str) -> None:
    merge_keys = dataset_config["merge_keys"]
    left_config = dataset_config["left_source"]
    right_config = dataset_config["right_source"]

    left_df = normalize_column_names(read_csv_with_metadata(left_config["path"]))
    right_df = normalize_column_names(read_csv_with_metadata(right_config["path"]))

    left_alias = "attendance"
    right_alias = "booking"

    join_condition = [
        F.col(f"{left_alias}.{key}") == F.col(f"{right_alias}.{key}")
        for key in merge_keys
    ]

    joined_df = left_df.alias(left_alias).join(
        right_df.alias(right_alias),
        on=join_condition,
        how="full_outer",
    )

    select_expressions = [
        F.coalesce(F.col(f"{left_alias}.{key}"), F.col(f"{right_alias}.{key}")).alias(key)
        for key in merge_keys
    ]

    for column_name in build_non_key_columns(left_df, merge_keys):
        alias_name = column_name
        if column_name in right_df.columns:
            alias_name = f"{column_name}_{left_config['name']}"
        select_expressions.append(F.col(f"{left_alias}.{column_name}").alias(alias_name))

    for column_name in build_non_key_columns(right_df, merge_keys):
        alias_name = column_name
        if column_name in left_df.columns:
            alias_name = f"{column_name}_{right_config['name']}"
        select_expressions.append(F.col(f"{right_alias}.{column_name}").alias(alias_name))

    visits_df = joined_df.select(*select_expressions)
    visits_df = visits_df.withColumn(
        "_source_file_name",
        F.concat_ws(
            ";",
            F.col(f"_source_file_name_{left_config['name']}"),
            F.col(f"_source_file_name_{right_config['name']}"),
        ),
    )
    visits_df = visits_df.drop(
        f"_source_file_name_{left_config['name']}",
        f"_source_file_name_{right_config['name']}",
    )

    visits_df = add_audit_columns(visits_df)

    target_table = build_target_table_name(bronze_schema, dataset_config["target_table"])
    write_delta_table(visits_df, target_table)


# COMMAND ----------
# Main execution

config = load_config(BRONZE_CONFIG_PATH)
bronze_schema = config["bronze_schema"]
datasets = config["datasets"]

ensure_bronze_schema_exists(bronze_schema)

load_single_csv_dataset(datasets["clients"], bronze_schema)
load_visits_dataset(datasets["visits"], bronze_schema)
load_single_csv_dataset(datasets["timeclock"], bronze_schema)
load_purchases_dataset(datasets["purchases"], bronze_schema)

print("Bronze ingestion completed successfully.")
