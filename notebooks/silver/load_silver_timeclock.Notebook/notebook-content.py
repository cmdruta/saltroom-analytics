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

# CELL ********************

# Silver timeclock load for Microsoft Fabric Lakehouse.
#
# Architecture note:
# silver.timeclock is a cleaned labor-hours entity at one row per staff timeclock entry.
# Clock-in and clock-out timestamps are intentionally ignored because they are unreliable for
# this project. Gold will later use this table for labor hours, staffing trends, labor
# efficiency, and coverage analysis.
#
# Ignored fields:
# - email
# - phone
# - clocked_in
# - clocked_out
#
# Grain:
# One row per source timeclock row / shift entry from bronze.timeclock.

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Runtime parameters

LOAD_MODE = "init"  # Supported values: init, refresh

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
BRONZE_TIMECLOCK_TABLE = f"{BRONZE_SCHEMA}.timeclock"
SILVER_TIMECLOCK_TABLE = f"{SILVER_SCHEMA}.timeclock"

VALID_LOAD_MODES = {"init", "refresh"}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Helper functions

def validate_load_mode(load_mode: str) -> str:
    normalized_mode = load_mode.strip().lower()
    if normalized_mode not in VALID_LOAD_MODES:
        raise ValueError(f"Unsupported LOAD_MODE '{load_mode}'. Expected one of: {sorted(VALID_LOAD_MODES)}")
    return normalized_mode


def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def ensure_schema_exists(schema_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def normalize_column_name(column_name: str) -> str:
    normalized = column_name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def normalize_column_names(dataframe: DataFrame) -> DataFrame:
    seen_names: dict[str, int] = {}
    renamed_columns = []

    for original_name in dataframe.columns:
        normalized_name = normalize_column_name(original_name) or "column"
        if normalized_name in seen_names:
            seen_names[normalized_name] += 1
            normalized_name = f"{normalized_name}_{seen_names[normalized_name]}"
        else:
            seen_names[normalized_name] = 0

        renamed_columns.append(F.col(f"`{original_name}`").alias(normalized_name))

    return dataframe.select(*renamed_columns)


def trim_all_string_fields(dataframe: DataFrame) -> DataFrame:
    columns = []

    for field in dataframe.schema.fields:
        if isinstance(field.dataType, T.StringType):
            columns.append(F.trim(F.col(field.name)).alias(field.name))
        else:
            columns.append(F.col(field.name))

    return dataframe.select(*columns)


def normalize_spaces(column_expression) -> F.Column:
    return F.regexp_replace(F.trim(column_expression.cast("string")), r"\s+", " ")


def null_if_blank(column_expression) -> F.Column:
    normalized_value = normalize_spaces(column_expression)
    return F.when((normalized_value == "") | (F.lower(normalized_value) == "null"), F.lit(None)).otherwise(normalized_value)


def parse_work_date(column_expression) -> F.Column:
    string_value = null_if_blank(column_expression)
    shortened_value = F.when(string_value.isNotNull(), F.substring(string_value, 1, 9)).otherwise(F.lit(None))
    parsed_timestamp = F.coalesce(
        F.to_timestamp(string_value, "d-MMM-yy"),
        F.to_timestamp(string_value, "dd-MMM-yy"),
        F.to_timestamp(string_value, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(string_value, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(string_value, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(string_value, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
    )
    return F.coalesce(
        column_expression.cast("date"),
        F.to_date(string_value, "d-MMM-yy"),
        F.to_date(string_value, "dd-MMM-yy"),
        F.to_date(string_value, "yyyy-MM-dd"),
        F.to_date(string_value, "M/d/yyyy"),
        F.to_date(string_value, "MM/dd/yyyy"),
        F.to_date(shortened_value, "d-MMM-yy"),
        F.to_date(shortened_value, "dd-MMM-yy"),
        F.to_date(parsed_timestamp),
    )


def parse_hours_decimal(column_expression) -> F.Column:
    cleaned_value = null_if_blank(column_expression)
    cleaned_value = F.regexp_replace(cleaned_value, ",", "")
    return cleaned_value.cast(T.DecimalType(10, 4))


def build_row_hash(dataframe: DataFrame) -> F.Column:
    return F.sha2(F.to_json(F.struct(*[F.col(column_name) for column_name in dataframe.columns])), 256)


def build_timeclock_key() -> F.Column:
    return F.sha2(
        F.concat_ws(
            "|",
            F.coalesce(F.col("staff_name_clean"), F.lit("")),
            F.coalesce(F.date_format(F.col("work_date"), "yyyy-MM-dd"), F.lit("")),
            F.coalesce(F.col("hours_worked").cast("string"), F.lit("")),
            F.coalesce(F.col("source_file_name"), F.lit("")),
            F.coalesce(F.col("_source_row_number").cast("string"), F.lit("")),
            F.coalesce(F.col("source_row_hash"), F.lit("")),
        ),
        256,
    )


def require_columns(dataframe: DataFrame, required_columns: list[str], table_name: str) -> None:
    missing_columns = [column_name for column_name in required_columns if column_name not in dataframe.columns]
    if missing_columns:
        raise ValueError(
            f"Required columns are missing from {table_name}: {missing_columns}. "
            f"Available columns: {dataframe.columns}"
        )


def build_primary_dq_status() -> F.Column:
    return (
        F.when(F.col("work_date_raw").isNull(), F.lit("FAIL_MISSING_WORK_DATE"))
        .when(F.col("hours_raw").isNull(), F.lit("FAIL_MISSING_HOURS"))
        .when(F.col("work_date").isNull(), F.lit("FAIL_MISSING_WORK_DATE"))
        .when(F.col("hours_worked_original").isNull(), F.lit("FAIL_INVALID_HOURS"))
        .when(F.col("hours_worked_original") <= F.lit(0).cast(T.DecimalType(10, 4)), F.lit("FAIL_INVALID_HOURS"))
        .when(F.col("duplicate_salt_timeclock_key_count") > 1, F.lit("WARN_DUPLICATE_TIMECLOCK_KEY"))
        .when(F.col("hours_defaulted_missed_clockout"), F.lit("WARN_HOURS_DEFAULTED_MISSED_CLOCKOUT"))
        .otherwise(F.lit("PASS"))
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load and validate Bronze source

active_load_mode = validate_load_mode(LOAD_MODE)

if not table_exists(BRONZE_TIMECLOCK_TABLE):
    raise ValueError(
        f"Required Bronze table {BRONZE_TIMECLOCK_TABLE} does not exist. "
        "Run the Bronze timeclock ingestion notebook before loading Silver timeclock."
    )

bronze_timeclock_df = trim_all_string_fields(normalize_column_names(spark.table(BRONZE_TIMECLOCK_TABLE)))
require_columns(bronze_timeclock_df, ["day", "staff_member", "hours"], BRONZE_TIMECLOCK_TABLE)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean and standardize the timeclock source.
#
# Ignored fields are intentionally not used:
# - email
# - phone
# - clocked_in
# - clocked_out

total_source_rows = bronze_timeclock_df.count()

prepared_timeclock_df = (
    bronze_timeclock_df
    .withColumn("source_row_hash", build_row_hash(bronze_timeclock_df))
    .withColumn(
        "_source_row_number",
        F.row_number().over(
            Window.orderBy(
                F.col("source_row_hash").asc(),
                F.col("staff_member").asc_nulls_last(),
                F.col("day").asc_nulls_last(),
            )
        ),
    )
    .withColumn("staff_name_clean", null_if_blank(F.col("staff_member")))
    .withColumn("work_date_raw", null_if_blank(F.col("day")))
    .withColumn("hours_raw", null_if_blank(F.col("hours")))
    .withColumn("work_date", parse_work_date(F.col("day")))
    .withColumn("hours_worked_original", parse_hours_decimal(F.col("hours")))
    .withColumn(
        "hours_defaulted_missed_clockout",
        F.col("hours_worked_original") > F.lit(12).cast(T.DecimalType(10, 4)),
    )
    .withColumn(
        "hours_worked",
        F.when(
            F.col("hours_defaulted_missed_clockout"),
            F.lit(5).cast(T.DecimalType(10, 4)),
        ).otherwise(F.col("hours_worked_original")),
    )
    .withColumn(
        "source_loaded_at",
        F.coalesce(F.col("ingestion_timestamp").cast("timestamp"), F.lit(None).cast("timestamp"))
        if "ingestion_timestamp" in bronze_timeclock_df.columns
        else F.lit(None).cast("timestamp"),
    )
    .withColumn(
        "source_file_name",
        F.col("source_file_name").cast("string") if "source_file_name" in bronze_timeclock_df.columns else F.lit(None).cast("string"),
    )
)

rows_missing_staff_member = prepared_timeclock_df.filter(F.col("staff_name_clean").isNull()).count()
prepared_timeclock_df = prepared_timeclock_df.filter(F.col("staff_name_clean").isNotNull())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build keys, DQ notes, and final silver.timeclock output

timeclock_work_df = (
    prepared_timeclock_df
    .withColumn("salt_timeclock_key", build_timeclock_key())
    .withColumn("silver_processed_at", F.current_timestamp())
)

timeclock_key_counts_df = (
    timeclock_work_df
    .groupBy("salt_timeclock_key")
    .agg(F.count(F.lit(1)).alias("duplicate_salt_timeclock_key_count"))
)

timeclock_work_df = timeclock_work_df.join(timeclock_key_counts_df, ["salt_timeclock_key"], "left")

final_silver_timeclock_df = (
    timeclock_work_df
    .withColumn(
        "dq_notes",
        F.concat_ws(
            "; ",
            F.when(F.col("work_date_raw").isNull(), F.lit("Missing work date.")),
            F.when(F.col("work_date_raw").isNotNull() & F.col("work_date").isNull(), F.lit("Failed work date parsing.")),
            F.when(F.col("hours_raw").isNull(), F.lit("Missing hours.")),
            F.when(F.col("hours_raw").isNotNull() & F.col("hours_worked_original").isNull(), F.lit("Failed hours parsing.")),
            F.when(F.col("hours_worked_original") <= F.lit(0).cast(T.DecimalType(10, 4)), F.lit("Hours are zero or negative.")),
            F.when(F.col("hours_defaulted_missed_clockout"), F.lit("Hours greater than 12; assumed missed clock-out and defaulted hours_worked to 5.")),
            F.when(F.col("duplicate_salt_timeclock_key_count") > 1, F.lit("Duplicate generated salt_timeclock_key.")),
        ),
    )
    .withColumn("dq_status", build_primary_dq_status())
    .select(
        "salt_timeclock_key",
        "staff_name_clean",
        "work_date",
        "hours_worked_original",
        "hours_worked",
        "source_file_name",
        "source_loaded_at",
        "silver_processed_at",
        "source_row_hash",
        "dq_status",
        "dq_notes",
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DQ summary and sample output

dq_summary_row = (
    timeclock_work_df
    .agg(
        F.lit(total_source_rows).alias("total_source_rows"),
        F.count(F.lit(1)).alias("final_silver_rows"),
        F.countDistinct("staff_name_clean").alias("distinct_staff_members"),
        F.min("work_date").alias("min_work_date"),
        F.max("work_date").alias("max_work_date"),
        F.sum(F.coalesce(F.col("hours_worked_original"), F.lit(0).cast(T.DecimalType(10, 4)))).alias("total_hours_worked_original"),
        F.sum(F.coalesce(F.col("hours_worked"), F.lit(0).cast(T.DecimalType(10, 4)))).alias("total_hours_worked"),
        F.lit(rows_missing_staff_member).alias("rows_missing_staff_member"),
        F.sum(F.when(F.col("work_date_raw").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("rows_missing_work_date"),
        F.sum(F.when(F.col("work_date_raw").isNotNull() & F.col("work_date").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_failed_date_parsing"),
        F.sum(F.when(F.col("hours_raw").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("rows_missing_hours"),
        F.sum(F.when(F.col("hours_raw").isNotNull() & F.col("hours_worked_original").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_failed_hours_parsing"),
        F.sum(F.when(F.col("hours_worked_original") <= F.lit(0).cast(T.DecimalType(10, 4)), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_zero_or_negative_hours"),
        F.sum(F.when(F.col("hours_defaulted_missed_clockout"), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_hours_greater_than_12_defaulted_to_5"),
        F.sum(F.when(F.col("duplicate_salt_timeclock_key_count") > 1, F.lit(1)).otherwise(F.lit(0))).alias("duplicate_salt_timeclock_key_count"),
    )
    .collect()[0]
)

summary_metric_order = list(dq_summary_row.asDict().keys())
dq_summary_rows = [
    (metric_name, str(dq_summary_row[metric_name]) if dq_summary_row[metric_name] is not None else None)
    for metric_name in summary_metric_order
]
dq_summary_df = spark.createDataFrame(dq_summary_rows, ["metric_name", "metric_value"])

display(dq_summary_df)
display(
    final_silver_timeclock_df
    .orderBy(F.col("work_date").desc_nulls_last(), F.col("staff_name_clean").asc_nulls_last())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write silver.timeclock

ensure_schema_exists(SILVER_SCHEMA)

(
    final_silver_timeclock_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TIMECLOCK_TABLE)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

written_timeclock_df = spark.table(SILVER_TIMECLOCK_TABLE)

final_row_count = written_timeclock_df.count()
distinct_staff_count = written_timeclock_df.select("staff_name_clean").where(F.col("staff_name_clean").isNotNull()).distinct().count()
totals_row = written_timeclock_df.agg(
    F.sum(F.coalesce(F.col("hours_worked_original"), F.lit(0).cast(T.DecimalType(10, 4)))).alias("total_hours_worked_original"),
    F.sum(F.coalesce(F.col("hours_worked"), F.lit(0).cast(T.DecimalType(10, 4)))).alias("total_hours_worked"),
    F.min("work_date").alias("min_work_date"),
    F.max("work_date").alias("max_work_date"),
).collect()[0]
dq_warning_count = written_timeclock_df.filter(F.col("dq_status") != "PASS").count()

print(
    f"Successfully loaded {SILVER_TIMECLOCK_TABLE} using LOAD_MODE='{active_load_mode}'. "
    f"Final silver.timeclock row count: {final_row_count}. "
    f"Distinct staff count: {distinct_staff_count}. "
    f"Total original hours: {totals_row['total_hours_worked_original']}. "
    f"Total corrected hours: {totals_row['total_hours_worked']}. "
    f"Date range: {totals_row['min_work_date']} to {totals_row['max_work_date']}. "
    f"DQ warnings/failures: {dq_warning_count}."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
