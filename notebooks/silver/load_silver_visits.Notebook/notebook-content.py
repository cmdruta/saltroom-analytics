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

# Silver visits load for Microsoft Fabric Lakehouse.
#
# Architecture note:
# silver.visits is a cleaned attendance-level entity at one row per visit record from
# bronze.visits_purchase_option. It is not the final reporting fact table. Gold will later
# create attendance facts, retention metrics, booking-source analysis, package utilization,
# and visit frequency models.
#
# Source strategy:
# - bronze.visits_purchase_option is the authoritative base
# - bronze.visits_booking_source is enrichment only for booking_source and booked_by
# - silver.clients provides client keys and matched source identifiers
#
# Field-shape note:
# Local reference extracts `exports/visits_purchase_option.xlsx` and
# `exports/visits_booking_source.xlsx` were used to confirm the current Bronze table field
# names. The core columns are:
# - base visits: service, date, time, staff, client, email, phone, client_id, option
# - booking source: client, email, phone, client_id, service, date, time, staff, booking_source, booked_by

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
BASE_VISITS_TABLE = f"{BRONZE_SCHEMA}.visits_purchase_option"
BOOKING_SOURCE_TABLE = f"{BRONZE_SCHEMA}.visits_booking_source"
SILVER_CLIENTS_TABLE = f"{SILVER_SCHEMA}.clients"
SILVER_VISITS_TABLE = f"{SILVER_SCHEMA}.visits"

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
    return F.when(normalized_value == "", F.lit(None)).otherwise(normalized_value)


def normalize_text_key(column_expression) -> F.Column:
    normalized_value = F.lower(normalize_spaces(column_expression))
    return F.when(normalized_value == "", F.lit(None)).otherwise(normalized_value)


def normalize_email(column_expression) -> F.Column:
    return normalize_text_key(column_expression)


def normalize_phone(column_expression) -> F.Column:
    digits_only = F.regexp_replace(F.coalesce(column_expression.cast("string"), F.lit("")), r"[^0-9]", "")
    return F.when(digits_only == "", F.lit(None)).otherwise(digits_only)


def extract_start_time_text(column_expression) -> F.Column:
    start_time = normalize_spaces(F.regexp_extract(F.coalesce(column_expression.cast("string"), F.lit("")), r"^([^-]+)", 1))
    return F.when(start_time == "", F.lit(None)).otherwise(start_time)


def parse_date_value(column_expression) -> F.Column:
    string_value = column_expression.cast("string")
    numeric_value = string_value.cast("double")
    excel_serial_date = F.when(
        numeric_value.isNotNull(),
        F.date_add(F.lit("1899-12-30").cast("date"), numeric_value.cast("int")),
    )

    return F.coalesce(
        column_expression.cast("date"),
        F.to_date(string_value, "MMM d, yyyy"),
        F.to_date(string_value, "MMM dd, yyyy"),
        F.to_date(string_value, "M/d/yyyy"),
        F.to_date(string_value, "MM/dd/yyyy"),
        F.to_date(string_value, "yyyy-MM-dd"),
        excel_serial_date,
    )


def parse_time_value(column_expression) -> F.Column:
    return F.coalesce(
        F.date_format(F.to_timestamp(column_expression, "h:mma"), "HH:mm:ss"),
        F.date_format(F.to_timestamp(column_expression, "hh:mma"), "HH:mm:ss"),
        F.date_format(F.to_timestamp(column_expression, "H:mm"), "HH:mm:ss"),
        F.date_format(F.to_timestamp(column_expression, "HH:mm:ss"), "HH:mm:ss"),
    )


def parse_visit_timestamp(date_expression, time_expression) -> F.Column:
    combined_value = F.concat_ws(" ", null_if_blank(date_expression), null_if_blank(time_expression))
    return F.coalesce(
        F.to_timestamp(combined_value, "MMM d, yyyy h:mma"),
        F.to_timestamp(combined_value, "MMM dd, yyyy h:mma"),
        F.to_timestamp(combined_value, "M/d/yyyy h:mma"),
        F.to_timestamp(combined_value, "MM/dd/yyyy h:mma"),
        F.to_timestamp(combined_value, "yyyy-MM-dd HH:mm:ss"),
    )


def first_matching_column(columns: list[str], candidate_names: list[str]) -> str | None:
    available_columns = {column_name.lower(): column_name for column_name in columns}

    for candidate_name in candidate_names:
        if candidate_name.lower() in available_columns:
            return available_columns[candidate_name.lower()]

    return None


def require_columns(dataframe: DataFrame, required_columns: list[str], table_name: str) -> None:
    missing_columns = [column_name for column_name in required_columns if column_name not in dataframe.columns]
    if missing_columns:
        raise ValueError(
            f"Required columns are missing from {table_name}: {missing_columns}. "
            f"Available columns: {dataframe.columns}"
        )


def build_row_hash(dataframe: DataFrame) -> F.Column:
    return F.sha2(F.to_json(F.struct(*[F.col(column_name) for column_name in dataframe.columns])), 256)


def build_visit_key() -> F.Column:
    return F.sha2(
        F.concat_ws(
            "|",
            F.coalesce(F.col("source_visit_id"), F.lit("")),
            F.coalesce(F.col("service_name"), F.lit("")),
            F.coalesce(F.date_format(F.col("visit_date"), "yyyy-MM-dd"), F.lit("")),
            F.coalesce(F.col("visit_time"), F.lit("")),
            F.coalesce(F.col("_visit_identifier_for_key"), F.lit("")),
            F.coalesce(F.col("_base_row_id").cast("string"), F.lit("")),
        ),
        256,
    )


def build_primary_dq_status() -> F.Column:
    return (
        F.when(F.col("visit_date").isNull(), F.lit("FAIL_MISSING_VISIT_DATE"))
        .when(F.col("service_name").isNull(), F.lit("FAIL_MISSING_SERVICE"))
        .when(F.col("duplicate_salt_visit_key_count") > 1, F.lit("WARN_DUPLICATE_VISIT_KEY"))
        .when(F.col("client_match_method") == "unmatched", F.lit("WARN_CLIENT_NOT_MATCHED"))
        .when(F.col("has_multiple_client_matches_resolved"), F.lit("WARN_MULTIPLE_CLIENT_MATCHES_RESOLVED"))
        .when(F.col("booking_source_match_method") == "unmatched", F.lit("WARN_BOOKING_SOURCE_NOT_MATCHED"))
        .when(F.col("has_multiple_booking_source_matches_resolved"), F.lit("WARN_MULTIPLE_BOOKING_SOURCE_MATCHES_RESOLVED"))
        .when(F.col("client_match_method") == "name_email_phone_score", F.lit("WARN_CLIENT_MATCHED_BY_NAME"))
        .otherwise(F.lit("PASS"))
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load and validate source tables

active_load_mode = validate_load_mode(LOAD_MODE)

for required_table in [BASE_VISITS_TABLE, BOOKING_SOURCE_TABLE, SILVER_CLIENTS_TABLE]:
    if not table_exists(required_table):
        raise ValueError(f"Required table {required_table} does not exist.")

base_visits_raw_df = trim_all_string_fields(normalize_column_names(spark.table(BASE_VISITS_TABLE)))
booking_source_raw_df = trim_all_string_fields(normalize_column_names(spark.table(BOOKING_SOURCE_TABLE)))
silver_clients_raw_df = trim_all_string_fields(normalize_column_names(spark.table(SILVER_CLIENTS_TABLE)))

require_columns(base_visits_raw_df, ["service", "date", "time", "client"], BASE_VISITS_TABLE)
require_columns(booking_source_raw_df, ["service", "client"], BOOKING_SOURCE_TABLE)
require_columns(
    silver_clients_raw_df,
    ["salt_client_key", "source_client_id", "source_member_id", "client"],
    SILVER_CLIENTS_TABLE,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardize the authoritative base visits table

base_date_column = first_matching_column(base_visits_raw_df.columns, ["date", "visit_date", "class_date"])
base_time_column = first_matching_column(base_visits_raw_df.columns, ["time", "visit_time", "class_time"])
base_client_id_column = first_matching_column(base_visits_raw_df.columns, ["client_id", "member_id", "user_id"])
base_source_visit_id_column = first_matching_column(base_visits_raw_df.columns, ["attendance_id", "visit_id", "appointment_id", "booking_id", "id"])
base_attendance_status_column = first_matching_column(base_visits_raw_df.columns, ["attendance_status", "status"])
base_option_column = first_matching_column(base_visits_raw_df.columns, ["option", "purchase_option", "purchase_option_name"])
base_option_type_column = first_matching_column(base_visits_raw_df.columns, ["purchase_option_type", "option_type"])
base_staff_column = first_matching_column(base_visits_raw_df.columns, ["staff", "class_staff", "instructor", "staff_name"])
base_location_column = first_matching_column(base_visits_raw_df.columns, ["location", "location_name", "site"])
base_service_category_column = first_matching_column(base_visits_raw_df.columns, ["service_category", "category"])
base_source_file_column = first_matching_column(base_visits_raw_df.columns, ["source_file_name"])
base_loaded_at_column = first_matching_column(base_visits_raw_df.columns, ["ingestion_timestamp", "source_loaded_at"])

base_visits_prepared_df = (
    base_visits_raw_df
    .withColumn("source_row_hash", build_row_hash(base_visits_raw_df))
    .withColumn(
        "_base_row_id",
        F.row_number().over(Window.orderBy(F.col("source_row_hash").asc(), F.col("service").asc_nulls_last(), F.col("client").asc_nulls_last())),
    )
    .withColumn("service_name", null_if_blank(F.col("service")))
    .withColumn("service_name_key", normalize_text_key(F.col("service")))
    .withColumn("service_category", null_if_blank(F.col(base_service_category_column)) if base_service_category_column else F.lit(None).cast("string"))
    .withColumn("visit_date_raw", null_if_blank(F.col(base_date_column)))
    .withColumn("visit_time_raw", null_if_blank(F.col(base_time_column)))
    .withColumn("visit_time_start_raw", extract_start_time_text(F.col(base_time_column)))
    .withColumn("visit_date", parse_date_value(F.col(base_date_column)))
    .withColumn("visit_time", parse_time_value(F.col("visit_time_start_raw")))
    .withColumn("visit_datetime", parse_visit_timestamp(F.col(base_date_column), F.col("visit_time_start_raw")))
    .withColumn("client_name_raw", null_if_blank(F.col("client")))
    .withColumn("client_name_key", normalize_text_key(F.col("client")))
    .withColumn("visit_client_identifier", null_if_blank(F.col(base_client_id_column)) if base_client_id_column else F.lit(None).cast("string"))
    .withColumn("visit_email_clean", normalize_email(F.col("email")) if "email" in base_visits_raw_df.columns else F.lit(None).cast("string"))
    .withColumn("visit_phone_clean", normalize_phone(F.col("phone")) if "phone" in base_visits_raw_df.columns else F.lit(None).cast("string"))
    .withColumn("source_visit_id", null_if_blank(F.col(base_source_visit_id_column)) if base_source_visit_id_column else F.lit(None).cast("string"))
    .withColumn("attendance_status", null_if_blank(F.col(base_attendance_status_column)) if base_attendance_status_column else F.lit(None).cast("string"))
    .withColumn("purchase_option_name", null_if_blank(F.col(base_option_column)) if base_option_column else F.lit(None).cast("string"))
    .withColumn("purchase_option_type", null_if_blank(F.col(base_option_type_column)) if base_option_type_column else F.lit(None).cast("string"))
    .withColumn("staff_name", null_if_blank(F.col(base_staff_column)) if base_staff_column else F.lit(None).cast("string"))
    .withColumn("location_name", null_if_blank(F.col(base_location_column)) if base_location_column else F.lit(None).cast("string"))
    .withColumn("source_file_name", F.col(base_source_file_column).cast("string") if base_source_file_column else F.lit(None).cast("string"))
    .withColumn("source_loaded_at", F.col(base_loaded_at_column).cast("timestamp") if base_loaded_at_column else F.lit(None).cast("timestamp"))
)

total_base_visit_rows = base_visits_prepared_df.count()
base_visit_columns = base_visits_prepared_df.columns


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardize booking source enrichment records

booking_date_column = first_matching_column(booking_source_raw_df.columns, ["class_date", "date", "visit_date"])
booking_time_column = first_matching_column(booking_source_raw_df.columns, ["class_time", "time", "visit_time"])
booking_client_id_column = first_matching_column(booking_source_raw_df.columns, ["client_id", "member_id", "user_id"])
booking_source_file_column = first_matching_column(booking_source_raw_df.columns, ["source_file_name"])
booking_loaded_at_column = first_matching_column(booking_source_raw_df.columns, ["ingestion_timestamp", "source_loaded_at"])

if not booking_date_column or not booking_time_column:
    raise ValueError(
        f"{BOOKING_SOURCE_TABLE} is missing recognizable visit date/time columns. "
        f"Available columns: {booking_source_raw_df.columns}"
    )

booking_source_prepared_df = (
    booking_source_raw_df
    .withColumn("_booking_row_hash", build_row_hash(booking_source_raw_df))
    .withColumn(
        "_booking_row_id",
        F.row_number().over(Window.orderBy(F.col("_booking_row_hash").asc(), F.col("service").asc_nulls_last(), F.col("client").asc_nulls_last())),
    )
    .withColumn("service_name_key", normalize_text_key(F.col("service")))
    .withColumn("visit_date", parse_date_value(F.col(booking_date_column)) if booking_date_column else F.lit(None).cast("date"))
    .withColumn("visit_time_start_raw", extract_start_time_text(F.col(booking_time_column)) if booking_time_column else F.lit(None).cast("string"))
    .withColumn("visit_time", parse_time_value(F.col("visit_time_start_raw")))
    .withColumn("client_name_key", normalize_text_key(F.col("client")))
    .withColumn("visit_client_identifier", null_if_blank(F.col(booking_client_id_column)) if booking_client_id_column else F.lit(None).cast("string"))
    .withColumn("visit_email_clean", normalize_email(F.col("email")) if "email" in booking_source_raw_df.columns else F.lit(None).cast("string"))
    .withColumn("visit_phone_clean", normalize_phone(F.col("phone")) if "phone" in booking_source_raw_df.columns else F.lit(None).cast("string"))
    .withColumn("booking_source", null_if_blank(F.col("booking_source")) if "booking_source" in booking_source_raw_df.columns else F.lit(None).cast("string"))
    .withColumn("booked_by", null_if_blank(F.col("booked_by")) if "booked_by" in booking_source_raw_df.columns else F.lit(None).cast("string"))
    .withColumn("source_file_name", F.col(booking_source_file_column).cast("string") if booking_source_file_column else F.lit(None).cast("string"))
    .withColumn("source_loaded_at", F.col(booking_loaded_at_column).cast("timestamp") if booking_loaded_at_column else F.lit(None).cast("timestamp"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enrich base visits with booking source data.
#
# Matching priority inside the shared service/date/time grain:
# 1. client identifier
# 2. normalized client name
# 3. normalized email
# 4. normalized phone

booking_candidates_df = (
    base_visits_prepared_df.alias("b")
    .join(
        booking_source_prepared_df.alias("s"),
        (F.col("b.service_name_key") == F.col("s.service_name_key"))
        & (F.col("b.visit_date") == F.col("s.visit_date"))
        & (F.col("b.visit_time") == F.col("s.visit_time")),
        "left",
    )
    .withColumn("has_booking_candidate", F.col("s._booking_row_id").isNotNull())
    .withColumn(
        "client_id_match_score",
        F.when(
            F.col("s._booking_row_id").isNotNull()
            & F.col("b.visit_client_identifier").isNotNull()
            & (F.col("b.visit_client_identifier") == F.col("s.visit_client_identifier")),
            F.lit(100),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "client_name_match_score",
        F.when(
            F.col("s._booking_row_id").isNotNull()
            & F.col("b.client_name_key").isNotNull()
            & (F.col("b.client_name_key") == F.col("s.client_name_key")),
            F.lit(75),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "email_match_score",
        F.when(
            F.col("s._booking_row_id").isNotNull()
            & F.col("b.visit_email_clean").isNotNull()
            & (F.col("b.visit_email_clean") == F.col("s.visit_email_clean")),
            F.lit(50),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "phone_match_score",
        F.when(
            F.col("s._booking_row_id").isNotNull()
            & F.col("b.visit_phone_clean").isNotNull()
            & (F.col("b.visit_phone_clean") == F.col("s.visit_phone_clean")),
            F.lit(50),
        ).otherwise(F.lit(0)),
    )
    .withColumn("time_grain_score", F.when(F.col("s._booking_row_id").isNotNull(), F.lit(25)).otherwise(F.lit(0)))
    .withColumn("booking_source_value_score", F.when(F.col("s.booking_source").isNotNull(), F.lit(10)).otherwise(F.lit(0)))
    .withColumn("booked_by_value_score", F.when(F.col("s.booked_by").isNotNull(), F.lit(10)).otherwise(F.lit(0)))
    .withColumn(
        "booking_source_match_score",
        F.col("client_id_match_score")
        + F.col("client_name_match_score")
        + F.col("email_match_score")
        + F.col("phone_match_score")
        + F.col("time_grain_score")
        + F.col("booking_source_value_score")
        + F.col("booked_by_value_score"),
    )
    .withColumn("_base_row_id", F.col("b._base_row_id"))
    .withColumn("_booking_row_id", F.col("s._booking_row_id"))
)

booking_candidate_counts_df = (
    booking_candidates_df
    .groupBy("_base_row_id")
    .agg(F.sum(F.when(F.col("has_booking_candidate"), F.lit(1)).otherwise(F.lit(0))).alias("booking_candidate_count"))
)

booking_match_window = Window.partitionBy("_base_row_id").orderBy(
    F.col("client_id_match_score").desc(),
    F.col("client_name_match_score").desc(),
    F.col("email_match_score").desc(),
    F.col("phone_match_score").desc(),
    F.col("booking_source_match_score").desc(),
    F.col("_booking_row_id").asc_nulls_last(),
)

booking_enriched_df = (
    booking_candidates_df
    .join(booking_candidate_counts_df, ["_base_row_id"], "left")
    .withColumn("_booking_match_rank", F.row_number().over(booking_match_window))
    .filter(F.col("_booking_match_rank") == 1)
    .withColumn(
        "booking_source_match_method",
        F.when(~F.col("has_booking_candidate"), F.lit("unmatched"))
        .when(F.col("client_id_match_score") > 0, F.lit("client_id"))
        .when(F.col("client_name_match_score") > 0, F.lit("client_name"))
        .when(F.col("email_match_score") > 0, F.lit("email"))
        .when(F.col("phone_match_score") > 0, F.lit("phone"))
        .otherwise(F.lit("unmatched")),
    )
    .withColumn(
        "has_multiple_booking_source_matches_resolved",
        (F.col("booking_candidate_count") > 1) & (F.col("booking_source_match_method") != "unmatched"),
    )
    .select(
        *[F.col(column_name) for column_name in base_visit_columns],
        F.col("booking_source"),
        F.col("booked_by"),
        "booking_source_match_method",
        "booking_source_match_score",
        "has_multiple_booking_source_matches_resolved",
    )
)
visit_match_base_columns = booking_enriched_df.columns


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Match visits to silver.clients.
#
# Priority:
# 1. visit client identifier against silver client source ids
# 2. fallback to normalized full name with email/phone support inside scoring

clients_prepared_df = (
    silver_clients_raw_df
    .withColumn("client_name_key", normalize_text_key(F.col("client")))
    .withColumn("source_client_id", null_if_blank(F.col("source_client_id")))
    .withColumn("source_member_id", null_if_blank(F.col("source_member_id")))
    .withColumn("email_clean", normalize_email(F.col("email_clean")) if "email_clean" in silver_clients_raw_df.columns else F.lit(None).cast("string"))
    .withColumn("phone_clean", normalize_phone(F.col("phone_clean")) if "phone_clean" in silver_clients_raw_df.columns else F.lit(None).cast("string"))
    .withColumn("client_status", normalize_text_key(F.col("client_status")) if "client_status" in silver_clients_raw_df.columns else F.lit(None).cast("string"))
    .withColumn("registration_date", F.col("registration_date").cast("date") if "registration_date" in silver_clients_raw_df.columns else F.lit(None).cast("date"))
    .withColumn("dedupe_score", F.coalesce(F.col("dedupe_score").cast("int"), F.lit(0)) if "dedupe_score" in silver_clients_raw_df.columns else F.lit(0))
    .withColumn("duplicate_name_count", F.coalesce(F.col("duplicate_name_count").cast("int"), F.lit(0)) if "duplicate_name_count" in silver_clients_raw_df.columns else F.lit(0))
)

client_id_reference_df = (
    clients_prepared_df
    .select(
        "salt_client_key",
        "source_client_id",
        "source_member_id",
        "client_name_key",
        "email_clean",
        "phone_clean",
        "client_status",
        "registration_date",
        "dedupe_score",
        "duplicate_name_count",
        F.col("source_member_id").alias("_match_identifier"),
    )
    .filter(F.col("_match_identifier").isNotNull())
    .unionByName(
        clients_prepared_df
        .select(
            "salt_client_key",
            "source_client_id",
            "source_member_id",
            "client_name_key",
            "email_clean",
            "phone_clean",
            "client_status",
            "registration_date",
            "dedupe_score",
            "duplicate_name_count",
            F.col("source_client_id").alias("_match_identifier"),
        )
        .filter(F.col("_match_identifier").isNotNull()),
        allowMissingColumns=True,
    )
)

client_id_match_window = Window.partitionBy("_match_identifier").orderBy(
    F.col("duplicate_name_count").asc_nulls_last(),
    F.col("dedupe_score").desc_nulls_last(),
    F.col("registration_date").desc_nulls_last(),
    F.col("source_client_id").asc_nulls_last(),
)

client_id_reference_df = (
    client_id_reference_df
    .withColumn("_client_id_reference_rank", F.row_number().over(client_id_match_window))
    .filter(F.col("_client_id_reference_rank") == 1)
)

id_joined_visits_df = (
    booking_enriched_df.alias("v")
    .join(
        client_id_reference_df.alias("c"),
        F.col("v.visit_client_identifier") == F.col("c._match_identifier"),
        "left",
    )
    .withColumn("client_id_match_found", F.col("c.salt_client_key").isNotNull())
)

id_matched_visits_df = (
    id_joined_visits_df
    .filter(F.col("client_id_match_found"))
    .withColumn("salt_client_key", F.col("c.salt_client_key"))
    .withColumn("source_client_id", F.col("c.source_client_id"))
    .withColumn("source_member_id", F.col("c.source_member_id"))
    .withColumn("client_match_method", F.lit("client_id_or_member_id"))
    .withColumn("client_match_score", F.lit(100))
    .withColumn("has_multiple_client_matches_resolved", F.lit(False))
    .select(
        *[F.col(column_name) for column_name in visit_match_base_columns],
        "salt_client_key",
        "source_client_id",
        "source_member_id",
        "client_match_method",
        "client_match_score",
        "has_multiple_client_matches_resolved",
    )
)

name_fallback_visits_df = (
    id_joined_visits_df
    .filter(~F.col("client_id_match_found"))
    .select(*[F.col(column_name) for column_name in visit_match_base_columns])
)

client_name_candidates_df = clients_prepared_df.select(
    F.col("salt_client_key").alias("candidate_salt_client_key"),
    F.col("source_client_id").alias("candidate_source_client_id"),
    F.col("source_member_id").alias("candidate_source_member_id"),
    F.col("client_name_key").alias("candidate_client_name_key"),
    F.col("email_clean").alias("candidate_email_clean"),
    F.col("phone_clean").alias("candidate_phone_clean"),
    F.col("client_status").alias("candidate_client_status"),
    F.col("registration_date").alias("candidate_registration_date"),
    F.col("dedupe_score").alias("candidate_dedupe_score"),
    F.col("duplicate_name_count").alias("candidate_duplicate_name_count"),
)

client_candidates_df = (
    name_fallback_visits_df.alias("v")
    .join(
        client_name_candidates_df.alias("c"),
        F.col("v.client_name_key") == F.col("c.candidate_client_name_key"),
        "left",
    )
    .withColumn("has_client_candidate", F.col("c.candidate_salt_client_key").isNotNull())
    .withColumn("name_match_score", F.when(F.col("has_client_candidate"), F.lit(75)).otherwise(F.lit(0)))
    .withColumn("email_match_score", F.when(F.col("v.visit_email_clean").isNotNull() & (F.col("v.visit_email_clean") == F.col("c.candidate_email_clean")), F.lit(50)).otherwise(F.lit(0)))
    .withColumn("phone_match_score", F.when(F.col("v.visit_phone_clean").isNotNull() & (F.col("v.visit_phone_clean") == F.col("c.candidate_phone_clean")), F.lit(50)).otherwise(F.lit(0)))
    .withColumn("registration_score", F.when(F.col("c.candidate_registration_date").isNotNull() & F.col("v.visit_date").isNotNull() & (F.col("c.candidate_registration_date") <= F.col("v.visit_date")), F.lit(25)).otherwise(F.lit(0)))
    .withColumn("status_score", F.when(F.coalesce(F.col("c.candidate_client_status"), F.lit("")).rlike("active|current|pass"), F.lit(20)).otherwise(F.lit(0)))
    .withColumn("duplicate_name_score", F.when(F.col("c.candidate_duplicate_name_count") == 1, F.lit(10)).otherwise(F.lit(0)))
    .withColumn(
        "client_match_score",
        F.when(
            F.col("has_client_candidate"),
            F.col("name_match_score")
            + F.col("email_match_score")
            + F.col("phone_match_score")
            + F.col("registration_score")
            + F.col("status_score")
            + F.col("duplicate_name_score")
            + F.coalesce(F.col("c.candidate_dedupe_score"), F.lit(0)),
        ).otherwise(F.lit(-1)),
    )
    .withColumn("_base_row_id", F.col("v._base_row_id"))
)

client_candidate_counts_df = (
    client_candidates_df
    .groupBy("_base_row_id")
    .agg(F.sum(F.when(F.col("has_client_candidate"), F.lit(1)).otherwise(F.lit(0))).alias("client_candidate_count"))
)

client_match_window = Window.partitionBy("_base_row_id").orderBy(
    F.col("has_client_candidate").desc(),
    F.col("client_match_score").desc(),
    F.col("candidate_source_client_id").asc_nulls_last(),
    F.col("candidate_salt_client_key").asc_nulls_last(),
)

fallback_client_matched_df = (
    client_candidates_df
    .join(client_candidate_counts_df, ["_base_row_id"], "left")
    .withColumn("_client_match_rank", F.row_number().over(client_match_window))
    .filter(F.col("_client_match_rank") == 1)
    .withColumn("salt_client_key", F.col("c.candidate_salt_client_key"))
    .withColumn("source_client_id", F.col("c.candidate_source_client_id"))
    .withColumn("source_member_id", F.col("c.candidate_source_member_id"))
    .withColumn(
        "client_match_method",
        F.when(F.col("has_client_candidate"), F.lit("name_email_phone_score")).otherwise(F.lit("unmatched")),
    )
    .withColumn(
        "has_multiple_client_matches_resolved",
        (F.col("client_candidate_count") > 1) & (F.col("client_match_method") != "unmatched"),
    )
    .select(
        *[F.col(column_name) for column_name in visit_match_base_columns],
        "salt_client_key",
        "source_client_id",
        "source_member_id",
        "client_match_method",
        "client_match_score",
        "has_multiple_client_matches_resolved",
    )
)

matched_visits_df = id_matched_visits_df.unionByName(fallback_client_matched_df, allowMissingColumns=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build visit keys, DQ status, and final Silver output

final_visits_work_df = (
    matched_visits_df
    .withColumn(
        "_visit_identifier_for_key",
        F.coalesce(
            F.col("source_visit_id"),
            F.col("visit_client_identifier"),
            F.col("client_name_key"),
            F.col("visit_email_clean"),
            F.col("visit_phone_clean"),
        ),
    )
    .withColumn("salt_visit_key", build_visit_key())
    .withColumn("silver_processed_at", F.current_timestamp())
)

visit_key_counts_df = (
    final_visits_work_df
    .groupBy("salt_visit_key")
    .agg(F.count(F.lit(1)).alias("duplicate_salt_visit_key_count"))
)

final_visits_work_df = final_visits_work_df.join(visit_key_counts_df, ["salt_visit_key"], "left")

final_silver_visits_df = (
    final_visits_work_df
    .withColumn(
        "dq_notes",
        F.concat_ws(
            "; ",
            F.when(F.col("visit_date_raw").isNull(), F.lit("missing visit date")),
            F.when(F.col("visit_date_raw").isNotNull() & F.col("visit_date").isNull(), F.lit("failed visit date parsing")),
            F.when(F.col("visit_time_raw").isNull(), F.lit("missing visit time")),
            F.when(F.col("visit_time_start_raw").isNotNull() & F.col("visit_time").isNull(), F.lit("failed visit time parsing")),
            F.when(F.col("service_name").isNull(), F.lit("missing service")),
            F.when(F.col("visit_client_identifier").isNull() & F.col("client_name_key").isNull() & F.col("visit_email_clean").isNull() & F.col("visit_phone_clean").isNull(), F.lit("missing client identifier, name, email, and phone for matching")),
            F.when(F.col("client_match_method") == "unmatched", F.lit("client not matched to silver.clients")),
            F.when(F.col("client_match_method") == "name_email_phone_score", F.lit("client matched by name fallback")),
            F.when(F.col("has_multiple_client_matches_resolved"), F.lit("multiple client matches resolved by score")),
            F.when(F.col("booking_source_match_method") == "unmatched", F.lit("booking source not matched")),
            F.when(F.col("has_multiple_booking_source_matches_resolved"), F.lit("multiple booking source matches resolved by score")),
            F.when(F.col("duplicate_salt_visit_key_count") > 1, F.lit("duplicate generated salt_visit_key")),
            F.when(F.col("attendance_status").isNull(), F.lit("missing attendance status")),
            F.when(F.col("purchase_option_name").isNull(), F.lit("missing purchase option")),
            F.when(F.col("booking_source_match_method") == "unmatched", F.lit("source row retained but booking enrichment unavailable")),
        ),
    )
    .withColumn("dq_status", build_primary_dq_status())
    .select(
        "salt_visit_key",
        "source_visit_id",
        "salt_client_key",
        "source_client_id",
        "source_member_id",
        "client_match_method",
        "client_match_score",
        "visit_date",
        "visit_time",
        "visit_datetime",
        "service_name",
        "service_category",
        "attendance_status",
        "purchase_option_name",
        "purchase_option_type",
        "staff_name",
        "location_name",
        "booking_source",
        "booked_by",
        "booking_source_match_method",
        "booking_source_match_score",
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
    final_visits_work_df
    .agg(
        F.lit(total_base_visit_rows).alias("total_base_visit_rows"),
        F.count(F.lit(1)).alias("final_silver_visit_rows"),
        F.sum(F.when(F.col("booking_source_match_method") == "client_id", F.lit(1)).otherwise(F.lit(0))).alias("rows_with_booking_source_matched_by_client_id"),
        F.sum(F.when(F.col("booking_source_match_method") == "client_name", F.lit(1)).otherwise(F.lit(0))).alias("rows_with_booking_source_matched_by_client_name"),
        F.sum(F.when(F.col("booking_source_match_method") == "email", F.lit(1)).otherwise(F.lit(0))).alias("rows_with_booking_source_matched_by_email"),
        F.sum(F.when(F.col("booking_source_match_method") == "phone", F.lit(1)).otherwise(F.lit(0))).alias("rows_with_booking_source_matched_by_phone"),
        F.sum(F.when(F.col("booking_source_match_method") == "unmatched", F.lit(1)).otherwise(F.lit(0))).alias("rows_with_no_booking_source_match"),
        F.sum(F.when(F.col("has_multiple_booking_source_matches_resolved"), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_multiple_booking_source_matches_resolved"),
        F.sum(F.when(F.col("client_match_method") == "client_id_or_member_id", F.lit(1)).otherwise(F.lit(0))).alias("rows_matched_to_clients_by_id"),
        F.sum(F.when(F.col("client_match_method") == "name_email_phone_score", F.lit(1)).otherwise(F.lit(0))).alias("rows_matched_to_clients_by_name_email_phone_score"),
        F.sum(F.when(F.col("client_match_method") == "unmatched", F.lit(1)).otherwise(F.lit(0))).alias("rows_unmatched_to_clients"),
        F.sum(F.when(F.col("has_multiple_client_matches_resolved"), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_multiple_client_matches_resolved"),
        F.sum(F.when(F.col("visit_date_raw").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("rows_missing_visit_date"),
        F.sum(F.when(F.col("visit_time_raw").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("rows_missing_visit_time"),
        F.sum(F.when(F.col("service_name").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("rows_missing_service"),
        F.sum(F.when(F.col("attendance_status").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("rows_missing_attendance_status"),
        F.sum(F.when(F.col("purchase_option_name").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("rows_missing_purchase_option"),
        F.sum(F.when(F.col("duplicate_salt_visit_key_count") > 1, F.lit(1)).otherwise(F.lit(0))).alias("duplicate_salt_visit_key_count"),
    )
    .collect()[0]
)

summary_metric_order = list(dq_summary_row.asDict().keys())
dq_summary_rows = [(metric_name, str(dq_summary_row[metric_name])) for metric_name in summary_metric_order]
dq_summary_df = spark.createDataFrame(dq_summary_rows, ["metric_name", "metric_value"])

display(dq_summary_df)
display(
    final_silver_visits_df
    .select(
        "salt_visit_key",
        "source_visit_id",
        "client_match_method",
        "booking_source_match_method",
        "visit_date",
        "visit_time",
        "service_name",
        "booking_source",
        "booked_by",
        "dq_status",
        "dq_notes",
    )
    .orderBy(F.col("visit_date").desc_nulls_last(), F.col("visit_time").desc_nulls_last())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write silver.visits

ensure_schema_exists(SILVER_SCHEMA)

(
    final_silver_visits_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_VISITS_TABLE)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

written_visits_df = spark.table(SILVER_VISITS_TABLE)

final_row_count = written_visits_df.count()
matched_client_count = written_visits_df.filter(F.col("salt_client_key").isNotNull()).count()
unmatched_client_count = written_visits_df.filter(F.col("salt_client_key").isNull()).count()
enriched_booking_source_count = written_visits_df.filter(F.col("booking_source").isNotNull()).count()
missing_booking_source_count = written_visits_df.filter(F.col("booking_source").isNull()).count()
dq_warning_count = written_visits_df.filter(F.col("dq_status") != "PASS").count()

print(
    f"Successfully loaded {SILVER_VISITS_TABLE} using LOAD_MODE='{active_load_mode}'. "
    f"Final silver.visits row count: {final_row_count}. "
    f"Matched to clients: {matched_client_count}. "
    f"Unmatched to clients: {unmatched_client_count}. "
    f"Enriched with booking_source: {enriched_booking_source_count}. "
    f"Missing booking_source: {missing_booking_source_count}. "
    f"DQ warnings/failures: {dq_warning_count}."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
