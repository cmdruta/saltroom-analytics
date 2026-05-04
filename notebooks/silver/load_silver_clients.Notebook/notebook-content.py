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

from datetime import datetime

if not batch_id:
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Silver clients load for Microsoft Fabric Lakehouse.
#
# Important project rule:
# WellnessLiving visits and purchases identify clients only by full name, so this notebook
# intentionally derives the practical client key from normalized first + last name.
# This is not a true MDM solution and can incorrectly merge different people who share a name.
# The notebook surfaces that risk through duplicate counts and DQ status / notes.

import json
import re
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import Window
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

BRONZE_CONFIG_CANDIDATE_PATHS = [
    "Files/config/bronze_sources.json",
    "/lakehouse/default/Files/config/bronze_sources.json",
    "../../config/bronze_sources.json",
]

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
TARGET_TABLE = "clients"

VALID_LOAD_MODES = {"init", "refresh"}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Helper functions

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


def validate_load_mode(load_mode: str) -> str:
    normalized_mode = load_mode.strip().lower()
    if normalized_mode not in VALID_LOAD_MODES:
        raise ValueError(f"Unsupported LOAD_MODE '{load_mode}'. Expected one of: {sorted(VALID_LOAD_MODES)}")
    return normalized_mode


def normalize_column_name(column_name: str) -> str:
    normalized = column_name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def normalize_column_names(dataframe: DataFrame) -> DataFrame:
    seen_names: dict[str, int] = {}
    renamed_columns = []

    for original_name in dataframe.columns:
        normalized_name = normalize_column_name(original_name)
        normalized_name = normalized_name or "column"

        if normalized_name in seen_names:
            seen_names[normalized_name] += 1
            normalized_name = f"{normalized_name}_{seen_names[normalized_name]}"
        else:
            seen_names[normalized_name] = 0

        renamed_columns.append(F.col(f"`{original_name}`").alias(normalized_name))

    return dataframe.select(*renamed_columns)


def trim_all_string_fields(dataframe: DataFrame) -> DataFrame:
    trimmed_columns = []

    for field in dataframe.schema.fields:
        if isinstance(field.dataType, T.StringType):
            trimmed_columns.append(F.trim(F.col(field.name)).alias(field.name))
        else:
            trimmed_columns.append(F.col(field.name))

    return dataframe.select(*trimmed_columns)


def normalize_spaces(column_expression) -> F.Column:
    return F.regexp_replace(F.trim(column_expression), r"\s+", " ")


def null_if_blank(column_expression) -> F.Column:
    normalized_value = normalize_spaces(column_expression.cast("string"))
    return F.when(normalized_value == "", F.lit(None)).otherwise(normalized_value)


def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def ensure_schema_exists(schema_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def first_matching_column(columns: list[str], candidate_names: list[str]) -> str | None:
    available_columns = {column_name.lower(): column_name for column_name in columns}

    for candidate_name in candidate_names:
        if candidate_name.lower() in available_columns:
            return available_columns[candidate_name.lower()]

    return None


def column_or_null(dataframe: DataFrame, candidate_names: list[str], data_type: str = "string"):
    matched_column = first_matching_column(dataframe.columns, candidate_names)
    if matched_column:
        return F.col(matched_column)
    return F.lit(None).cast(data_type)


def standardize_phone(column_expression) -> F.Column:
    return null_if_blank(column_expression)


def normalize_client_name(first_name_expression, last_name_expression) -> F.Column:
    first_name = F.coalesce(first_name_expression, F.lit(""))
    last_name = F.coalesce(last_name_expression, F.lit(""))
    combined = F.concat_ws(" ", first_name, last_name)
    normalized = normalize_spaces(combined)
    return F.when(normalized == "", F.lit(None)).otherwise(F.lower(normalized))


def parse_date_if_present(column_expression) -> F.Column:
    return F.coalesce(
        F.to_date(F.to_timestamp(column_expression, "MMM d, yyyy hh:mma")),
        F.to_date(F.to_timestamp(column_expression, "MMM dd, yyyy hh:mma")),
        F.to_date(F.to_timestamp(column_expression, "M/d/yyyy h:mma")),
        F.to_date(F.to_timestamp(column_expression, "MM/dd/yyyy h:mma")),
        F.to_date(column_expression, "MMM d, yyyy"),
        F.to_date(column_expression, "MMM dd, yyyy"),
        F.to_date(column_expression, "M/d/yyyy"),
        F.to_date(column_expression, "MM/dd/yyyy"),
        F.to_date(column_expression, "yyyy-MM-dd"),
    )


def count_non_null_fields(dataframe: DataFrame, excluded_columns: set[str]) -> F.Column:
    score_parts = []

    for column_name in dataframe.columns:
        if column_name in excluded_columns:
            continue
        score_parts.append(F.when(F.col(column_name).isNotNull(), F.lit(1)).otherwise(F.lit(0)))

    return sum(score_parts, F.lit(0))


def build_dq_notes() -> F.Column:
    return F.concat_ws(
        "; ",
        F.when(F.col("duplicate_name_count") > 1, F.lit("duplicate normalized client name merged into one silver record")),
        F.when(F.col("source_client_id").isNull(), F.lit("missing source client id")),
        F.when(F.col("source_member_id").isNull(), F.lit("missing source member id")),
        F.when(F.col("email_clean").isNull(), F.lit("missing email")),
        F.when(F.col("phone_clean").isNull(), F.lit("missing phone")),
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load configuration and validate Bronze source

active_load_mode = validate_load_mode(load_mode)
bronze_config = load_config(BRONZE_CONFIG_CANDIDATE_PATHS)
bronze_clients_config = bronze_config["datasets"]["clients"]

bronze_clients_table = f"{BRONZE_SCHEMA}.{bronze_clients_config['target_table']}"
silver_clients_table = f"{SILVER_SCHEMA}.{TARGET_TABLE}"

if not table_exists(bronze_clients_table):
    raise ValueError(
        f"Required Bronze table {bronze_clients_table} does not exist. "
        "Run the Bronze clients ingestion notebook before loading Silver clients."
    )

bronze_clients_df = spark.table(bronze_clients_table)

if "source_file_name" not in bronze_clients_df.columns:
    raise ValueError(
        f"Bronze table {bronze_clients_table} is missing source_file_name. "
        "The Bronze ingestion audit columns are required for Silver lineage and DQ checks."
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardize and trim Bronze clients

bronze_clients_df = normalize_column_names(bronze_clients_df)
bronze_clients_df = trim_all_string_fields(bronze_clients_df)

first_name_column = first_matching_column(bronze_clients_df.columns, ["first_name", "client_first_name", "firstname"])
last_name_column = first_matching_column(bronze_clients_df.columns, ["last_name", "client_last_name", "lastname"])

if not first_name_column or not last_name_column:
    raise ValueError(
        f"Bronze clients table {bronze_clients_table} is missing recognizable first/last name columns. "
        f"Available columns: {bronze_clients_df.columns}"
    )

source_client_id_column = first_matching_column(bronze_clients_df.columns, ["user_id", "client_id", "source_client_id", "id"])
member_id_column = first_matching_column(bronze_clients_df.columns, ["member_id", "client_id"])
email_column = first_matching_column(bronze_clients_df.columns, ["email", "email_address", "username"])
primary_phone_column = first_matching_column(bronze_clients_df.columns, ["phone_number", "phone", "mobile_phone", "cell_phone"])
home_phone_column = first_matching_column(bronze_clients_df.columns, ["home_phone"])
work_phone_column = first_matching_column(bronze_clients_df.columns, ["work_phone"])
status_column = first_matching_column(bronze_clients_df.columns, ["client_type", "status", "client_status", "active_status", "check_in_status"])
registration_date_column = first_matching_column(
    bronze_clients_df.columns,
    ["client_since_date", "registration_date", "registered_date", "date_registered", "created_date", "created_at", "join_date"],
)
source_loaded_at_column = first_matching_column(bronze_clients_df.columns, ["ingestion_timestamp", "source_loaded_at"])

bronze_clients_df = bronze_clients_df.withColumn("_source_row_hash", F.sha2(F.to_json(F.struct(*[F.col(column_name) for column_name in bronze_clients_df.columns])), 256))
bronze_clients_df = bronze_clients_df.withColumn("_source_row_sequence", F.row_number().over(Window.orderBy("_source_row_hash")))

prepared_clients_df = (
    bronze_clients_df
    .withColumn("client_first_name_clean", null_if_blank(F.col(first_name_column)))
    .withColumn("client_last_name_clean", null_if_blank(F.col(last_name_column)))
    .withColumn("client_full_name_clean", null_if_blank(F.concat_ws(" ", F.col("client_first_name_clean"), F.col("client_last_name_clean"))))
    .withColumn("client", normalize_client_name(F.col("client_first_name_clean"), F.col("client_last_name_clean")))
    .withColumn("source_client_id", null_if_blank(column_or_null(bronze_clients_df, [source_client_id_column] if source_client_id_column else [], "string")))
    .withColumn("source_member_id", null_if_blank(column_or_null(bronze_clients_df, [member_id_column] if member_id_column else [], "string")))
    .withColumn("email_clean", F.lower(null_if_blank(column_or_null(bronze_clients_df, [email_column] if email_column else [], "string"))))
    .withColumn(
        "phone_clean",
        F.coalesce(
            standardize_phone(column_or_null(bronze_clients_df, [primary_phone_column] if primary_phone_column else [], "string")),
            standardize_phone(column_or_null(bronze_clients_df, [home_phone_column] if home_phone_column else [], "string")),
            standardize_phone(column_or_null(bronze_clients_df, [work_phone_column] if work_phone_column else [], "string")),
        ),
    )
    .withColumn("client_status", null_if_blank(column_or_null(bronze_clients_df, [status_column] if status_column else [], "string")))
    .withColumn("registration_date", parse_date_if_present(column_or_null(bronze_clients_df, [registration_date_column] if registration_date_column else [], "string")))
    .withColumn("source_loaded_at", column_or_null(bronze_clients_df, [source_loaded_at_column] if source_loaded_at_column else [], "timestamp"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter unusable names and build deduplication scores
#
# The inspected wl_clients_full_20260420_v2 export contains:
# - User ID
# - Member ID
# - Username (used as the available email-like identifier)
# - Phone Number / Home Phone / Work Phone
# - Date of Birth
# - Client Type
# - Client Since Date
# - address/profile fields
#
# It still does not include modified/last-visit/purchase timestamps, so the practical
# dedupe strategy is:
# 1. Prefer records with User ID
# 2. Prefer records with Member ID
# 3. Prefer records with email/username
# 4. Prefer records with any phone
# 5. Prefer active-ish client type / status
# 6. Prefer rows with more populated fields overall
# 7. Prefer the latest Client Since Date
# 8. Use deterministic source row hash as the final tie-breaker

total_bronze_rows = bronze_clients_df.count()

named_clients_df = prepared_clients_df.filter(F.col("client").isNotNull())
rows_removed_blank_name = total_bronze_rows - named_clients_df.count()

duplicate_window = Window.partitionBy("client")
dedupe_window = Window.partitionBy("client").orderBy(
    F.col("dedupe_score").desc(),
    F.col("completeness_score").desc(),
    F.col("registration_date").desc_nulls_last(),
    F.col("_source_row_hash").asc(),
)

excluded_from_completeness = {
    "_source_row_hash",
    "_source_row_sequence",
    "client",
    "client_first_name_clean",
    "client_last_name_clean",
    "client_full_name_clean",
}

scored_clients_df = (
    named_clients_df
    .withColumn("has_source_client_id_score", F.when(F.col("source_client_id").isNotNull(), F.lit(100)).otherwise(F.lit(0)))
    .withColumn("has_member_id_score", F.when(F.col("source_member_id").isNotNull(), F.lit(30)).otherwise(F.lit(0)))
    .withColumn("has_email_score", F.when(F.col("email_clean").isNotNull(), F.lit(25)).otherwise(F.lit(0)))
    .withColumn("has_phone_score", F.when(F.col("phone_clean").isNotNull(), F.lit(20)).otherwise(F.lit(0)))
    .withColumn(
        "active_status_score",
        F.when(
            F.col("client_status").isNull(),
            F.lit(0),
        )
        .when(F.lower(F.col("client_status")).contains("inactive"), F.lit(0))
        .when(F.lower(F.col("client_status")).contains("prospect"), F.lit(2))
        .when(F.lower(F.col("client_status")).contains("active"), F.lit(10))
        .otherwise(F.lit(5)),
    )
    .withColumn("completeness_score", count_non_null_fields(named_clients_df, excluded_from_completeness))
    .withColumn(
        "dedupe_score",
        F.col("has_source_client_id_score")
        + F.col("has_member_id_score")
        + F.col("has_email_score")
        + F.col("has_phone_score")
        + F.col("active_status_score")
        + F.col("completeness_score"),
    )
    .withColumn("duplicate_name_count", F.count(F.lit(1)).over(duplicate_window))
    .withColumn("dedupe_rank", F.row_number().over(dedupe_window))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build silver.clients

silver_clients_df = (
    scored_clients_df
    .filter(F.col("dedupe_rank") == 1)
    .withColumn("salt_client_key", F.sha2(F.col("client"), 256))
    .withColumn(
        "dq_status",
        F.when(F.col("duplicate_name_count") > 1, F.lit("WARN_DUPLICATE_NAME_MERGED"))
        .otherwise(F.lit("PASS")),
    )
    .withColumn("dq_notes", build_dq_notes())
    .withColumn("silver_processed_at", F.current_timestamp())
    .select(
        "salt_client_key",
        "client",
        "client_first_name_clean",
        "client_last_name_clean",
        "client_full_name_clean",
        "source_client_id",
        "source_member_id",
        "email_clean",
        "phone_clean",
        "client_status",
        "registration_date",
        "source_file_name",
        "source_loaded_at",
        "silver_processed_at",
        "dedupe_rank",
        "dedupe_score",
        "duplicate_name_count",
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

# DQ summary

duplicate_groups_count = (
    scored_clients_df
    .select("client", "duplicate_name_count")
    .distinct()
    .filter(F.col("duplicate_name_count") > 1)
    .count()
)

max_duplicate_count_row = scored_clients_df.agg(F.max("duplicate_name_count").alias("max_duplicate_count")).collect()[0]
max_duplicate_count = max_duplicate_count_row["max_duplicate_count"] or 0

final_silver_row_count = silver_clients_df.count()
rows_missing_email = silver_clients_df.filter(F.col("email_clean").isNull()).count()
rows_missing_phone = silver_clients_df.filter(F.col("phone_clean").isNull()).count()
rows_missing_source_client_id = silver_clients_df.filter(F.col("source_client_id").isNull()).count()
rows_missing_source_member_id = silver_clients_df.filter(F.col("source_member_id").isNull()).count()
rows_where_dedupe_chose_among_duplicates = silver_clients_df.filter(F.col("duplicate_name_count") > 1).count()

dq_summary_rows = [
    ("total_bronze_source_rows", total_bronze_rows),
    ("rows_removed_due_to_blank_name", rows_removed_blank_name),
    ("distinct_client_after_name_normalization", scored_clients_df.select("client").distinct().count()),
    ("duplicate_name_groups", duplicate_groups_count),
    ("max_duplicate_count_for_single_name", max_duplicate_count),
    ("final_silver_row_count", final_silver_row_count),
    ("rows_missing_email", rows_missing_email),
    ("rows_missing_phone", rows_missing_phone),
    ("rows_missing_source_client_id", rows_missing_source_client_id),
    ("rows_missing_source_member_id", rows_missing_source_member_id),
    ("rows_where_dedupe_chose_among_duplicates", rows_where_dedupe_chose_among_duplicates),
]

dq_summary_df = spark.createDataFrame(dq_summary_rows, ["metric_name", "metric_value"])

display(dq_summary_df)
display(
    silver_clients_df
    .groupBy("dq_status")
    .count()
    .orderBy("dq_status")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write silver.clients

ensure_schema_exists(SILVER_SCHEMA)

if "batch_id" not in silver_clients_df.columns:
    silver_clients_df = silver_clients_df.withColumn("batch_id", lit(batch_id))

(
    silver_clients_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_clients_table)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    f"Successfully loaded {silver_clients_table} using LOAD_MODE='{active_load_mode}'. "
    f"Final silver row count: {final_silver_row_count}."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
