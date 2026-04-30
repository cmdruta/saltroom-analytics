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

# Silver purchases load for Microsoft Fabric Lakehouse.
#
# Architecture note:
# silver.purchases is a cleaned transaction-level entity at one row per WellnessLiving
# purchase line item. It is not the final Power BI fact table. Gold will later create
# curated sales facts, revenue summaries, client value metrics, membership/package facts,
# and other reporting-ready models.
#
# Client identity note:
# WellnessLiving purchases do not reliably carry a unique client identifier. This notebook
# matches purchases to silver.clients using Member ID first, then falls back to normalized
# full name plus email/phone support. That fallback can incorrectly merge different people
# with the same name, so the notebook keeps explicit client match method and DQ warnings.

from __future__ import annotations

import re
from decimal import Decimal

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
BRONZE_PURCHASES_TABLE = f"{BRONZE_SCHEMA}.purchases"
SILVER_CLIENTS_TABLE = f"{SILVER_SCHEMA}.clients"
SILVER_PURCHASES_TABLE = f"{SILVER_SCHEMA}.purchases"

VALID_LOAD_MODES = {"init", "refresh"}
AMOUNT_TOLERANCE = Decimal("0.05")


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
    trimmed_columns = []

    for field in dataframe.schema.fields:
        if isinstance(field.dataType, T.StringType):
            trimmed_columns.append(F.trim(F.col(field.name)).alias(field.name))
        else:
            trimmed_columns.append(F.col(field.name))

    return dataframe.select(*trimmed_columns)


def normalize_spaces(column_expression) -> F.Column:
    return F.regexp_replace(F.trim(column_expression.cast("string")), r"\s+", " ")


def null_if_blank(column_expression) -> F.Column:
    normalized_value = normalize_spaces(column_expression)
    return F.when(normalized_value == "", F.lit(None)).otherwise(normalized_value)


def normalize_text_key(column_expression) -> F.Column:
    normalized_value = F.lower(normalize_spaces(column_expression))
    return F.when(normalized_value == "", F.lit(None)).otherwise(normalized_value)


def normalize_phone(column_expression) -> F.Column:
    digits_only = F.regexp_replace(F.coalesce(column_expression.cast("string"), F.lit("")), r"[^0-9]", "")
    return F.when(digits_only == "", F.lit(None)).otherwise(digits_only)


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


def parse_currency_to_decimal(column_expression) -> F.Column:
    raw_value = null_if_blank(column_expression)
    normalized_value = F.regexp_replace(raw_value, ",", "")
    normalized_value = F.regexp_replace(normalized_value, r"^\((.*)\)$", r"-\1")
    normalized_value = F.regexp_replace(normalized_value, r"[$]", "")
    normalized_value = F.regexp_replace(normalized_value, r"\+", "")
    return normalized_value.cast(T.DecimalType(18, 2))


def parse_purchase_date(column_expression) -> F.Column:
    return F.coalesce(
        F.to_date(column_expression, "MMM d, yyyy"),
        F.to_date(column_expression, "MMM dd, yyyy"),
        F.to_date(column_expression, "M/d/yyyy"),
        F.to_date(column_expression, "MM/dd/yyyy"),
        F.to_date(column_expression, "yyyy-MM-dd"),
    )


def parse_purchase_timestamp(date_expression, time_expression) -> F.Column:
    combined_value = F.concat_ws(" ", null_if_blank(date_expression), null_if_blank(time_expression))
    return F.coalesce(
        F.to_timestamp(combined_value, "MMM d, yyyy h:mma"),
        F.to_timestamp(combined_value, "MMM dd, yyyy h:mma"),
        F.to_timestamp(combined_value, "M/d/yyyy h:mma"),
        F.to_timestamp(combined_value, "MM/dd/yyyy h:mma"),
        F.to_timestamp(combined_value, "yyyy-MM-dd HH:mm:ss"),
    )


def non_blank_decimal_input(column_expression) -> F.Column:
    return null_if_blank(column_expression).isNotNull()


def build_notes_array(*note_columns: F.Column) -> F.Column:
    return F.array_remove(F.array(*note_columns), F.lit(None))


def build_primary_dq_status() -> F.Column:
    return (
        F.when(F.col("source_purchase_item_id").isNull(), F.lit("FAIL_MISSING_PURCHASE_ITEM_ID"))
        .when(F.col("client_match_method") == "unmatched", F.lit("WARN_CLIENT_NOT_MATCHED"))
        .when(F.col("has_duplicate_source_purchase_item_id"), F.lit("WARN_DUPLICATE_SOURCE_PURCHASE_ITEM_ID"))
        .when(F.col("has_failed_amount_parsing"), F.lit("WARN_AMOUNT_PARSE"))
        .when(F.col("has_failed_datetime_parsing"), F.lit("WARN_DATETIME_PARSE"))
        .when(F.col("has_amount_reconciliation_warning"), F.lit("WARN_AMOUNT_RECONCILIATION"))
        .when(F.col("has_multiple_client_matches_resolved"), F.lit("WARN_MULTIPLE_CLIENT_MATCHES_RESOLVED"))
        .when(F.col("client_match_method").isin("name_email_phone_score", "name_only"), F.lit("WARN_CLIENT_MATCHED_BY_NAME"))
        .when(F.col("has_revenue_category_cleanup_warning"), F.lit("WARN_REVENUE_CATEGORY_CLEANUP"))
        .when(F.col("source_member_id").isNull(), F.lit("WARN_MISSING_MEMBER_ID"))
        .otherwise(F.lit("PASS"))
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load source tables and validate runtime assumptions

active_load_mode = validate_load_mode(LOAD_MODE)

if not table_exists(BRONZE_PURCHASES_TABLE):
    raise ValueError(
        f"Required Bronze table {BRONZE_PURCHASES_TABLE} does not exist. "
        "Run the Bronze purchases ingestion notebook before loading Silver purchases."
    )

if not table_exists(SILVER_CLIENTS_TABLE):
    raise ValueError(
        f"Required Silver table {SILVER_CLIENTS_TABLE} does not exist. "
        "Run the Silver clients notebook before loading Silver purchases."
    )

bronze_purchases_raw_df = trim_all_string_fields(normalize_column_names(spark.table(BRONZE_PURCHASES_TABLE)))
silver_clients_raw_df = trim_all_string_fields(normalize_column_names(spark.table(SILVER_CLIENTS_TABLE)))

require_columns(
    bronze_purchases_raw_df,
    [
        "date",
        "time",
        "client",
        "member_id",
        "email",
        "phone",
        "revenue_category",
        "item",
        "price",
        "subtotal",
        "discount_code",
        "discount_amount",
        "total_net_sales",
        "total_taxes",
        "total_paid",
        "account_change",
        "sold_by",
        "k_purchase_item",
        "text_payment_method_base",
        "source_file_name",
        "ingestion_timestamp",
    ],
    BRONZE_PURCHASES_TABLE,
)

require_columns(
    silver_clients_raw_df,
    [
        "salt_client_key",
        "client",
        "source_client_id",
        "source_member_id",
        "email_clean",
        "phone_clean",
        "client_status",
        "registration_date",
        "dedupe_score",
        "duplicate_name_count",
    ],
    SILVER_CLIENTS_TABLE,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardize Bronze purchases and parse raw business fields.
#
# Refresh handling note:
# Bronze purchases may contain overlapping yearly extracts. To avoid carrying duplicate line
# items into Silver, this notebook deduplicates by k_purchase_item and keeps the latest row
# by Bronze load timestamp, then source file name, then purchase datetime, then row hash.

bronze_purchase_row_count = bronze_purchases_raw_df.count()

prepared_purchases_df = (
    bronze_purchases_raw_df
    .withColumn("source_purchase_item_id", null_if_blank(F.col("k_purchase_item")))
    .withColumn("client_name_raw", null_if_blank(F.col("client")))
    .withColumn("client_name_key", normalize_text_key(F.col("client")))
    .withColumn("source_member_id", null_if_blank(F.col("member_id")))
    .withColumn("purchase_email_clean", normalize_text_key(F.col("email")))
    .withColumn("purchase_phone_clean", normalize_phone(F.col("phone")))
    .withColumn("source_loaded_at", F.col("ingestion_timestamp").cast("timestamp"))
    .withColumn("purchase_date", parse_purchase_date(F.col("date")))
    .withColumn("purchase_datetime", parse_purchase_timestamp(F.col("date"), F.col("time")))
    .withColumn("purchase_time", F.date_format(F.col("purchase_datetime"), "HH:mm:ss"))
    .withColumn("item_name_raw", null_if_blank(F.col("item")))
    .withColumn("item_name_clean", normalize_spaces(F.col("item")))
    .withColumn(
        "item_name_clean",
        F.when(
            F.lower(F.col("item_name_clean")).rlike("gift\\s+(card|certificate)"),
            F.lit("Gift Card"),
        ).otherwise(F.col("item_name_clean")),
    )
    .withColumn("revenue_category_raw", null_if_blank(F.col("revenue_category")))
    .withColumn("revenue_category_trimmed", normalize_spaces(F.col("revenue_category")))
    .withColumn("revenue_category_parts", F.split(F.coalesce(F.col("revenue_category_trimmed"), F.lit("")), r"\s*,\s*"))
    .withColumn(
        "revenue_category_first_non_blank",
        F.expr("filter(revenue_category_parts, x -> x is not null and trim(x) <> '')[0]"),
    )
    .withColumn("has_multi_value_revenue_category", F.size(F.expr("filter(revenue_category_parts, x -> x is not null and trim(x) <> '')")) > 1)
    .withColumn(
        "revenue_category_clean",
        F.when(F.lower(F.coalesce(F.col("item_name_raw"), F.lit(""))).contains("membership"), F.lit("Membership"))
        .when(F.lower(F.coalesce(F.col("item_name_raw"), F.lit(""))).contains("double dip"), F.lit("Intro Offers"))
        .when(~F.col("has_multi_value_revenue_category"), F.col("revenue_category_trimmed"))
        .otherwise(F.col("revenue_category_first_non_blank")),
    )
    .withColumn("price_amount", parse_currency_to_decimal(F.col("price")))
    .withColumn("subtotal_amount", parse_currency_to_decimal(F.col("subtotal")))
    .withColumn("discount_amount", parse_currency_to_decimal(F.col("discount_amount")))
    .withColumn("net_sales_amount", parse_currency_to_decimal(F.col("total_net_sales")))
    .withColumn("_parsed_tax_amount", parse_currency_to_decimal(F.col("total_taxes")))
    .withColumn(
        "tax_amount",
        F.when(null_if_blank(F.col("total_taxes")).isNull(), F.lit(0).cast(T.DecimalType(18, 2)))
        .otherwise(F.col("_parsed_tax_amount")),
    )
    .withColumn("total_paid_amount", parse_currency_to_decimal(F.col("total_paid")))
    .withColumn("account_change_amount", parse_currency_to_decimal(F.col("account_change")))
    .withColumn("discount_code", null_if_blank(F.col("discount_code")))
    .withColumn("payment_method_base", null_if_blank(F.col("text_payment_method_base")))
    .withColumn("sold_by", null_if_blank(F.col("sold_by")))
    .withColumn("source_file_name", F.col("source_file_name").cast("string"))
)

prepared_purchases_df = prepared_purchases_df.withColumn(
    "_source_row_hash",
    F.sha2(F.to_json(F.struct(*[F.col(column_name) for column_name in prepared_purchases_df.columns])), 256),
)
prepared_purchases_df = prepared_purchases_df.withColumn(
    "_purchase_row_id",
    F.row_number().over(
        Window.orderBy(
            F.col("_source_row_hash").asc(),
            F.col("source_file_name").asc_nulls_last(),
            F.col("source_loaded_at").asc_nulls_last(),
            F.col("source_purchase_item_id").asc_nulls_last(),
            F.col("client_name_raw").asc_nulls_last(),
            F.col("purchase_datetime").asc_nulls_last(),
        )
    ),
)

purchase_duplicate_stats_df = (
    prepared_purchases_df
    .filter(F.col("source_purchase_item_id").isNotNull())
    .groupBy("source_purchase_item_id")
    .agg(F.count(F.lit(1)).alias("source_purchase_item_id_count"))
)

prepared_purchases_df = prepared_purchases_df.join(purchase_duplicate_stats_df, ["source_purchase_item_id"], "left")
prepared_purchases_df = prepared_purchases_df.withColumn(
    "source_purchase_item_id_count",
    F.coalesce(F.col("source_purchase_item_id_count"), F.lit(1)),
)
prepared_purchases_df = prepared_purchases_df.withColumn(
    "has_duplicate_source_purchase_item_id",
    F.col("source_purchase_item_id_count") > 1,
)

dedupe_window = Window.partitionBy("source_purchase_item_id").orderBy(
    F.col("source_loaded_at").desc_nulls_last(),
    F.col("source_file_name").desc_nulls_last(),
    F.col("purchase_datetime").desc_nulls_last(),
    F.col("_source_row_hash").asc(),
)

deduped_keyed_purchases_df = (
    prepared_purchases_df
    .filter(F.col("source_purchase_item_id").isNotNull())
    .withColumn("_source_purchase_dedupe_rank", F.row_number().over(dedupe_window))
    .filter(F.col("_source_purchase_dedupe_rank") == 1)
)

deduped_unkeyed_purchases_df = prepared_purchases_df.filter(F.col("source_purchase_item_id").isNull())

source_purchases_df = deduped_keyed_purchases_df.unionByName(deduped_unkeyed_purchases_df, allowMissingColumns=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare client reference data for matching

clients_df = (
    silver_clients_raw_df
    .withColumn("client", normalize_text_key(F.col("client")))
    .withColumn("source_member_id", null_if_blank(F.col("source_member_id")))
    .withColumn("email_clean", normalize_text_key(F.col("email_clean")))
    .withColumn("phone_clean", normalize_phone(F.col("phone_clean")))
    .withColumn("client_status", normalize_text_key(F.col("client_status")))
    .withColumn("source_client_id", null_if_blank(F.col("source_client_id")))
    .withColumn("registration_date", F.col("registration_date").cast("date"))
    .withColumn("dedupe_score", F.coalesce(F.col("dedupe_score").cast("int"), F.lit(0)))
    .withColumn("duplicate_name_count", F.coalesce(F.col("duplicate_name_count").cast("int"), F.lit(0)))
)

member_id_window = Window.partitionBy("source_member_id").orderBy(
    F.col("duplicate_name_count").asc_nulls_last(),
    F.col("dedupe_score").desc_nulls_last(),
    F.col("registration_date").desc_nulls_last(),
    F.col("source_client_id").asc_nulls_last(),
)

member_id_clients_df = (
    clients_df
    .filter(F.col("source_member_id").isNotNull())
    .withColumn("_member_id_rank", F.row_number().over(member_id_window))
    .filter(F.col("_member_id_rank") == 1)
    .select(
        F.col("source_member_id").alias("_match_member_id"),
        F.col("salt_client_key").alias("member_salt_client_key"),
        F.col("source_client_id").alias("member_source_client_id"),
    )
)

name_match_candidates_df = clients_df.select(
    F.col("salt_client_key").alias("candidate_salt_client_key"),
    F.col("client").alias("candidate_client_name_key"),
    F.col("source_client_id").alias("candidate_source_client_id"),
    F.col("source_member_id").alias("candidate_source_member_id"),
    F.col("email_clean").alias("candidate_email_clean"),
    F.col("phone_clean").alias("candidate_phone_clean"),
    F.col("client_status").alias("candidate_client_status"),
    F.col("registration_date").alias("candidate_registration_date"),
    F.col("dedupe_score").alias("candidate_dedupe_score"),
    F.col("duplicate_name_count").alias("candidate_duplicate_name_count"),
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Match purchases to clients.
#
# Match priority:
# 1. Member ID to silver.clients.source_member_id
# 2. Fallback to normalized client full name
# 3. When multiple name candidates exist, select the best match deterministically using
#    email, phone, registration date, client status, duplicate count, and dedupe score.

member_joined_purchases_df = (
    source_purchases_df.alias("p")
    .join(
        member_id_clients_df.alias("m"),
        F.col("p.source_member_id") == F.col("m._match_member_id"),
        "left",
    )
    .withColumn("member_match_found", F.col("m.member_salt_client_key").isNotNull())
)

member_matched_purchases_df = (
    member_joined_purchases_df
    .filter(F.col("member_match_found"))
    .withColumn("salt_client_key", F.col("m.member_salt_client_key"))
    .withColumn("source_client_id", F.col("m.member_source_client_id"))
    .withColumn("client_match_method", F.lit("member_id"))
    .withColumn("client_match_score", F.lit(1000))
    .withColumn("name_match_candidate_count", F.lit(0))
    .withColumn("has_multiple_client_matches_resolved", F.lit(False))
    .select("p.*", "salt_client_key", "source_client_id", "client_match_method", "client_match_score", "name_match_candidate_count", "has_multiple_client_matches_resolved")
)

name_fallback_purchases_df = member_joined_purchases_df.filter(~F.col("member_match_found")).select("p.*")

name_match_candidates_joined_df = (
    name_fallback_purchases_df.alias("p")
    .join(
        name_match_candidates_df.alias("c"),
        F.col("p.client_name_key") == F.col("c.candidate_client_name_key"),
        "left",
    )
    .withColumn("has_name_candidate", F.col("c.candidate_salt_client_key").isNotNull())
    .withColumn("email_match_score", F.when((F.col("p.purchase_email_clean").isNotNull()) & (F.col("p.purchase_email_clean") == F.col("c.candidate_email_clean")), F.lit(100)).otherwise(F.lit(0)))
    .withColumn("phone_match_score", F.when((F.col("p.purchase_phone_clean").isNotNull()) & (F.col("p.purchase_phone_clean") == F.col("c.candidate_phone_clean")), F.lit(100)).otherwise(F.lit(0)))
    .withColumn("registration_date_score", F.when((F.col("c.candidate_registration_date").isNotNull()) & (F.col("p.purchase_date").isNotNull()) & (F.col("c.candidate_registration_date") <= F.col("p.purchase_date")), F.lit(50)).otherwise(F.lit(0)))
    .withColumn("candidate_member_id_score", F.when(F.col("c.candidate_source_member_id").isNotNull(), F.lit(25)).otherwise(F.lit(0)))
    .withColumn(
        "active_client_score",
        F.when(F.coalesce(F.col("c.candidate_client_status"), F.lit("")).rlike("active|current|pass"), F.lit(20)).otherwise(F.lit(0)),
    )
    .withColumn("duplicate_name_score", F.when(F.col("c.candidate_duplicate_name_count") == 1, F.lit(10)).otherwise(F.lit(0)))
    .withColumn(
        "client_match_score",
        F.when(
            F.col("has_name_candidate"),
            F.col("email_match_score")
            + F.col("phone_match_score")
            + F.col("registration_date_score")
            + F.col("candidate_member_id_score")
            + F.col("active_client_score")
            + F.col("duplicate_name_score")
            + F.coalesce(F.col("c.candidate_dedupe_score"), F.lit(0)),
        ).otherwise(F.lit(-1)),
    )
)

name_candidate_counts_df = (
    name_match_candidates_joined_df
    .groupBy("_purchase_row_id")
    .agg(F.sum(F.when(F.col("has_name_candidate"), F.lit(1)).otherwise(F.lit(0))).alias("name_match_candidate_count"))
)

name_match_window = Window.partitionBy("_purchase_row_id").orderBy(
    F.col("has_name_candidate").desc(),
    F.col("client_match_score").desc(),
    F.col("candidate_source_client_id").asc_nulls_last(),
    F.col("candidate_salt_client_key").asc_nulls_last(),
)

best_name_matches_df = (
    name_match_candidates_joined_df
    .join(name_candidate_counts_df, ["_purchase_row_id"], "left")
    .withColumn("_name_match_rank", F.row_number().over(name_match_window))
    .filter(F.col("_name_match_rank") == 1)
    .withColumn(
        "client_match_method",
        F.when(~F.col("has_name_candidate"), F.lit("unmatched"))
        .when((F.col("email_match_score") > 0) | (F.col("phone_match_score") > 0), F.lit("name_email_phone_score"))
        .otherwise(F.lit("name_only")),
    )
    .withColumn("salt_client_key", F.col("candidate_salt_client_key"))
    .withColumn("source_client_id", F.col("candidate_source_client_id"))
    .withColumn("has_multiple_client_matches_resolved", F.col("name_match_candidate_count") > 1)
    .select("p.*", "salt_client_key", "source_client_id", "client_match_method", "client_match_score", "name_match_candidate_count", "has_multiple_client_matches_resolved")
)

matched_purchases_df = member_matched_purchases_df.unionByName(best_name_matches_df, allowMissingColumns=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build row-level DQ flags, final output columns, and summary views

enriched_purchases_df = (
    matched_purchases_df
    .withColumn("salt_purchase_key", F.when(F.col("source_purchase_item_id").isNotNull(), F.sha2(F.col("source_purchase_item_id"), 256)))
    .withColumn("silver_processed_at", F.current_timestamp())
    .withColumn("has_missing_purchase_date", F.col("purchase_date").isNull())
    .withColumn(
        "has_failed_datetime_parsing",
        null_if_blank(F.col("date")).isNotNull() & null_if_blank(F.col("time")).isNotNull() & F.col("purchase_datetime").isNull()
    )
    .withColumn(
        "has_failed_amount_parsing",
        (
            (non_blank_decimal_input(F.col("price")) & F.col("price_amount").isNull())
            | (non_blank_decimal_input(F.col("subtotal")) & F.col("subtotal_amount").isNull())
            | (non_blank_decimal_input(F.col("discount_amount")) & F.col("discount_amount").isNull())
            | (non_blank_decimal_input(F.col("total_net_sales")) & F.col("net_sales_amount").isNull())
            | (non_blank_decimal_input(F.col("total_taxes")) & F.col("_parsed_tax_amount").isNull())
            | (non_blank_decimal_input(F.col("total_paid")) & F.col("total_paid_amount").isNull())
            | (non_blank_decimal_input(F.col("account_change")) & F.col("account_change_amount").isNull())
        )
    )
    .withColumn("has_tax_defaulted_to_zero", null_if_blank(F.col("total_taxes")).isNull())
    .withColumn(
        "has_revenue_category_cleanup_warning",
        F.col("has_multi_value_revenue_category")
        | F.lower(F.coalesce(F.col("item_name_raw"), F.lit(""))).contains("membership")
        | F.lower(F.coalesce(F.col("item_name_raw"), F.lit(""))).contains("double dip"),
    )
    .withColumn(
        "has_revenue_category_first_value_fallback",
        F.col("has_multi_value_revenue_category")
        & ~F.lower(F.coalesce(F.col("item_name_raw"), F.lit(""))).contains("membership")
        & ~F.lower(F.coalesce(F.col("item_name_raw"), F.lit(""))).contains("double dip"),
    )
    .withColumn("has_gift_card_normalization", F.col("item_name_clean") == F.lit("Gift Card"))
    .withColumn("has_negative_total_paid_amount", F.col("total_paid_amount") < F.lit(0).cast(T.DecimalType(18, 2)))
    .withColumn(
        "expected_total_amount",
        F.coalesce(F.col("net_sales_amount"), F.lit(0).cast(T.DecimalType(18, 2)))
        + F.coalesce(F.col("tax_amount"), F.lit(0).cast(T.DecimalType(18, 2)))
        + F.coalesce(F.col("account_change_amount"), F.lit(0).cast(T.DecimalType(18, 2))),
    )
    .withColumn(
        "has_amount_reconciliation_warning",
        F.when(
            F.col("total_paid_amount").isNull() | F.col("net_sales_amount").isNull(),
            F.lit(False),
        ).otherwise(F.abs(F.col("total_paid_amount") - F.col("expected_total_amount")) > F.lit(float(AMOUNT_TOLERANCE))),
    )
)

enriched_purchases_df = enriched_purchases_df.withColumn(
    "dq_notes_array",
    build_notes_array(
        F.when(F.col("source_purchase_item_id").isNull(), F.lit("missing source purchase item id")),
        F.when(F.col("has_duplicate_source_purchase_item_id"), F.lit("duplicate source_purchase_item_id found in bronze purchases; latest row kept for silver")),
        F.when(F.col("has_missing_purchase_date"), F.lit("missing purchase date")),
        F.when(F.col("has_failed_datetime_parsing"), F.lit("failed date/time parsing")),
        F.when(F.col("has_failed_amount_parsing"), F.lit("failed amount parsing for one or more monetary fields")),
        F.when(F.col("source_member_id").isNull(), F.lit("missing member id")),
        F.when(F.col("client_match_method").isin("name_email_phone_score", "name_only"), F.lit("client matched by normalized name fallback")),
        F.when(F.col("client_match_method") == "unmatched", F.lit("client not matched to silver.clients")),
        F.when(F.col("has_multiple_client_matches_resolved"), F.lit("multiple client matches resolved by deterministic score")),
        F.when(F.col("has_revenue_category_cleanup_warning"), F.lit("revenue category cleaned from item/category rules")),
        F.when(F.col("has_revenue_category_first_value_fallback"), F.lit("multi-value revenue category kept first non-blank value")),
        F.when(F.col("has_gift_card_normalization"), F.lit("item normalized to Gift Card")),
        F.when(F.col("has_negative_total_paid_amount"), F.lit("negative total paid amount")),
        F.when(F.col("has_tax_defaulted_to_zero"), F.lit("tax amount defaulted to 0 because Total Taxes was blank")),
        F.when(F.col("has_amount_reconciliation_warning"), F.lit("amount reconciliation warning: total_paid_amount differs from net_sales_amount + tax_amount + account_change_amount")),
    ),
)

final_silver_purchases_df = (
    enriched_purchases_df
    .withColumn("dq_notes", F.concat_ws("; ", F.col("dq_notes_array")))
    .withColumn("dq_status", build_primary_dq_status())
    .select(
        "salt_purchase_key",
        "source_purchase_item_id",
        "salt_client_key",
        "source_client_id",
        "source_member_id",
        "client_match_method",
        "client_match_score",
        "client_name_raw",
        "client_name_key",
        "purchase_date",
        "purchase_time",
        "purchase_datetime",
        "revenue_category_clean",
        "item_name_clean",
        "price_amount",
        "subtotal_amount",
        "discount_code",
        "discount_amount",
        "net_sales_amount",
        "tax_amount",
        "total_paid_amount",
        "account_change_amount",
        "payment_method_base",
        "sold_by",
        "source_file_name",
        "source_loaded_at",
        "silver_processed_at",
        "dq_status",
        "dq_notes",
    )
)

duplicate_purchase_item_group_count = purchase_duplicate_stats_df.filter(F.col("source_purchase_item_id_count") > 1).count()

dq_summary_row = (
    enriched_purchases_df
    .agg(
        F.lit(bronze_purchase_row_count).alias("total_source_rows"),
        F.count(F.lit(1)).alias("final_silver_rows"),
        F.countDistinct("source_purchase_item_id").alias("distinct_source_purchase_item_id"),
        F.lit(duplicate_purchase_item_group_count).alias("duplicate_source_purchase_item_id_count"),
        F.sum(F.when(F.col("source_purchase_item_id").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("rows_missing_source_purchase_item_id"),
        F.sum(F.when(F.col("source_member_id").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("rows_missing_member_id"),
        F.sum(F.when(F.col("client_match_method") == "member_id", F.lit(1)).otherwise(F.lit(0))).alias("rows_matched_by_member_id"),
        F.sum(F.when(F.col("client_match_method") == "name_email_phone_score", F.lit(1)).otherwise(F.lit(0))).alias("rows_matched_by_name_email_phone_score"),
        F.sum(F.when(F.col("client_match_method") == "name_only", F.lit(1)).otherwise(F.lit(0))).alias("rows_matched_by_name_only"),
        F.sum(F.when(F.col("client_match_method") == "unmatched", F.lit(1)).otherwise(F.lit(0))).alias("rows_unmatched_to_clients"),
        F.sum(F.when(F.col("has_multiple_client_matches_resolved"), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_multiple_client_matches_resolved"),
        F.sum(F.when(F.col("has_gift_card_normalization"), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_gift_card_item_normalization"),
        F.sum(F.when(F.col("revenue_category_clean") == "Membership", F.lit(1)).otherwise(F.lit(0))).alias("rows_with_membership_revenue_category_normalization"),
        F.sum(F.when(F.col("revenue_category_clean") == "Intro Offers", F.lit(1)).otherwise(F.lit(0))).alias("rows_with_double_dip_intro_offers_normalization"),
        F.sum(F.when(F.col("has_revenue_category_cleanup_warning"), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_multi_value_revenue_category_cleanup"),
        F.sum(F.when(F.col("has_tax_defaulted_to_zero"), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_tax_amount_defaulted_to_zero"),
        F.sum(F.when(F.col("has_negative_total_paid_amount"), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_negative_total_paid_amount"),
        F.sum(F.when(F.col("has_amount_reconciliation_warning"), F.lit(1)).otherwise(F.lit(0))).alias("rows_with_amount_reconciliation_warnings"),
        F.sum(F.coalesce(F.col("subtotal_amount"), F.lit(0).cast(T.DecimalType(18, 2)))).alias("total_subtotal_amount"),
        F.sum(F.coalesce(F.col("discount_amount"), F.lit(0).cast(T.DecimalType(18, 2)))).alias("total_discount_amount"),
        F.sum(F.coalesce(F.col("net_sales_amount"), F.lit(0).cast(T.DecimalType(18, 2)))).alias("total_net_sales_amount"),
        F.sum(F.coalesce(F.col("tax_amount"), F.lit(0).cast(T.DecimalType(18, 2)))).alias("total_tax_amount"),
        F.sum(F.coalesce(F.col("total_paid_amount"), F.lit(0).cast(T.DecimalType(18, 2)))).alias("total_total_paid_amount"),
    )
    .collect()[0]
)

summary_metric_order = [
    "total_source_rows",
    "final_silver_rows",
    "distinct_source_purchase_item_id",
    "duplicate_source_purchase_item_id_count",
    "rows_missing_source_purchase_item_id",
    "rows_missing_member_id",
    "rows_matched_by_member_id",
    "rows_matched_by_name_email_phone_score",
    "rows_matched_by_name_only",
    "rows_unmatched_to_clients",
    "rows_with_multiple_client_matches_resolved",
    "rows_with_gift_card_item_normalization",
    "rows_with_membership_revenue_category_normalization",
    "rows_with_double_dip_intro_offers_normalization",
    "rows_with_multi_value_revenue_category_cleanup",
    "rows_with_tax_amount_defaulted_to_zero",
    "rows_with_negative_total_paid_amount",
    "rows_with_amount_reconciliation_warnings",
    "total_subtotal_amount",
    "total_discount_amount",
    "total_net_sales_amount",
    "total_tax_amount",
    "total_total_paid_amount",
]

dq_summary_rows = [
    (metric_name, str(dq_summary_row[metric_name]) if dq_summary_row[metric_name] is not None else None)
    for metric_name in summary_metric_order
]

dq_summary_df = spark.createDataFrame(dq_summary_rows, ["metric_name", "metric_value"])

display(dq_summary_df)
display(
    final_silver_purchases_df
    .select(
        "source_purchase_item_id",
        "client_name_raw",
        "client_match_method",
        "source_member_id",
        "source_client_id",
        "purchase_date",
        "revenue_category_clean",
        "item_name_clean",
        "total_paid_amount",
        "dq_status",
        "dq_notes",
    )
    .orderBy(F.col("purchase_date").desc_nulls_last(), F.col("source_purchase_item_id").desc_nulls_last())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write silver.purchases

ensure_schema_exists(SILVER_SCHEMA)

(
    final_silver_purchases_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_PURCHASES_TABLE)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_row_count = final_silver_purchases_df.count()
matched_row_count = final_silver_purchases_df.filter(F.col("client_match_method") != "unmatched").count()
unmatched_row_count = final_silver_purchases_df.filter(F.col("client_match_method") == "unmatched").count()
dq_warning_count = final_silver_purchases_df.filter(F.col("dq_status") != "PASS").count()

print(
    f"Successfully loaded {SILVER_PURCHASES_TABLE} using LOAD_MODE='{active_load_mode}'. "
    f"Final row count: {final_row_count}. "
    f"Matched to clients: {matched_row_count}. "
    f"Unmatched: {unmatched_row_count}. "
    f"DQ warnings/failures: {dq_warning_count}."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
