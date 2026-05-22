# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cf3cdc22-7eba-4baa-91fa-90ee77479818",
# META       "default_lakehouse_name": "saltroom_lakehouse",
# META       "default_lakehouse_workspace_id": "7b7cf7f0-7b72-4f1f-a6e0-c0c594ce6216",
# META       "known_lakehouses": [
# META         {
# META           "id": "cf3cdc22-7eba-4baa-91fa-90ee77479818"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Parameters
load_mode = "refresh"
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

# Gold client retention snapshot for Salt Room Analytics.
# This notebook creates one row per client per snapshot_date for retention analytics,
# dashboards, and future churn scoring.

import json

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

GOLD_SCHEMA = "gold"
DQ_LOAD_WARNINGS_TABLE = "dq.load_warnings"
NOTEBOOK_NAME = "load_gold_client_retention_snapshot"
PROCESS_NAME = "load_gold_client_retention_snapshot"

DIM_CLIENT_TABLE = f"{GOLD_SCHEMA}.dim_client"
FACT_VISIT_TABLE = f"{GOLD_SCHEMA}.fact_visit"
FACT_PURCHASE_TABLE = f"{GOLD_SCHEMA}.fact_purchase"
DIM_DATE_TABLE = f"{GOLD_SCHEMA}.dim_date"
DIM_SERVICE_TABLE = f"{GOLD_SCHEMA}.dim_service"
DIM_PURCHASE_ITEM_TABLE = f"{GOLD_SCHEMA}.dim_purchase_item"
TARGET_TABLE = f"{GOLD_SCHEMA}.client_retention_snapshot"

VALID_LOAD_MODES = {"init", "refresh"}
SNAPSHOT_DATE_VALUE = spark.sql("SELECT current_date() AS snapshot_date").first()["snapshot_date"]
SNAPSHOT_DATE_SQL_LITERAL = SNAPSHOT_DATE_VALUE.isoformat()
SNAPSHOT_DATE = F.lit(SNAPSHOT_DATE_VALUE).cast("date")
CURRENT_TIMESTAMP_UTC = F.current_timestamp()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if load_mode not in VALID_LOAD_MODES:
    raise ValueError(f"Unsupported LOAD_MODE '{load_mode}'. Expected one of: {sorted(VALID_LOAD_MODES)}")


def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def ensure_schema_exists(schema_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def require_table(table_name: str) -> DataFrame:
    if not table_exists(table_name):
        write_dq_result(
            batch_id,
            "gold",
            NOTEBOOK_NAME,
            PROCESS_NAME,
            TARGET_TABLE,
            "source_table_exists",
            "ERROR",
            "GOLD_RETENTION_SOURCE_TABLE_MISSING",
            f"Required source table {table_name} does not exist.",
            0,
            {"missing_table": table_name},
            "FAIL",
        )
        raise ValueError(f"Required source table {table_name} does not exist.")
    return spark.table(table_name)


def require_columns(dataframe: DataFrame, required_columns: list[str], table_name: str) -> None:
    missing_columns = [column_name for column_name in required_columns if column_name not in dataframe.columns]
    if missing_columns:
        write_dq_result(
            batch_id,
            "gold",
            NOTEBOOK_NAME,
            PROCESS_NAME,
            TARGET_TABLE,
            "required_columns_exist",
            "ERROR",
            "GOLD_RETENTION_REQUIRED_COLUMNS_MISSING",
            f"Required columns are missing from {table_name}.",
            0,
            {"table_name": table_name, "missing_columns": missing_columns},
            "FAIL",
        )
        raise ValueError(f"Missing required columns in {table_name}: {missing_columns}")


def sample_record(dataframe: DataFrame, condition, columns: list[str] | None = None) -> dict | None:
    sample_df = dataframe.filter(condition)
    if columns:
        sample_df = sample_df.select(*[column_name for column_name in columns if column_name in sample_df.columns])
    rows = sample_df.limit(1).collect()
    return rows[0].asDict(recursive=True) if rows else None


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

    ensure_schema_exists("dq")
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
    check_name: str,
    warning_code: str,
    warning_message: str,
    affected_row_count: int,
    sample_record: dict | None = None,
    status: str = "WARN",
) -> None:
    if affected_row_count > 0:
        severity = "ERROR" if status == "FAIL" else "WARNING"
        write_dq_result(
            batch_id,
            "gold",
            NOTEBOOK_NAME,
            PROCESS_NAME,
            TARGET_TABLE,
            check_name,
            severity,
            warning_code,
            warning_message,
            affected_row_count,
            sample_record,
            status,
        )


def write_dq_info(
    check_name: str,
    warning_code: str,
    warning_message: str,
    affected_row_count: int,
    sample_record: dict | None = None,
) -> None:
    write_dq_result(
        batch_id,
        "gold",
        NOTEBOOK_NAME,
        PROCESS_NAME,
        TARGET_TABLE,
        check_name,
        "INFO",
        warning_code,
        warning_message,
        affected_row_count,
        sample_record,
        "PASS",
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read and validate Gold source tables using physical Lakehouse column names.
ensure_schema_exists(GOLD_SCHEMA)

source_table_names = [
    DIM_CLIENT_TABLE,
    FACT_VISIT_TABLE,
    FACT_PURCHASE_TABLE,
    DIM_DATE_TABLE,
    DIM_SERVICE_TABLE,
    DIM_PURCHASE_ITEM_TABLE,
]

for source_table_name in source_table_names:
    require_table(source_table_name)

clients_df = spark.table(DIM_CLIENT_TABLE)
visits_df = spark.table(FACT_VISIT_TABLE)
purchases_df = spark.table(FACT_PURCHASE_TABLE)
date_df = spark.table(DIM_DATE_TABLE)
service_df = spark.table(DIM_SERVICE_TABLE)
purchase_item_df = spark.table(DIM_PURCHASE_ITEM_TABLE)

require_columns(clients_df, ["client_key", "client_name"], DIM_CLIENT_TABLE)
require_columns(
    visits_df,
    ["client_key", "visit_date_key", "service_key", "purchase_item_key", "visit_count"],
    FACT_VISIT_TABLE,
)
require_columns(
    purchases_df,
    ["client_key", "purchase_date_key", "purchase_item_key", "net_sales_amount", "discount_code"],
    FACT_PURCHASE_TABLE,
)
require_columns(date_df, ["date_key", "calendar_date"], DIM_DATE_TABLE)
require_columns(service_df, ["service_key", "service_name"], DIM_SERVICE_TABLE)
require_columns(purchase_item_df, ["purchase_item_key", "purchase_item_name"], DIM_PURCHASE_ITEM_TABLE)

write_dq_info(
    "source_tables_exist",
    "GOLD_RETENTION_SOURCE_TABLES_EXIST",
    "All required Gold source tables exist.",
    len(source_table_names),
    {"source_tables": source_table_names},
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build visit-derived retention features.
# Every row in gold.fact_visit is treated as an attended/completed client visit.
visit_dates_df = date_df.select(
    F.col("date_key").alias("visit_date_key"),
    F.col("calendar_date").cast("date").alias("visit_date"),
)

completed_visits_df = (
    visits_df
    .join(visit_dates_df, ["visit_date_key"], "left")
    .filter(F.col("visit_date").isNotNull())
)

snapshot_context_df = spark.range(1).select(SNAPSHOT_DATE.alias("snapshot_date"))

lifetime_visits_df = (
    completed_visits_df
    .groupBy("client_key")
    .agg(
        F.max("visit_date").alias("last_visit_date"),
        F.sum(F.coalesce(F.col("visit_count"), F.lit(1))).cast("long").alias("lifetime_visits"),
        F.sum(
            F.when(F.col("visit_date") >= F.date_sub(SNAPSHOT_DATE, 30), F.coalesce(F.col("visit_count"), F.lit(1))).otherwise(F.lit(0))
        ).cast("long").alias("visits_last_30_days"),
        F.sum(
            F.when(F.col("visit_date") >= F.date_sub(SNAPSHOT_DATE, 60), F.coalesce(F.col("visit_count"), F.lit(1))).otherwise(F.lit(0))
        ).cast("long").alias("visits_last_60_days"),
        F.sum(
            F.when(F.col("visit_date") >= F.date_sub(SNAPSHOT_DATE, 90), F.coalesce(F.col("visit_count"), F.lit(1))).otherwise(F.lit(0))
        ).cast("long").alias("visits_last_90_days"),
    )
)

last_visit_window = Window.partitionBy("client_key").orderBy(
    F.col("visit_date").desc(),
    F.col("source_visit_id").desc_nulls_last() if "source_visit_id" in completed_visits_df.columns else F.col("visit_date_key").desc(),
)

last_visit_detail_df = (
    completed_visits_df
    .withColumn("_last_visit_rank", F.row_number().over(last_visit_window))
    .filter(F.col("_last_visit_rank") == 1)
    .select(
        "client_key",
        F.col("service_key").alias("last_service_key"),
        F.col("purchase_item_key").alias("last_purchase_item_key"),
    )
    .join(service_df.select("service_key", "service_name"), F.col("last_service_key") == F.col("service_key"), "left")
    .drop("service_key")
    .withColumnRenamed("service_name", "last_service_name")
    .join(purchase_item_df.select("purchase_item_key", "purchase_item_name"), F.col("last_purchase_item_key") == F.col("purchase_item_key"), "left")
    .drop("purchase_item_key")
    .withColumnRenamed("purchase_item_name", "last_purchase_item_name")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build revenue-derived retention features.
purchase_dates_df = date_df.select(
    F.col("date_key").alias("purchase_date_key"),
    F.col("calendar_date").cast("date").alias("purchase_date"),
)

purchases_with_dates_df = purchases_df.join(purchase_dates_df, ["purchase_date_key"], "left")

revenue_features_df = (
    purchases_with_dates_df
    .groupBy("client_key")
    .agg(
        F.sum(F.coalesce(F.col("net_sales_amount"), F.lit(0))).cast("double").alias("lifetime_revenue"),
        F.sum(
            F.when(F.col("purchase_date") >= F.date_sub(SNAPSHOT_DATE, 90), F.coalesce(F.col("net_sales_amount"), F.lit(0))).otherwise(F.lit(0))
        ).cast("double").alias("revenue_last_90_days"),
    )
)

last_purchase_window = Window.partitionBy("client_key").orderBy(
    F.col("purchase_date").desc_nulls_last(),
    F.col("source_purchase_item_id").desc_nulls_last() if "source_purchase_item_id" in purchases_with_dates_df.columns else F.col("purchase_date_key").desc_nulls_last(),
)

last_discount_df = (
    purchases_with_dates_df
    .withColumn("_last_purchase_rank", F.row_number().over(last_purchase_window))
    .filter(F.col("_last_purchase_rank") == 1)
    .select("client_key", F.col("discount_code").alias("last_discount_code"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Assemble one snapshot row per client and apply deterministic retention logic.
client_base_df = clients_df.select("client_key", F.col("client_name"))

snapshot_base_df = (
    client_base_df
    .crossJoin(snapshot_context_df)
    .join(lifetime_visits_df, ["client_key"], "left")
    .join(last_visit_detail_df, ["client_key"], "left")
    .join(revenue_features_df, ["client_key"], "left")
    .join(last_discount_df, ["client_key"], "left")
    .withColumn("lifetime_visits", F.coalesce(F.col("lifetime_visits"), F.lit(0).cast("long")))
    .withColumn("visits_last_30_days", F.coalesce(F.col("visits_last_30_days"), F.lit(0).cast("long")))
    .withColumn("visits_last_60_days", F.coalesce(F.col("visits_last_60_days"), F.lit(0).cast("long")))
    .withColumn("visits_last_90_days", F.coalesce(F.col("visits_last_90_days"), F.lit(0).cast("long")))
    .withColumn("lifetime_revenue", F.coalesce(F.col("lifetime_revenue"), F.lit(0.0)))
    .withColumn("revenue_last_90_days", F.coalesce(F.col("revenue_last_90_days"), F.lit(0.0)))
    .withColumn("days_since_last_visit", F.when(F.col("last_visit_date").isNull(), F.lit(None).cast("int")).otherwise(F.datediff(F.col("snapshot_date"), F.col("last_visit_date"))))
)

revenue_quartile_window = Window.orderBy(F.col("lifetime_revenue").desc(), F.col("client_key").asc())

snapshot_scored_df = (
    snapshot_base_df
    .withColumn("lifetime_revenue_quartile", F.ntile(4).over(revenue_quartile_window))
    .withColumn(
        "retention_status",
        F.when(F.col("last_visit_date").isNull(), F.lit("Never Visited"))
        .when(F.col("days_since_last_visit") <= 30, F.lit("Active"))
        .when(F.col("days_since_last_visit").between(31, 60), F.lit("Slipping"))
        .when(F.col("days_since_last_visit").between(61, 90), F.lit("At Risk"))
        .when(F.col("days_since_last_visit") > 90, F.lit("Lost"))
        .otherwise(F.lit(None).cast("string"))
    )
)

service_name_lower = F.lower(F.coalesce(F.col("last_service_name"), F.lit("")))
is_salt_service = service_name_lower.rlike("salt|halotherapy")
is_recovery_service = service_name_lower.rlike("sauna|cold plunge|contrast|red light|recovery")
is_high_lifetime_revenue = (F.col("lifetime_revenue_quartile") == 1) & (F.col("lifetime_revenue") > 0)
has_meaningful_lifetime_revenue = F.col("lifetime_revenue") > 0

final_snapshot_df = (
    snapshot_scored_df
    .withColumn(
        "retention_priority",
        F.when((F.col("retention_status").isin("At Risk", "Lost")) & has_meaningful_lifetime_revenue, F.lit("High"))
        .when(F.col("retention_status") == "Slipping", F.lit("Medium"))
        .when(F.col("retention_status") == "Active", F.lit("Low"))
        .when(F.col("retention_status") == "Never Visited", F.lit("Review"))
        .otherwise(F.lit("Review"))
    )
    .withColumn(
        "recommended_action",
        F.when(F.col("retention_status") == "Never Visited", F.lit("Send first-visit activation offer"))
        .when(F.col("retention_status") == "Active", F.lit("No immediate retention action"))
        .when((F.col("retention_status") == "Slipping") & is_salt_service, F.lit("Send salt therapy comeback offer"))
        .when((F.col("retention_status") == "Slipping") & is_recovery_service, F.lit("Promote Double Dip or recovery bundle"))
        .when((F.col("retention_status") == "At Risk") & is_high_lifetime_revenue, F.lit("Personal staff follow-up"))
        .when(F.col("retention_status") == "At Risk", F.lit("Send win-back offer"))
        .when(F.col("retention_status") == "Lost", F.lit("Send reactivation campaign"))
        .otherwise(F.lit("Review manually"))
    )
    .withColumn("batch_id", F.lit(batch_id).cast("string"))
    .withColumn("created_at_utc", CURRENT_TIMESTAMP_UTC)
    .withColumn("updated_at_utc", CURRENT_TIMESTAMP_UTC)
    .select(
        "snapshot_date",
        "batch_id",
        "client_key",
        "client_name",
        "last_visit_date",
        "days_since_last_visit",
        "lifetime_visits",
        "visits_last_30_days",
        "visits_last_60_days",
        "visits_last_90_days",
        "lifetime_revenue",
        "revenue_last_90_days",
        "last_service_key",
        "last_service_name",
        "last_purchase_item_key",
        "last_purchase_item_name",
        "last_discount_code",
        "retention_status",
        "retention_priority",
        "recommended_action",
        "created_at_utc",
        "updated_at_utc",
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Idempotent load: replace only the current snapshot_date and preserve prior snapshots.
if table_exists(TARGET_TABLE):
    spark.sql(f"DELETE FROM {TARGET_TABLE} WHERE snapshot_date = DATE '{SNAPSHOT_DATE_SQL_LITERAL}'")
    (
        final_snapshot_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
else:
    (
        final_snapshot_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

if not table_exists(TARGET_TABLE):
    write_dq_result(
        batch_id,
        "gold",
        NOTEBOOK_NAME,
        PROCESS_NAME,
        TARGET_TABLE,
        "target_table_exists_after_load",
        "ERROR",
        "GOLD_RETENTION_TARGET_TABLE_MISSING_AFTER_LOAD",
        "Target table was not found after the retention snapshot load.",
        0,
        {"target_table": TARGET_TABLE},
        "FAIL",
    )
    raise ValueError(f"Target table {TARGET_TABLE} was not found after load.")

write_dq_info(
    "target_table_exists_after_load",
    "GOLD_RETENTION_TARGET_TABLE_EXISTS_AFTER_LOAD",
    "Target table exists after the retention snapshot load.",
    1,
    {"target_table": TARGET_TABLE},
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DQ checks for current snapshot.
target_snapshot_df = spark.table(TARGET_TABLE).filter(F.col("snapshot_date") == SNAPSHOT_DATE)

dim_client_count = clients_df.select("client_key").distinct().count()
snapshot_row_count = target_snapshot_df.count()

duplicate_count = (
    target_snapshot_df
    .groupBy("client_key", "snapshot_date")
    .agg(F.count(F.lit(1)).alias("row_count"))
    .filter(F.col("row_count") > 1)
    .count()
)
null_client_key_count = target_snapshot_df.filter(F.col("client_key").isNull()).count()
null_retention_status_count = target_snapshot_df.filter(F.col("retention_status").isNull()).count()
negative_days_since_last_visit_count = target_snapshot_df.filter(F.col("days_since_last_visit") < 0).count()
negative_lifetime_visits_count = target_snapshot_df.filter(F.col("lifetime_visits") < 0).count()
negative_lifetime_revenue_count = target_snapshot_df.filter(F.col("lifetime_revenue") < 0).count()
never_visited_count = target_snapshot_df.filter(F.col("retention_status") == "Never Visited").count()
at_risk_count = target_snapshot_df.filter(F.col("retention_status") == "At Risk").count()
lost_count = target_snapshot_df.filter(F.col("retention_status") == "Lost").count()
row_count_difference = abs(snapshot_row_count - dim_client_count)

write_dq_warning(
    "duplicate_client_snapshot_rows",
    "GOLD_RETENTION_DUPLICATE_CLIENT_SNAPSHOT_ROWS",
    "Target table contains duplicate client_key + snapshot_date rows.",
    duplicate_count,
    sample_record(target_snapshot_df, F.col("client_key").isNotNull(), ["client_key", "snapshot_date"]),
    "FAIL",
)
write_dq_warning(
    "client_key_not_null",
    "GOLD_RETENTION_NULL_CLIENT_KEY",
    "Retention snapshot rows have null client_key.",
    null_client_key_count,
    sample_record(target_snapshot_df, F.col("client_key").isNull()),
    "FAIL",
)
write_dq_warning(
    "retention_status_not_null",
    "GOLD_RETENTION_NULL_RETENTION_STATUS",
    "Retention snapshot rows have null retention_status.",
    null_retention_status_count,
    sample_record(target_snapshot_df, F.col("retention_status").isNull()),
    "FAIL",
)
write_dq_warning(
    "days_since_last_visit_non_negative",
    "GOLD_RETENTION_NEGATIVE_DAYS_SINCE_LAST_VISIT",
    "Retention snapshot rows have negative days_since_last_visit.",
    negative_days_since_last_visit_count,
    sample_record(target_snapshot_df, F.col("days_since_last_visit") < 0),
    "FAIL",
)
write_dq_warning(
    "lifetime_visits_non_negative",
    "GOLD_RETENTION_NEGATIVE_LIFETIME_VISITS",
    "Retention snapshot rows have negative lifetime_visits.",
    negative_lifetime_visits_count,
    sample_record(target_snapshot_df, F.col("lifetime_visits") < 0),
    "FAIL",
)
write_dq_warning(
    "lifetime_revenue_non_negative",
    "GOLD_RETENTION_NEGATIVE_LIFETIME_REVENUE",
    "Retention snapshot rows have negative lifetime_revenue.",
    negative_lifetime_revenue_count,
    sample_record(target_snapshot_df, F.col("lifetime_revenue") < 0),
    "FAIL",
)
write_dq_warning(
    "row_count_matches_dim_client",
    "GOLD_RETENTION_ROW_COUNT_MISMATCH_DIM_CLIENT",
    "Retention snapshot row count does not match distinct dim_client client count.",
    row_count_difference,
    {"snapshot_row_count": snapshot_row_count, "dim_client_count": dim_client_count},
    "WARN",
)
write_dq_info(
    "never_visited_clients_reported",
    "GOLD_RETENTION_NEVER_VISITED_CLIENTS_REPORTED",
    "Never Visited client count for current snapshot.",
    never_visited_count,
    {"retention_status": "Never Visited", "count": never_visited_count},
)
write_dq_info(
    "at_risk_clients_reported",
    "GOLD_RETENTION_AT_RISK_CLIENTS_REPORTED",
    "At Risk client count for current snapshot.",
    at_risk_count,
    {"retention_status": "At Risk", "count": at_risk_count},
)
write_dq_info(
    "lost_clients_reported",
    "GOLD_RETENTION_LOST_CLIENTS_REPORTED",
    "Lost client count for current snapshot.",
    lost_count,
    {"retention_status": "Lost", "count": lost_count},
)

dq_summary_rows = [
    ("dim_client_count", dim_client_count),
    ("snapshot_row_count", snapshot_row_count),
    ("duplicate_client_snapshot_rows", duplicate_count),
    ("null_client_key_rows", null_client_key_count),
    ("null_retention_status_rows", null_retention_status_count),
    ("negative_days_since_last_visit_rows", negative_days_since_last_visit_count),
    ("negative_lifetime_visits_rows", negative_lifetime_visits_count),
    ("negative_lifetime_revenue_rows", negative_lifetime_revenue_count),
    ("never_visited_clients", never_visited_count),
    ("at_risk_clients", at_risk_count),
    ("lost_clients", lost_count),
]
dq_summary_df = spark.createDataFrame(dq_summary_rows, ["metric_name", "metric_value"])

display(dq_summary_df)
display(
    target_snapshot_df
    .groupBy("retention_status", "retention_priority")
    .count()
    .orderBy("retention_status", "retention_priority")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    f"Successfully loaded {TARGET_TABLE} for snapshot_date={SNAPSHOT_DATE_SQL_LITERAL} using LOAD_MODE='{load_mode}'. "
    f"Rows written for current snapshot: {snapshot_row_count}. "
    f"Never Visited: {never_visited_count}. At Risk: {at_risk_count}. Lost: {lost_count}."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
