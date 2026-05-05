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

# Gold star-schema loader for Salt Room Analytics.
# This notebook rebuilds the Gold layer from Silver entities and writes
# dimensions, facts, and a centralized DQ warning table.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from functools import reduce
import json

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SILVER_CLIENTS_TABLE = f"{SILVER_SCHEMA}.clients"
SILVER_PURCHASES_TABLE = f"{SILVER_SCHEMA}.purchases"
SILVER_VISITS_TABLE = f"{SILVER_SCHEMA}.visits"
SILVER_TIMECLOCK_TABLE = f"{SILVER_SCHEMA}.timeclock"

DIM_DATE_TABLE = f"{GOLD_SCHEMA}.dim_date"
DIM_CLIENT_TABLE = f"{GOLD_SCHEMA}.dim_client"
DIM_SERVICE_TABLE = f"{GOLD_SCHEMA}.dim_service"
DIM_STAFF_TABLE = f"{GOLD_SCHEMA}.dim_staff"
DIM_PURCHASE_ITEM_TABLE = f"{GOLD_SCHEMA}.dim_purchase_item"

FACT_PURCHASE_TABLE = f"{GOLD_SCHEMA}.fact_purchase"
FACT_VISIT_TABLE = f"{GOLD_SCHEMA}.fact_visit"
FACT_TIMECLOCK_TABLE = f"{GOLD_SCHEMA}.fact_timeclock"
GOLD_DQ_WARNINGS_TABLE = f"{GOLD_SCHEMA}.gold_dq_warnings"
DQ_LOAD_WARNINGS_TABLE = "dq.load_warnings"
NOTEBOOK_NAME = "load_gold_data"

VALID_LOAD_MODES = {"init", "refresh"}
CURRENT_TIMESTAMP_UTC = F.current_timestamp()
DECIMAL_18_2 = T.DecimalType(18, 2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if load_mode not in VALID_LOAD_MODES:
    raise ValueError(f"Unsupported LOAD_MODE '{load_mode}'. Expected one of: {sorted(VALID_LOAD_MODES)}")


def require_table(table_name: str) -> DataFrame:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Required source table '{table_name}' does not exist.")
    return spark.table(table_name)


def require_columns(dataframe: DataFrame, required_columns: list[str], table_name: str) -> None:
    missing_columns = [column_name for column_name in required_columns if column_name not in dataframe.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in {table_name}: {missing_columns}")


def null_if_blank(column_expression) -> F.Column:
    trimmed_value = F.trim(F.col(column_expression) if isinstance(column_expression, str) else column_expression)
    return F.when(trimmed_value == "", F.lit(None)).otherwise(trimmed_value)


def normalize_name(column_expression) -> F.Column:
    value = null_if_blank(column_expression)
    return F.when(value.isNull(), F.lit(None)).otherwise(F.lower(F.regexp_replace(value, r"\s+", " ")))


def normalize_text(column_expression) -> F.Column:
    value = null_if_blank(column_expression)
    return F.when(value.isNull(), F.lit(None)).otherwise(F.regexp_replace(value, r"\s+", " "))


def add_surrogate_key(dataframe: DataFrame, key_name: str, order_columns: list[str]) -> DataFrame:
    window_spec = Window.orderBy(*[F.col(column_name).asc_nulls_last() for column_name in order_columns])
    return dataframe.withColumn(key_name, F.row_number().over(window_spec).cast("int"))


def union_all(dataframes: list[DataFrame]) -> DataFrame:
    return reduce(lambda left_df, right_df: left_df.unionByName(right_df), dataframes)


def create_warning_df(
    dataframe: DataFrame,
    table_name: str,
    source_table: str,
    source_record_expression,
    condition,
    warning_type: str,
    warning_message: str,
    severity: str = "WARN",
) -> DataFrame:
    source_record_column = source_record_expression if isinstance(source_record_expression, F.Column) else F.col(source_record_expression)
    return (
        dataframe.filter(condition)
        .select(
            F.lit(table_name).alias("table_name"),
            F.lit(source_table).alias("source_table"),
            source_record_column.cast("string").alias("source_record_id"),
            F.lit(warning_type).alias("warning_type"),
            F.lit(warning_message).alias("warning_message"),
            F.lit(severity).alias("severity"),
            CURRENT_TIMESTAMP_UTC.alias("created_at_utc"),
        )
    )


def write_delta_table(dataframe: DataFrame, table_name: str) -> None:
    dataframe.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(table_name)


def ensure_schema_exists(schema_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


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


def write_dq_warning_from_dataframe(
    warning_dataframe: DataFrame,
    process_name: str,
    target_table: str,
    check_name: str,
    warning_code: str,
    warning_message: str,
    status: str = "WARN",
) -> None:
    affected_row_count = warning_dataframe.count()
    if affected_row_count == 0:
        return

    sample_rows = warning_dataframe.limit(1).collect()
    sample_record = sample_rows[0].asDict(recursive=True) if sample_rows else None
    severity = "ERROR" if status == "FAIL" else "WARNING"
    write_dq_result(
        batch_id,
        "gold",
        NOTEBOOK_NAME,
        process_name,
        target_table,
        check_name,
        severity,
        warning_code,
        warning_message,
        affected_row_count,
        sample_record,
        status,
    )


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
            "gold",
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


def is_self_booked_expression(booked_by_column: str = "booked_by_name_key", booking_source_column: str = "booking_source_key") -> F.Column:
    booked_by_key = F.coalesce(F.col(booked_by_column), F.lit(""))
    booking_source_key = F.coalesce(F.col(booking_source_column), F.lit(""))
    combined_key = F.concat_ws(" ", booked_by_key, booking_source_key)
    return (
        F.col(booked_by_column).isNull()
        | combined_key.contains("online")
        | combined_key.contains("client")
        | combined_key.contains("self")
        | combined_key.contains("web")
        | combined_key.contains("app")
        | combined_key.contains("portal")
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read and validate Silver sources using the current production schema.
silver_clients_df = require_table(SILVER_CLIENTS_TABLE)
silver_purchases_df = require_table(SILVER_PURCHASES_TABLE)
silver_visits_df = require_table(SILVER_VISITS_TABLE)
silver_timeclock_df = require_table(SILVER_TIMECLOCK_TABLE)

require_columns(
    silver_clients_df,
    ["salt_client_key", "client", "source_client_id", "source_member_id", "client_status", "registration_date"],
    SILVER_CLIENTS_TABLE,
)
require_columns(
    silver_purchases_df,
    [
        "source_purchase_item_id",
        "salt_client_key",
        "purchase_date",
        "revenue_category_clean",
        "item_name_clean",
        "subtotal_amount",
        "discount_amount",
        "net_sales_amount",
        "tax_amount",
        "total_paid_amount",
        "sold_by",
        "discount_code",
        "source_file_name",
        "source_loaded_at",
    ],
    SILVER_PURCHASES_TABLE,
)
require_columns(
    silver_visits_df,
    [
        "salt_client_key",
        "visit_date",
        "service_name",
        "purchase_option_name",
        "staff_name",
        "booked_by",
        "booking_source",
        "source_file_name",
        "source_loaded_at",
    ],
    SILVER_VISITS_TABLE,
)
require_columns(
    silver_timeclock_df,
    ["staff_name_clean", "work_date", "hours_worked", "source_file_name", "source_loaded_at", "source_row_hash"],
    SILVER_TIMECLOCK_TABLE,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Rebuild mode: drop and recreate all Gold objects from Silver.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

for table_name in [
    GOLD_DQ_WARNINGS_TABLE,
    FACT_PURCHASE_TABLE,
    FACT_VISIT_TABLE,
    FACT_TIMECLOCK_TABLE,
    DIM_PURCHASE_ITEM_TABLE,
    DIM_STAFF_TABLE,
    DIM_SERVICE_TABLE,
    DIM_CLIENT_TABLE,
    DIM_DATE_TABLE,
]:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dimension: Date
purchase_date_bounds_df = silver_purchases_df.select(F.col("purchase_date").cast("date").alias("calendar_date")).filter(F.col("calendar_date").isNotNull())
visit_date_bounds_df = silver_visits_df.select(F.col("visit_date").cast("date").alias("calendar_date")).filter(F.col("calendar_date").isNotNull())
timeclock_date_bounds_df = silver_timeclock_df.select(F.col("work_date").cast("date").alias("calendar_date")).filter(F.col("calendar_date").isNotNull())

date_bounds_row = (
    union_all([purchase_date_bounds_df, visit_date_bounds_df, timeclock_date_bounds_df])
    .agg(F.min("calendar_date").alias("min_date"), F.max("calendar_date").alias("max_date"))
    .collect()[0]
)

if date_bounds_row["min_date"] is None or date_bounds_row["max_date"] is None:
    raise ValueError("Could not derive a valid Gold date range from the Silver tables.")

dim_date_df = (
    spark.sql(
        f"""
        SELECT explode(sequence(to_date('{date_bounds_row["min_date"]}'), to_date('{date_bounds_row["max_date"]}'), interval 1 day)) AS calendar_date
        """
    )
    .select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("calendar_date"),
        F.dayofmonth("calendar_date").alias("day"),
        F.date_format("calendar_date", "EEEE").alias("day_name"),
        F.dayofweek("calendar_date").alias("day_of_week"),
        F.weekofyear("calendar_date").alias("week_of_year"),
        F.month("calendar_date").alias("month"),
        F.date_format("calendar_date", "MMMM").alias("month_name"),
        F.quarter("calendar_date").alias("quarter"),
        F.year("calendar_date").alias("year"),
        F.dayofweek("calendar_date").isin([1, 7]).alias("is_weekend"),
        CURRENT_TIMESTAMP_UTC.alias("created_at_utc"),
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dimension: Client
dim_client_base_df = (
    silver_clients_df
    .select(
        F.col("salt_client_key").alias("silver_client_key"),
        F.col("source_client_id").cast("string").alias("source_client_id"),
        F.col("source_member_id").cast("string").alias("source_member_id"),
        F.coalesce(null_if_blank("client_full_name_clean"), null_if_blank("client")).alias("client_name"),
        null_if_blank("client_status").alias("client_status"),
        F.col("registration_date").cast("date").alias("registration_date"),
        F.lit(None).cast("string").alias("zipcode"),
    )
    .dropDuplicates(["silver_client_key"])
)

dim_client_actual_df = add_surrogate_key(dim_client_base_df, "client_key", ["client_name", "silver_client_key"])
dim_client_unknown_df = spark.createDataFrame(
    [(0, None, None, None, "Unknown", None, None, None)],
    schema=T.StructType(
        [
            T.StructField("client_key", T.IntegerType(), False),
            T.StructField("silver_client_key", T.StringType(), True),
            T.StructField("source_client_id", T.StringType(), True),
            T.StructField("source_member_id", T.StringType(), True),
            T.StructField("client_name", T.StringType(), True),
            T.StructField("client_status", T.StringType(), True),
            T.StructField("registration_date", T.DateType(), True),
            T.StructField("zipcode", T.StringType(), True),
        ]
    ),
)
dim_client_df = union_all([dim_client_unknown_df, dim_client_actual_df]).withColumn("created_at_utc", CURRENT_TIMESTAMP_UTC)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dimension: Service
dim_service_base_df = (
    silver_visits_df
    .select(
        normalize_text("service_name").alias("service_name"),
        normalize_text("service_category").alias("service_category"),
    )
    .filter(F.col("service_name").isNotNull())
    .dropDuplicates(["service_name", "service_category"])
)

dim_service_actual_df = add_surrogate_key(dim_service_base_df, "service_key", ["service_name", "service_category"])
dim_service_unknown_df = spark.createDataFrame(
    [(0, "Unknown", None)],
    schema=T.StructType(
        [
            T.StructField("service_key", T.IntegerType(), False),
            T.StructField("service_name", T.StringType(), True),
            T.StructField("service_category", T.StringType(), True),
        ]
    ),
)
dim_service_df = union_all([dim_service_unknown_df, dim_service_actual_df]).withColumn("created_at_utc", CURRENT_TIMESTAMP_UTC)

dim_service_lookup_join_df = (
    dim_service_df
    .withColumn("service_category_join_key", F.coalesce(F.col("service_category"), F.lit("__null__")))
    .select("service_key", "service_name", "service_category_join_key")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dimension: Staff
visit_staff_df = (
    silver_visits_df
    .select(normalize_text("staff_name").alias("staff_name"), F.lit("Service Staff").alias("staff_role"))
    .filter(F.col("staff_name").isNotNull())
)
visit_booked_by_df = (
    silver_visits_df
    .withColumn("booked_by_name_key", normalize_name("booked_by"))
    .withColumn("booking_source_key", normalize_name("booking_source"))
    .filter(F.col("booked_by_name_key").isNotNull() & ~is_self_booked_expression())
    .select(normalize_text("booked_by").alias("staff_name"), F.lit("Booking Staff").alias("staff_role"))
)
purchase_staff_df = (
    silver_purchases_df
    .select(normalize_text("sold_by").alias("staff_name"), F.lit("Sales Staff").alias("staff_role"))
    .filter(F.col("staff_name").isNotNull())
)
timeclock_staff_df = (
    silver_timeclock_df
    .select(normalize_text("staff_name_clean").alias("staff_name"), F.lit("Labor Staff").alias("staff_role"))
    .filter(F.col("staff_name").isNotNull())
)

dim_staff_roles_df = union_all([visit_staff_df, visit_booked_by_df, purchase_staff_df, timeclock_staff_df])
dim_staff_base_df = (
    dim_staff_roles_df
    .groupBy("staff_name")
    .agg(
        F.collect_set("staff_role").alias("staff_roles"),
        F.countDistinct("staff_role").alias("staff_role_count"),
    )
    .withColumn(
        "staff_type",
        F.when(F.col("staff_role_count") > 1, F.lit("Multiple"))
        .otherwise(F.element_at(F.array_sort(F.col("staff_roles")), 1)),
    )
    .select("staff_name", "staff_type")
)

dim_staff_actual_df = add_surrogate_key(dim_staff_base_df, "staff_key", ["staff_name"])
dim_staff_special_df = spark.createDataFrame(
    [
        (-1, "Online", "System / Client Self-Booking"),
        (0, "Unknown", "Unknown"),
    ],
    schema=T.StructType(
        [
            T.StructField("staff_key", T.IntegerType(), False),
            T.StructField("staff_name", T.StringType(), True),
            T.StructField("staff_type", T.StringType(), True),
        ]
    ),
)
dim_staff_df = union_all([dim_staff_special_df, dim_staff_actual_df]).withColumn("created_at_utc", CURRENT_TIMESTAMP_UTC)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dimension: Purchase Item
purchase_items_from_sales_df = (
    silver_purchases_df
    .select(
        normalize_text("item_name_clean").alias("purchase_item_name"),
        normalize_text("revenue_category_clean").alias("revenue_category"),
    )
    .filter(F.col("purchase_item_name").isNotNull())
)
purchase_items_from_visits_df = (
    silver_visits_df
    .select(
        normalize_text("purchase_option_name").alias("purchase_item_name"),
        F.lit(None).cast("string").alias("revenue_category"),
    )
    .filter(F.col("purchase_item_name").isNotNull())
)

dim_purchase_item_base_df = (
    union_all([purchase_items_from_sales_df, purchase_items_from_visits_df])
    .groupBy("purchase_item_name")
    .agg(F.max("revenue_category").alias("revenue_category"))
)

dim_purchase_item_actual_df = add_surrogate_key(dim_purchase_item_base_df, "purchase_item_key", ["purchase_item_name"])
dim_purchase_item_unknown_df = spark.createDataFrame(
    [(0, "Unknown", None)],
    schema=T.StructType(
        [
            T.StructField("purchase_item_key", T.IntegerType(), False),
            T.StructField("purchase_item_name", T.StringType(), True),
            T.StructField("revenue_category", T.StringType(), True),
        ]
    ),
)
dim_purchase_item_df = union_all([dim_purchase_item_unknown_df, dim_purchase_item_actual_df]).withColumn("created_at_utc", CURRENT_TIMESTAMP_UTC)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Persist dimensions first so they can be used as lookups for the facts.
write_delta_table(dim_date_df, DIM_DATE_TABLE)
write_delta_table(dim_client_df, DIM_CLIENT_TABLE)
write_delta_table(dim_service_df, DIM_SERVICE_TABLE)
write_delta_table(dim_staff_df, DIM_STAFF_TABLE)
write_delta_table(dim_purchase_item_df, DIM_PURCHASE_ITEM_TABLE)

dim_date_lookup_df = spark.table(DIM_DATE_TABLE)
dim_client_lookup_df = spark.table(DIM_CLIENT_TABLE)
dim_service_lookup_df = spark.table(DIM_SERVICE_TABLE)
dim_staff_lookup_df = spark.table(DIM_STAFF_TABLE)
dim_purchase_item_lookup_df = spark.table(DIM_PURCHASE_ITEM_TABLE)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fact: Purchase
purchase_base_df = (
    silver_purchases_df
    .select(
        F.col("source_purchase_item_id").cast("string").alias("source_purchase_item_id"),
        F.col("salt_client_key").alias("silver_client_key"),
        F.col("purchase_date").cast("date").alias("purchase_date"),
        normalize_text("item_name_clean").alias("purchase_item_name"),
        normalize_text("sold_by").alias("sold_by_name"),
        null_if_blank("discount_code").alias("discount_code"),
        F.col("subtotal_amount").cast(DECIMAL_18_2).alias("gross_amount"),
        F.col("discount_amount").cast(DECIMAL_18_2).alias("discount_amount"),
        F.col("net_sales_amount").cast(DECIMAL_18_2).alias("net_sales_amount"),
        F.col("tax_amount").cast(DECIMAL_18_2).alias("tax_amount"),
        F.col("total_paid_amount").cast(DECIMAL_18_2).alias("total_paid_amount"),
        F.lit(1).cast("int").alias("quantity"),
        F.col("source_file_name"),
        F.col("source_loaded_at").cast("timestamp").alias("source_loaded_at"),
    )
)

purchase_invalid_date_warnings_df = create_warning_df(
    purchase_base_df,
    FACT_PURCHASE_TABLE,
    SILVER_PURCHASES_TABLE,
    "source_purchase_item_id",
    F.col("purchase_date").isNull(),
    "INVALID_PURCHASE_DATE",
    "Purchase row excluded from fact_purchase because purchase_date is null or invalid.",
    "ERROR",
)

fact_purchase_df = (
    purchase_base_df
    .filter(F.col("purchase_date").isNotNull())
    .join(dim_date_lookup_df.select("date_key", "calendar_date"), F.col("purchase_date") == F.col("calendar_date"), "inner")
    .withColumnRenamed("date_key", "purchase_date_key")
    .drop("calendar_date")
    .join(dim_client_lookup_df.select("client_key", "silver_client_key"), ["silver_client_key"], "left")
    .join(dim_purchase_item_lookup_df.select("purchase_item_key", "purchase_item_name"), ["purchase_item_name"], "left")
    .join(dim_staff_lookup_df.select(F.col("staff_key").alias("sold_by_staff_key"), F.col("staff_name").alias("sold_by_name")), ["sold_by_name"], "left")
    .withColumn("client_key", F.coalesce(F.col("client_key"), F.lit(0)))
    .withColumn("purchase_item_key", F.coalesce(F.col("purchase_item_key"), F.lit(0)))
    .withColumn("sold_by_staff_key", F.coalesce(F.col("sold_by_staff_key"), F.lit(0)))
    .select(
        "purchase_date_key",
        "client_key",
        "purchase_item_key",
        "sold_by_staff_key",
        "source_purchase_item_id",
        "gross_amount",
        "discount_amount",
        "net_sales_amount",
        "tax_amount",
        "total_paid_amount",
        "quantity",
        "discount_code",
        "source_file_name",
        "source_loaded_at",
        CURRENT_TIMESTAMP_UTC.alias("created_at_utc"),
    )
)

purchase_client_warnings_df = create_warning_df(
    fact_purchase_df,
    FACT_PURCHASE_TABLE,
    SILVER_PURCHASES_TABLE,
    "source_purchase_item_id",
    F.col("client_key") == 0,
    "UNKNOWN_CLIENT_KEY",
    "Purchase row used client_key = 0 because the client could not be resolved from silver.clients.",
)
purchase_item_warnings_df = create_warning_df(
    fact_purchase_df,
    FACT_PURCHASE_TABLE,
    SILVER_PURCHASES_TABLE,
    "source_purchase_item_id",
    F.col("purchase_item_key") == 0,
    "UNKNOWN_PURCHASE_ITEM_KEY",
    "Purchase row used purchase_item_key = 0 because the purchase item could not be resolved.",
)
purchase_staff_warnings_df = create_warning_df(
    fact_purchase_df,
    FACT_PURCHASE_TABLE,
    SILVER_PURCHASES_TABLE,
    "source_purchase_item_id",
    F.col("sold_by_staff_key") == 0,
    "UNKNOWN_SOLD_BY_STAFF_KEY",
    "Purchase row used sold_by_staff_key = 0 because the sold_by staff name could not be resolved.",
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fact: Visit
visit_base_df = (
    silver_visits_df
    .select(
        F.col("source_visit_id").cast("string").alias("source_visit_id"),
        F.col("salt_client_key").alias("silver_client_key"),
        F.col("visit_date").cast("date").alias("visit_date"),
        normalize_text("service_name").alias("service_name"),
        normalize_text("service_category").alias("service_category"),
        F.coalesce(normalize_text("service_category"), F.lit("__null__")).alias("service_category_join_key"),
        normalize_text("purchase_option_name").alias("purchase_item_name"),
        normalize_text("staff_name").alias("staff_name"),
        normalize_text("booked_by").alias("booked_by_name"),
        normalize_name("booked_by").alias("booked_by_name_key"),
        normalize_name("booking_source").alias("booking_source_key"),
        null_if_blank("attendance_status").alias("attendance_status"),
        null_if_blank("booking_source").alias("booking_source"),
        F.col("source_file_name"),
        F.col("source_loaded_at").cast("timestamp").alias("source_loaded_at"),
    )
)

visit_record_id_expr = F.coalesce(
    F.col("source_visit_id"),
    F.sha2(F.concat_ws("|", F.col("silver_client_key"), F.col("service_name"), F.col("source_file_name")), 256),
)

visit_invalid_date_warnings_df = create_warning_df(
    visit_base_df,
    FACT_VISIT_TABLE,
    SILVER_VISITS_TABLE,
    visit_record_id_expr,
    F.col("visit_date").isNull(),
    "INVALID_VISIT_DATE",
    "Visit row excluded from fact_visit because visit_date is null or invalid.",
    "ERROR",
)

fact_visit_stage_df = (
    visit_base_df
    .filter(F.col("visit_date").isNotNull())
    .join(dim_date_lookup_df.select("date_key", "calendar_date"), F.col("visit_date") == F.col("calendar_date"), "inner")
    .withColumnRenamed("date_key", "visit_date_key")
    .drop("calendar_date")
    .join(dim_client_lookup_df.select("client_key", "silver_client_key"), ["silver_client_key"], "left")
    .join(dim_service_lookup_join_df, ["service_name", "service_category_join_key"], "left")
    .join(dim_purchase_item_lookup_df.select("purchase_item_key", "purchase_item_name"), ["purchase_item_name"], "left")
    .join(dim_staff_lookup_df.select("staff_key", "staff_name"), ["staff_name"], "left")
    .join(dim_staff_lookup_df.select(F.col("staff_key").alias("resolved_booked_by_staff_key"), F.col("staff_name").alias("booked_by_name")), ["booked_by_name"], "left")
    .withColumn("client_key", F.coalesce(F.col("client_key"), F.lit(0)))
    .withColumn("service_key", F.coalesce(F.col("service_key"), F.lit(0)))
    .withColumn("purchase_item_key", F.coalesce(F.col("purchase_item_key"), F.lit(0)))
    .withColumn("staff_key", F.coalesce(F.col("staff_key"), F.lit(0)))
    .withColumn(
        "booked_by_staff_key",
        F.when(is_self_booked_expression(), F.lit(-1))
        .otherwise(F.coalesce(F.col("resolved_booked_by_staff_key"), F.lit(0))),
    )
    .withColumn(
        "attended_flag",
        F.when(F.col("attendance_status").isNull(), F.lit(None).cast("int"))
        .when(F.lower(F.col("attendance_status")).contains("attend"), F.lit(1))
        .when(F.lower(F.col("attendance_status")).contains("show"), F.lit(1))
        .when(F.lower(F.col("attendance_status")).contains("complete"), F.lit(1))
        .when(F.lower(F.col("attendance_status")).contains("cancel"), F.lit(0))
        .when(F.lower(F.col("attendance_status")).contains("no show"), F.lit(0))
        .otherwise(F.lit(None).cast("int")),
    )
)

fact_visit_df = fact_visit_stage_df.select(
    "visit_date_key",
    "client_key",
    "service_key",
    "staff_key",
    "booked_by_staff_key",
    "purchase_item_key",
    "attendance_status",
    "booking_source",
    F.lit(1).cast("int").alias("visit_count"),
    "attended_flag",
    "source_visit_id",
    "source_file_name",
    "source_loaded_at",
    CURRENT_TIMESTAMP_UTC.alias("created_at_utc"),
)

visit_client_warnings_df = create_warning_df(
    fact_visit_stage_df,
    FACT_VISIT_TABLE,
    SILVER_VISITS_TABLE,
    visit_record_id_expr,
    F.col("client_key") == 0,
    "UNKNOWN_CLIENT_KEY",
    "Visit row used client_key = 0 because the client could not be resolved from silver.clients.",
)
visit_service_warnings_df = create_warning_df(
    fact_visit_stage_df,
    FACT_VISIT_TABLE,
    SILVER_VISITS_TABLE,
    visit_record_id_expr,
    F.col("service_key") == 0,
    "UNKNOWN_SERVICE_KEY",
    "Visit row used service_key = 0 because the service could not be resolved.",
)
visit_purchase_item_warnings_df = create_warning_df(
    fact_visit_stage_df,
    FACT_VISIT_TABLE,
    SILVER_VISITS_TABLE,
    visit_record_id_expr,
    F.col("purchase_item_key") == 0,
    "UNKNOWN_PURCHASE_ITEM_KEY",
    "Visit row used purchase_item_key = 0 because the visit option could not be resolved.",
)
visit_staff_warnings_df = create_warning_df(
    fact_visit_stage_df,
    FACT_VISIT_TABLE,
    SILVER_VISITS_TABLE,
    visit_record_id_expr,
    F.col("staff_key") == 0,
    "UNKNOWN_STAFF_KEY",
    "Visit row used staff_key = 0 because the visit staff name could not be resolved.",
)
visit_booked_by_warnings_df = create_warning_df(
    fact_visit_stage_df,
    FACT_VISIT_TABLE,
    SILVER_VISITS_TABLE,
    visit_record_id_expr,
    (F.col("booked_by_staff_key") == 0) & ~is_self_booked_expression(),
    "UNKNOWN_BOOKED_BY_STAFF_KEY",
    "Visit row used booked_by_staff_key = 0 because booked_by should map to a real staff member but could not be resolved.",
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fact: Timeclock
timeclock_base_df = (
    silver_timeclock_df
    .select(
        F.col("salt_timeclock_key").cast("string").alias("source_timeclock_record_id"),
        F.col("work_date").cast("date").alias("work_date"),
        normalize_text("staff_name_clean").alias("staff_name"),
        F.col("hours_worked").cast(DECIMAL_18_2).alias("hours_worked"),
        F.col("source_row_hash").cast("string").alias("source_row_hash"),
        F.col("source_file_name"),
        F.col("source_loaded_at").cast("timestamp").alias("source_loaded_at"),
    )
)

timeclock_invalid_date_warnings_df = create_warning_df(
    timeclock_base_df,
    FACT_TIMECLOCK_TABLE,
    SILVER_TIMECLOCK_TABLE,
    "source_timeclock_record_id",
    F.col("work_date").isNull(),
    "INVALID_WORK_DATE",
    "Timeclock row excluded from fact_timeclock because work_date is null or invalid.",
    "ERROR",
)

fact_timeclock_df = (
    timeclock_base_df
    .filter(F.col("work_date").isNotNull())
    .join(dim_date_lookup_df.select("date_key", "calendar_date"), F.col("work_date") == F.col("calendar_date"), "inner")
    .withColumnRenamed("date_key", "work_date_key")
    .drop("calendar_date")
    .join(dim_staff_lookup_df.select("staff_key", "staff_name"), ["staff_name"], "left")
    .withColumn("staff_key", F.coalesce(F.col("staff_key"), F.lit(0)))
    .select(
        "work_date_key",
        "staff_key",
        "hours_worked",
        F.lit(1).cast("int").alias("shift_count"),
        "source_timeclock_record_id",
        "source_row_hash",
        "source_file_name",
        "source_loaded_at",
        CURRENT_TIMESTAMP_UTC.alias("created_at_utc"),
    )
)

timeclock_staff_warnings_df = create_warning_df(
    fact_timeclock_df,
    FACT_TIMECLOCK_TABLE,
    SILVER_TIMECLOCK_TABLE,
    "source_timeclock_record_id",
    F.col("staff_key") == 0,
    "UNKNOWN_STAFF_KEY",
    "Timeclock row used staff_key = 0 because the staff name could not be resolved.",
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Persist facts and consolidated Gold DQ warnings.
write_delta_table(fact_purchase_df, FACT_PURCHASE_TABLE)
write_delta_table(fact_visit_df, FACT_VISIT_TABLE)
write_delta_table(fact_timeclock_df, FACT_TIMECLOCK_TABLE)

warning_frames = [
    purchase_invalid_date_warnings_df,
    purchase_client_warnings_df,
    purchase_item_warnings_df,
    purchase_staff_warnings_df,
    visit_invalid_date_warnings_df,
    visit_client_warnings_df,
    visit_service_warnings_df,
    visit_purchase_item_warnings_df,
    visit_staff_warnings_df,
    visit_booked_by_warnings_df,
    timeclock_invalid_date_warnings_df,
    timeclock_staff_warnings_df,
]

gold_dq_check_specs = [
    (purchase_invalid_date_warnings_df, "load_gold_fact_purchase", FACT_PURCHASE_TABLE, "invalid_purchase_date", "GOLD_FACT_PURCHASE_INVALID_PURCHASE_DATE", "Purchase rows were excluded from fact_purchase because purchase_date is null or invalid.", "FAIL"),
    (purchase_client_warnings_df, "load_gold_fact_purchase", FACT_PURCHASE_TABLE, "unknown_client_key", "GOLD_FACT_PURCHASE_UNKNOWN_CLIENT_KEY", "Purchase rows used client_key = 0 because the client could not be resolved from silver.clients.", "WARN"),
    (purchase_item_warnings_df, "load_gold_fact_purchase", FACT_PURCHASE_TABLE, "unknown_purchase_item_key", "GOLD_FACT_PURCHASE_UNKNOWN_PURCHASE_ITEM_KEY", "Purchase rows used purchase_item_key = 0 because the purchase item could not be resolved.", "WARN"),
    (purchase_staff_warnings_df, "load_gold_fact_purchase", FACT_PURCHASE_TABLE, "unknown_sold_by_staff_key", "GOLD_FACT_PURCHASE_UNKNOWN_SOLD_BY_STAFF_KEY", "Purchase rows used sold_by_staff_key = 0 because the sold_by staff name could not be resolved.", "WARN"),
    (visit_invalid_date_warnings_df, "load_gold_fact_visit", FACT_VISIT_TABLE, "invalid_visit_date", "GOLD_FACT_VISIT_INVALID_VISIT_DATE", "Visit rows were excluded from fact_visit because visit_date is null or invalid.", "FAIL"),
    (visit_client_warnings_df, "load_gold_fact_visit", FACT_VISIT_TABLE, "unknown_client_key", "GOLD_FACT_VISIT_UNKNOWN_CLIENT_KEY", "Visit rows used client_key = 0 because the client could not be resolved from silver.clients.", "WARN"),
    (visit_service_warnings_df, "load_gold_fact_visit", FACT_VISIT_TABLE, "unknown_service_key", "GOLD_FACT_VISIT_UNKNOWN_SERVICE_KEY", "Visit rows used service_key = 0 because the service could not be resolved.", "WARN"),
    (visit_purchase_item_warnings_df, "load_gold_fact_visit", FACT_VISIT_TABLE, "unknown_purchase_item_key", "GOLD_FACT_VISIT_UNKNOWN_PURCHASE_ITEM_KEY", "Visit rows used purchase_item_key = 0 because the visit option could not be resolved.", "WARN"),
    (visit_staff_warnings_df, "load_gold_fact_visit", FACT_VISIT_TABLE, "unknown_staff_key", "GOLD_FACT_VISIT_UNKNOWN_STAFF_KEY", "Visit rows used staff_key = 0 because the visit staff name could not be resolved.", "WARN"),
    (visit_booked_by_warnings_df, "load_gold_fact_visit", FACT_VISIT_TABLE, "unknown_booked_by_staff_key", "GOLD_FACT_VISIT_UNKNOWN_BOOKED_BY_STAFF_KEY", "Visit rows used booked_by_staff_key = 0 because booked_by should map to a real staff member but could not be resolved.", "WARN"),
    (timeclock_invalid_date_warnings_df, "load_gold_fact_timeclock", FACT_TIMECLOCK_TABLE, "invalid_work_date", "GOLD_FACT_TIMECLOCK_INVALID_WORK_DATE", "Timeclock rows were excluded from fact_timeclock because work_date is null or invalid.", "FAIL"),
    (timeclock_staff_warnings_df, "load_gold_fact_timeclock", FACT_TIMECLOCK_TABLE, "unknown_staff_key", "GOLD_FACT_TIMECLOCK_UNKNOWN_STAFF_KEY", "Timeclock rows used staff_key = 0 because the staff name could not be resolved.", "WARN"),
]

for warning_dataframe, process_name, target_table, check_name, warning_code, warning_message, status in gold_dq_check_specs:
    write_dq_warning_from_dataframe(
        warning_dataframe,
        process_name,
        target_table,
        check_name,
        warning_code,
        warning_message,
        status,
    )

gold_dq_warnings_stage_df = union_all(warning_frames)
gold_dq_warnings_df = add_surrogate_key(
    gold_dq_warnings_stage_df,
    "dq_warning_id",
    ["table_name", "source_record_id", "warning_type", "warning_message"],
).select(
    "dq_warning_id",
    "table_name",
    "source_table",
    "source_record_id",
    "warning_type",
    "warning_message",
    "severity",
    "created_at_utc",
)

write_delta_table(gold_dq_warnings_df, GOLD_DQ_WARNINGS_TABLE)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display Gold load diagnostics.
gold_summary_df = spark.createDataFrame(
    [
        ("dim_date", spark.table(DIM_DATE_TABLE).count()),
        ("dim_client", spark.table(DIM_CLIENT_TABLE).count()),
        ("dim_service", spark.table(DIM_SERVICE_TABLE).count()),
        ("dim_staff", spark.table(DIM_STAFF_TABLE).count()),
        ("dim_purchase_item", spark.table(DIM_PURCHASE_ITEM_TABLE).count()),
        ("fact_purchase", spark.table(FACT_PURCHASE_TABLE).count()),
        ("fact_visit", spark.table(FACT_VISIT_TABLE).count()),
        ("fact_timeclock", spark.table(FACT_TIMECLOCK_TABLE).count()),
        ("gold_dq_warnings", spark.table(GOLD_DQ_WARNINGS_TABLE).count()),
    ],
    ["gold_table", "row_count"],
)
display(gold_summary_df)

gold_warning_summary_df = (
    spark.table(GOLD_DQ_WARNINGS_TABLE)
    .groupBy("table_name", "warning_type", "severity")
    .agg(F.count("*").alias("warning_count"))
    .orderBy(F.col("table_name").asc(), F.col("warning_count").desc())
)
display(gold_warning_summary_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table(FACT_PURCHASE_TABLE).limit(20))
display(spark.table(FACT_VISIT_TABLE).limit(20))
display(spark.table(FACT_TIMECLOCK_TABLE).limit(20))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

purchase_count = spark.table(FACT_PURCHASE_TABLE).count()
visit_count = spark.table(FACT_VISIT_TABLE).count()
timeclock_count = spark.table(FACT_TIMECLOCK_TABLE).count()
warning_count = spark.table(GOLD_DQ_WARNINGS_TABLE).count()

print(
    f"Gold model load completed successfully. "
    f"Purchases: {purchase_count}, Visits: {visit_count}, Timeclock: {timeclock_count}, "
    f"DQ warnings: {warning_count}."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
