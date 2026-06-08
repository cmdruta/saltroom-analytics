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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import types as T

GOLD_SCHEMA = "gold"
GOLD_DQ_WARNINGS_TABLE = "gold.gold_dq_warnings"

gold_dq_warnings_schema = T.StructType([
      T.StructField("dq_warning_id", T.LongType(), True),
      T.StructField("table_name", T.StringType(), True),
      T.StructField("source_table", T.StringType(), True),
      T.StructField("source_record_id", T.StringType(), True),
      T.StructField("warning_type", T.StringType(), True),
      T.StructField("warning_message", T.StringType(), True),
      T.StructField("severity", T.StringType(), True),
      T.StructField("created_at_utc", T.TimestampType(), True),
  ])

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

empty_df = spark.createDataFrame([], gold_dq_warnings_schema)

(
      empty_df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(GOLD_DQ_WARNINGS_TABLE)
  )

spark.table(GOLD_DQ_WARNINGS_TABLE).printSchema()
print(f"Created {GOLD_DQ_WARNINGS_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
