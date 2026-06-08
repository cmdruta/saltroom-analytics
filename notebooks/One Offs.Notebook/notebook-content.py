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

spark.table("gold.dim_date") \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.dim_client_registration_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("REFRESH TABLE gold.dim_client_registration_date")
spark.sql("SELECT COUNT(*) FROM gold.dim_client_registration_date").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
