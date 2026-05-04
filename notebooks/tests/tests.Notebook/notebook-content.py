# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

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

spark.sql("SHOW TABLES IN bronze").show(truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
