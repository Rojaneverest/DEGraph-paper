# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — orders
# MAGIC
# MAGIC Streams new order events out of the landing volume into the bronze
# MAGIC `orders_raw` Delta table via Auto Loader. Trigger is `availableNow` so
# MAGIC the job processes whatever has accumulated since the last run and then
# MAGIC exits — typical batch-flavored streaming pattern for hourly schedules.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * The `%run ../_resources/setup` cell pulls in `database`, `volume_path`,
# MAGIC   `checkpoint_root`, `schema_root`, and `autoloader_common_options`.
# MAGIC   These resolve the f-strings below to concrete paths and FQNs.
# MAGIC * The `cloudFiles` reader maps to a `Reads` edge whose source is an
# MAGIC   `ExternalSource` node at the resolved volume path.
# MAGIC * `df.writeStream.table(...)` maps to a `Writes` edge to the
# MAGIC   `main.dbdemos_ecom.orders_raw` Table node.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the landing volume as a stream
# MAGIC
# MAGIC Auto Loader infers schema on first run and persists it under
# MAGIC `schema_root` so subsequent runs don't re-infer.

# COMMAND ----------
orders_landing = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_root}/orders")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{volume_path}/orders")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Add operational metadata
# MAGIC
# MAGIC Stamp every row with the ingest timestamp and the originating file
# MAGIC name. These columns are downstream invariants — silver transformations
# MAGIC rely on `ingested_ts` for late-arriving-data ordering.

# COMMAND ----------
orders_with_meta = (
    orders_landing
        .withColumn("ingested_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Write to bronze
# MAGIC
# MAGIC `availableNow` trigger flushes the backlog and exits. The checkpoint
# MAGIC under `checkpoint_root` is what makes the job resumable.

# COMMAND ----------
(orders_with_meta
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_root}/orders")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table(f"{database}.orders_raw"))
