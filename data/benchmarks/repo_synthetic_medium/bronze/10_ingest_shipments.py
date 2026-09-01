# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — shipments
# MAGIC
# MAGIC Streams fulfillment / shipping events into the bronze `shipments_raw`
# MAGIC Delta table via Auto Loader. The carrier webhooks (UPS, FedEx, USPS,
# MAGIC DHL, local couriers) emit one JSON record per parcel state change; the
# MAGIC ingestion service writes them to the landing volume and this notebook
# MAGIC picks them up via the standard `availableNow` batch-flavored streaming
# MAGIC pattern.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * The `cloudFiles` reader maps to a `Reads` edge whose source is an
# MAGIC   `ExternalSource` node at `{volume_path}/shipments`.
# MAGIC * Two `withColumn` calls stamp operational metadata (Derives edges).
# MAGIC * `df.writeStream.table(...)` is a streaming `Writes` edge to
# MAGIC   `main.dbdemos_ecom.shipments_raw`.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the landing volume as a stream

# COMMAND ----------
shipments_landing = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_root}/shipments")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{volume_path}/shipments")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Add operational metadata

# COMMAND ----------
shipments_with_meta = (
    shipments_landing
        .withColumn("ingested_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Write to bronze

# COMMAND ----------
(shipments_with_meta
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_root}/shipments")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table(f"{database}.shipments_raw"))
