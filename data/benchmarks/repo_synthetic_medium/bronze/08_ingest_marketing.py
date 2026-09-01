# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — marketing attribution
# MAGIC
# MAGIC Streams inbound UTM-tracker landing-visit records into the bronze
# MAGIC `marketing_attribution_raw` table via Auto Loader. The tracker emits one
# MAGIC JSON record per visit; volume is high enough (~5M/day) that a streaming
# MAGIC ingestion is preferred over batch.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * The `cloudFiles` reader maps to a `Reads` edge whose source is an
# MAGIC   `ExternalSource` node at `{volume_path}/marketing_attribution`.
# MAGIC * Two `withColumn` calls stamp operational metadata (Derives edges).
# MAGIC * `df.writeStream.table(...)` is a streaming `Writes` edge to
# MAGIC   `main.dbdemos_ecom.marketing_attribution_raw`.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the landing volume as a stream

# COMMAND ----------
attribution_landing = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_root}/marketing_attribution")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{volume_path}/marketing_attribution")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Add operational metadata
# MAGIC
# MAGIC Stamp ingest timestamp + source file for traceability.

# COMMAND ----------
attribution_with_meta = (
    attribution_landing
        .withColumn("ingested_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Write to bronze

# COMMAND ----------
(attribution_with_meta
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_root}/marketing_attribution")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table(f"{database}.marketing_attribution_raw"))
