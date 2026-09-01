# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — returns
# MAGIC
# MAGIC Streams new return RMA events out of the landing volume into the bronze
# MAGIC `returns_raw` Delta table via Auto Loader. Returns flow through the same
# MAGIC `availableNow` batch-flavored streaming pattern as orders — the
# MAGIC reverse-logistics CDC feed lands ~hourly and the job picks up whatever
# MAGIC has accumulated since the last run.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * The `%run ../_resources/setup` cell pulls in `database`, `volume_path`,
# MAGIC   `checkpoint_root`, `schema_root`. These resolve the f-strings below to
# MAGIC   concrete paths and FQNs.
# MAGIC * The `cloudFiles` reader maps to a `Reads` edge whose source is an
# MAGIC   `ExternalSource` node at the resolved volume path.
# MAGIC * Two `withColumn` calls stamp `ingested_ts` and `source_file` (Derives
# MAGIC   edges).
# MAGIC * `df.writeStream.table(...)` maps to a streaming `Writes` edge to the
# MAGIC   `main.dbdemos_ecom.returns_raw` Table node.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the landing volume as a stream
# MAGIC
# MAGIC Auto Loader infers the JSON schema on first run and persists it under
# MAGIC `schema_root/returns`. Subsequent runs reuse the persisted schema and
# MAGIC pick up new files via the cloudFiles file notifier.

# COMMAND ----------
returns_landing = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_root}/returns")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{volume_path}/returns")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Add operational metadata
# MAGIC
# MAGIC `ingested_ts` is required by downstream silver for late-arriving-data
# MAGIC ordering; `source_file` traces each row back to its origin JSON.

# COMMAND ----------
returns_with_meta = (
    returns_landing
        .withColumn("ingested_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Write to bronze
# MAGIC
# MAGIC `availableNow` trigger flushes the backlog and exits. The checkpoint
# MAGIC under `checkpoint_root/returns` is what makes the job resumable across
# MAGIC scheduled invocations.

# COMMAND ----------
(returns_with_meta
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_root}/returns")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table(f"{database}.returns_raw"))
