# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — inventory snapshots
# MAGIC
# MAGIC Loads daily per-(warehouse, product) inventory snapshots emitted by the
# MAGIC warehouse-management system as Parquet files partitioned by `dt=YYYY-MM-DD`.
# MAGIC Uses a partition-glob `spark.read.parquet` read; the WMS guarantees one
# MAGIC complete snapshot directory per day, so a batch read is sufficient.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * `spark.read.parquet(...)` is a `Reads` edge whose source is an
# MAGIC   `ExternalSource` node at the resolved partition glob.
# MAGIC * Two `withColumn` calls for operational metadata (Derives edges).
# MAGIC * `saveAsTable(...)` is a `Writes` edge with `mode=append` and a
# MAGIC   `partitionBy` clause — `partition_cols=["snapshot_ts"]` should appear
# MAGIC   in the compact graph's writes-edge payload (the audit added this in
# MAGIC   commit 56dec19).

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the day's WMS snapshot
# MAGIC
# MAGIC The glob `dt=*` matches every date partition; in production a date
# MAGIC parameter narrows this to the latest unprocessed partition. Snapshots
# MAGIC are columnar Parquet and the WMS schema is stable, so no schema
# MAGIC inference / evolution handling is needed.

# COMMAND ----------
inventory_landing = (
    spark.read
        .format("parquet")
        .load(f"{volume_path}/inventory_snapshots/dt=*")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Add operational metadata
# MAGIC
# MAGIC Same `ingested_ts` + `source_file` convention. `input_file_name()`
# MAGIC captures the originating Parquet file path including its `dt=` partition.

# COMMAND ----------
inventory_with_meta = (
    inventory_landing
        .withColumn("ingested_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Append to bronze, partitioned by snapshot timestamp
# MAGIC
# MAGIC `partitionBy("snapshot_ts")` keeps each day's snapshots in its own
# MAGIC partition for efficient downstream "latest snapshot per day" filtering
# MAGIC (used by `silver/inventory_daily.py`).

# COMMAND ----------
(inventory_with_meta.write
    .format("delta")
    .mode("append")
    .partitionBy("snapshot_ts")
    .option("mergeSchema", "true")
    .saveAsTable(f"{database}.inventory_snapshots_raw"))
