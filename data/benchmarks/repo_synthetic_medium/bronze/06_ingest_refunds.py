# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — refunds
# MAGIC
# MAGIC Pulls refund-transaction records from the payment-processor's webhook
# MAGIC delivery dump (newline-delimited JSON files dropped into the landing
# MAGIC volume by an upstream Lambda). Uses a plain `spark.read.json` batch read
# MAGIC rather than Auto Loader because the upstream Lambda guarantees
# MAGIC at-most-once-per-day delivery in a small number of well-formed files —
# MAGIC no schema-drift handling needed.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * `spark.read.json(...)` resolves to a `Reads` edge whose source is an
# MAGIC   `ExternalSource` node at the resolved volume glob path.
# MAGIC * Two `withColumn` calls stamp operational metadata (Derives edges).
# MAGIC * `saveAsTable(...)` with `mode=append` is a `Writes` edge to
# MAGIC   `main.dbdemos_ecom.refunds_raw`. Append mode is correct because each
# MAGIC   webhook delivery is a strict superset of new refunds since the last run.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the day's webhook drop
# MAGIC
# MAGIC The glob `refunds/*.json` matches all files in the day's directory; the
# MAGIC orchestrator is responsible for pointing this notebook at the right day
# MAGIC partition via an upstream parameter (out of scope here).

# COMMAND ----------
refunds_landing = (
    spark.read
        .format("json")
        .option("multiline", "false")
        .load(f"{volume_path}/refunds/*.json")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Add operational metadata
# MAGIC
# MAGIC Same `ingested_ts` + `source_file` convention as the Auto Loader-based
# MAGIC notebooks so downstream silver code can treat all bronze tables
# MAGIC uniformly.

# COMMAND ----------
refunds_with_meta = (
    refunds_landing
        .withColumn("ingested_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Append to bronze
# MAGIC
# MAGIC `mode=append` is appropriate here — the upstream guarantees each batch
# MAGIC contains only refunds not previously delivered. There is no de-dup
# MAGIC step at this layer; downstream silver is responsible for refund-id
# MAGIC dedup if needed.

# COMMAND ----------
(refunds_with_meta.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{database}.refunds_raw"))
