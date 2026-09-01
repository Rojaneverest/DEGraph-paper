# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — clickstream events
# MAGIC
# MAGIC Streams behavioral / clickstream events out of the landing volume into
# MAGIC `events_raw`. Each event carries a JSON `payload` whose schema varies
# MAGIC with `event_type`, so this notebook delegates parsing to the imported
# MAGIC `parse_event_payload` helper from `utils.event_parsers`.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC The `parse_event_payload(df)` call is **deliberately not registered**
# MAGIC in `.degraph/helpers.json`. This makes it the benchmark's "unregistered
# MAGIC helper" case: the extractor will emit one `OpaqueTransform` edge with
# MAGIC `is_passthrough=false` to preserve the DataFrame's lineage chain
# MAGIC across the call, plus a `GraphWarning` naming the unregistered helper.
# MAGIC The Q&A pair on lineage gaps probes whether an LLM reading the graph
# MAGIC can surface this warning.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F
from utils.event_parsers import parse_event_payload

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Stream the raw event landing zone
# MAGIC
# MAGIC Events arrive as one JSON object per line, one file per kafka-connect
# MAGIC flush window.

# COMMAND ----------
events_landing = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_root}/events")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{volume_path}/events")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Parse the payload column
# MAGIC
# MAGIC `parse_event_payload` lifts `page_url`, `referrer`, `user_agent`, and
# MAGIC `cart_total` out of the JSON payload and drops the original. Body lives
# MAGIC in `utils/event_parsers.py`; the call site is what DEGraph sees.

# COMMAND ----------
events_parsed = parse_event_payload(events_landing)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Stamp ingest metadata and write to bronze

# COMMAND ----------
events_final = (
    events_parsed
        .withColumn("ingested_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------
(events_final
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_root}/events")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table(f"{database}.events_raw"))
