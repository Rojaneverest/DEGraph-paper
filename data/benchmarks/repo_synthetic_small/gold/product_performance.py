# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — product_performance
# MAGIC
# MAGIC Computes per-product, per-day aggregate metrics from
# MAGIC `silver.ecom.orders_cleaned`, then upserts them into
# MAGIC `gold.ecom.product_performance` using the registered `DeltaMergeSink`
# MAGIC class. The list of metrics computed is loaded from an out-of-repo JSON
# MAGIC config file so non-engineering operators can add or remove metrics
# MAGIC without redeploying the pipeline.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC This file exercises two of the trickiest schema features:
# MAGIC
# MAGIC * **Dynamic aggregation list (`dynamic=True`)** — `metric_columns` is
# MAGIC   read from a JSON file whose path comes from `os.environ.get(...)`,
# MAGIC   none of which the SafeEvaluator can resolve to a literal. The loop
# MAGIC   over `metric_columns` builds the `aggregations` list at runtime;
# MAGIC   `.agg(*aggregations, ...)` star-unpacks an opaque list into the
# MAGIC   aggregation call. Per `dev/methodology.md` Gap E handling, the
# MAGIC   extractor emits **one** `Aggregates` edge with `dynamic=True`,
# MAGIC   `agg_ops=[<unresolved>]`, `output_cols=[<unresolved>]`, and a
# MAGIC   `dynamic_note` describing why the list could not be enumerated.
# MAGIC   The static `F.count("order_id")` portion of the agg is preserved
# MAGIC   in the same edge's `agg_ops` alongside the unresolved marker.
# MAGIC
# MAGIC * **Custom sink class (`sink_class` populated)** — `DeltaMergeSink` is
# MAGIC   registered in `.degraph/sinks.json`. The extractor reads the registry,
# MAGIC   resolves the `target_table=f"{database}.product_performance"` kwarg
# MAGIC   against the `database` symbol imported via `%run`, and emits a
# MAGIC   `Writes` edge with `sink_class="DeltaMergeSink"`, `mode="merge"`,
# MAGIC   and the resolved table FQN as target.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
import json
import os

from pyspark.sql import functions as F

from utils.sinks import DeltaMergeSink

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the silver source

# COMMAND ----------
orders = spark.table(f"{database}.orders_cleaned")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Load metric configuration from out-of-repo JSON
# MAGIC
# MAGIC The list of numeric columns to roll up per product per day lives in a
# MAGIC config file outside the repo. The path is read from an env var; the
# MAGIC file contents are runtime JSON. SafeEvaluator can resolve neither, so
# MAGIC the downstream `aggregations` list is opaque to DEGraph.

# COMMAND ----------
config_path = os.environ.get("METRIC_CONFIG_PATH", "/dbfs/configs/metrics.json")
with open(config_path) as f:
    metric_columns = json.load(f)["product_performance"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Build the aggregation list dynamically
# MAGIC
# MAGIC One `sum`, one `avg`, one `max` per metric. List length and column
# MAGIC names are runtime-determined.

# COMMAND ----------
aggregations = []
for col_name in metric_columns:
    aggregations.append(F.sum(col_name).alias(f"sum_{col_name}"))
    aggregations.append(F.avg(col_name).alias(f"avg_{col_name}"))
    aggregations.append(F.max(col_name).alias(f"max_{col_name}"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Group and aggregate
# MAGIC
# MAGIC The static `F.count("order_id")` is enumerable; the `*aggregations`
# MAGIC star-unpack is not. The extractor records both in one Aggregates edge,
# MAGIC the latter as `<unresolved>` entries with `dynamic=True`.

# COMMAND ----------
daily_perf = (
    orders
    .groupBy("product_id", "order_date")
    .agg(*aggregations, F.count("order_id").alias("order_count"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Upsert into gold via the registered DeltaMergeSink
# MAGIC
# MAGIC Standard SCD-Type-1 merge on (product_id, order_date). The sink class
# MAGIC is registered, so DEGraph emits a proper Writes edge with sink_class
# MAGIC populated and mode=merge — no `<unresolved>` markers, no warnings.

# COMMAND ----------
sink = DeltaMergeSink(
    target_table=f"{database}.product_performance",
    merge_keys=["product_id", "order_date"],
    mode="merge",
)
sink.run(daily_perf)
