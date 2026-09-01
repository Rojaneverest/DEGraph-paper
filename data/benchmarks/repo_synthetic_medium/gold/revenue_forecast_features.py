# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — revenue_forecast_features
# MAGIC
# MAGIC The feature table consumed by the downstream revenue-forecast model.
# MAGIC Joins three existing gold tables — `customer_ltv`, `product_performance`,
# MAGIC and `inventory_turnover_daily` — into a single wide row per
# MAGIC `(customer_id, product_id, forecast_date)` grain. Includes a dynamic
# MAGIC feature list (loaded from `$FORECAST_CONFIG_PATH`) so the data-science
# MAGIC team can add or remove features without redeploying this pipeline.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC This file is the deepest multi-hop file in the entire benchmark — every
# MAGIC ground-truth column in this table traces back through 4 or more edges
# MAGIC to its bronze origin. It exercises two of the trickiest schema features
# MAGIC the extractor supports:
# MAGIC
# MAGIC * **Dynamic aggregation list (`dynamic=True`)** — `feature_columns` is
# MAGIC   read from a JSON file whose path comes from `os.environ.get(...)`,
# MAGIC   neither of which the SafeEvaluator can resolve to a literal. The loop
# MAGIC   over `feature_columns` builds the `aggregations` list at runtime;
# MAGIC   `.agg(*aggregations, ...)` star-unpacks an opaque list. The
# MAGIC   extractor emits one `Aggregates` edge with `dynamic=True`,
# MAGIC   `agg_ops=[<unresolved>]`, `output_cols=[<unresolved>]`, and a
# MAGIC   `dynamic_note`. The static parts (lifetime_revenue, total_orders,
# MAGIC   avg_turnover) are preserved in the same edge.
# MAGIC * **Many-hop reverse-lineage** — `lifetime_revenue` traces back 5 hops
# MAGIC   to `bronze.orders_raw.total_amount`. The graph contains every edge;
# MAGIC   a downstream LLM should be able to reconstruct the chain.
# MAGIC * **Three Reads edges** (customer_ltv, product_performance,
# MAGIC   inventory_turnover_daily) and **two Joins** (on `customer_id` then
# MAGIC   on `(product_id, forecast_date)`).

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
import json
import os

from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the three upstream gold tables

# COMMAND ----------
ltv = spark.table(f"{database}.customer_ltv")
prod_perf = spark.table(f"{database}.product_performance")
turnover  = spark.table(f"{database}.inventory_turnover_daily")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Load the dynamic feature list from out-of-repo JSON
# MAGIC
# MAGIC Same pattern as `product_performance.py`: a runtime list of columns to
# MAGIC roll up, sourced from a JSON config file whose path comes from an env
# MAGIC var. SafeEvaluator can resolve neither path nor file contents, so the
# MAGIC downstream `aggregations` list is opaque to DEGraph.

# COMMAND ----------
config_path = os.environ.get("FORECAST_CONFIG_PATH", "/dbfs/configs/forecast.json")
with open(config_path) as f:
    feature_columns = json.load(f)["revenue_forecast"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Join the three gold sources
# MAGIC
# MAGIC The grain of the feature table is one row per
# MAGIC `(customer_id, product_id, forecast_date)` — `forecast_date` is taken
# MAGIC from the turnover table's `snapshot_date`. The LTV table is per
# MAGIC customer; the product-performance and turnover tables are per product
# MAGIC per day.

# COMMAND ----------
ltv_x_perf = ltv.join(
    prod_perf,
    F.lit(True),  # cross-join: every customer pairs with every product-day grain
    how="inner",
)

joined = ltv_x_perf.join(
    turnover,
    on=["product_id"],
    how="left",
).withColumnRenamed("snapshot_date", "forecast_date")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Build the dynamic aggregation list
# MAGIC
# MAGIC One avg + one max + one stddev per configured feature. List length and
# MAGIC column names are runtime-determined.

# COMMAND ----------
aggregations = []
for col_name in feature_columns:
    aggregations.append(F.avg(col_name).alias(f"avg_{col_name}"))
    aggregations.append(F.max(col_name).alias(f"max_{col_name}"))
    aggregations.append(F.stddev(col_name).alias(f"stddev_{col_name}"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Aggregate to the feature-table grain
# MAGIC
# MAGIC The static aggregates — `lifetime_revenue`, `total_orders`,
# MAGIC `avg_turnover` — are statically enumerable. The `*aggregations`
# MAGIC star-unpack is not.

# COMMAND ----------
features = (
    joined
    .groupBy("customer_id", "product_id", "forecast_date")
    .agg(
        *aggregations,
        F.first("lifetime_revenue").alias("lifetime_revenue"),
        F.first("total_orders").alias("total_orders"),
        F.avg("turnover_rate").alias("avg_turnover"),
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Write to gold

# COMMAND ----------
(features.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.revenue_forecast_features"))
