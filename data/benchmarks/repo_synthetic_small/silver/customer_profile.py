# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — customer_profile
# MAGIC
# MAGIC Builds a per-customer profile by joining cleaned customer records with
# MAGIC their order history and the latest behavioral event per customer.
# MAGIC The latest-event-per-customer pattern uses a window function with
# MAGIC `row_number()`; the suffixed events DataFrame uses the registered
# MAGIC `rename_columns_with_suffix` helper to keep the joined columns distinct.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC This file is the densest in the benchmark for edge-type coverage. Per
# MAGIC the inventory's coverage matrix:
# MAGIC
# MAGIC * **Reads** (3) — `orders_raw`, `customers_raw`, `events_raw`
# MAGIC * **Derives with `window_spec`** (1) — `row_number().over(W.partitionBy("customer_id").orderBy("event_ts" desc))`.
# MAGIC   Captures the partition keys and order keys so an impact-analysis
# MAGIC   query on `event_ts` flags this window as at-risk.
# MAGIC * **Filters** (1) — `rn == 1` to keep only the latest event per customer
# MAGIC * **Projects** (1) — drop the helper column `rn`
# MAGIC * **Derives (registered suffix_rename)** (many) — `rename_columns_with_suffix(events_latest, "_last_event")`
# MAGIC   is registered as `"kind": "suffix_rename"`, so the extractor emits one
# MAGIC   explicit Derives edge per renamed column, mapping
# MAGIC   `col_name → col_name + "_last_event"`.
# MAGIC * **Joins** (2) — customers ⨝ orders, then result ⨝ events_suffixed
# MAGIC * **Aggregates** (1) — final `.groupBy(...).agg(...)`
# MAGIC * **Writes** (1) — to `silver.ecom.customer_profile`

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.column_transformations import rename_columns_with_suffix

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the three bronze sources

# COMMAND ----------
orders_df = spark.table(f"{database}.orders_raw")
customers_df = spark.table(f"{database}.customers_raw")
events_df = spark.table(f"{database}.events_raw")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Window-deduplicate events: keep only the latest event per customer
# MAGIC
# MAGIC Order events by descending `event_ts` within each customer partition,
# MAGIC assign a row number, keep row 1, drop the helper column.

# COMMAND ----------
events_window = Window.partitionBy("customer_id").orderBy(F.col("event_ts").desc())
events_ranked = events_df.withColumn("rn", F.row_number().over(events_window))
events_latest = events_ranked.filter(F.col("rn") == 1).drop("rn")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Suffix-rename the events columns
# MAGIC
# MAGIC Every column from `events_latest` becomes `<name>_last_event` so the
# MAGIC three-way join can keep them distinct from the orders / customers
# MAGIC columns. The join key on the events side becomes
# MAGIC `customer_id_last_event`.

# COMMAND ----------
events_suffixed = rename_columns_with_suffix(events_latest, "_last_event")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Three-way join: customers ⨝ orders ⨝ events
# MAGIC
# MAGIC Two `Join` edges, executed left-to-right.

# COMMAND ----------
co = customers_df.join(orders_df, on="customer_id", how="inner")

profile = co.join(
    events_suffixed,
    co["customer_id"] == events_suffixed["customer_id_last_event"],
    how="left",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Aggregate per customer
# MAGIC
# MAGIC One row per (customer_id, email, country_code, signup_ts). Aggregates
# MAGIC summarize the customer's order history and surface the last-event
# MAGIC timestamp for use by gold-tier recency metrics.

# COMMAND ----------
customer_profile = (
    profile
    .groupBy("customer_id", "email", "country_code", "signup_ts")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("lifetime_revenue"),
        F.max("order_ts").alias("last_order_ts"),
        F.max("event_ts_last_event").alias("last_event_ts"),
        F.countDistinct("product_id").alias("distinct_products_ordered"),
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Write to silver

# COMMAND ----------
(customer_profile.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.customer_profile"))
