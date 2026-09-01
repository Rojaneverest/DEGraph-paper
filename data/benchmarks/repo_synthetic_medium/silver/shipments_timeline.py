# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — shipments_timeline
# MAGIC
# MAGIC Builds a per-order shipment summary from the bronze `shipments_raw`
# MAGIC table. Many orders ship as multiple parcels (split shipments from
# MAGIC different warehouses, partial back-orders, etc.), so the silver layer
# MAGIC collapses the per-parcel detail to one row per order with shipment
# MAGIC count, first-ship and last-deliver timestamps, and the set of distinct
# MAGIC carriers used. The result is joined to `orders_raw` to enrich each row
# MAGIC with the original `customer_id` for downstream per-customer analyses.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * **Reads** (2) — `shipments_raw`, `orders_raw`.
# MAGIC * **Derives with window_spec** (1) — `row_number().over(...)`
# MAGIC   partitioned by `order_id`, ordered by `ship_ts` ascending; assigns
# MAGIC   sequence numbers to each parcel within an order (`shipment_seq`).
# MAGIC * **Aggregates** (1) — `groupBy(order_id).agg(...)` with
# MAGIC   `count(shipment_id)`, `min(ship_ts)`, `max(delivered_ts)`,
# MAGIC   `collect_set(carrier)`.
# MAGIC * **Joins** (1) — aggregated result ⨝ orders ON order_id (inner) to
# MAGIC   pull `customer_id` into the silver output.
# MAGIC * **Writes** (1) — `saveAsTable(f"{database}.shipments_timeline")`,
# MAGIC   `mode=overwrite`.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the two bronze sources

# COMMAND ----------
shipments_df = spark.table(f"{database}.shipments_raw")
orders_df    = spark.table(f"{database}.orders_raw")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Assign within-order shipment sequence via window
# MAGIC
# MAGIC `shipment_seq` is "this was the Nth parcel for this order in ship-time
# MAGIC order." Useful downstream for analyses that distinguish first shipment
# MAGIC from subsequent ones (e.g. backfill detection).

# COMMAND ----------
shipment_seq_window = Window.partitionBy("order_id").orderBy(F.col("ship_ts").asc())
shipments_seq = shipments_df.withColumn("shipment_seq", F.row_number().over(shipment_seq_window))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Aggregate to one row per order
# MAGIC
# MAGIC `collect_set(carrier)` produces the set of distinct carriers that handled
# MAGIC any parcel for this order — a small array, never large enough to be a
# MAGIC performance concern.

# COMMAND ----------
timeline = (
    shipments_seq
    .groupBy("order_id")
    .agg(
        F.count("shipment_id").alias("shipment_count"),
        F.min("ship_ts").alias("first_ship_ts"),
        F.max("delivered_ts").alias("last_delivered_ts"),
        F.collect_set("carrier").alias("carriers_used"),
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Join with orders to bring in customer_id
# MAGIC
# MAGIC Inner join — orders that have no shipment record yet (pending fulfillment)
# MAGIC are dropped at this layer. Adding `customer_id` to the silver row makes
# MAGIC the table directly joinable to customer-grain gold tables.

# COMMAND ----------
timeline_enriched = timeline.join(
    orders_df.select("order_id", "customer_id"),
    on="order_id",
    how="inner",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Write to silver

# COMMAND ----------
(timeline_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.shipments_timeline"))
