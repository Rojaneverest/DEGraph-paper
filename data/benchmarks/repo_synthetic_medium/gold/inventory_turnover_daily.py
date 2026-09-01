# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — inventory_turnover_daily
# MAGIC
# MAGIC Joins `silver.inventory_daily` (per warehouse / product / day on-hand
# MAGIC stock) with `silver.orders_cleaned` (per order, by date) to compute
# MAGIC daily turnover metrics: how many units sold today, how that compares to
# MAGIC the rolling-average daily sell-rate, and how many days of stock remain.
# MAGIC Written via the registered `DeltaMergeSink` so the gold table is
# MAGIC incrementally upserted on (`product_id, warehouse_id, snapshot_date`).
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * The `spark.sql(...)` block produces:
# MAGIC   * **Reads** (2) — `inventory_daily`, `orders_cleaned`.
# MAGIC   * **Aggregates** (1) — `daily_sales` CTE aggregates orders by
# MAGIC     `(product_id, order_date)` with `sum(quantity) AS sold_qty`,
# MAGIC     `count(order_id) AS sales_count`.
# MAGIC   * **Joins** (1) — `inventory_daily` ⨝ `daily_sales` on
# MAGIC     `product_id` and the day key (inventory.snapshot_date =
# MAGIC     daily_sales.order_date), LEFT JOIN.
# MAGIC   * **Derives** (2) — `turnover_rate`, `days_of_stock` (both
# MAGIC     NULLIF-safe).
# MAGIC * **Writes** (1) — `DeltaMergeSink` custom sink with
# MAGIC   `mode=merge` and `merge_keys=[product_id, warehouse_id,
# MAGIC   snapshot_date]`.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from utils.sinks import DeltaMergeSink

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Compute daily turnover via SQL with CTEs
# MAGIC
# MAGIC `daily_sales` rolls up `orders_cleaned` to one row per (product, day).
# MAGIC The main SELECT joins it to the daily inventory snapshot and computes
# MAGIC turnover_rate (today's sales as a fraction of on-hand stock) and
# MAGIC days_of_stock (on-hand stock divided by today's sales — an estimate of
# MAGIC how many days the warehouse can sustain at today's rate).

# COMMAND ----------
turnover_df = spark.sql(f"""
    WITH daily_sales AS (
        SELECT product_id,
               order_date AS snapshot_date,
               SUM(quantity)    AS sold_qty,
               COUNT(order_id)  AS sales_count
        FROM {database}.orders_cleaned
        GROUP BY product_id, order_date
    ),
    inventory_base AS (
        SELECT warehouse_id,
               product_id,
               snapshot_date,
               on_hand_units,
               reserved_units,
               available_units,
               utilization_pct
        FROM {database}.inventory_daily
    )
    SELECT inv.warehouse_id,
           inv.product_id,
           inv.snapshot_date,
           inv.on_hand_units,
           inv.reserved_units,
           inv.available_units,
           inv.utilization_pct,
           COALESCE(ds.sold_qty, 0)     AS sold_qty,
           COALESCE(ds.sales_count, 0)  AS sales_count,
           COALESCE(ds.sold_qty, 0) / NULLIF(inv.on_hand_units, 0)   AS turnover_rate,
           inv.on_hand_units / NULLIF(ds.sold_qty, 0)                AS days_of_stock
    FROM inventory_base inv
    LEFT JOIN daily_sales ds USING (product_id, snapshot_date)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Upsert into gold via DeltaMergeSink
# MAGIC
# MAGIC The grain is one row per (product, warehouse, day). Re-running this
# MAGIC notebook for a day already processed must update those rows in place,
# MAGIC not duplicate them — hence the merge-keyed sink rather than overwrite.

# COMMAND ----------
sink = DeltaMergeSink(
    target_table=f"{database}.inventory_turnover_daily",
    merge_keys=["product_id", "warehouse_id", "snapshot_date"],
    mode="merge",
)
sink.run(turnover_df)
