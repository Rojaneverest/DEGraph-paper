# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — inventory_daily
# MAGIC
# MAGIC Builds a per-(warehouse, product, day) inventory table from the bronze
# MAGIC `inventory_snapshots_raw` table. The WMS occasionally emits multiple
# MAGIC snapshots within a single day (e.g. an early-morning baseline plus a
# MAGIC midday re-count); we deduplicate to the latest snapshot per
# MAGIC (warehouse_id, product_id, snapshot_date) using a window function.
# MAGIC Derives `utilization_pct` for downstream stockout monitoring and filters
# MAGIC out empty stock rows (`on_hand_units > 0`) since those carry no
# MAGIC analytical signal for the turnover gold table.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * **Reads** (1) — `inventory_snapshots_raw`.
# MAGIC * **Derives with window_spec** (1) — `row_number().over(...)` partitioned
# MAGIC   by `(warehouse_id, product_id, snapshot_date)`, ordered by
# MAGIC   `snapshot_ts` descending.
# MAGIC * **Filters** (2) — `rn == 1` (the dedup filter) and
# MAGIC   `on_hand_units > 0` (the empty-stock filter).
# MAGIC * **Projects** (1) — drop the helper `rn` column.
# MAGIC * **Derives** (2) — `snapshot_date` (date-cast of snapshot_ts) and
# MAGIC   `utilization_pct` (reserved_units / on_hand_units).
# MAGIC * **Writes** (1) — `saveAsTable(f"{database}.inventory_daily")`,
# MAGIC   `mode=overwrite`.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the bronze snapshots

# COMMAND ----------
snapshots = spark.table(f"{database}.inventory_snapshots_raw")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Derive snapshot_date for partitioning the dedup window
# MAGIC
# MAGIC The WMS emits `snapshot_ts` at sub-day precision; the analytical grain
# MAGIC is one row per (warehouse, product, day), so we cast to a date first.

# COMMAND ----------
snapshots = snapshots.withColumn("snapshot_date", F.to_date(F.col("snapshot_ts")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Window-deduplicate to latest snapshot per (warehouse, product, day)
# MAGIC
# MAGIC When the WMS emits multiple snapshots in one day, keep the most recent
# MAGIC one (highest `snapshot_ts`).

# COMMAND ----------
dedup_window = (
    Window
    .partitionBy("warehouse_id", "product_id", "snapshot_date")
    .orderBy(F.col("snapshot_ts").desc())
)

snapshots_ranked = snapshots.withColumn("rn", F.row_number().over(dedup_window))
snapshots_latest = snapshots_ranked.filter(F.col("rn") == 1).drop("rn")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Compute utilization and filter empty-stock rows
# MAGIC
# MAGIC `utilization_pct` is the fraction of on-hand stock currently reserved
# MAGIC for open orders; values close to 1 indicate near-stockout. Rows with
# MAGIC zero on-hand stock carry no analytical signal and are dropped here.

# COMMAND ----------
inventory_daily = (
    snapshots_latest
        .withColumn(
            "utilization_pct",
            F.col("reserved_units") / F.when(F.col("on_hand_units") == 0, F.lit(None)).otherwise(F.col("on_hand_units")),
        )
        .filter(F.col("on_hand_units") > 0)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Write to silver

# COMMAND ----------
(inventory_daily.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.inventory_daily"))
