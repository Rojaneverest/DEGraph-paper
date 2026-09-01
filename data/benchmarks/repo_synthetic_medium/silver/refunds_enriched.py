# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — refunds_enriched
# MAGIC
# MAGIC Builds a per-refund enriched table by joining `refunds_raw` to its
# MAGIC originating return (via `return_id`) and then to the original order
# MAGIC (via `order_id`). The resulting silver row captures the entire reverse
# MAGIC -logistics trail for a single refund — refund amount and method, the
# MAGIC reason on the underlying RMA, and the original order's economic context
# MAGIC (currency, total amount, customer). Derives a `refund_ratio` =
# MAGIC refund_amount / total_amount and a `days_to_refund` time-delta column.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * **Reads** (3) — `refunds_raw`, `returns_raw`, `orders_raw`.
# MAGIC * **Joins** (2) — refunds ⨝ returns ON return_id (inner), then
# MAGIC   the resulting ⨝ orders ON order_id (left).
# MAGIC * **Derives** (3) — `refund_ratio`, `days_to_refund`, `refund_date`.
# MAGIC * **Writes** (1) — `saveAsTable(f"{database}.refunds_enriched")` with
# MAGIC   `mode=overwrite`.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the three bronze sources
# MAGIC
# MAGIC Refunds settle returns settle orders — three-table join chain.

# COMMAND ----------
refunds_df = spark.table(f"{database}.refunds_raw")
returns_df = spark.table(f"{database}.returns_raw")
orders_df  = spark.table(f"{database}.orders_raw")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Join refunds → returns → orders
# MAGIC
# MAGIC The refund→return join is inner: a refund without an RMA is data
# MAGIC corruption and should not propagate. The (refund+return)→order join is
# MAGIC LEFT so that orphaned-order refunds (which occasionally appear because
# MAGIC of upstream archival of very old orders) still appear in silver with
# MAGIC null order context.

# COMMAND ----------
refunds_with_return = refunds_df.join(returns_df, on="return_id", how="inner")

enriched = refunds_with_return.join(orders_df, on="order_id", how="left")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Derive economic and timing metrics
# MAGIC
# MAGIC `refund_ratio` is the fraction of the original order that was refunded
# MAGIC — denominated in the original order currency. `NULLIF(total_amount, 0)`
# MAGIC avoids divide-by-zero on the (rare) free-promo orders.

# COMMAND ----------
enriched = (
    enriched
        .withColumn(
            "refund_ratio",
            F.col("refund_amount") / F.when(F.col("total_amount") == 0, F.lit(None)).otherwise(F.col("total_amount")),
        )
        .withColumn("days_to_refund", F.datediff(F.col("processed_ts"), F.col("return_ts")))
        .withColumn("refund_date", F.to_date(F.col("processed_ts")))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Write to silver

# COMMAND ----------
(enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.refunds_enriched"))
