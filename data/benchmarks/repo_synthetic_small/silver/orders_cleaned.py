# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — orders_cleaned
# MAGIC
# MAGIC Reads `bronze.ecom.orders_raw`, applies type/value normalization to the
# MAGIC string and numeric columns, derives a few date-part columns for
# MAGIC downstream aggregation convenience, drops the operational columns that
# MAGIC downstream consumers do not need, filters out cancelled and zero-quantity
# MAGIC orders, and writes the result to `silver.ecom.orders_cleaned`.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC This file exercises five distinct edge types in one pipeline. Listed in
# MAGIC the order they appear at runtime:
# MAGIC
# MAGIC 1. **Reads** (1) — `spark.table(f"{database}.orders_raw")`.
# MAGIC 2. **OpaqueTransform** (1) — `trim_string_columns(df)` is registered in
# MAGIC    `.degraph/helpers.json` as `"kind": "passthrough"`, so the extractor
# MAGIC    emits an OpaqueTransform with `is_passthrough=true` instead of the
# MAGIC    default `false`. Column set is preserved.
# MAGIC 3. **Derives** (8) — one per `withColumn`. Includes both pure cleanups
# MAGIC    (`upper`, `lower`, `greatest`, `round`) and genuinely new derived
# MAGIC    columns (`order_date`, `order_year`, `order_month`, `is_high_value`).
# MAGIC 4. **Projects** (1) — `.drop("_rescued_data", "source_file")` removes
# MAGIC    columns without producing new ones; this is a Projects edge, not a
# MAGIC    Derives edge.
# MAGIC 5. **Filters** (1) — `.filter(...)` restricts rows; the `predicate_id`
# MAGIC    references an interned Expression node.
# MAGIC 6. **Writes** (1) — `.saveAsTable(f"{database}.orders_cleaned")` to the
# MAGIC    silver Table node.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F
from utils.column_transformations import trim_string_columns

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read bronze

# COMMAND ----------
orders = spark.table(f"{database}.orders_raw")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Trim string columns (registered passthrough helper)
# MAGIC
# MAGIC Replaces leading/trailing whitespace in every string-typed column.
# MAGIC Schema-preserving — the column set going in equals the column set
# MAGIC coming out, only values change. Registered, so DEGraph captures this
# MAGIC as a passthrough OpaqueTransform rather than dropping it.

# COMMAND ----------
orders = trim_string_columns(orders)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Normalize values and derive date parts
# MAGIC
# MAGIC Eight column-level transformations — five cleanups, three genuinely new
# MAGIC columns. Each `withColumn` becomes one `Derives` edge in the graph.

# COMMAND ----------
orders = (
    orders
        .withColumn("currency", F.upper(F.col("currency")))
        .withColumn("status", F.lower(F.col("status")))
        .withColumn("quantity", F.greatest(F.col("quantity"), F.lit(0)))
        .withColumn("total_amount", F.round(F.col("total_amount"), 2))
        .withColumn("order_date", F.to_date(F.col("order_ts")))
        .withColumn("order_year", F.year(F.col("order_ts")))
        .withColumn("order_month", F.month(F.col("order_ts")))
        .withColumn("is_high_value", F.col("total_amount") > F.lit(1000))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Drop operational columns
# MAGIC
# MAGIC `_rescued_data` and `source_file` were useful at the bronze tier for
# MAGIC debugging Auto Loader; they have no analytic value downstream.

# COMMAND ----------
orders = orders.drop("_rescued_data", "source_file")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Filter invalid orders
# MAGIC
# MAGIC Drop rows where the order was cancelled or the cleaned-up quantity is
# MAGIC zero (the `greatest(..., 0)` cleanup converts negatives, then this
# MAGIC filter drops the genuinely-zero ones).

# COMMAND ----------
orders = orders.filter((F.col("status") != "cancelled") & (F.col("quantity") > 0))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Write to silver

# COMMAND ----------
(orders.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.orders_cleaned"))
