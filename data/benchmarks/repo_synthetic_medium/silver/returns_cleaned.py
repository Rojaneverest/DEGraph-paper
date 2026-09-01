# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — returns_cleaned
# MAGIC
# MAGIC Reads `bronze.ecom.returns_raw`, applies the standard string-trim helper,
# MAGIC normalizes the reason code and warehouse identifier to canonical case,
# MAGIC derives `return_date` for downstream daily aggregation, drops the
# MAGIC operational columns, filters out malformed rows (zero / negative return
# MAGIC quantity, which a CDC race condition occasionally produces), and writes
# MAGIC the result to `silver.ecom.returns_cleaned`.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC Mirrors the small-benchmark `orders_cleaned.py` structure intentionally —
# MAGIC same five-edge-type fingerprint on a different bronze source:
# MAGIC
# MAGIC 1. **Reads** (1) — `spark.table(f"{database}.returns_raw")`.
# MAGIC 2. **OpaqueTransform** (1) — `trim_string_columns(df)` is registered
# MAGIC    `kind=passthrough`, so the extractor emits an OpaqueTransform with
# MAGIC    `is_passthrough=true`. Column set preserved.
# MAGIC 3. **Derives** (4) — one per `withColumn` (reason normalization,
# MAGIC    warehouse_id canonicalization, return_date derived from return_ts,
# MAGIC    plus a boolean is_high_qty flag).
# MAGIC 4. **Projects** (1) — `.drop("_rescued_data", "source_file")`.
# MAGIC 5. **Filters** (1) — `.filter(F.col("return_qty") > 0)`.
# MAGIC 6. **Writes** (1) — `.saveAsTable(f"{database}.returns_cleaned")`.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F
from utils.column_transformations import trim_string_columns

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read bronze

# COMMAND ----------
returns = spark.table(f"{database}.returns_raw")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Trim string columns (registered passthrough helper)
# MAGIC
# MAGIC Same idiom as `orders_cleaned.py`. The trim helper is column-set
# MAGIC preserving, so the extractor emits a passthrough OpaqueTransform.

# COMMAND ----------
returns = trim_string_columns(returns)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Normalize values and derive a date column
# MAGIC
# MAGIC Four column-level transformations. `reason_code` and `warehouse_id` are
# MAGIC cleaned to canonical case; `return_date` exists for downstream daily
# MAGIC rollups; `is_high_qty` is a downstream filter aid.

# COMMAND ----------
returns = (
    returns
        .withColumn("reason_code", F.lower(F.col("reason_code")))
        .withColumn("warehouse_id", F.upper(F.col("warehouse_id")))
        .withColumn("return_date", F.to_date(F.col("return_ts")))
        .withColumn("is_high_qty", F.col("return_qty") > F.lit(5))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Drop operational columns

# COMMAND ----------
returns = returns.drop("_rescued_data", "source_file")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Filter out malformed rows
# MAGIC
# MAGIC A rare CDC race condition produces zero-qty returns; drop them here so
# MAGIC downstream gold aggregates do not see a phantom "0-unit return."

# COMMAND ----------
returns = returns.filter(F.col("return_qty") > 0)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Write to silver

# COMMAND ----------
(returns.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.returns_cleaned"))
