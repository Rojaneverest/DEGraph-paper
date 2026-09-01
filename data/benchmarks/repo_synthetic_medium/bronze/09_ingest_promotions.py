# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — promotions
# MAGIC
# MAGIC Refreshes the small `promotions_raw` table from a daily JSON dump that
# MAGIC the marketing CMS exports to the landing volume. Promo data is
# MAGIC slow-changing (~hundreds of active codes at any time) so a full-table
# MAGIC overwrite from a single canonical JSON file is the simplest pattern.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * `spark.read.json(...)` resolves to a `Reads` edge whose source is an
# MAGIC   `ExternalSource` node at `{volume_path}/promotions/latest.json`.
# MAGIC * Two `withColumn` calls add operational metadata (Derives edges).
# MAGIC * `saveAsTable(...)` with `mode=overwrite` writes the bronze table.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the canonical promotions JSON
# MAGIC
# MAGIC The CMS exports the entire active-promo catalog once per day. We treat
# MAGIC the latest file as the authoritative state and overwrite the bronze
# MAGIC table from it.

# COMMAND ----------
promotions_landing = (
    spark.read
        .format("json")
        .option("multiline", "true")
        .load(f"{volume_path}/promotions/latest.json")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Add operational metadata

# COMMAND ----------
promotions_with_meta = (
    promotions_landing
        .withColumn("ingested_ts", F.current_timestamp())
        .withColumn("source_file", F.lit(f"{volume_path}/promotions/latest.json"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Overwrite bronze
# MAGIC
# MAGIC The CMS export is the system of record for active promos; an overwrite
# MAGIC is the correct semantics. Schema-overwrite tolerates additive column
# MAGIC changes from upstream.

# COMMAND ----------
(promotions_with_meta.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.promotions_raw"))
