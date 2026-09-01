# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — products
# MAGIC
# MAGIC Refreshes the bronze `products_raw` table from the upstream PostgreSQL
# MAGIC catalog service via JDBC. Unlike the order / event ingestion notebooks,
# MAGIC the catalog is small enough (~50K SKUs) and changes slowly enough that a
# MAGIC full-table batch overwrite is preferred over streaming.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * The `spark.read.jdbc(...)` call maps to a `Reads` edge whose source is
# MAGIC   an `ExternalSource` node identified by the JDBC URL (the extractor does
# MAGIC   not resolve JDBC sources to Table nodes; they are external).
# MAGIC * Two `withColumn` calls stamp operational metadata — these are `Derives`
# MAGIC   edges.
# MAGIC * The `saveAsTable(...)` writes a `mode=overwrite` `Writes` edge to the
# MAGIC   bronze `products_raw` Table node.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the upstream product catalog over JDBC
# MAGIC
# MAGIC Connection coordinates come from environment secrets (mocked here as a
# MAGIC literal URL). Auto Loader is not used because the source is a queryable
# MAGIC RDBMS, not a file landing zone.

# COMMAND ----------
products_jdbc = (
    spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://catalog/products")
        .option("dbtable", "public.products")
        .option("user", "etl_reader")
        .option("password", "***redacted***")
        .load()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Add operational metadata
# MAGIC
# MAGIC `ingested_ts` lets downstream silver code reason about freshness;
# MAGIC `source_file` is set to the JDBC URL so the same field works whether the
# MAGIC origin was a file or a database.

# COMMAND ----------
products_with_meta = (
    products_jdbc
        .withColumn("ingested_ts", F.current_timestamp())
        .withColumn("source_file", F.lit("jdbc:postgresql://catalog/products"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Full-table overwrite into bronze
# MAGIC
# MAGIC The catalog is the system of record; a daily overwrite is acceptable.
# MAGIC `overwriteSchema=true` accommodates additive column changes from
# MAGIC upstream without manual ALTER TABLE.

# COMMAND ----------
(products_with_meta.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.products_raw"))
