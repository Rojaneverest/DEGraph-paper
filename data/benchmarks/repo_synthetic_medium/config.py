# Databricks notebook source
# MAGIC %md
# MAGIC # Repo configuration
# MAGIC
# MAGIC Catalog, database, and volume-path constants shared by every notebook in
# MAGIC this repo. Imported transitively via `%run ./_resources/setup` from each
# MAGIC bronze/silver/gold notebook — the setup notebook in turn imports from
# MAGIC this module.
# MAGIC
# MAGIC Centralizing these constants means the entire pipeline can be retargeted
# MAGIC at a different catalog or volume by editing a single file. Downstream
# MAGIC code references the values via f-strings:
# MAGIC
# MAGIC     spark.readStream.format("cloudFiles").load(f"{volume_path}/orders")
# MAGIC     df.writeStream.table(f"{database}.orders_raw")
# MAGIC
# MAGIC The DEGraph extractor's SafeEvaluator must resolve these f-strings back
# MAGIC to concrete table FQNs — see `dev/methodology.md` Decision 3.6 for the
# MAGIC %run symbol-import mechanism that makes this resolution possible.

# COMMAND ----------

catalog = "main"
db = "dbdemos_ecom"
volume_path = f"/Volumes/{catalog}/{db}/landing"
