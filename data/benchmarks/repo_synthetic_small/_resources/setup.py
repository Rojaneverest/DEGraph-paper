# Databricks notebook source
# MAGIC %md
# MAGIC # Setup notebook
# MAGIC
# MAGIC Invoked via `%run ./_resources/setup` (or `%run ../_resources/setup` from
# MAGIC subdirectories) at the top of every bronze/silver/gold notebook.
# MAGIC
# MAGIC Imports the repo-wide constants from `config.py`, then derives a handful
# MAGIC of additional convenience values (the fully-qualified database name, the
# MAGIC checkpoint root, the schema root) that downstream notebooks build their
# MAGIC table FQNs and Auto Loader checkpoints from.
# MAGIC
# MAGIC ### Why this matters for DEGraph
# MAGIC
# MAGIC Per `dev/methodology.md` Decision 3.6, when the extractor encounters a
# MAGIC `%run` cell it parses the target notebook in "symbol-export mode" and
# MAGIC imports the top-level variable assignments into the calling file's
# MAGIC SafeEvaluator symbol table. The assignments below — `database`,
# MAGIC `checkpoint_root`, `schema_root` — are exactly what gets exported.
# MAGIC Recursion is capped at depth 2, which is enough to follow this file's
# MAGIC own import from `config.py`.

# COMMAND ----------

from config import catalog, db, volume_path

# COMMAND ----------

database = f"{catalog}.{db}"
checkpoint_root = f"{volume_path}/_checkpoints"
schema_root = f"{volume_path}/_schemas"

# Auto Loader options reused across every bronze ingestion notebook. Centralized
# here so a single edit retargets every stream.
autoloader_common_options = {
    "cloudFiles.schemaEvolutionMode": "addNewColumns",
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.includeExistingFiles": "true",
}
