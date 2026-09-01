# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — customer_ltv
# MAGIC
# MAGIC Builds a customer-lifetime-value table from `silver.ecom.customer_profile`.
# MAGIC The core computation runs as a single `spark.sql(...)` block with CTEs:
# MAGIC one CTE that lifts the raw silver columns, one that computes a
# MAGIC per-country average for the lifetime-revenue-vs-country comparison,
# MAGIC and a final SELECT that joins them and assigns a revenue quartile via
# MAGIC a window function. After the SQL block, a Python-side `withColumn`
# MAGIC assigns a categorical tier label from the quartile.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC The `spark.sql(...)` block is parsed by `sqlglot` and its lineage is
# MAGIC integrated into the file subgraph:
# MAGIC
# MAGIC * The `FROM main.dbdemos_ecom.customer_profile` in the `base` CTE
# MAGIC   produces one `Reads` edge to the silver Table node.
# MAGIC * Each CTE becomes a file-local `DataFrame` node (`base`, `country_avg`,
# MAGIC   `ranked`).
# MAGIC * The `GROUP BY country_code` in `country_avg` becomes one `Aggregates`
# MAGIC   edge with `group_keys=["country_code"]`.
# MAGIC * The `LEFT JOIN country_avg ca USING (country_code)` in `ranked`
# MAGIC   becomes one `Joins` edge.
# MAGIC * The `NTILE(4) OVER (ORDER BY lifetime_revenue DESC)` becomes one
# MAGIC   `Derives` edge with `window_spec` populated.
# MAGIC * After the SQL block, the Python `withColumn("ltv_tier", ...)` is one
# MAGIC   additional `Derives` edge from the SQL-output DataFrame to the
# MAGIC   final tiered DataFrame.
# MAGIC * `saveAsTable` is the final `Writes` edge to
# MAGIC   `main.dbdemos_ecom.customer_ltv`.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Compute base LTV via SQL with CTEs

# COMMAND ----------
ltv_df = spark.sql(f"""
    WITH base AS (
        SELECT customer_id,
               email,
               country_code,
               signup_ts,
               total_orders,
               lifetime_revenue,
               last_order_ts,
               last_event_ts
        FROM {database}.customer_profile
    ),
    country_avg AS (
        SELECT country_code,
               AVG(lifetime_revenue) AS avg_country_ltv
        FROM base
        GROUP BY country_code
    ),
    ranked AS (
        SELECT b.customer_id,
               b.email,
               b.country_code,
               b.signup_ts,
               b.total_orders,
               b.lifetime_revenue,
               b.last_order_ts,
               b.last_event_ts,
               DATEDIFF(CURRENT_DATE(), b.last_order_ts) AS days_since_last_order,
               b.lifetime_revenue / NULLIF(ca.avg_country_ltv, 0) AS ltv_vs_country_avg,
               NTILE(4) OVER (ORDER BY b.lifetime_revenue DESC) AS revenue_quartile
        FROM base b
        LEFT JOIN country_avg ca USING (country_code)
    )
    SELECT * FROM ranked
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Add a categorical LTV tier
# MAGIC
# MAGIC Python-side derivation: quartile 1 = platinum, 2 = gold, 3 = silver,
# MAGIC 4 = bronze. Keeps the SQL block focused on numeric computation.

# COMMAND ----------
ltv_tiered = ltv_df.withColumn(
    "ltv_tier",
    F.when(F.col("revenue_quartile") == 1, F.lit("platinum"))
     .when(F.col("revenue_quartile") == 2, F.lit("gold"))
     .when(F.col("revenue_quartile") == 3, F.lit("silver"))
     .otherwise(F.lit("bronze")),
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Write to gold

# COMMAND ----------
(ltv_tiered.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.customer_ltv"))
