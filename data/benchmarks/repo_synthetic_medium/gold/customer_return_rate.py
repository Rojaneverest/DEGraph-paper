# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — customer_return_rate
# MAGIC
# MAGIC Computes per-customer return-rate metrics by joining `customer_profile`
# MAGIC (which already aggregates per-customer order counts and revenue) with
# MAGIC a returns aggregate built from `returns_cleaned`. Produces one row per
# MAGIC customer with `total_returns`, `total_return_qty`, and `return_rate`
# MAGIC (= total_returns / total_orders, NULLIF-safe).
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC Single `spark.sql(...)` block with two CTEs:
# MAGIC
# MAGIC * The `FROM main.dbdemos_ecom.returns_cleaned` in the `returns_agg` CTE
# MAGIC   produces one `Reads` edge to that silver Table node.
# MAGIC * The `FROM main.dbdemos_ecom.customer_profile` in the `customer_base`
# MAGIC   CTE produces one `Reads` edge.
# MAGIC * `returns_agg` becomes a file-local `DataFrame` node containing the
# MAGIC   `groupBy customer_id` → `count(return_id), sum(return_qty)`
# MAGIC   aggregate; this is one `Aggregates` edge.
# MAGIC * The final SELECT joins the two CTEs on `customer_id` (one `Joins`
# MAGIC   edge) and computes `return_rate` (a `Derives` edge).
# MAGIC * `saveAsTable` is the final `Writes` edge with `mode=overwrite`.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Compute customer return rate via SQL with CTEs

# COMMAND ----------
return_rate_df = spark.sql(f"""
    WITH returns_agg AS (
        SELECT customer_id,
               COUNT(return_id)   AS total_returns,
               SUM(return_qty)    AS total_return_qty
        FROM {database}.returns_cleaned
        GROUP BY customer_id
    ),
    customer_base AS (
        SELECT customer_id,
               email,
               country_code,
               total_orders,
               lifetime_revenue
        FROM {database}.customer_profile
    )
    SELECT cb.customer_id,
           cb.email,
           cb.country_code,
           cb.total_orders,
           cb.lifetime_revenue,
           COALESCE(ra.total_returns, 0)    AS total_returns,
           COALESCE(ra.total_return_qty, 0) AS total_return_qty,
           COALESCE(ra.total_returns, 0) / NULLIF(cb.total_orders, 0) AS return_rate
    FROM customer_base cb
    LEFT JOIN returns_agg ra USING (customer_id)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Write to gold

# COMMAND ----------
(return_rate_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.customer_return_rate"))
