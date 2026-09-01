# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — attribution_funnel_daily
# MAGIC
# MAGIC Builds the daily marketing-attribution funnel: visits → add-to-cart →
# MAGIC checkout → purchase, broken out per UTM campaign per day. Reads
# MAGIC `marketing_touchpoints` (for visit volume and campaign labels) and
# MAGIC `orders_cleaned` (for the purchase tier of the funnel). Computes a
# MAGIC final `conversion_rate` = purchases / visits with NULLIF safety.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC Single `spark.sql(...)` block with several CTEs — each layer of the
# MAGIC funnel is one CTE that counts distinct sessions / orders per
# MAGIC `(utm_campaign, funnel_date)`. The extractor should produce:
# MAGIC
# MAGIC * **Reads** (3) — `marketing_touchpoints`, `events_raw` (for the
# MAGIC   add-to-cart / checkout event-type counts), `orders_cleaned`.
# MAGIC * **Aggregates** (≥3) — one per funnel-stage CTE.
# MAGIC * **Joins** (≥2) — funnel-stage CTEs joined on
# MAGIC   `(utm_campaign, funnel_date)`.
# MAGIC * **Derives** (1) — `conversion_rate`.
# MAGIC * **Writes** (1) — `mode=overwrite`, `partitionBy=[funnel_date]`.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Compute the funnel via SQL with CTEs
# MAGIC
# MAGIC Each CTE counts the distinct sessions at one funnel stage. The visit
# MAGIC stage comes from `marketing_touchpoints`; the cart and checkout stages
# MAGIC come from `events_raw` filtered by `event_type`; the purchase stage
# MAGIC comes from `orders_cleaned`. All four stages are joined on
# MAGIC `(utm_campaign, funnel_date)` to produce one row per stage-set per day.

# COMMAND ----------
funnel_df = spark.sql(f"""
    WITH visits AS (
        SELECT utm_campaign,
               DATE(last_seen_ts)        AS funnel_date,
               COUNT(DISTINCT session_count) AS visit_count
        FROM {database}.marketing_touchpoints
        GROUP BY utm_campaign, DATE(last_seen_ts)
    ),
    cart_events AS (
        SELECT mt.utm_campaign,
               DATE(ev.event_ts) AS funnel_date,
               COUNT(DISTINCT ev.session_id) AS cart_count
        FROM {database}.events_raw ev
        JOIN {database}.marketing_touchpoints mt USING (customer_id)
        WHERE ev.event_type = 'add_to_cart'
        GROUP BY mt.utm_campaign, DATE(ev.event_ts)
    ),
    checkout_events AS (
        SELECT mt.utm_campaign,
               DATE(ev.event_ts) AS funnel_date,
               COUNT(DISTINCT ev.session_id) AS checkout_count
        FROM {database}.events_raw ev
        JOIN {database}.marketing_touchpoints mt USING (customer_id)
        WHERE ev.event_type = 'checkout'
        GROUP BY mt.utm_campaign, DATE(ev.event_ts)
    ),
    purchases AS (
        SELECT mt.utm_campaign,
               oc.order_date    AS funnel_date,
               COUNT(DISTINCT oc.order_id) AS purchase_count
        FROM {database}.orders_cleaned oc
        JOIN {database}.marketing_touchpoints mt USING (customer_id)
        GROUP BY mt.utm_campaign, oc.order_date
    )
    SELECT v.utm_campaign,
           v.funnel_date,
           v.visit_count,
           COALESCE(c.cart_count, 0)      AS cart_count,
           COALESCE(ck.checkout_count, 0) AS checkout_count,
           COALESCE(p.purchase_count, 0)  AS purchase_count,
           COALESCE(p.purchase_count, 0) / NULLIF(v.visit_count, 0) AS conversion_rate
    FROM visits v
    LEFT JOIN cart_events     c  USING (utm_campaign, funnel_date)
    LEFT JOIN checkout_events ck USING (utm_campaign, funnel_date)
    LEFT JOIN purchases       p  USING (utm_campaign, funnel_date)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Write to gold, partitioned by funnel_date

# COMMAND ----------
(funnel_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("funnel_date")
    .saveAsTable(f"{database}.attribution_funnel_daily"))
