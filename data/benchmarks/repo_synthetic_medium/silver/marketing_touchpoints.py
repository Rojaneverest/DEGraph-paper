# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — marketing_touchpoints
# MAGIC
# MAGIC Joins UTM-tracker attribution records (`marketing_attribution_raw`) with
# MAGIC behavioral events (`events_raw`) on `session_id` to tie each visit to
# MAGIC the events that occurred within it. Computes a first-touch channel per
# MAGIC session via a window function (earliest visit per session), then
# MAGIC aggregates per (customer_id, utm_campaign) to produce a silver-tier
# MAGIC marketing-engagement table.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * **Reads** (2) — `marketing_attribution_raw`, `events_raw`.
# MAGIC * **Joins** (1) — attribution ⨝ events ON session_id (inner).
# MAGIC * **Derives with window_spec** (1) — `row_number().over(...)` partitioned
# MAGIC   by `session_id`, ordered by `visit_ts` ascending; assigns rank 1 to
# MAGIC   the first-touch visit per session.
# MAGIC * **Filters** (1) — `rn == 1` to keep only the first-touch row per
# MAGIC   session before aggregation. (The non-first-touch rows still inform
# MAGIC   the event count via the join.)
# MAGIC * **Aggregates** (1) — `groupBy(customer_id, utm_campaign).agg(...)`
# MAGIC   with `count(event_id)`, `max(visit_ts)`, `countDistinct(session_id)`.
# MAGIC * **Writes** (1) — `saveAsTable(f"{database}.marketing_touchpoints")`,
# MAGIC   `mode=overwrite`.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the two bronze sources

# COMMAND ----------
attribution_df = spark.table(f"{database}.marketing_attribution_raw")
events_df = spark.table(f"{database}.events_raw")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Join attribution to behavioral events on session_id
# MAGIC
# MAGIC The session is the natural join key: a UTM visit creates a session,
# MAGIC and behavioral events emitted during that session inherit its
# MAGIC attribution. Inner join — visits with no behavioral events are not
# MAGIC interesting for marketing analytics and are dropped here.

# COMMAND ----------
joined = attribution_df.join(events_df, on="session_id", how="inner")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Compute first-touch via window function
# MAGIC
# MAGIC For each session, rank visits by ascending `visit_ts` and keep rank 1.
# MAGIC This is the channel that originally brought the session into the funnel
# MAGIC — multi-channel attribution is out of scope at this layer.

# COMMAND ----------
first_touch_window = Window.partitionBy("session_id").orderBy(F.col("visit_ts").asc())
joined_ranked = joined.withColumn("rn", F.row_number().over(first_touch_window))
first_touch = joined_ranked.filter(F.col("rn") == 1).drop("rn")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Aggregate per (customer_id, utm_campaign)
# MAGIC
# MAGIC One row per customer-and-campaign pair. The aggregates capture engagement
# MAGIC volume (`event_count`), recency (`last_seen_ts`), and breadth (number of
# MAGIC distinct sessions the customer had under this campaign).

# COMMAND ----------
touchpoints = (
    first_touch
    .groupBy("customer_id", "utm_campaign")
    .agg(
        F.count("event_id").alias("event_count"),
        F.max("visit_ts").alias("last_seen_ts"),
        F.countDistinct("session_id").alias("session_count"),
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Write to silver

# COMMAND ----------
(touchpoints.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.marketing_touchpoints"))
