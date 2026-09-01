# E6 — Claude Pro Manual Experiment Prompts
# repo_synthetic_small | 10 questions | RAW + GRAPH modes
#
# HOW TO USE THIS FILE:
#   CHAT A = RAW mode  → open a brand-new Claude chat, paste STEP A-1 first,
#                         then paste each question block (A-Q1 through A-Q10)
#                         one at a time. Copy each answer into the answer sheet.
#
#   CHAT B = GRAPH mode → open another brand-new Claude chat, paste STEP B-1 first,
#                         then paste each question block (B-Q1 through B-Q10).
#
# IMPORTANT: Start a completely fresh chat for each mode.
#            Do NOT use the same chat you used for the other mode.

================================================================================
CHAT A — RAW MODE
================================================================================

~~~ STEP A-1: Paste this FIRST in a new Claude chat (do this once) ~~~

You are an expert data engineer. You will be given context about a PySpark repository (either as source code or as a DEGraph lineage graph) and asked impact-analysis questions. Answer precisely and concisely based solely on the provided context. If the context does not contain enough information to answer fully, say so explicitly and explain what is missing rather than guessing.

# DEGraph Benchmark - repo_synthetic_small
# The following is the source code of this PySpark repository.

### FILE: _resources\setup.py
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


### FILE: bronze\01_ingest_orders.py
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — orders
# MAGIC
# MAGIC Streams new order events out of the landing volume into the bronze
# MAGIC `orders_raw` Delta table via Auto Loader. Trigger is `availableNow` so
# MAGIC the job processes whatever has accumulated since the last run and then
# MAGIC exits — typical batch-flavored streaming pattern for hourly schedules.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC * The `%run ../_resources/setup` cell pulls in `database`, `volume_path`,
# MAGIC   `checkpoint_root`, `schema_root`, and `autoloader_common_options`.
# MAGIC   These resolve the f-strings below to concrete paths and FQNs.
# MAGIC * The `cloudFiles` reader maps to a `Reads` edge whose source is an
# MAGIC   `ExternalSource` node at the resolved volume path.
# MAGIC * `df.writeStream.table(...)` maps to a `Writes` edge to the
# MAGIC   `main.dbdemos_ecom.orders_raw` Table node.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the landing volume as a stream
# MAGIC
# MAGIC Auto Loader infers schema on first run and persists it under
# MAGIC `schema_root` so subsequent runs don't re-infer.

# COMMAND ----------
orders_landing = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_root}/orders")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{volume_path}/orders")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Add operational metadata
# MAGIC
# MAGIC Stamp every row with the ingest timestamp and the originating file
# MAGIC name. These columns are downstream invariants — silver transformations
# MAGIC rely on `ingested_ts` for late-arriving-data ordering.

# COMMAND ----------
orders_with_meta = (
    orders_landing
        .withColumn("ingested_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Write to bronze
# MAGIC
# MAGIC `availableNow` trigger flushes the backlog and exits. The checkpoint
# MAGIC under `checkpoint_root` is what makes the job resumable.

# COMMAND ----------
(orders_with_meta
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_root}/orders")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table(f"{database}.orders_raw"))


### FILE: bronze\03_ingest_events.py
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — clickstream events
# MAGIC
# MAGIC Streams behavioral / clickstream events out of the landing volume into
# MAGIC `events_raw`. Each event carries a JSON `payload` whose schema varies
# MAGIC with `event_type`, so this notebook delegates parsing to the imported
# MAGIC `parse_event_payload` helper from `utils.event_parsers`.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC The `parse_event_payload(df)` call is **deliberately not registered**
# MAGIC in `.degraph/helpers.json`. This makes it the benchmark's "unregistered
# MAGIC helper" case: the extractor will emit one `OpaqueTransform` edge with
# MAGIC `is_passthrough=false` to preserve the DataFrame's lineage chain
# MAGIC across the call, plus a `GraphWarning` naming the unregistered helper.
# MAGIC The Q&A pair on lineage gaps probes whether an LLM reading the graph
# MAGIC can surface this warning.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F
from utils.event_parsers import parse_event_payload

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Stream the raw event landing zone
# MAGIC
# MAGIC Events arrive as one JSON object per line, one file per kafka-connect
# MAGIC flush window.

# COMMAND ----------
events_landing = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_root}/events")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{volume_path}/events")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Parse the payload column
# MAGIC
# MAGIC `parse_event_payload` lifts `page_url`, `referrer`, `user_agent`, and
# MAGIC `cart_total` out of the JSON payload and drops the original. Body lives
# MAGIC in `utils/event_parsers.py`; the call site is what DEGraph sees.

# COMMAND ----------
events_parsed = parse_event_payload(events_landing)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Stamp ingest metadata and write to bronze

# COMMAND ----------
events_final = (
    events_parsed
        .withColumn("ingested_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------
(events_final
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_root}/events")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table(f"{database}.events_raw"))


### FILE: config.py
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


### FILE: ddl\bronze_tables.sql
-- ============================================================================
-- Bronze layer DDL — e-commerce synthetic benchmark
-- ============================================================================
--
-- Per dev/methodology.md Decision 3.3, the DEGraph extractor REQUIRES the
-- column schemas of all source (bronze) tables to be available. These CREATE
-- TABLE statements provide that schema in a form the extractor parses with
-- sqlglot during the per-file extraction pipeline (outline.md §4.2, "DDL
-- collector" step).
--
-- The schemas defined here are used by:
--
--   1. Downstream column-level lineage extraction. When a silver notebook does
--      `.select(col("customer_id"), col("email"), ...)`, the extractor checks
--      these column names against the registered schema and emits proper
--      Derives edges with resolved source_cols[].
--
--   2. Symbolic `.columns` resolution. The dynamic-column pattern in
--      gold/product_performance.py iterates over `df.columns` to build a
--      loop-generated agg list; that loop is enumerated statically because
--      the column list is known from the DDL.
--
--   3. ExternalSource→Table FQN resolution. Auto Loader reads from
--      /Volumes/.../landing/<dataset> paths; the extractor matches the inferred
--      bronze table name against the DDL to confirm the write target exists.
--
-- Convention: every bronze table ends in `_raw`, lives under the
-- `main.dbdemos_ecom` catalog/schema, is Delta-backed, and includes the
-- standard `ingested_ts`, `source_file`, and (where Auto Loader is the
-- ingester) `_rescued_data` columns for operational traceability.
-- ============================================================================

CREATE TABLE IF NOT EXISTS main.dbdemos_ecom.orders_raw (
  order_id         STRING        COMMENT 'Order UUID; primary key',
  customer_id      STRING        COMMENT 'FK to customers_raw.customer_id',
  product_id       STRING        COMMENT 'FK to products_raw.product_id',
  quantity         INT           COMMENT 'Number of units ordered',
  unit_price       DECIMAL(18,2) COMMENT 'Price per unit at order time, pre-discount',
  total_amount     DECIMAL(18,2) COMMENT 'quantity * unit_price (denormalized at ingest)',
  currency         STRING        COMMENT 'ISO 4217 currency code',
  status           STRING        COMMENT 'one of: pending, paid, refunded, cancelled',
  order_ts         TIMESTAMP     COMMENT 'When the order was placed (server clock)',
  ingested_ts      TIMESTAMP     COMMENT 'When this row was ingested into bronze',
  source_file      STRING        COMMENT 'Auto Loader: originating file path',
  _rescued_data    STRING        COMMENT 'Auto Loader: malformed-record salvage column'
) USING DELTA
  COMMENT 'Raw order events ingested from S3 via Auto Loader.';

CREATE TABLE IF NOT EXISTS main.dbdemos_ecom.customers_raw (
  customer_id      STRING        COMMENT 'Customer UUID; primary key',
  email            STRING        COMMENT 'Login email (PII)',
  first_name       STRING        COMMENT 'Given name (PII)',
  last_name        STRING        COMMENT 'Family name (PII)',
  country_code     STRING        COMMENT 'ISO 3166-1 alpha-2 country code',
  signup_ts        TIMESTAMP     COMMENT 'Account creation timestamp',
  marketing_opt_in BOOLEAN       COMMENT 'CAN-SPAM / GDPR marketing consent flag',
  ingested_ts      TIMESTAMP     COMMENT 'When this row was ingested into bronze',
  source_file      STRING        COMMENT 'Auto Loader: originating file path',
  _rescued_data    STRING        COMMENT 'Auto Loader: malformed-record salvage column'
) USING DELTA
  COMMENT 'Raw customer profile records ingested from the CRM export.';

CREATE TABLE IF NOT EXISTS main.dbdemos_ecom.products_raw (
  product_id       STRING        COMMENT 'Product UUID; primary key',
  name             STRING        COMMENT 'Display name',
  category         STRING        COMMENT 'Top-level product taxonomy',
  list_price       DECIMAL(18,2) COMMENT 'Sticker price (pre-discount)',
  active           BOOLEAN       COMMENT 'Available for sale flag',
  ingested_ts      TIMESTAMP     COMMENT 'When this row was ingested into bronze',
  source_file      STRING        COMMENT 'Source file path'
) USING DELTA
  COMMENT 'Product catalog snapshot ingested from the external catalog service.';

CREATE TABLE IF NOT EXISTS main.dbdemos_ecom.events_raw (
  event_id         STRING        COMMENT 'Event UUID; primary key',
  customer_id      STRING        COMMENT 'FK to customers_raw.customer_id; NULL for anonymous',
  event_type       STRING        COMMENT 'one of: page_view, add_to_cart, checkout, purchase',
  product_id       STRING        COMMENT 'Optional FK to products_raw.product_id',
  session_id       STRING        COMMENT 'Browser / mobile-app session identifier',
  event_ts         TIMESTAMP     COMMENT 'When the event was emitted (client clock)',
  ingested_ts      TIMESTAMP     COMMENT 'When this row was ingested into bronze',
  payload          STRING        COMMENT 'JSON payload; schema varies by event_type',
  _rescued_data    STRING        COMMENT 'Auto Loader: malformed-record salvage column'
) USING DELTA
  COMMENT 'Raw clickstream / behavioral events ingested from a Kafka-fed stream.';


### FILE: gold\customer_ltv.py
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


### FILE: gold\product_performance.py
# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — product_performance
# MAGIC
# MAGIC Computes per-product, per-day aggregate metrics from
# MAGIC `silver.ecom.orders_cleaned`, then upserts them into
# MAGIC `gold.ecom.product_performance` using the registered `DeltaMergeSink`
# MAGIC class. The list of metrics computed is loaded from an out-of-repo JSON
# MAGIC config file so non-engineering operators can add or remove metrics
# MAGIC without redeploying the pipeline.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC This file exercises two of the trickiest schema features:
# MAGIC
# MAGIC * **Dynamic aggregation list (`dynamic=True`)** — `metric_columns` is
# MAGIC   read from a JSON file whose path comes from `os.environ.get(...)`,
# MAGIC   none of which the SafeEvaluator can resolve to a literal. The loop
# MAGIC   over `metric_columns` builds the `aggregations` list at runtime;
# MAGIC   `.agg(*aggregations, ...)` star-unpacks an opaque list into the
# MAGIC   aggregation call. Per `dev/methodology.md` Gap E handling, the
# MAGIC   extractor emits **one** `Aggregates` edge with `dynamic=True`,
# MAGIC   `agg_ops=[<unresolved>]`, `output_cols=[<unresolved>]`, and a
# MAGIC   `dynamic_note` describing why the list could not be enumerated.
# MAGIC   The static `F.count("order_id")` portion of the agg is preserved
# MAGIC   in the same edge's `agg_ops` alongside the unresolved marker.
# MAGIC
# MAGIC * **Custom sink class (`sink_class` populated)** — `DeltaMergeSink` is
# MAGIC   registered in `.degraph/sinks.json`. The extractor reads the registry,
# MAGIC   resolves the `target_table=f"{database}.product_performance"` kwarg
# MAGIC   against the `database` symbol imported via `%run`, and emits a
# MAGIC   `Writes` edge with `sink_class="DeltaMergeSink"`, `mode="merge"`,
# MAGIC   and the resolved table FQN as target.

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
import json
import os

from pyspark.sql import functions as F

from utils.sinks import DeltaMergeSink

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the silver source

# COMMAND ----------
orders = spark.table(f"{database}.orders_cleaned")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Load metric configuration from out-of-repo JSON
# MAGIC
# MAGIC The list of numeric columns to roll up per product per day lives in a
# MAGIC config file outside the repo. The path is read from an env var; the
# MAGIC file contents are runtime JSON. SafeEvaluator can resolve neither, so
# MAGIC the downstream `aggregations` list is opaque to DEGraph.

# COMMAND ----------
config_path = os.environ.get("METRIC_CONFIG_PATH", "/dbfs/configs/metrics.json")
with open(config_path) as f:
    metric_columns = json.load(f)["product_performance"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Build the aggregation list dynamically
# MAGIC
# MAGIC One `sum`, one `avg`, one `max` per metric. List length and column
# MAGIC names are runtime-determined.

# COMMAND ----------
aggregations = []
for col_name in metric_columns:
    aggregations.append(F.sum(col_name).alias(f"sum_{col_name}"))
    aggregations.append(F.avg(col_name).alias(f"avg_{col_name}"))
    aggregations.append(F.max(col_name).alias(f"max_{col_name}"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Group and aggregate
# MAGIC
# MAGIC The static `F.count("order_id")` is enumerable; the `*aggregations`
# MAGIC star-unpack is not. The extractor records both in one Aggregates edge,
# MAGIC the latter as `<unresolved>` entries with `dynamic=True`.

# COMMAND ----------
daily_perf = (
    orders
    .groupBy("product_id", "order_date")
    .agg(*aggregations, F.count("order_id").alias("order_count"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Upsert into gold via the registered DeltaMergeSink
# MAGIC
# MAGIC Standard SCD-Type-1 merge on (product_id, order_date). The sink class
# MAGIC is registered, so DEGraph emits a proper Writes edge with sink_class
# MAGIC populated and mode=merge — no `<unresolved>` markers, no warnings.

# COMMAND ----------
sink = DeltaMergeSink(
    target_table=f"{database}.product_performance",
    merge_keys=["product_id", "order_date"],
    mode="merge",
)
sink.run(daily_perf)


### FILE: silver\customer_profile.py
# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — customer_profile
# MAGIC
# MAGIC Builds a per-customer profile by joining cleaned customer records with
# MAGIC their order history and the latest behavioral event per customer.
# MAGIC The latest-event-per-customer pattern uses a window function with
# MAGIC `row_number()`; the suffixed events DataFrame uses the registered
# MAGIC `rename_columns_with_suffix` helper to keep the joined columns distinct.
# MAGIC
# MAGIC ### DEGraph extraction notes
# MAGIC
# MAGIC This file is the densest in the benchmark for edge-type coverage. Per
# MAGIC the inventory's coverage matrix:
# MAGIC
# MAGIC * **Reads** (3) — `orders_raw`, `customers_raw`, `events_raw`
# MAGIC * **Derives with `window_spec`** (1) — `row_number().over(W.partitionBy("customer_id").orderBy("event_ts" desc))`.
# MAGIC   Captures the partition keys and order keys so an impact-analysis
# MAGIC   query on `event_ts` flags this window as at-risk.
# MAGIC * **Filters** (1) — `rn == 1` to keep only the latest event per customer
# MAGIC * **Projects** (1) — drop the helper column `rn`
# MAGIC * **Derives (registered suffix_rename)** (many) — `rename_columns_with_suffix(events_latest, "_last_event")`
# MAGIC   is registered as `"kind": "suffix_rename"`, so the extractor emits one
# MAGIC   explicit Derives edge per renamed column, mapping
# MAGIC   `col_name → col_name + "_last_event"`.
# MAGIC * **Joins** (2) — customers ⨝ orders, then result ⨝ events_suffixed
# MAGIC * **Aggregates** (1) — final `.groupBy(...).agg(...)`
# MAGIC * **Writes** (1) — to `silver.ecom.customer_profile`

# COMMAND ----------
# MAGIC %run ../_resources/setup

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.column_transformations import rename_columns_with_suffix

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read the three bronze sources

# COMMAND ----------
orders_df = spark.table(f"{database}.orders_raw")
customers_df = spark.table(f"{database}.customers_raw")
events_df = spark.table(f"{database}.events_raw")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Window-deduplicate events: keep only the latest event per customer
# MAGIC
# MAGIC Order events by descending `event_ts` within each customer partition,
# MAGIC assign a row number, keep row 1, drop the helper column.

# COMMAND ----------
events_window = Window.partitionBy("customer_id").orderBy(F.col("event_ts").desc())
events_ranked = events_df.withColumn("rn", F.row_number().over(events_window))
events_latest = events_ranked.filter(F.col("rn") == 1).drop("rn")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Suffix-rename the events columns
# MAGIC
# MAGIC Every column from `events_latest` becomes `<name>_last_event` so the
# MAGIC three-way join can keep them distinct from the orders / customers
# MAGIC columns. The join key on the events side becomes
# MAGIC `customer_id_last_event`.

# COMMAND ----------
events_suffixed = rename_columns_with_suffix(events_latest, "_last_event")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Three-way join: customers ⨝ orders ⨝ events
# MAGIC
# MAGIC Two `Join` edges, executed left-to-right.

# COMMAND ----------
co = customers_df.join(orders_df, on="customer_id", how="inner")

profile = co.join(
    events_suffixed,
    co["customer_id"] == events_suffixed["customer_id_last_event"],
    how="left",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Aggregate per customer
# MAGIC
# MAGIC One row per (customer_id, email, country_code, signup_ts). Aggregates
# MAGIC summarize the customer's order history and surface the last-event
# MAGIC timestamp for use by gold-tier recency metrics.

# COMMAND ----------
customer_profile = (
    profile
    .groupBy("customer_id", "email", "country_code", "signup_ts")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("lifetime_revenue"),
        F.max("order_ts").alias("last_order_ts"),
        F.max("event_ts_last_event").alias("last_event_ts"),
        F.countDistinct("product_id").alias("distinct_products_ordered"),
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Write to silver

# COMMAND ----------
(customer_profile.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.customer_profile"))


### FILE: silver\orders_cleaned.py
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


### FILE: utils\column_transformations.py
"""Column-transformation helpers reused across silver pipelines.

These are the kind of utility functions every production Databricks repo
accumulates — a few generic operations on DataFrame column sets, factored
out so individual silver notebooks can stay focused on business logic.

### Why this file matters for the DEGraph benchmark

The two functions below appear in `silver/orders_cleaned.py` and
`silver/customer_profile.py` as imported helpers. The DEGraph extractor
will encounter call sites of the form

    df = trim_string_columns(df)
    df = rename_columns_with_suffix(df, "_claim")

and must decide how to represent them. Per `dev/methodology.md` Decision 3.7:

- WITHOUT a known-helper registry entry, both calls produce an
  `OpaqueTransform` edge with `is_passthrough=false` — the variable's
  identity is preserved through the call but column-level semantics are lost.

- WITH a registry entry in `.degraph/helpers.json` declaring `trim_string_columns`
  as `"kind": "passthrough"` and `rename_columns_with_suffix` as
  `"kind": "suffix_rename"`, the extractor emits semantically-rich edges:
  an `OpaqueTransform` with `is_passthrough=true` for the trim (preserves
  the column set, just mutates values), and one explicit `Derives` edge
  per renamed column for the suffix rename.

This file therefore exercises the "lift ⚠️ → ✅" mechanism that the
registry provides, and validates that the registry-driven path produces
strictly more information than the default opaque path.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Strip leading/trailing whitespace from every string-typed column.

    Pure column-set passthrough: the output schema matches the input schema
    column-for-column; only the string values are mutated. Registered in
    `.degraph/helpers.json` as `"kind": "passthrough"`.
    """
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_cols:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
    return df


def rename_columns_with_suffix(df: DataFrame, suffix: str) -> DataFrame:
    """Append `suffix` to every column name in `df`.

    Used in silver-layer joins to disambiguate columns that share names
    across the two sides of a join (e.g. both `orders` and `customers`
    have a `customer_id` — only one survives, but other columns may need
    suffixing). Registered in `.degraph/helpers.json` as
    `"kind": "suffix_rename"` with `suffix_arg: 1` so the extractor knows
    to read the suffix from the second positional argument.
    """
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name + suffix)
    return df


### FILE: utils\event_parsers.py
"""Event-payload parsing helpers used by the events ingest pipeline.

The events_raw stream carries a JSON `payload` column whose schema varies
by `event_type`. This module factors the parsing logic out of the bronze
ingest notebook.

### Why this file matters for the DEGraph benchmark

`bronze/03_ingest_events.py` calls `parse_event_payload(df)` exactly once.
Unlike the helpers in `column_transformations.py`, this function is
DELIBERATELY NOT REGISTERED in `.degraph/helpers.json`. That makes it the
benchmark's representative of the "unregistered helper" case:

- The extractor encounters `df = parse_event_payload(df)`.
- No registry entry exists.
- Per `dev/methodology.md` Decision 3.7 default behavior, it emits an
  `OpaqueTransform` edge with `is_passthrough=false` — meaning the column
  set may have changed but the extractor cannot say how.
- A `GraphWarning` is appended noting "unregistered helper
  parse_event_payload at bronze/03_ingest_events.py:N".

The ground-truth graph for this benchmark must reflect that: one
OpaqueTransform edge plus the warning. The Q&A pair on "lineage gaps"
specifically probes whether the LLM can read GraphWarnings and report
"there is an unregistered helper here that changes the columns in an
unknown way."
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType


_PAYLOAD_SCHEMA = StructType([
    StructField("page_url", StringType(), nullable=True),
    StructField("referrer", StringType(), nullable=True),
    StructField("user_agent", StringType(), nullable=True),
    StructField("cart_total", StringType(), nullable=True),
])


def parse_event_payload(df: DataFrame) -> DataFrame:
    """Extract structured fields from the JSON `payload` column.

    Adds four new columns — `page_url`, `referrer`, `user_agent`,
    `cart_total` — parsed from the payload JSON, then drops the raw
    `payload` column. Column-set change is non-trivial: one column out,
    four columns in. This is exactly the kind of transformation the
    DEGraph extractor cannot infer without analyzing the body, and which
    must be reported as opaque-with-warning when not registered.
    """
    parsed = df.withColumn("_parsed", F.from_json(F.col("payload"), _PAYLOAD_SCHEMA))
    parsed = (
        parsed
        .withColumn("page_url", F.col("_parsed.page_url"))
        .withColumn("referrer", F.col("_parsed.referrer"))
        .withColumn("user_agent", F.col("_parsed.user_agent"))
        .withColumn("cart_total", F.col("_parsed.cart_total").cast("decimal(18,2)"))
        .drop("_parsed", "payload")
    )
    return parsed


### FILE: utils\sinks.py
"""Custom sink classes for non-trivial write patterns.

Production Databricks repos frequently wrap writes in custom classes —
typically to encapsulate merge / upsert / SCD logic that the bare
`df.write.saveAsTable` API does not express directly. This module hosts
the `DeltaMergeSink` class used by `gold/product_performance.py`.

### Why this file matters for the DEGraph benchmark

A bare `DeltaMergeSink(target_table=..., merge_keys=[...]).run()` call
site is **invisible to a naive extractor**: the extractor sees a method
call on an instance of an imported class, with no `.write` and no
`saveAsTable` anywhere. Without explicit recognition, the write would be
silently dropped — which is the single worst failure mode for a lineage
tool (silent gap, no warning).

Per `dev/methodology.md` Decision 3.8, DEGraph handles this with a
two-tier mechanism:

1. **Registered (preferred):** `.degraph/sinks.json` maps `DeltaMergeSink`
   to its semantics — which kwarg holds the target table FQN
   (`target_table`), which holds the write mode (`mode`, default `merge`).
   The extractor emits a proper `Writes` edge with `sink_class="DeltaMergeSink"`
   and `mode="merge"`.

2. **Heuristic fallback:** if the class were *not* registered but matched
   the `*Sink` / `*Writer` name pattern AND had a kwarg in
   `{target_table, target, sink_table}`, the extractor would still emit
   a `Writes` edge (with `<unresolved>` mode) plus a `GraphWarning`.
   This benchmark exercises path 1 — the class IS registered.
"""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable


class DeltaMergeSink:
    """Upsert a source DataFrame into a Delta target via MERGE.

    Encapsulates the standard "SCD-Type-1" upsert pattern: rows matching
    on `merge_keys` are updated in place; non-matching source rows are
    inserted. Encapsulated here rather than inlined at the call site
    because three different gold notebooks use the same pattern with
    different keys.
    """

    def __init__(
        self,
        target_table: str,
        merge_keys: List[str],
        mode: str = "merge",
        update_condition: Optional[str] = None,
    ) -> None:
        self.target_table = target_table
        self.merge_keys = merge_keys
        self.mode = mode
        self.update_condition = update_condition

    def run(self, df: DataFrame) -> None:
        """Execute the merge."""
        spark = SparkSession.getActiveSession()
        if not spark.catalog.tableExists(self.target_table):
            df.write.format("delta").saveAsTable(self.target_table)
            return

        target = DeltaTable.forName(spark, self.target_table)
        condition = " AND ".join(f"t.{k} = s.{k}" for k in self.merge_keys)
        builder = target.alias("t").merge(df.alias("s"), condition)
        if self.update_condition:
            builder = builder.whenMatchedUpdateAll(condition=self.update_condition)
        else:
            builder = builder.whenMatchedUpdateAll()
        builder.whenNotMatchedInsertAll().execute()



~~~ END OF STEP A-1 ~~~

--- STEP A-Q1 (Q1 | direct-cross-file-impact | medium) ---
Paste this as your next message in Chat A:

---

Question: If the column `customer_id` in `main.dbdemos_ecom.orders_raw` is renamed to `cust_id`, which silver and gold tables need updating? List the affected tables and briefly explain why.

--- STEP A-Q2 (Q2 | producer-consumer | easy) ---
Paste this as your next message in Chat A:

---

Question: Which file in this repo produces `main.dbdemos_ecom.orders_cleaned`?

--- STEP A-Q3 (Q3 | transitive-multi-hop-impact | medium) ---
Paste this as your next message in Chat A:

---

Question: If the column `last_event_ts` is dropped from `main.dbdemos_ecom.customer_profile`, which downstream gold-tier columns become undefined?

--- STEP A-Q4 (Q4 | reverse-lineage | hard) ---
Paste this as your next message in Chat A:

---

Question: Which bronze-layer columns contribute, directly or transitively, to `gold.customer_ltv.lifetime_revenue`? List the source table and column for each.

--- STEP A-Q5 (Q5 | counterfactual | hard) ---
Paste this as your next message in Chat A:

---

Question: If the `trim_string_columns(orders)` call at line 56 of `silver/orders_cleaned.py` is removed, what specifically changes in the lineage graph, and does it affect the column set of `main.dbdemos_ecom.orders_cleaned`?

--- STEP A-Q6 (Q6 | producer-consumer | easy) ---
Paste this as your next message in Chat A:

---

Question: Which file consumes `main.dbdemos_ecom.events_raw`, and what table does that file produce?

--- STEP A-Q7 (Q7 | aggregate-semantics | hard) ---
Paste this as your next message in Chat A:

---

Question: What columns from `silver.orders_cleaned` are aggregated into `gold.product_performance`, and what aggregation functions are applied? Be precise about what the graph can and cannot tell you.

--- STEP A-Q8 (Q8 | window-function-impact | medium) ---
Paste this as your next message in Chat A:

---

Question: The latest-event window in `silver/customer_profile.py` partitions on `customer_id` and orders by `event_ts` descending. If `event_ts` becomes nullable in `bronze.events_raw`, which downstream silver and gold columns are at risk of becoming unstable or unreliable?

--- STEP A-Q9 (Q9 | custom-sink | easy) ---
Paste this as your next message in Chat A:

---

Question: What table receives the writes from `gold/product_performance.py`, what write mode is used, and what merge keys does the sink apply?

--- STEP A-Q10 (Q10 | coverage-of-unknowns | medium) ---
Paste this as your next message in Chat A:

---

Question: What lineage gaps or unknowns does the graph itself report for this repo? List each, the file/location it applies to, and what the gap means for downstream analysis.

================================================================================
CHAT B — GRAPH MODE
================================================================================

~~~ STEP B-1: Paste this FIRST in a new Claude chat (do this once) ~~~

You are an expert data engineer. You will be given context about a PySpark repository (either as source code or as a DEGraph lineage graph) and asked impact-analysis questions. Answer precisely and concisely based solely on the provided context. If the context does not contain enough information to answer fully, say so explicitly and explain what is missing rather than guessing.

# DEGraph Benchmark - repo_synthetic_small
# The following is the DEGraph lineage graph for this PySpark repository.
# Use it to answer questions about data lineage, column provenance, and impact.

{
  "tables": [
    {
      "fqn": "main.dbdemos_ecom.orders_raw",
      "written_by": [
        "bronze/01_ingest_orders.py"
      ],
      "read_by": [
        "silver/customer_profile.py",
        "silver/orders_cleaned.py"
      ],
      "columns": [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "total_amount",
        "currency",
        "status",
        "order_ts",
        "ingested_ts",
        "source_file",
        "_rescued_data"
      ]
    },
    {
      "fqn": "main.dbdemos_ecom.customers_raw",
      "written_by": [
        "bronze/02_ingest_customers.ipynb"
      ],
      "read_by": [
        "silver/customer_profile.py"
      ],
      "columns": [
        "customer_id",
        "email",
        "first_name",
        "last_name",
        "country_code",
        "signup_ts",
        "marketing_opt_in",
        "ingested_ts",
        "source_file",
        "_rescued_data"
      ]
    },
    {
      "fqn": "main.dbdemos_ecom.products_raw",
      "written_by": [],
      "read_by": [],
      "columns": [
        "product_id",
        "name",
        "category",
        "list_price",
        "active",
        "ingested_ts",
        "source_file"
      ]
    },
    {
      "fqn": "main.dbdemos_ecom.events_raw",
      "written_by": [
        "bronze/03_ingest_events.py"
      ],
      "read_by": [
        "silver/customer_profile.py"
      ],
      "columns": [
        "event_id",
        "customer_id",
        "event_type",
        "product_id",
        "session_id",
        "event_ts",
        "ingested_ts",
        "payload",
        "_rescued_data"
      ]
    },
    {
      "fqn": "main.dbdemos_ecom.customer_profile",
      "written_by": [
        "silver/customer_profile.py"
      ],
      "read_by": [
        "gold/customer_ltv.py"
      ]
    },
    {
      "fqn": "main.dbdemos_ecom.customer_ltv",
      "written_by": [
        "gold/customer_ltv.py"
      ],
      "read_by": []
    },
    {
      "fqn": "main.dbdemos_ecom.orders_cleaned",
      "written_by": [
        "silver/orders_cleaned.py"
      ],
      "read_by": [
        "gold/product_performance.py"
      ]
    },
    {
      "fqn": "main.dbdemos_ecom.product_performance",
      "written_by": [
        "gold/product_performance.py"
      ],
      "read_by": []
    }
  ],
  "edges": [
    {
      "kind": "reads",
      "file": "bronze/01_ingest_orders.py",
      "src": "ext:/Volumes/main/dbdemos_ecom/landing/orders",
      "tgt": "bronze/01_ingest_orders.py:orders_landing",
      "streaming": false
    },
    {
      "kind": "derives",
      "file": "bronze/01_ingest_orders.py",
      "src": "bronze/01_ingest_orders.py:orders_landing",
      "tgt": "bronze/01_ingest_orders.py:orders_with_meta",
      "output_col": "ingested_ts"
    },
    {
      "kind": "derives",
      "file": "bronze/01_ingest_orders.py",
      "src": "bronze/01_ingest_orders.py:orders_landing",
      "tgt": "bronze/01_ingest_orders.py:orders_with_meta",
      "output_col": "source_file"
    },
    {
      "kind": "writes",
      "file": "bronze/01_ingest_orders.py",
      "src": "bronze/01_ingest_orders.py:orders_with_meta",
      "tgt": "table:main.dbdemos_ecom.orders_raw",
      "mode": "append",
      "format": "delta",
      "streaming": true
    },
    {
      "kind": "reads",
      "file": "bronze/02_ingest_customers.ipynb",
      "src": "ext:/Volumes/main/dbdemos_ecom/landing/customers",
      "tgt": "bronze/02_ingest_customers.ipynb:cell5:customers_landing",
      "streaming": false
    },
    {
      "kind": "derives",
      "file": "bronze/02_ingest_customers.ipynb",
      "src": "bronze/02_ingest_customers.ipynb:cell5:customers_landing",
      "tgt": "bronze/02_ingest_customers.ipynb:cell7:customers_with_meta",
      "output_col": "ingested_ts"
    },
    {
      "kind": "derives",
      "file": "bronze/02_ingest_customers.ipynb",
      "src": "bronze/02_ingest_customers.ipynb:cell5:customers_landing",
      "tgt": "bronze/02_ingest_customers.ipynb:cell7:customers_with_meta",
      "output_col": "source_file"
    },
    {
      "kind": "writes",
      "file": "bronze/02_ingest_customers.ipynb",
      "src": "bronze/02_ingest_customers.ipynb:cell7:customers_with_meta",
      "tgt": "table:main.dbdemos_ecom.customers_raw",
      "mode": "append",
      "format": "delta",
      "streaming": true
    },
    {
      "kind": "reads",
      "file": "bronze/03_ingest_events.py",
      "src": "ext:/Volumes/main/dbdemos_ecom/landing/events",
      "tgt": "bronze/03_ingest_events.py:events_landing",
      "streaming": false
    },
    {
      "kind": "opaque_transform",
      "file": "bronze/03_ingest_events.py",
      "src": "bronze/03_ingest_events.py:events_landing",
      "tgt": "bronze/03_ingest_events.py:events_parsed",
      "operator": "utils.event_parsers.parse_event_payload",
      "is_passthrough": false
    },
    {
      "kind": "derives",
      "file": "bronze/03_ingest_events.py",
      "src": "bronze/03_ingest_events.py:events_parsed",
      "tgt": "bronze/03_ingest_events.py:events_final",
      "output_col": "ingested_ts"
    },
    {
      "kind": "derives",
      "file": "bronze/03_ingest_events.py",
      "src": "bronze/03_ingest_events.py:events_parsed",
      "tgt": "bronze/03_ingest_events.py:events_final",
      "output_col": "source_file"
    },
    {
      "kind": "writes",
      "file": "bronze/03_ingest_events.py",
      "src": "bronze/03_ingest_events.py:events_final",
      "tgt": "table:main.dbdemos_ecom.events_raw",
      "mode": "append",
      "format": "delta",
      "streaming": true
    },
    {
      "kind": "reads",
      "file": "gold/customer_ltv.py",
      "src": "table:main.dbdemos_ecom.customer_profile",
      "tgt": "gold/customer_ltv.py:base_cte",
      "streaming": false
    },
    {
      "kind": "aggregates",
      "file": "gold/customer_ltv.py",
      "src": "gold/customer_ltv.py:base_cte",
      "tgt": "gold/customer_ltv.py:country_avg_cte",
      "group_keys": [
        "country_code"
      ],
      "agg_ops": [
        "avg"
      ],
      "output_cols": [
        "avg_country_ltv"
      ]
    },
    {
      "kind": "joins",
      "file": "gold/customer_ltv.py",
      "src": "gold/customer_ltv.py:base_cte",
      "tgt": "gold/customer_ltv.py:ranked_joined",
      "join_type": "left",
      "join_keys": [
        [
          "country_code",
          "country_code"
        ]
      ],
      "right_src": "gold/customer_ltv.py:country_avg_cte"
    },
    {
      "kind": "derives",
      "file": "gold/customer_ltv.py",
      "src": "gold/customer_ltv.py:ranked_joined",
      "tgt": "gold/customer_ltv.py:ranked_cte",
      "output_col": "days_since_last_order"
    },
    {
      "kind": "derives",
      "file": "gold/customer_ltv.py",
      "src": "gold/customer_ltv.py:ranked_joined",
      "tgt": "gold/customer_ltv.py:ranked_cte",
      "output_col": "ltv_vs_country_avg"
    },
    {
      "kind": "derives",
      "file": "gold/customer_ltv.py",
      "src": "gold/customer_ltv.py:ranked_joined",
      "tgt": "gold/customer_ltv.py:ranked_cte",
      "output_col": "revenue_quartile",
      "window_spec": {
        "partition_cols": [],
        "order_cols": [
          "b.lifetime_revenue"
        ],
        "frame": null
      }
    },
    {
      "kind": "projects",
      "file": "gold/customer_ltv.py",
      "src": "gold/customer_ltv.py:ranked_cte",
      "tgt": "gold/customer_ltv.py:ltv_df",
      "kept_cols": [
        "*"
      ]
    },
    {
      "kind": "derives",
      "file": "gold/customer_ltv.py",
      "src": "gold/customer_ltv.py:ltv_df",
      "tgt": "gold/customer_ltv.py:ltv_tiered",
      "output_col": "ltv_tier"
    },
    {
      "kind": "writes",
      "file": "gold/customer_ltv.py",
      "src": "gold/customer_ltv.py:ltv_tiered",
      "tgt": "table:main.dbdemos_ecom.customer_ltv",
      "mode": "overwrite",
      "format": "delta",
      "streaming": false
    },
    {
      "kind": "reads",
      "file": "gold/product_performance.py",
      "src": "table:main.dbdemos_ecom.orders_cleaned",
      "tgt": "gold/product_performance.py:orders",
      "streaming": false
    },
    {
      "kind": "aggregates",
      "file": "gold/product_performance.py",
      "src": "gold/product_performance.py:orders",
      "tgt": "gold/product_performance.py:daily_perf",
      "group_keys": [
        "product_id",
        "order_date"
      ],
      "agg_ops": [
        "<unresolved>",
        "count"
      ],
      "output_cols": [
        "<unresolved>",
        "order_count"
      ],
      "dynamic": true,
      "dynamic_note": "agg list star-unpacked from a runtime-built variable; column list cannot be enumerated statically"
    },
    {
      "kind": "writes",
      "file": "gold/product_performance.py",
      "src": "gold/product_performance.py:daily_perf",
      "tgt": "table:main.dbdemos_ecom.product_performance",
      "mode": "merge",
      "format": null,
      "streaming": false,
      "sink_class": "DeltaMergeSink",
      "merge_keys": [
        "product_id",
        "order_date"
      ]
    },
    {
      "kind": "reads",
      "file": "silver/customer_profile.py",
      "src": "table:main.dbdemos_ecom.orders_raw",
      "tgt": "silver/customer_profile.py:orders_df",
      "streaming": false
    },
    {
      "kind": "reads",
      "file": "silver/customer_profile.py",
      "src": "table:main.dbdemos_ecom.customers_raw",
      "tgt": "silver/customer_profile.py:customers_df",
      "streaming": false
    },
    {
      "kind": "reads",
      "file": "silver/customer_profile.py",
      "src": "table:main.dbdemos_ecom.events_raw",
      "tgt": "silver/customer_profile.py:events_df",
      "streaming": false
    },
    {
      "kind": "derives",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:events_df",
      "tgt": "silver/customer_profile.py:events_ranked",
      "output_col": "rn"
    },
    {
      "kind": "filters",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:events_ranked",
      "tgt": "silver/customer_profile.py:_anon1",
      "referenced_cols": [
        "rn"
      ]
    },
    {
      "kind": "projects",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:_anon1",
      "tgt": "silver/customer_profile.py:events_latest",
      "removed_cols": [
        "rn"
      ]
    },
    {
      "kind": "derives",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:events_latest",
      "tgt": "silver/customer_profile.py:events_suffixed",
      "output_col": "event_id_last_event",
      "source_cols": [
        "event_id"
      ]
    },
    {
      "kind": "derives",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:events_latest",
      "tgt": "silver/customer_profile.py:events_suffixed",
      "output_col": "customer_id_last_event",
      "source_cols": [
        "customer_id"
      ]
    },
    {
      "kind": "derives",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:events_latest",
      "tgt": "silver/customer_profile.py:events_suffixed",
      "output_col": "event_type_last_event",
      "source_cols": [
        "event_type"
      ]
    },
    {
      "kind": "derives",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:events_latest",
      "tgt": "silver/customer_profile.py:events_suffixed",
      "output_col": "product_id_last_event",
      "source_cols": [
        "product_id"
      ]
    },
    {
      "kind": "derives",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:events_latest",
      "tgt": "silver/customer_profile.py:events_suffixed",
      "output_col": "session_id_last_event",
      "source_cols": [
        "session_id"
      ]
    },
    {
      "kind": "derives",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:events_latest",
      "tgt": "silver/customer_profile.py:events_suffixed",
      "output_col": "event_ts_last_event",
      "source_cols": [
        "event_ts"
      ]
    },
    {
      "kind": "derives",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:events_latest",
      "tgt": "silver/customer_profile.py:events_suffixed",
      "output_col": "ingested_ts_last_event",
      "source_cols": [
        "ingested_ts"
      ]
    },
    {
      "kind": "derives",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:events_latest",
      "tgt": "silver/customer_profile.py:events_suffixed",
      "output_col": "payload_last_event",
      "source_cols": [
        "payload"
      ]
    },
    {
      "kind": "derives",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:events_latest",
      "tgt": "silver/customer_profile.py:events_suffixed",
      "output_col": "_rescued_data_last_event",
      "source_cols": [
        "_rescued_data"
      ]
    },
    {
      "kind": "joins",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:customers_df",
      "tgt": "silver/customer_profile.py:co",
      "join_type": "inner",
      "join_keys": [
        [
          "customer_id",
          "customer_id"
        ]
      ],
      "right_src": "silver/customer_profile.py:orders_df"
    },
    {
      "kind": "joins",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:co",
      "tgt": "silver/customer_profile.py:profile",
      "join_type": "left",
      "join_keys": [
        [
          "customer_id",
          "customer_id_last_event"
        ]
      ],
      "right_src": "silver/customer_profile.py:events_suffixed"
    },
    {
      "kind": "aggregates",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:profile",
      "tgt": "silver/customer_profile.py:customer_profile",
      "group_keys": [
        "customer_id",
        "email",
        "country_code",
        "signup_ts"
      ],
      "agg_ops": [
        "count",
        "sum",
        "max",
        "max",
        "countDistinct"
      ],
      "output_cols": [
        "total_orders",
        "lifetime_revenue",
        "last_order_ts",
        "last_event_ts",
        "distinct_products_ordered"
      ]
    },
    {
      "kind": "writes",
      "file": "silver/customer_profile.py",
      "src": "silver/customer_profile.py:customer_profile",
      "tgt": "table:main.dbdemos_ecom.customer_profile",
      "mode": "overwrite",
      "format": "delta",
      "streaming": false
    },
    {
      "kind": "reads",
      "file": "silver/orders_cleaned.py",
      "src": "table:main.dbdemos_ecom.orders_raw",
      "tgt": "silver/orders_cleaned.py:orders",
      "streaming": false
    },
    {
      "kind": "opaque_transform",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "silver/orders_cleaned.py:orders",
      "operator": "utils.column_transformations.trim_string_columns",
      "is_passthrough": true,
      "opaque_kind": "passthrough"
    },
    {
      "kind": "derives",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "silver/orders_cleaned.py:orders",
      "output_col": "currency"
    },
    {
      "kind": "derives",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "silver/orders_cleaned.py:orders",
      "output_col": "status"
    },
    {
      "kind": "derives",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "silver/orders_cleaned.py:orders",
      "output_col": "quantity"
    },
    {
      "kind": "derives",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "silver/orders_cleaned.py:orders",
      "output_col": "total_amount"
    },
    {
      "kind": "derives",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "silver/orders_cleaned.py:orders",
      "output_col": "order_date"
    },
    {
      "kind": "derives",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "silver/orders_cleaned.py:orders",
      "output_col": "order_year"
    },
    {
      "kind": "derives",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "silver/orders_cleaned.py:orders",
      "output_col": "order_month"
    },
    {
      "kind": "derives",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "silver/orders_cleaned.py:orders",
      "output_col": "is_high_value"
    },
    {
      "kind": "projects",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "silver/orders_cleaned.py:orders",
      "removed_cols": [
        "_rescued_data",
        "source_file"
      ]
    },
    {
      "kind": "filters",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "silver/orders_cleaned.py:orders",
      "referenced_cols": [
        "status",
        "quantity"
      ]
    },
    {
      "kind": "writes",
      "file": "silver/orders_cleaned.py",
      "src": "silver/orders_cleaned.py:orders",
      "tgt": "table:main.dbdemos_ecom.orders_cleaned",
      "mode": "overwrite",
      "format": "delta",
      "streaming": false
    }
  ],
  "warnings": [
    {
      "file": "bronze/03_ingest_events.py",
      "category": "opaque-call-fallback",
      "message": "Unregistered function 'utils.event_parsers.parse_event_payload' called with DataFrame arg; column-set change across this call is unknown."
    },
    {
      "file": "gold/product_performance.py",
      "category": "dynamic-aggregation",
      "message": "agg list contains star-unpacked runtime variable in gold/product_performance.py:81"
    },
    {
      "file": "(assembler)",
      "category": "orphan-table",
      "message": "Table 'main.dbdemos_ecom.products_raw' appears in DDL but is never read or written by any file in this repo."
    }
  ]
}

~~~ END OF STEP B-1 ~~~

--- STEP B-Q1 (Q1 | direct-cross-file-impact | medium) ---
Paste this as your next message in Chat B:

---

Question: If the column `customer_id` in `main.dbdemos_ecom.orders_raw` is renamed to `cust_id`, which silver and gold tables need updating? List the affected tables and briefly explain why.

--- STEP B-Q2 (Q2 | producer-consumer | easy) ---
Paste this as your next message in Chat B:

---

Question: Which file in this repo produces `main.dbdemos_ecom.orders_cleaned`?

--- STEP B-Q3 (Q3 | transitive-multi-hop-impact | medium) ---
Paste this as your next message in Chat B:

---

Question: If the column `last_event_ts` is dropped from `main.dbdemos_ecom.customer_profile`, which downstream gold-tier columns become undefined?

--- STEP B-Q4 (Q4 | reverse-lineage | hard) ---
Paste this as your next message in Chat B:

---

Question: Which bronze-layer columns contribute, directly or transitively, to `gold.customer_ltv.lifetime_revenue`? List the source table and column for each.

--- STEP B-Q5 (Q5 | counterfactual | hard) ---
Paste this as your next message in Chat B:

---

Question: If the `trim_string_columns(orders)` call at line 56 of `silver/orders_cleaned.py` is removed, what specifically changes in the lineage graph, and does it affect the column set of `main.dbdemos_ecom.orders_cleaned`?

--- STEP B-Q6 (Q6 | producer-consumer | easy) ---
Paste this as your next message in Chat B:

---

Question: Which file consumes `main.dbdemos_ecom.events_raw`, and what table does that file produce?

--- STEP B-Q7 (Q7 | aggregate-semantics | hard) ---
Paste this as your next message in Chat B:

---

Question: What columns from `silver.orders_cleaned` are aggregated into `gold.product_performance`, and what aggregation functions are applied? Be precise about what the graph can and cannot tell you.

--- STEP B-Q8 (Q8 | window-function-impact | medium) ---
Paste this as your next message in Chat B:

---

Question: The latest-event window in `silver/customer_profile.py` partitions on `customer_id` and orders by `event_ts` descending. If `event_ts` becomes nullable in `bronze.events_raw`, which downstream silver and gold columns are at risk of becoming unstable or unreliable?

--- STEP B-Q9 (Q9 | custom-sink | easy) ---
Paste this as your next message in Chat B:

---

Question: What table receives the writes from `gold/product_performance.py`, what write mode is used, and what merge keys does the sink apply?

--- STEP B-Q10 (Q10 | coverage-of-unknowns | medium) ---
Paste this as your next message in Chat B:

---

Question: What lineage gaps or unknowns does the graph itself report for this repo? List each, the file/location it applies to, and what the gap means for downstream analysis.

================================================================================
ANSWER SHEET — paste this back to Claude Code when done
================================================================================

Copy each answer exactly as Claude gave it. Keep [blank] if Claude gave no answer.

### Q1 RAW
**Affected Tables:**

1. **`silver/customer_profile`** — **MUST UPDATE CODE**
   - **Why:** Line 48 explicitly joins on `customer_id`:
     ```python
     co = customers_df.join(orders_df, on="customer_id", how="inner")
     ```
     If `customer_id` is renamed to `cust_id` in `orders_raw`, this join will fail with "column not found."

2. **`gold/customer_ltv`** — **MUST UPDATE CODE**
   - **Why:** Lines 27–28 select `customer_id` explicitly in the SQL:
     ```sql
     SELECT customer_id, email, country_code, ...
     FROM {database}.customer_profile
     ```
     If the column is missing from `customer_profile` (propagated from the schema change), the query fails.

3. **`silver/orders_cleaned`** — **Schema affected, but no code change required**
   - **Why:** The table passes `customer_id` through unchanged (no explicit transformation). Renaming in the source automatically propagates to this table's schema, but the notebook's code continues to work—it just produces a table with `cust_id` instead of `customer_id`.

4. **`gold/product_performance`** — **Not affected**
   - **Why:** This table groups by `product_id` and `order_date` only, never references `customer_id`. The schema change in `orders_cleaned` does not impact it.

**Summary:** Two tables require code updates: `customer_profile` (join key) and `customer_ltv` (column selection). Additionally, the DDL in `bronze_tables.sql` should be updated to reflect the renamed column in `orders_raw`.

### Q1 GRAPH
## Affected Tables

**Silver layer tables that need updating:**

1. **`customer_profile`** ✓ MUST UPDATE
   - **Why:** `silver/customer_profile.py` directly uses `customer_id` from `orders_raw` in two critical places:
     - **Inner join** between `customers_df` and `orders_df` on `customer_id`
     - **Aggregation group key** that includes `customer_id`
   - The code will break unless these references are changed to `cust_id`

2. **`orders_cleaned`** ✓ LIKELY NEEDS UPDATE
   - **Why:** `silver/orders_cleaned.py` reads `orders_raw`. While the lineage shows no explicit transformations on `customer_id`, if the code references it by name anywhere (joins, filters, or selects), it will fail. The column is passed through to the output table.

**Gold layer tables:**

3. **`customer_ltv`** ✗ No direct update needed
   - Reads `customer_profile`, not `orders_raw` directly
   - `customer_id` flows through `customer_profile` to `customer_ltv`, but the code groups by `country_code` only and doesn't explicitly reference `customer_id`

4. **`product_performance`** ✗ No update needed
   - Reads `orders_cleaned` but groups by `product_id` and `order_date`; no dependency on `customer_id`

**Summary:** Update at least **`customer_profile`** (certain), and **`orders_cleaned`** (likely, unless the code never references `customer_id` by name).

### Q2 RAW
**File:** `silver/orders_cleaned.py`

**Evidence:** Lines 110–114 at the end of the notebook:

```python
(orders.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.orders_cleaned"))
```

The `{database}` variable resolves to `main.dbdemos_ecom` (imported via `%run ../_resources/setup` from `config.py`), so the full table FQN is `main.dbdemos_ecom.orders_cleaned`.

### Q2 GRAPH
**`silver/orders_cleaned.py`**

This file reads from `main.dbdemos_ecom.orders_raw`, applies cleaning and transformation operations (string trimming, data type conversions, filtering), and writes the result to `main.dbdemos_ecom.orders_cleaned` in overwrite mode.

### Q3 RAW
**Affected Gold Column:** `last_event_ts` in `main.dbdemos_ecom.customer_ltv`

**Evidence:**

In `gold/customer_ltv.py`, lines 27–35, the SQL explicitly selects `last_event_ts` from `customer_profile`:

```sql
WITH base AS (
    SELECT customer_id,
           email,
           country_code,
           signup_ts,
           total_orders,
           lifetime_revenue,
           last_order_ts,
           last_event_ts,  ← directly selected
    FROM {database}.customer_profile
),
```

This column is passed through unchanged into the final `customer_ltv` table (no transformation applied to it). If `last_event_ts` is dropped from `customer_profile`, the query will fail when trying to select a non-existent column, preventing the entire table from being written.

**Note:** `gold/product_performance` is unaffected—it reads from `orders_cleaned`, not `customer_profile`.

### Q3 GRAPH
**Insufficient information to answer definitively.**

The lineage shows that `gold/customer_ltv.py` reads `customer_profile` and derives four output columns:
- `days_since_last_order`
- `ltv_vs_country_avg`
- `revenue_quartile`
- `ltv_tier`

However, the DEGraph does **not** include `source_cols` information for the derives edges in the gold layer. This means I cannot trace which output columns depend on `last_event_ts` from the lineage alone.

**What can be inferred:**
- `days_since_last_order` likely derives from `last_order_ts` (not `last_event_ts`)
- `ltv_vs_country_avg` and `revenue_quartile` likely depend on `lifetime_revenue`
- `ltv_tier` likely depends on LTV metrics

To answer this question with certainty, you would need to:
1. Examine the source code of `gold/customer_ltv.py` to see which columns are actually used in the derivation logic
2. Or request a more detailed lineage output that includes `source_cols` for gold-tier transformations

### Q4 RAW
**Bronze Column Contributing to `gold.customer_ltv.lifetime_revenue`:**

| Source Table | Column |
|---|---|
| `main.dbdemos_ecom.orders_raw` | `total_amount` |

**Lineage Path:**

1. **Bronze:** `orders_raw.total_amount` (source data, unchanged during ingestion)
2. **Silver:** `orders_cleaned.total_amount` ← rounded version: `F.round(F.col("total_amount"), 2)` (silver/orders_cleaned.py, line 64)
3. **Silver:** `customer_profile.lifetime_revenue` ← aggregated: `F.sum("total_amount").alias("lifetime_revenue")` (silver/customer_profile.py, line 94)
4. **Gold:** `customer_ltv.lifetime_revenue` ← passed through unchanged from `customer_profile` (gold/customer_ltv.py, line 28)

**In summary:** Only `orders_raw.total_amount` contributes. It is neither dropped nor joined away; it flows through the entire pipeline from bronze → silver aggregation → gold pass-through, accumulating values per customer via the `SUM` operation.

### Q4 GRAPH
**Tracing backward from `gold.customer_ltv.lifetime_revenue`:**

The `lifetime_revenue` column in the gold `customer_ltv` table originates from the silver `customer_profile` table, where it is created by aggregating orders data. Here are the contributing bronze-layer columns:

| Source Table | Source Column | Role |
|---|---|---|
| `orders_raw` | `total_amount` | **Primary source** — summed to compute `lifetime_revenue` |
| `orders_raw` | `customer_id` | Used to join with customers and as grouping key in aggregation |
| `customers_raw` | `customer_id` | Join key (matches on `orders_raw.customer_id`) |
| `customers_raw` | `email` | Grouping key in the aggregation that produces `lifetime_revenue` |
| `customers_raw` | `country_code` | Grouping key in the aggregation that produces `lifetime_revenue` |
| `customers_raw` | `signup_ts` | Grouping key in the aggregation that produces `lifetime_revenue` |

**Why these columns:**

In `silver/customer_profile.py`, an inner join combines `customers_raw` with `orders_raw` on `customer_id`. The resulting `profile` dataframe is then aggregated with `SUM(total_amount)` grouped by `[customer_id, email, country_code, signup_ts]` to produce `lifetime_revenue`. This aggregated column flows unchanged through the gold layer to `customer_ltv`.

**Note:** `events_raw` is also joined into `customer_profile` but does not contribute to the `lifetime_revenue` computation itself (it's a left join used for other derived columns).

### Q5 RAW
**Changes to the Lineage Graph:**

One `OpaqueTransform` edge is **removed**:
- Currently: `OpaqueTransform` edge from the post-read DataFrame to the withColumn chain, with `is_passthrough=true`
- After removal: The `Reads` edge from `orders_raw` connects directly to the first `Derives` edge (the `currency` withColumn)

**Effect on Column Set of `orders_cleaned`:**

**No change.** The column set remains identical.

**Why:**

`trim_string_columns` is a **passthrough transformation** — it only mutates string column values (strips leading/trailing whitespace). The function definition shows it returns a DataFrame with the identical schema:

```python
def trim_string_columns(df: DataFrame) -> DataFrame:
    """Pure column-set passthrough: the output schema matches the input schema
    column-for-column; only the string values are mutated.
    """
```

The downstream `withColumn`, `drop`, and `filter` operations produce the same output columns regardless of whether the trim call exists, because the trim never changes the column set.

**Summary:**

- **Graph change:** One `OpaqueTransform(is_passthrough=true)` edge is eliminated; the lineage chain becomes slightly shorter
- **Column set impact:** None — `orders_cleaned` retains the same columns with potentially dirtier string values (untrimmed whitespace)

### Q5 GRAPH
**Lineage Graph Change:**

The `opaque_transform` edge representing the `trim_string_columns` call would be removed:

```json
{
  "kind": "opaque_transform",
  "file": "silver/orders_cleaned.py",
  "src": "silver/orders_cleaned.py:orders",
  "tgt": "silver/orders_cleaned.py:orders",
  "operator": "utils.column_transformations.trim_string_columns",
  "is_passthrough": true,
  "opaque_kind": "passthrough"
}
```

The transformation sequence would become:  
**Read** → **Derives** → **Project** → **Filter** → **Write**  
(skipping the trim step)

---

**Effect on Column Set of `orders_cleaned`:**

**No change to the column set.**

The `trim_string_columns` transformation is marked as `is_passthrough: true`, which means:
- It modifies **values** (removes whitespace from string columns) but does **not** change the schema
- All columns from `orders_raw` pass through unchanged
- Removing it would have no effect on which columns appear in the final table

The column set is determined by:
1. Initial columns from `orders_raw` (12 columns)
2. **+** Derived columns (8 new columns: `currency`, `status`, `quantity`, `total_amount`, `order_date`, `order_year`, `order_month`, `is_high_value`)
3. **−** Projected columns removed (2 columns: `_rescued_data`, `source_file`)

Since the trim operation is purely a value-level transformation with no impact on the column structure, removing it would only affect **data quality** (whitespace handling), not the schema of `orders_cleaned`.

### Q6 RAW
**File:** `silver/customer_profile.py`

**Table Produced:** `main.dbdemos_ecom.customer_profile`

**Evidence:**

Line 36 reads `events_raw`:
```python
events_df = spark.table(f"{database}.events_raw")
```

Lines 105–109 write the output:
```python
(customer_profile.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.customer_profile"))
```

Where `{database}` resolves to `main.dbdemos_ecom`, so the output table is `main.dbdemos_ecom.customer_profile`.

### Q6 GRAPH
**File:** `silver/customer_profile.py`

**Table produced:** `main.dbdemos_ecom.customer_profile`

This file reads from three raw tables (`orders_raw`, `customers_raw`, and `events_raw`), joins them, and produces an enriched customer profile table in the silver layer.

### Q7 RAW
**What the Graph CAN Tell You:**

**Static aggregation (resolvable):**
- Column: `order_id`
- Function: `count`
- Output: `order_count`

**Group keys:** `product_id`, `order_date`

**What the Graph CANNOT Tell You:**

- **Which columns** from `orders_cleaned` are aggregated — `metric_columns` is loaded at runtime from an external JSON file:
  ```python
  config_path = os.environ.get("METRIC_CONFIG_PATH", "/dbfs/configs/metrics.json")
  metric_columns = json.load(f)["product_performance"]
  ```
  The SafeEvaluator cannot resolve environment variables or file I/O.

- **What aggregation functions** are applied to those columns — The loop creates `sum`, `avg`, and `max` for each metric:
  ```python
  aggregations.append(F.sum(col_name).alias(f"sum_{col_name}"))
  aggregations.append(F.avg(col_name).alias(f"avg_{col_name}"))
  aggregations.append(F.max(col_name).alias(f"max_{col_name}"))
  ```
  But the actual columns processed are unknown.

- **Output column names** — Dynamically named as `sum_<col>`, `avg_<col>`, `max_<col>`, but the exact set is unresolvable.

**Graph Representation:**

Per the DEGraph documentation (lines 18–28), the extractor emits **one `Aggregates` edge** with:
- `dynamic=True`
- `agg_ops=[count("order_id"), <unresolved>]`
- `output_cols=[order_count, <unresolved>]`
- `dynamic_note` explaining: "The metric columns and their aggregations are loaded from an external JSON config file determined by an environment variable and cannot be statically enumerated."

### Q7 GRAPH
**What the graph clearly shows:**

| Aggregation Function | Output Column | Source Column(s) |
|---|---|---|
| `count` | `order_count` | (implicit—counting rows per group) |

**Grouping keys:** `product_id`, `order_date`

---

**What the graph cannot tell you:**

The lineage contains a critical limitation flagged in the warnings section:

```json
{
  "category": "dynamic-aggregation",
  "message": "agg list contains star-unpacked runtime variable in gold/product_performance.py:81"
}
```

The aggregates edge shows:

```json
"agg_ops": [
  "<unresolved>",
  "count"
],
"output_cols": [
  "<unresolved>",
  "order_count"
],
"dynamic": true,
"dynamic_note": "agg list contains star-unpacked runtime variable..."
```

This means:
- **At least one additional aggregation** is constructed dynamically at runtime using a variable (marked `<unresolved>`)
- The actual aggregation functions applied to that unresolved aggregation cannot be determined statically
- The source columns feeding those aggregations are unknown
- The total number of aggregations and output columns cannot be enumerated

**To answer fully, you would need to:**
1. Examine line 81 of `gold/product_performance.py` to see how the aggregation list is built
2. Identify what columns and functions are included in the runtime-constructed variable

### Q8 RAW
**At-Risk Columns:**

1. **`silver.customer_profile.last_event_ts`** — Directly at risk
2. **`gold.customer_ltv.last_event_ts`** — Indirectly at risk (cascaded from silver)

**Why These Columns Become Unreliable:**

**In `silver/customer_profile.py` (lines 48–53):**
```python
events_window = Window.partitionBy("customer_id").orderBy(F.col("event_ts").desc())
events_ranked = events_df.withColumn("rn", F.row_number().over(events_window))
events_latest = events_ranked.filter(F.col("rn") == 1).drop("rn")  # ← selects row with rn=1
```

If `event_ts` becomes nullable:
- **NULL handling in ORDER BY:** Spark sorts NULLs to the beginning of descending order (before non-NULL values)
- **Unstable selection:** When a customer has events with both NULL and non-NULL `event_ts` values, `row_number()` may unpredictably assign `rn=1` to a row with NULL `event_ts` instead of the actual latest (highest) timestamp
- **Cascading effect:** Line 99 aggregates this unreliable column:
  ```python
  F.max("event_ts_last_event").alias("last_event_ts"),
  ```
  If `event_ts_last_event` contains NULL (or was selected unpredictably), the aggregation propagates the unreliability

**Downstream Impact:**

In `gold/customer_ltv.py` (line 28), `last_event_ts` is passed through unchanged:
```sql
SELECT ... last_event_ts FROM {database}.customer_profile
```

So the unreliability cascades to the gold table.

**Summary:** Only `last_event_ts` columns in both silver and gold are at risk. Other event columns (e.g., `event_id`, `event_type`, `payload`) are joined but not explicitly written to the final tables, so they don't propagate downstream.

### Q8 GRAPH
**At Risk in Silver Layer:**

**Directly affected columns in `silver.customer_profile`:**
- `event_ts_last_event` — directly derived from `event_ts` of the selected "latest" event
- `last_event_ts` — aggregated via `MAX(event_ts_last_event)`

**Indirectly affected columns in `silver.customer_profile`:**
- `event_id_last_event`
- `event_type_last_event`
- `product_id_last_event`
- `session_id_last_event`
- `ingested_ts_last_event`
- `payload_last_event`
- `_rescued_data_last_event`

All these `*_last_event` columns derive from the same row selected via the window function `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC)`. If `event_ts` becomes nullable, the sort order becomes **undefined** (NULLs sort position is database-dependent), making it unpredictable which event is ranked as "1", thus invalidating all event columns for that "latest" record.

---

**Potential Impact in Gold Layer:**

The lineage shows that `gold.customer_ltv` reads `customer_profile` with `projects: [kept_cols: "*"]`, meaning all columns (including the compromised event columns) flow downstream. However, **the graph cannot definitively identify which gold columns depend on these event values** because:

- The `derives` edges in `customer_ltv` lack `source_cols` information
- We cannot determine if `days_since_last_order`, `ltv_vs_country_avg`, `revenue_quartile`, or `ltv_tier` use any event-related columns

**To fully assess gold-layer risk, you would need to examine the source code** to see if any of those derived metrics incorporate event timestamps or event-based features.

---

**Why this matters:**

Nullable `event_ts` breaks a **critical assumption** in the window function: that timestamps can be reliably ordered. The lineage shows the mechanism but **cannot tell you** whether the code includes NULL-handling logic (e.g., `NULLS FIRST/LAST`, filtering NULLs before the window, etc.) that might mitigate this risk.

### Q9 RAW
**Target Table:** `main.dbdemos_ecom.product_performance`

**Write Mode:** `merge`

**Merge Keys:** `["product_id", "order_date"]`

**Evidence:**

Lines 50–55 in `gold/product_performance.py`:

```python
sink = DeltaMergeSink(
    target_table=f"{database}.product_performance",
    merge_keys=["product_id", "order_date"],
    mode="merge",
)
sink.run(daily_perf)
```

Where `{database}` resolves to `main.dbdemos_ecom` (from `config.py`).

The `DeltaMergeSink` class is registered in `.degraph/sinks.json`, allowing the DEGraph extractor to resolve all three parameters explicitly and emit a proper `Writes` edge with `sink_class="DeltaMergeSink"`, `mode="merge"`, and the merge keys populated.

### Q9 GRAPH
**Target Table:** `main.dbdemos_ecom.product_performance`

**Write Mode:** `merge` (Delta Lake merge operation)

**Merge Keys:** 
- `product_id`
- `order_date`

**Additional Details:**
- Sink class: `DeltaMergeSink` — indicates a Delta Lake merge sink
- Streaming: `false` — batch operation
- Format: Delta (implicit from sink class)

This merge operation means that incoming rows from `gold/product_performance.py` are matched against existing rows in `product_performance` using the composite key `(product_id, order_date)`. Matching rows are updated, and non-matching rows are inserted, providing upsert semantics for daily product performance aggregations.

### Q10 RAW
**Lineage Gaps and Unknowns Reported by the Graph:**

---

### **Gap 1: Unregistered Helper — `parse_event_payload`**

**File/Location:** `bronze/03_ingest_events.py`, line ~30 (call to `parse_event_payload(events_landing)`)

**Graph Representation:**
- `OpaqueTransform` edge with `is_passthrough=false`
- `GraphWarning` naming "unregistered helper parse_event_payload"

**What the Gap Means:**
The extractor cannot determine which columns are added, removed, or renamed by this helper. Column-level lineage is severed at this point. 

**Actual behavior (from `utils/event_parsers.py`):**
- **Removes:** `payload` column
- **Adds:** `page_url`, `referrer`, `user_agent`, `cart_total` (parsed from JSON payload)

**Downstream Impact:**
- Column-level lineage from `events_raw` to downstream tables (`customer_profile`, `customer_ltv`) is incomplete
- Impact analysis cannot reliably determine which source event columns contribute to later tables

---

### **Gap 2: Dynamic Aggregation List**

**File/Location:** `gold/product_performance.py`, lines 47–49 (the `.agg(*aggregations, ...)` call)

**Graph Representation:**
- `Aggregates` edge with `dynamic=True`
- `agg_ops=[count("order_id"), <unresolved>]`
- `output_cols=[order_count, <unresolved>]`
- `dynamic_note` explaining why enumeration failed

**What the Gap Means:**
The extractor cannot determine which columns from `orders_cleaned` are aggregated or which aggregation functions are applied to them (beyond the static `count("order_id")`). The complete output schema of `product_performance` is unknown.

**Why (lines 28–32):**
```python
config_path = os.environ.get("METRIC_CONFIG_PATH", "/dbfs/configs/metrics.json")
metric_columns = json.load(f)["product_performance"]
```
The metric columns and aggregation specification come from an environment variable and external JSON file, which the SafeEvaluator cannot resolve.

**Downstream Impact:**
- Cannot determine the full set of columns in `product_performance`
- Cannot trace which source columns contribute to which output metrics
- Impact analysis on `orders_cleaned` columns is incomplete with respect to `product_performance`

---

**Summary:** Two gaps in total. The first prevents column-level traceability through an unregistered utility function; the second hides a dynamic aggregation controlled by runtime configuration.

### Q10 GRAPH
**Lineage Gaps and Unknowns Reported:**

| File/Location | Category | Gap Description | Impact on Downstream Analysis |
|---|---|---|---|
| `bronze/03_ingest_events.py` | `opaque-call-fallback` | Unregistered function `utils.event_parsers.parse_event_payload` called with DataFrame argument; **column-set changes across this call are unknown** | Cannot determine which columns are added, removed, or transformed by the event parsing step. The column output of `events_raw` may not be fully understood. Downstream consumers (`silver/customer_profile.py`) could be missing lineage information for critical event columns. |
| `gold/product_performance.py:81` | `dynamic-aggregation` | Aggregation list constructed at runtime using star-unpacked variable; **agg_ops and output_cols cannot be enumerated statically** | Cannot determine which columns from `orders_cleaned` are actually aggregated or what aggregation functions (beyond the one explicit `count`) are applied. Column-level lineage to `product_performance` output is incomplete. Impact analysis for changes to source columns is unreliable. |
| `main.dbdemos_ecom.products_raw` | `orphan-table` | Table appears in DDL but is **never read or written by any file in this repo** | Dead code / unused data asset. No lineage path to any downstream tables. Indicates incomplete pipeline or obsolete schema definition. Not a technical lineage gap, but signals data governance issue. |

---

**Summary Impact:**

- **Column-level impact tracking broken:** You cannot reliably answer "which source columns feed into X output column" for events in `events_raw` → `customer_profile` or for aggregations in `product_performance`
- **Downstream transformation assumptions at risk:** Any code depending on specific column presence/absence in these tables is not fully validated by the lineage graph
- **Impact analysis incomplete:** Changes to columns in `orders_cleaned` or `events_raw` cannot be fully traced to gold-layer outputs
