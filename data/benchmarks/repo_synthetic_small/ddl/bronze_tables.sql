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
