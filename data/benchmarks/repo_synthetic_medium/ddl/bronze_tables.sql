-- ============================================================================
-- Bronze layer DDL — e-commerce synthetic benchmark (MEDIUM)
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
--
-- This MEDIUM benchmark extends the SMALL benchmark with reverse-logistics
-- (returns, refunds), inventory snapshots, marketing attribution, promotion
-- definitions, and shipping events — six additional bronze tables on top of
-- the original four (orders, customers, products, events).
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

-- ----------------------------------------------------------------------------
-- MEDIUM-ONLY: reverse logistics, inventory, marketing, fulfillment
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS main.dbdemos_ecom.returns_raw (
  return_id        STRING        COMMENT 'Return RMA UUID; primary key',
  order_id         STRING        COMMENT 'FK to orders_raw.order_id; the original order being returned',
  customer_id      STRING        COMMENT 'FK to customers_raw.customer_id; denormalized for join economy',
  product_id       STRING        COMMENT 'FK to products_raw.product_id; the returned SKU',
  return_qty       INT           COMMENT 'Units returned; may be less than original order qty (partial return)',
  reason_code      STRING        COMMENT 'one of: defective, wrong_item, no_longer_wanted, size_fit, other',
  return_ts        TIMESTAMP     COMMENT 'When the customer initiated the return',
  warehouse_id     STRING        COMMENT 'Receiving warehouse; FK to inventory_snapshots_raw.warehouse_id',
  ingested_ts      TIMESTAMP     COMMENT 'When this row was ingested into bronze',
  source_file      STRING        COMMENT 'Auto Loader: originating file path',
  _rescued_data    STRING        COMMENT 'Auto Loader: malformed-record salvage column'
) USING DELTA
  COMMENT 'Raw return RMA events ingested from the reverse-logistics CDC stream.';

CREATE TABLE IF NOT EXISTS main.dbdemos_ecom.refunds_raw (
  refund_id        STRING        COMMENT 'Refund transaction UUID; primary key',
  return_id        STRING        COMMENT 'FK to returns_raw.return_id; the RMA this refund settles',
  order_id         STRING        COMMENT 'FK to orders_raw.order_id; denormalized for join economy',
  refund_amount    DECIMAL(18,2) COMMENT 'Amount credited back to the customer in original order currency',
  currency         STRING        COMMENT 'ISO 4217 currency code; should match originating order',
  payment_method   STRING        COMMENT 'one of: credit_card, paypal, store_credit, bank_transfer',
  processed_ts     TIMESTAMP     COMMENT 'When the payment processor confirmed the refund',
  ingested_ts      TIMESTAMP     COMMENT 'When this row was ingested into bronze',
  source_file      STRING        COMMENT 'Source file path'
) USING DELTA
  COMMENT 'Raw refund transactions emitted by the payment-processor webhook.';

CREATE TABLE IF NOT EXISTS main.dbdemos_ecom.inventory_snapshots_raw (
  snapshot_id      STRING        COMMENT 'Snapshot UUID; primary key for the (warehouse, product, day) grain',
  warehouse_id     STRING        COMMENT 'Warehouse code; e.g. WH-US-WEST-01',
  product_id       STRING        COMMENT 'FK to products_raw.product_id',
  on_hand_units    INT           COMMENT 'Units physically present in this warehouse at snapshot time',
  reserved_units   INT           COMMENT 'Units allocated to open orders but not yet shipped',
  available_units  INT           COMMENT 'on_hand_units - reserved_units (denormalized at ingest)',
  snapshot_ts      TIMESTAMP     COMMENT 'When the warehouse system emitted this snapshot (typically daily)',
  ingested_ts      TIMESTAMP     COMMENT 'When this row was ingested into bronze',
  source_file      STRING        COMMENT 'Source file path'
) USING DELTA
  COMMENT 'Daily per-(warehouse, product) inventory snapshots from the WMS.';

CREATE TABLE IF NOT EXISTS main.dbdemos_ecom.marketing_attribution_raw (
  visit_id         STRING        COMMENT 'Visit UUID; primary key for a single inbound landing visit',
  session_id       STRING        COMMENT 'FK to events_raw.session_id; ties the visit to behavioral events',
  customer_id      STRING        COMMENT 'FK to customers_raw.customer_id; NULL for pre-login attribution',
  utm_source       STRING        COMMENT 'e.g. google, facebook, newsletter, organic',
  utm_medium       STRING        COMMENT 'e.g. cpc, email, social, referral',
  utm_campaign     STRING        COMMENT 'Free-form campaign identifier set by marketing',
  landing_url      STRING        COMMENT 'First-touch landing page URL on this visit',
  visit_ts         TIMESTAMP     COMMENT 'When the visit began',
  ingested_ts      TIMESTAMP     COMMENT 'When this row was ingested into bronze',
  source_file      STRING        COMMENT 'Source file path'
) USING DELTA
  COMMENT 'Raw inbound attribution records emitted by the front-end UTM tracker.';

CREATE TABLE IF NOT EXISTS main.dbdemos_ecom.promotions_raw (
  promo_code       STRING        COMMENT 'Promotion code; primary key',
  promo_type       STRING        COMMENT 'one of: percent_off, flat_off, free_shipping, bogo',
  discount_value   DECIMAL(18,2) COMMENT 'Numeric discount: percent (0-100) or flat amount, semantics vary by promo_type',
  min_order_amount DECIMAL(18,2) COMMENT 'Minimum cart subtotal to apply the promo; NULL = no minimum',
  starts_ts        TIMESTAMP     COMMENT 'Promo activation timestamp',
  ends_ts          TIMESTAMP     COMMENT 'Promo expiration timestamp',
  active           BOOLEAN       COMMENT 'Hard kill switch independent of starts_ts / ends_ts',
  ingested_ts      TIMESTAMP     COMMENT 'When this row was ingested into bronze',
  source_file      STRING        COMMENT 'Source file path'
) USING DELTA
  COMMENT 'Promotion definitions ingested daily from the marketing CMS.';

CREATE TABLE IF NOT EXISTS main.dbdemos_ecom.shipments_raw (
  shipment_id      STRING        COMMENT 'Shipment UUID; primary key (one per parcel)',
  order_id         STRING        COMMENT 'FK to orders_raw.order_id; an order may have multiple shipments',
  carrier          STRING        COMMENT 'one of: ups, fedex, usps, dhl, local_courier',
  tracking_number  STRING        COMMENT 'Carrier-issued tracking ID',
  warehouse_id     STRING        COMMENT 'Originating warehouse; FK to inventory_snapshots_raw.warehouse_id',
  ship_ts          TIMESTAMP     COMMENT 'When the parcel left the warehouse',
  delivered_ts     TIMESTAMP     COMMENT 'When the carrier reported delivery; NULL until confirmed',
  status           STRING        COMMENT 'one of: label_created, in_transit, delivered, lost, returned_to_sender',
  ingested_ts      TIMESTAMP     COMMENT 'When this row was ingested into bronze',
  source_file      STRING        COMMENT 'Auto Loader: originating file path',
  _rescued_data    STRING        COMMENT 'Auto Loader: malformed-record salvage column'
) USING DELTA
  COMMENT 'Raw fulfillment / shipping events ingested from carrier webhooks via Auto Loader.';
