# DEGraph DSL v1 — data-lineage graph in compact line format.
# Read each line as ONE record. Sections marked by @HEADERS.
#
# Symbols:
#   T<n>  table        (defined in @TABLES; reference elsewhere)
#   D<n>  dataframe    (file-local intermediate; reference in edges)
#   X<n>  predicate    (defined in @PREDICATES; referenced as X<n> in rule_logic)
#
# Sections (in order):
#   @TABLES    one per line: T<n>|<fqn>|<col1,col2,...>|by=<writer_file>|rb=<reader_file1,...>
#              by= the file that PRODUCES (writes/creates) this table — its WRITER/PRODUCER
#              rb= the file(s) that CONSUME this table as input — its READERS/CONSUMERS
#              IMPORTANT: by= ≠ consumer; rb= ≠ producer. by= is the source/writer; rb= is the sink/reader.
#   @PROV      column provenance:  <Tn>.<col>=<role>:<src1>,<src2>[;op=<f>][;win=<spec>]
#              roles: P=passthrough  D=derived  A=aggregate  G=group_key
#              sources are either "<Tn>.<col>" or bare column names (file-local)
#   @PREDS     X<n>=<predicate text>
#   @RULES    named when-chain Python vars.  Two shapes:
#              FIRST-MATCH (has else): <var>=X1=>v;X2=>v;else=>v
#                — returns the FIRST matching value; if nothing fires returns else-value (often NULL).
#              ARRAY-ACC (no else):   <var>=X1=>v;X2=>v;X3=>v
#                — ALL matching predicates fire; results collected into an array;
#                  if nothing fires the array is EMPTY [] (NOT null).
#              To distinguish: look for `else=>` at the end of the rule chain.
#              [→col] suffix on a rule means that rule's result is stored directly in col
#              (confirmed by a cv= annotation on the D-edge in @EDGES).
#   @PDICTS   shared predicate dicts:  <var>|<label>=<X<n> or text>
#   @PUSE     predicate cross-reference:  X<n>:<rule_or_edge>,<rule_or_edge>,...
#              lists every named rule + derives edge that references X<n>.
#              If two entries share X<n>, they implement IDENTICAL predicate logic —
#              when X<n> fires, ALL listed columns/rules respond to the same condition.
#              CRITICAL: this means first-match columns (else in @RULES) and
#              array-accumulator columns (no else in @RULES) that share an X<n>
#              can NEVER produce conflicting NULL-vs-non-empty states:
#              if no X<n> fires → first-match = NULL AND array-acc = [] (both empty).
#   @EDGES    one edge per line, code-prefixed:
#     R|<Tn>|<Dn>[|pc=c1,c2][|stream]                  reads table->df
#     W|<Dn>|<Tn>|<mode>[|<fmt>][|mk=k1,k2][|pc=p1,p2][|sink=<class>][|kw=k=v,...] writes df->table
#          sink= custom sink class (e.g. MergeSink) used instead of df.write.saveAsTable
#          kw=   extra constructor kwargs for the sink (e.g. enable_leap_column=True)
#     D|<Dn>|<Dn>|<out>[|sc=c1,c2][|ex=<expr>][|cv=<var>][|rl=X1=>v;...;else=>v][|ws=part(...)/ord(...)][|sf=...][|nf=f1,f2]
#                                                       derives one new column
#                  ex= source expression text (truncated to 80 chars) for non-when-chain derives.
#                      Gives the LLM the actual PySpark/SQL expression rather than just the col list.
#                  cv= named Python Column variable from @RULES used as the expression.
#                      Trace: @RULES entry <var> → this output column.
#                  sf = struct field list (array(struct(...)) outputs).
#                       format: <field>:<kind>=<value> separated by ';'
#                       kinds: L=literal C=column E=expr
#                       For E with nested when-chain, value is:
#                         <expr-text>~~rl=<X1=>v;X2=>v;else=>v>
#                       where ~~rl= delimits the inner conditional rules.
#                  nf = lit-null placeholder field names (collapsed)
#     F|<Dn>|<Dn>[|rc=c1,c2][|pr=<pred>]               filters df->df (row restrict)
#          pr= predicate expression text (truncated to 80 chars); gives the actual filter condition.
#     P|<Dn>|<Dn>[|rm=c1,c2][|kp=c1,c2|*]              projects df->df (col restrict)
#     J|<Dn>|<Dn>|<Dn>|<type>[|jk=l1=r1,l2=r2][|rpc=...][|lpc=...]
#                                                       join (left,right -> target)
#     A|<Dn>|<Dn>[|by=<file>]|gk=k1,k2|out=col=op(in);col=op(in)[|dyn=<note>]   aggregates
#          by= the file this aggregation is performed IN (disambiguates same-shaped GROUP BYs across files)
#          gk= aggregate GROUP key — NOT the same as a window part() partition key
#          dyn= agg column list cannot be enumerated statically (runtime variable); note explains why
#     O|<Dn>|<Dn>|<operator>[|pass]                    opaque helper transform
#          pass = column-set passthrough (output has SAME columns as input;
#                 values may change but NO columns are added or removed;
#                 removing a |pass transform NEVER changes the column set)
#   @WARN     <file>|<category>|<message>
#
# Reverse-lineage rule: to find "which input columns supply VALUES to T<n>.col",
# read @PROV first. If T<n>.col is absent from @PROV, fall back to @EDGES.
# Join keys (jk=) and group keys (gk=) describe row-matching, NOT value sources.
#
# Parsing note: '|' is the FIELD separator AND a legitimate Python operator
# (bitwise/logical OR) inside predicate text. Predicate text always appears
# AFTER a fixed prefix (X<n>=, var=, label=, rl=, ws=, sf=) and runs to end-of-
# line. Split each record on '|' from the LEFT for the first few structural
# fields, then treat the remainder as a single body. Pipes inside that body
# are part of the expression, not new fields.

@TABLES
T0|main.dbdemos_ecom.orders_raw|order_id,customer_id,product_id,quantity,unit_price,total_amount,currency,status,order_ts,ingested_ts,source_file,_rescued_data|by=bronze/01_ingest_orders.py|rb=silver/customer_profile.py,silver/orders_cleaned.py,silver/refunds_enriched.py,silver/shipments_timeline.py
T1|main.dbdemos_ecom.customers_raw|customer_id,email,first_name,last_name,country_code,signup_ts,marketing_opt_in,ingested_ts,source_file,_rescued_data|by=bronze/02_ingest_customers.ipynb|rb=silver/customer_profile.py
T2|main.dbdemos_ecom.products_raw|product_id,name,category,list_price,active,ingested_ts,source_file|by=bronze/04_ingest_products.py|rb=
T3|main.dbdemos_ecom.events_raw|event_id,customer_id,event_type,product_id,session_id,event_ts,ingested_ts,payload,_rescued_data|by=bronze/03_ingest_events.py|rb=gold/attribution_funnel_daily.py,silver/customer_profile.py,silver/marketing_touchpoints.py
T4|main.dbdemos_ecom.returns_raw|return_id,order_id,customer_id,product_id,return_qty,reason_code,return_ts,warehouse_id,ingested_ts,source_file,_rescued_data|by=bronze/05_ingest_returns.py|rb=silver/refunds_enriched.py,silver/returns_cleaned.py
T5|main.dbdemos_ecom.refunds_raw|refund_id,return_id,order_id,refund_amount,currency,payment_method,processed_ts,ingested_ts,source_file|by=bronze/06_ingest_refunds.py|rb=silver/refunds_enriched.py
T6|main.dbdemos_ecom.inventory_snapshots_raw|snapshot_id,warehouse_id,product_id,on_hand_units,reserved_units,available_units,snapshot_ts,ingested_ts,source_file|by=bronze/07_ingest_inventory.py|rb=silver/inventory_daily.py
T7|main.dbdemos_ecom.marketing_attribution_raw|visit_id,session_id,customer_id,utm_source,utm_medium,utm_campaign,landing_url,visit_ts,ingested_ts,source_file|by=bronze/08_ingest_marketing.py|rb=silver/marketing_touchpoints.py
T8|main.dbdemos_ecom.promotions_raw|promo_code,promo_type,discount_value,min_order_amount,starts_ts,ends_ts,active,ingested_ts,source_file|by=bronze/09_ingest_promotions.py|rb=
T9|main.dbdemos_ecom.shipments_raw|shipment_id,order_id,carrier,tracking_number,warehouse_id,ship_ts,delivered_ts,status,ingested_ts,source_file,_rescued_data|by=bronze/10_ingest_shipments.py|rb=silver/shipments_timeline.py
T10|<unresolved>||by=|rb=bronze/04_ingest_products.py
T11|main.dbdemos_ecom.marketing_touchpoints|customer_id,utm_campaign,event_count,last_seen_ts,session_count|by=silver/marketing_touchpoints.py|rb=gold/attribution_funnel_daily.py
T12|main.dbdemos_ecom.orders_cleaned|order_id,customer_id,product_id,quantity,unit_price,total_amount,currency,status,order_ts,ingested_ts,order_date,order_year,order_month,is_high_value|by=silver/orders_cleaned.py|rb=gold/attribution_funnel_daily.py,gold/inventory_turnover_daily.py,gold/product_performance.py
T13|main.dbdemos_ecom.attribution_funnel_daily|v.utm_campaign,v.funnel_date,v.visit_count,cart_count,checkout_count,purchase_count,conversion_rate|by=gold/attribution_funnel_daily.py|rb=
T14|main.dbdemos_ecom.customer_profile|customer_id,email,country_code,signup_ts,total_orders,lifetime_revenue,last_order_ts,last_event_ts,distinct_products_ordered|by=silver/customer_profile.py|rb=gold/customer_ltv.py,gold/customer_return_rate.py
T15|main.dbdemos_ecom.customer_ltv|customer_id,email,country_code,signup_ts,total_orders,lifetime_revenue,last_order_ts,last_event_ts,days_since_last_order,ltv_vs_country_avg,revenue_quartile,ltv_tier|by=gold/customer_ltv.py|rb=gold/revenue_forecast_features.py
T16|main.dbdemos_ecom.returns_cleaned|return_id,order_id,customer_id,product_id,return_qty,reason_code,return_ts,warehouse_id,ingested_ts,return_date,is_high_qty|by=silver/returns_cleaned.py|rb=gold/customer_return_rate.py
T17|main.dbdemos_ecom.customer_return_rate|cb.customer_id,cb.email,cb.country_code,cb.total_orders,cb.lifetime_revenue,total_returns,total_return_qty,return_rate|by=gold/customer_return_rate.py|rb=
T18|main.dbdemos_ecom.inventory_daily|snapshot_id,warehouse_id,product_id,on_hand_units,reserved_units,available_units,snapshot_ts,ingested_ts,source_file,snapshot_date,utilization_pct|by=silver/inventory_daily.py|rb=gold/inventory_turnover_daily.py
T19|main.dbdemos_ecom.inventory_turnover_daily|inv.warehouse_id,inv.product_id,inv.snapshot_date,inv.on_hand_units,inv.reserved_units,inv.available_units,inv.utilization_pct,sold_qty,sales_count,turnover_rate,days_of_stock|by=gold/inventory_turnover_daily.py|rb=gold/revenue_forecast_features.py
T20|main.dbdemos_ecom.product_performance|product_id,order_date,order_count|by=gold/product_performance.py|rb=gold/revenue_forecast_features.py
T21|main.dbdemos_ecom.revenue_forecast_features|customer_id,product_id,forecast_date,lifetime_revenue,total_orders,avg_turnover|by=gold/revenue_forecast_features.py|rb=
T22|main.dbdemos_ecom.refunds_enriched|refund_id,return_id,order_id,refund_amount,currency,payment_method,processed_ts,ingested_ts,source_file,customer_id,product_id,return_qty,reason_code,return_ts,warehouse_id,_rescued_data,quantity,unit_price,total_amount,status,order_ts,refund_ratio,days_to_refund,refund_date|by=silver/refunds_enriched.py|rb=
T23|main.dbdemos_ecom.shipments_timeline|order_id,shipment_count,first_ship_ts,last_delivered_ts,carriers_used,customer_id|by=silver/shipments_timeline.py|rb=

@PROV
T0.order_id=P:ext:/Volumes/main/dbdemos_ecom/landing/orders.order_id
T0.customer_id=P:ext:/Volumes/main/dbdemos_ecom/landing/orders.customer_id
T0.product_id=P:ext:/Volumes/main/dbdemos_ecom/landing/orders.product_id
T0.quantity=P:ext:/Volumes/main/dbdemos_ecom/landing/orders.quantity
T0.unit_price=P:ext:/Volumes/main/dbdemos_ecom/landing/orders.unit_price
T0.total_amount=P:ext:/Volumes/main/dbdemos_ecom/landing/orders.total_amount
T0.currency=P:ext:/Volumes/main/dbdemos_ecom/landing/orders.currency
T0.status=P:ext:/Volumes/main/dbdemos_ecom/landing/orders.status
T0.order_ts=P:ext:/Volumes/main/dbdemos_ecom/landing/orders.order_ts
T0.ingested_ts=D:
T0.source_file=D:_metadata.file_path
T0._rescued_data=P:ext:/Volumes/main/dbdemos_ecom/landing/orders._rescued_data
T1.customer_id=P:ext:/Volumes/main/dbdemos_ecom/landing/customers.customer_id
T1.email=P:ext:/Volumes/main/dbdemos_ecom/landing/customers.email
T1.first_name=P:ext:/Volumes/main/dbdemos_ecom/landing/customers.first_name
T1.last_name=P:ext:/Volumes/main/dbdemos_ecom/landing/customers.last_name
T1.country_code=P:ext:/Volumes/main/dbdemos_ecom/landing/customers.country_code
T1.signup_ts=P:ext:/Volumes/main/dbdemos_ecom/landing/customers.signup_ts
T1.marketing_opt_in=P:ext:/Volumes/main/dbdemos_ecom/landing/customers.marketing_opt_in
T1.ingested_ts=D:
T1.source_file=D:_metadata.file_path
T1._rescued_data=P:ext:/Volumes/main/dbdemos_ecom/landing/customers._rescued_data
T2.product_id=P:T10.product_id
T2.name=P:T10.name
T2.category=P:T10.category
T2.list_price=P:T10.list_price
T2.active=P:T10.active
T2.ingested_ts=D:
T2.source_file=D:
T3.ingested_ts=D:
T4.return_id=P:ext:/Volumes/main/dbdemos_ecom/landing/returns.return_id
T4.order_id=P:ext:/Volumes/main/dbdemos_ecom/landing/returns.order_id
T4.customer_id=P:ext:/Volumes/main/dbdemos_ecom/landing/returns.customer_id
T4.product_id=P:ext:/Volumes/main/dbdemos_ecom/landing/returns.product_id
T4.return_qty=P:ext:/Volumes/main/dbdemos_ecom/landing/returns.return_qty
T4.reason_code=P:ext:/Volumes/main/dbdemos_ecom/landing/returns.reason_code
T4.return_ts=P:ext:/Volumes/main/dbdemos_ecom/landing/returns.return_ts
T4.warehouse_id=P:ext:/Volumes/main/dbdemos_ecom/landing/returns.warehouse_id
T4.ingested_ts=D:
T4.source_file=D:_metadata.file_path
T4._rescued_data=P:ext:/Volumes/main/dbdemos_ecom/landing/returns._rescued_data
T5.refund_id=P:ext:/Volumes/main/dbdemos_ecom/landing/refunds/*.json.refund_id
T5.return_id=P:ext:/Volumes/main/dbdemos_ecom/landing/refunds/*.json.return_id
T5.order_id=P:ext:/Volumes/main/dbdemos_ecom/landing/refunds/*.json.order_id
T5.refund_amount=P:ext:/Volumes/main/dbdemos_ecom/landing/refunds/*.json.refund_amount
T5.currency=P:ext:/Volumes/main/dbdemos_ecom/landing/refunds/*.json.currency
T5.payment_method=P:ext:/Volumes/main/dbdemos_ecom/landing/refunds/*.json.payment_method
T5.processed_ts=P:ext:/Volumes/main/dbdemos_ecom/landing/refunds/*.json.processed_ts
T5.ingested_ts=D:
T5.source_file=D:
T6.snapshot_id=P:ext:/Volumes/main/dbdemos_ecom/landing/inventory_snapshots/dt=*.snapshot_id
T6.warehouse_id=P:ext:/Volumes/main/dbdemos_ecom/landing/inventory_snapshots/dt=*.warehouse_id
T6.product_id=P:ext:/Volumes/main/dbdemos_ecom/landing/inventory_snapshots/dt=*.product_id
T6.on_hand_units=P:ext:/Volumes/main/dbdemos_ecom/landing/inventory_snapshots/dt=*.on_hand_units
T6.reserved_units=P:ext:/Volumes/main/dbdemos_ecom/landing/inventory_snapshots/dt=*.reserved_units
T6.available_units=P:ext:/Volumes/main/dbdemos_ecom/landing/inventory_snapshots/dt=*.available_units
T6.snapshot_ts=P:ext:/Volumes/main/dbdemos_ecom/landing/inventory_snapshots/dt=*.snapshot_ts
T6.ingested_ts=D:
T6.source_file=D:
T7.visit_id=P:ext:/Volumes/main/dbdemos_ecom/landing/marketing_attribution.visit_id
T7.session_id=P:ext:/Volumes/main/dbdemos_ecom/landing/marketing_attribution.session_id
T7.customer_id=P:ext:/Volumes/main/dbdemos_ecom/landing/marketing_attribution.customer_id
T7.utm_source=P:ext:/Volumes/main/dbdemos_ecom/landing/marketing_attribution.utm_source
T7.utm_medium=P:ext:/Volumes/main/dbdemos_ecom/landing/marketing_attribution.utm_medium
T7.utm_campaign=P:ext:/Volumes/main/dbdemos_ecom/landing/marketing_attribution.utm_campaign
T7.landing_url=P:ext:/Volumes/main/dbdemos_ecom/landing/marketing_attribution.landing_url
T7.visit_ts=P:ext:/Volumes/main/dbdemos_ecom/landing/marketing_attribution.visit_ts
T7.ingested_ts=D:
T7.source_file=D:_metadata.file_path
T8.promo_code=P:ext:/Volumes/main/dbdemos_ecom/landing/promotions/latest.json.promo_code
T8.promo_type=P:ext:/Volumes/main/dbdemos_ecom/landing/promotions/latest.json.promo_type
T8.discount_value=P:ext:/Volumes/main/dbdemos_ecom/landing/promotions/latest.json.discount_value
T8.min_order_amount=P:ext:/Volumes/main/dbdemos_ecom/landing/promotions/latest.json.min_order_amount
T8.starts_ts=P:ext:/Volumes/main/dbdemos_ecom/landing/promotions/latest.json.starts_ts
T8.ends_ts=P:ext:/Volumes/main/dbdemos_ecom/landing/promotions/latest.json.ends_ts
T8.active=P:ext:/Volumes/main/dbdemos_ecom/landing/promotions/latest.json.active
T8.ingested_ts=D:
T8.source_file=D:
T9.shipment_id=P:ext:/Volumes/main/dbdemos_ecom/landing/shipments.shipment_id
T9.order_id=P:ext:/Volumes/main/dbdemos_ecom/landing/shipments.order_id
T9.carrier=P:ext:/Volumes/main/dbdemos_ecom/landing/shipments.carrier
T9.tracking_number=P:ext:/Volumes/main/dbdemos_ecom/landing/shipments.tracking_number
T9.warehouse_id=P:ext:/Volumes/main/dbdemos_ecom/landing/shipments.warehouse_id
T9.ship_ts=P:ext:/Volumes/main/dbdemos_ecom/landing/shipments.ship_ts
T9.delivered_ts=P:ext:/Volumes/main/dbdemos_ecom/landing/shipments.delivered_ts
T9.status=P:ext:/Volumes/main/dbdemos_ecom/landing/shipments.status
T9.ingested_ts=D:
T9.source_file=D:_metadata.file_path
T9._rescued_data=P:ext:/Volumes/main/dbdemos_ecom/landing/shipments._rescued_data
T11.customer_id=G:T7.customer_id
T11.utm_campaign=G:T7.utm_campaign
T11.event_count=A:T3.event_id;op=count
T11.last_seen_ts=A:T7.visit_ts;op=max
T11.session_count=A:T7.session_id;op=countDistinct
T12.order_id=P:T0.order_id
T12.customer_id=P:T0.customer_id
T12.product_id=P:T0.product_id
T12.quantity=D:T0.quantity
T12.unit_price=P:T0.unit_price
T12.total_amount=D:T0.total_amount
T12.currency=D:T0.currency
T12.status=D:T0.status
T12.order_ts=P:T0.order_ts
T12.ingested_ts=P:T0.ingested_ts
T12.order_date=D:T0.order_ts
T12.order_year=D:T0.order_ts
T12.order_month=D:T0.order_ts
T12.is_high_value=D:T0.total_amount
T14.customer_id=G:T1.customer_id
T14.email=G:T1.email
T14.country_code=G:T1.country_code
T14.signup_ts=G:T1.signup_ts
T14.total_orders=A:T0.order_id;op=count
T14.lifetime_revenue=A:T0.total_amount;op=sum
T14.last_order_ts=A:T0.order_ts;op=max
T14.last_event_ts=A:event_ts_last_event;op=max
T14.distinct_products_ordered=A:T0.product_id;op=countDistinct
T15.customer_id=P:T14.customer_id
T15.email=P:T14.email
T15.country_code=P:T14.country_code
T15.signup_ts=P:T14.signup_ts
T15.total_orders=P:T14.total_orders
T15.lifetime_revenue=P:T14.lifetime_revenue
T15.last_order_ts=P:T14.last_order_ts
T15.last_event_ts=P:T14.last_event_ts
T15.days_since_last_order=D:
T15.ltv_vs_country_avg=D:
T15.revenue_quartile=D:T14.lifetime_revenue;win=ord(lifetime_revenue desc)
T15.ltv_tier=D:T14.lifetime_revenue
T16.return_id=P:T4.return_id
T16.order_id=P:T4.order_id
T16.customer_id=P:T4.customer_id
T16.product_id=P:T4.product_id
T16.return_qty=P:T4.return_qty
T16.reason_code=D:T4.reason_code
T16.return_ts=P:T4.return_ts
T16.warehouse_id=D:T4.warehouse_id
T16.ingested_ts=P:T4.ingested_ts
T16.return_date=D:T4.return_ts
T16.is_high_qty=D:T4.return_qty
T18.snapshot_id=P:T6.snapshot_id
T18.warehouse_id=P:T6.warehouse_id
T18.product_id=P:T6.product_id
T18.on_hand_units=P:T6.on_hand_units
T18.reserved_units=P:T6.reserved_units
T18.available_units=P:T6.available_units
T18.snapshot_ts=P:T6.snapshot_ts
T18.ingested_ts=P:T6.ingested_ts
T18.source_file=P:T6.source_file
T18.snapshot_date=D:T6.snapshot_ts
T18.utilization_pct=D:T6.reserved_units,T6.on_hand_units
T20.product_id=G:T12.product_id
T20.order_date=G:T12.order_date
T20.order_count=A:T12.order_id;op=count
T21.customer_id=G:T15.customer_id
T21.product_id=G:T20.product_id
T21.forecast_date=G:
T21.lifetime_revenue=A:T15.lifetime_revenue;op=first
T21.total_orders=A:T15.total_orders;op=first
T21.avg_turnover=A:T19.turnover_rate;op=avg
T22.refund_id=P:T5.refund_id
T22.return_id=P:T5.return_id
T22.order_id=P:T5.order_id
T22.refund_amount=P:T5.refund_amount
T22.currency=P:T5.currency
T22.payment_method=P:T5.payment_method
T22.processed_ts=P:T5.processed_ts
T22.ingested_ts=P:T5.ingested_ts
T22.source_file=P:T5.source_file
T22.customer_id=P:T5.customer_id
T22.product_id=P:T5.product_id
T22.return_qty=P:T5.return_qty
T22.reason_code=P:T5.reason_code
T22.return_ts=P:T5.return_ts
T22.warehouse_id=P:T5.warehouse_id
T22._rescued_data=P:T5._rescued_data
T22.quantity=P:T5.quantity
T22.unit_price=P:T5.unit_price
T22.total_amount=P:T5.total_amount
T22.status=P:T5.status
T22.order_ts=P:T5.order_ts
T22.refund_ratio=D:T5.refund_amount,T0.total_amount
T22.days_to_refund=D:T5.processed_ts,T4.return_ts
T22.refund_date=D:T5.processed_ts
T23.order_id=G:T9.order_id
T23.shipment_count=A:T9.shipment_id;op=count
T23.first_ship_ts=A:T9.ship_ts;op=min
T23.last_delivered_ts=A:T9.delivered_ts;op=max
T23.carriers_used=A:T9.carrier;op=collect_set
T23.customer_id=P:T0.customer_id

@PREDS
X0=F.col('revenue_quartile') == 1
X1=F.col('revenue_quartile') == 2
X2=F.col('revenue_quartile') == 3

@PUSE
X0:ltv_tier
X1:ltv_tier
X2:ltv_tier

@EDGES
R|D0|D1
D|D1|D2|ingested_ts|ex=F.current_timestamp()
D|D1|D2|source_file|sc=_metadata.file_path|ex=F.col('_metadata.file_path')
W|D2|T0|append|delta|stream
R|D3|D4
D|D4|D5|ingested_ts|ex=F.current_timestamp()
D|D4|D5|source_file|sc=_metadata.file_path|ex=F.col('_metadata.file_path')
W|D5|T1|append|delta|stream
R|D6|D7
O|D7|D8|utils.event_parsers.parse_event_payload
D|D8|D9|ingested_ts|ex=F.current_timestamp()
D|D8|D9|source_file|sc=_metadata.file_path|ex=F.col('_metadata.file_path')
W|D9|T3|append|delta|stream
R|T10|D10
D|D10|D11|ingested_ts|ex=F.current_timestamp()
D|D10|D11|source_file|ex=F.col('_metadata.file_path')
W|D11|T2|overwrite|delta
R|D12|D13
D|D13|D14|ingested_ts|ex=F.current_timestamp()
D|D13|D14|source_file|sc=_metadata.file_path|ex=F.col('_metadata.file_path')
W|D14|T4|append|delta|stream
R|D15|D16
D|D16|D17|ingested_ts|ex=F.current_timestamp()
D|D16|D17|source_file|ex=F.col('_metadata.file_path')
W|D17|T5|append|delta
R|D18|D19
D|D19|D20|ingested_ts|ex=F.current_timestamp()
D|D19|D20|source_file|ex=F.col('_metadata.file_path')
W|D20|T6|append|delta|pc=snapshot_ts
R|D21|D22
D|D22|D23|ingested_ts|ex=F.current_timestamp()
D|D22|D23|source_file|sc=_metadata.file_path|ex=F.col('_metadata.file_path')
W|D23|T7|append|delta|stream
R|D24|D25
D|D25|D26|ingested_ts|ex=F.current_timestamp()
D|D25|D26|source_file|ex=F.col('_metadata.file_path')
W|D26|T8|overwrite|delta
R|D27|D28
D|D28|D29|ingested_ts|ex=F.current_timestamp()
D|D28|D29|source_file|sc=_metadata.file_path|ex=F.col('_metadata.file_path')
W|D29|T9|append|delta|stream
R|T11|D30|pc=utm_campaign
D|D30|D30|funnel_date
D|D30|D30|visit_count
R|T3|D31|pc=utm_campaign
D|D31|D31|funnel_date
D|D31|D31|cart_count
R|T3|D32|pc=utm_campaign
D|D32|D32|funnel_date
D|D32|D32|checkout_count
R|T12|D33|pc=utm_campaign,funnel_date
D|D33|D33|purchase_count
J|D30|D31|D34|left|jk=utm_campaign=utm_campaign,funnel_date=funnel_date
J|D34|D32|D34|left|jk=utm_campaign=utm_campaign,funnel_date=funnel_date
J|D34|D33|D34|left|jk=utm_campaign=utm_campaign,funnel_date=funnel_date
P|D30|D35|kp=v.utm_campaign,v.funnel_date,v.visit_count,cart_count,checkout_count,purchase_count,conversion_rate
W|D35|T13|overwrite|delta|pc=funnel_date
R|T14|D36|pc=customer_id,email,country_code,signup_ts,total_orders,lifetime_revenue,last_order_ts,last_event_ts
A|D36|D37|by=gold/customer_ltv.py|gk=country_code|out=avg_country_ltv=avg(lifetime_revenue)
J|D36|D37|D38|left|jk=country_code=country_code
D|D38|D39|days_since_last_order
D|D38|D39|ltv_vs_country_avg
D|D38|D39|revenue_quartile|sc=lifetime_revenue|ws=ord(lifetime_revenue desc)
P|D39|D40|kp=customer_id,email,country_code,signup_ts,total_orders,lifetime_revenue,last_order_ts,last_event_ts,days_since_last_order,ltv_vs_country_avg,revenue_quartile
D|D40|D41|ltv_tier|sc=revenue_quartile|rl=X0=>F.lit('platinum');X1=>F.lit('gold');X2=>F.lit('silver');else=>F.lit('bronze')
W|D41|T15|overwrite|delta
R|T16|D42|pc=customer_id
D|D42|D42|total_returns
D|D42|D42|total_return_qty
R|T14|D43|pc=customer_id,email,country_code,total_orders,lifetime_revenue
J|D43|D42|D44|left|jk=customer_id=customer_id
P|D43|D45|kp=cb.customer_id,cb.email,cb.country_code,cb.total_orders,cb.lifetime_revenue,total_returns,total_return_qty,return_rate
W|D45|T17|overwrite|delta
R|T12|D46|pc=product_id,snapshot_date
D|D46|D46|sold_qty
D|D46|D46|sales_count
R|T18|D47|pc=warehouse_id,product_id,snapshot_date,on_hand_units,reserved_units,available_units,utilization_pct
J|D47|D46|D48|left|jk=product_id=product_id,snapshot_date=snapshot_date
P|D47|D49|kp=inv.warehouse_id,inv.product_id,inv.snapshot_date,inv.on_hand_units,inv.reserved_units,inv.available_units,inv.utilization_pct,sold_qty,sales_count,turnover_rate,days_of_stock
W|D49|T19|merge|mk=product_id,warehouse_id,snapshot_date|sink=DeltaMergeSink
R|T12|D50
A|D50|D51|by=gold/product_performance.py|gk=product_id,order_date|out=<unresolved>=<unresolved>(<unresolved>);order_count=count(order_id)|dyn=agg list star-unpacked from a runtime-built variable; column list cannot be enumerated statically; controlled by env-var(s): $METRIC_CONFIG_PATH
W|D51|T20|merge|mk=product_id,order_date|sink=DeltaMergeSink
R|T15|D52
R|T20|D53
R|T19|D54
J|D52|D53|D55|inner
J|D55|D54|D56|left|jk=product_id=product_id
D|D56|D57|forecast_date|sc=snapshot_date|ex=F.current_timestamp()
A|D57|D58|by=gold/revenue_forecast_features.py|gk=customer_id,product_id,forecast_date|out=<unresolved>=<unresolved>(<unresolved>);lifetime_revenue=first(lifetime_revenue);total_orders=first(total_orders);avg_turnover=avg(turnover_rate)|dyn=agg list star-unpacked from a runtime-built variable; column list cannot be enumerated statically; controlled by env-var(s): $FORECAST_CONFIG_PATH
W|D58|T21|overwrite|delta
R|T0|D59
R|T1|D60
R|T3|D61
D|D61|D62|rn|sc=customer_id,event_ts|ex=F.current_timestamp()|ws=part(customer_id)/ord(event_ts desc)
F|D62|D63|rc=rn|pr=F.col('_metadata.file_path')
P|D63|D64|rm=rn
D|D64|D65|event_id_last_event|sc=event_id
D|D64|D65|customer_id_last_event|sc=customer_id
D|D64|D65|event_type_last_event|sc=event_type
D|D64|D65|product_id_last_event|sc=product_id
D|D64|D65|session_id_last_event|sc=session_id
D|D64|D65|event_ts_last_event|sc=event_ts
D|D64|D65|ingested_ts_last_event|sc=ingested_ts
D|D64|D65|payload_last_event|sc=payload
D|D64|D65|_rescued_data_last_event|sc=_rescued_data
J|D60|D59|D66|inner|jk=customer_id=customer_id
J|D66|D65|D67|left|jk=customer_id=customer_id_last_event
A|D67|D68|by=silver/customer_profile.py|gk=customer_id,email,country_code,signup_ts|out=total_orders=count(order_id);lifetime_revenue=sum(total_amount);last_order_ts=max(order_ts);last_event_ts=max(event_ts_last_event);distinct_products_ordered=countDistinct(product_id)
W|D68|T14|overwrite|delta
R|T6|D69
D|D69|D69|snapshot_date|sc=snapshot_ts|ex=F.current_timestamp()
D|D69|D70|rn|sc=warehouse_id,product_id,snapshot_date,snapshot_ts|ex=F.col('_metadata.file_path')|ws=part(warehouse_id,product_id,snapshot_date)/ord(snapshot_ts desc)
F|D70|D71|rc=rn|pr=F.col('rn') == 1
P|D71|D72|rm=rn
D|D72|D73|utilization_pct|sc=reserved_units,on_hand_units|ex=F.col('reserved_units') / F.when(F.col('on_hand_units') == 0, F.lit(None)).ot...
F|D73|D74|rc=on_hand_units|pr=F.col('on_hand_units') > 0
W|D74|T18|overwrite|delta
R|T7|D75
R|T3|D76
J|D75|D76|D77|inner|jk=session_id=session_id
D|D77|D78|rn|sc=session_id,visit_ts|ex=F.current_timestamp()|ws=part(session_id)/ord(visit_ts asc)
F|D78|D79|rc=rn|pr=F.col('_metadata.file_path')
P|D79|D80|rm=rn
A|D80|D81|by=silver/marketing_touchpoints.py|gk=customer_id,utm_campaign|out=event_count=count(event_id);last_seen_ts=max(visit_ts);session_count=countDistinct(session_id)
W|D81|T11|overwrite|delta
R|T0|D82
O|D82|D82|utils.column_transformations.trim_string_columns|pass
D|D82|D82|currency|sc=currency|ex=F.current_timestamp()
D|D82|D82|status|sc=status|ex=F.col('_metadata.file_path')
D|D82|D82|quantity|sc=quantity|ex=F.col('rn') == 1
D|D82|D82|total_amount|sc=total_amount|ex=F.col('reserved_units') / F.when(F.col('on_hand_units') == 0, F.lit(None)).ot...
D|D82|D82|order_date|sc=order_ts|ex=F.col('on_hand_units') > 0
D|D82|D82|order_year|sc=order_ts|ex=F.year(F.col('order_ts'))
D|D82|D82|order_month|sc=order_ts|ex=F.month(F.col('order_ts'))
D|D82|D82|is_high_value|sc=total_amount|ex=F.col('total_amount') > F.lit(1000)
P|D82|D82|rm=_rescued_data,source_file
F|D82|D82|rc=status,quantity|pr=(F.col('status') != 'cancelled') & (F.col('quantity') > 0)
W|D82|T12|overwrite|delta
R|T5|D83
R|T4|D84
R|T0|D85
J|D83|D84|D86|inner|jk=return_id=return_id
J|D86|D85|D87|left|jk=order_id=order_id
D|D87|D87|refund_ratio|sc=refund_amount,total_amount|ex=F.current_timestamp()
D|D87|D87|days_to_refund|sc=processed_ts,return_ts|ex=F.col('_metadata.file_path')
D|D87|D87|refund_date|sc=processed_ts|ex=F.col('rn') == 1
W|D87|T22|overwrite|delta
R|T4|D88
O|D88|D88|utils.column_transformations.trim_string_columns|pass
D|D88|D88|reason_code|sc=reason_code|ex=F.current_timestamp()
D|D88|D88|warehouse_id|sc=warehouse_id|ex=F.col('_metadata.file_path')
D|D88|D88|return_date|sc=return_ts|ex=F.col('rn') == 1
D|D88|D88|is_high_qty|sc=return_qty|ex=F.col('reserved_units') / F.when(F.col('on_hand_units') == 0, F.lit(None)).ot...
P|D88|D88|rm=_rescued_data,source_file
F|D88|D88|rc=return_qty|pr=F.col('on_hand_units') > 0
W|D88|T16|overwrite|delta
R|T9|D89
R|T0|D90
D|D89|D91|shipment_seq|sc=order_id,ship_ts|ex=F.current_timestamp()|ws=part(order_id)/ord(ship_ts asc)
A|D91|D92|by=silver/shipments_timeline.py|gk=order_id|out=shipment_count=count(shipment_id);first_ship_ts=min(ship_ts);last_delivered_ts=max(delivered_ts);carriers_used=collect_set(carrier)
J|D92|D90|D93|inner|jk=order_id=order_id|rpc=order_id,customer_id
W|D93|T23|overwrite|delta

@WARN
bronze/03_ingest_events.py|opaque-call-fallback|Unregistered function 'utils.event_parsers.parse_event_payload' called with DataFrame arg; column-set change across this call is unknown.
gold/product_performance.py|dynamic-aggregation|agg list contains star-unpacked runtime variable in gold/product_performance.py:81
gold/revenue_forecast_features.py|dynamic-aggregation|agg list contains star-unpacked runtime variable in gold/revenue_forecast_features.py:102
