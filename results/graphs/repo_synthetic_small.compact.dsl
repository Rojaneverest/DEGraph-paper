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
T0|main.dbdemos_ecom.orders_raw|order_id,customer_id,product_id,quantity,unit_price,total_amount,currency,status,order_ts,ingested_ts,source_file,_rescued_data|by=bronze/01_ingest_orders.py|rb=silver/customer_profile.py,silver/orders_cleaned.py
T1|main.dbdemos_ecom.customers_raw|customer_id,email,first_name,last_name,country_code,signup_ts,marketing_opt_in,ingested_ts,source_file,_rescued_data|by=bronze/02_ingest_customers.ipynb|rb=silver/customer_profile.py
T2|main.dbdemos_ecom.products_raw|product_id,name,category,list_price,active,ingested_ts,source_file|by=|rb=
T3|main.dbdemos_ecom.events_raw|event_id,customer_id,event_type,product_id,session_id,event_ts,ingested_ts,payload,_rescued_data|by=bronze/03_ingest_events.py|rb=silver/customer_profile.py
T4|main.dbdemos_ecom.customer_profile|customer_id,email,country_code,signup_ts,total_orders,lifetime_revenue,last_order_ts,last_event_ts,distinct_products_ordered|by=silver/customer_profile.py|rb=gold/customer_ltv.py
T5|main.dbdemos_ecom.customer_ltv|customer_id,email,country_code,signup_ts,total_orders,lifetime_revenue,last_order_ts,last_event_ts,days_since_last_order,ltv_vs_country_avg,revenue_quartile,ltv_tier|by=gold/customer_ltv.py|rb=
T6|main.dbdemos_ecom.orders_cleaned|order_id,customer_id,product_id,quantity,unit_price,total_amount,currency,status,order_ts,ingested_ts,order_date,order_year,order_month,is_high_value|by=silver/orders_cleaned.py|rb=gold/product_performance.py
T7|main.dbdemos_ecom.product_performance|product_id,order_date,order_count|by=gold/product_performance.py|rb=

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
T3.ingested_ts=D:
T4.customer_id=G:T1.customer_id
T4.email=G:T1.email
T4.country_code=G:T1.country_code
T4.signup_ts=G:T1.signup_ts
T4.total_orders=A:T0.order_id;op=count
T4.lifetime_revenue=A:T0.total_amount;op=sum
T4.last_order_ts=A:T0.order_ts;op=max
T4.last_event_ts=A:event_ts_last_event;op=max
T4.distinct_products_ordered=A:T0.product_id;op=countDistinct
T5.customer_id=P:T4.customer_id
T5.email=P:T4.email
T5.country_code=P:T4.country_code
T5.signup_ts=P:T4.signup_ts
T5.total_orders=P:T4.total_orders
T5.lifetime_revenue=P:T4.lifetime_revenue
T5.last_order_ts=P:T4.last_order_ts
T5.last_event_ts=P:T4.last_event_ts
T5.days_since_last_order=D:
T5.ltv_vs_country_avg=D:
T5.revenue_quartile=D:T4.lifetime_revenue;win=ord(lifetime_revenue desc)
T5.ltv_tier=D:T4.lifetime_revenue
T6.order_id=P:T0.order_id
T6.customer_id=P:T0.customer_id
T6.product_id=P:T0.product_id
T6.quantity=D:T0.quantity
T6.unit_price=P:T0.unit_price
T6.total_amount=D:T0.total_amount
T6.currency=D:T0.currency
T6.status=D:T0.status
T6.order_ts=P:T0.order_ts
T6.ingested_ts=P:T0.ingested_ts
T6.order_date=D:T0.order_ts
T6.order_year=D:T0.order_ts
T6.order_month=D:T0.order_ts
T6.is_high_value=D:T0.total_amount
T7.product_id=G:T6.product_id
T7.order_date=G:T6.order_date
T7.order_count=A:T6.order_id;op=count

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
R|T4|D10|pc=customer_id,email,country_code,signup_ts,total_orders,lifetime_revenue,last_order_ts,last_event_ts
A|D10|D11|by=gold/customer_ltv.py|gk=country_code|out=avg_country_ltv=avg(lifetime_revenue)
J|D10|D11|D12|left|jk=country_code=country_code
D|D12|D13|days_since_last_order
D|D12|D13|ltv_vs_country_avg
D|D12|D13|revenue_quartile|sc=lifetime_revenue|ws=ord(lifetime_revenue desc)
P|D13|D14|kp=customer_id,email,country_code,signup_ts,total_orders,lifetime_revenue,last_order_ts,last_event_ts,days_since_last_order,ltv_vs_country_avg,revenue_quartile
D|D14|D15|ltv_tier|sc=revenue_quartile|rl=X0=>F.lit('platinum');X1=>F.lit('gold');X2=>F.lit('silver');else=>F.lit('bronze')
W|D15|T5|overwrite|delta
R|T6|D16
A|D16|D17|by=gold/product_performance.py|gk=product_id,order_date|out=<unresolved>=<unresolved>(<unresolved>);order_count=count(order_id)|dyn=agg list star-unpacked from a runtime-built variable; column list cannot be enumerated statically; controlled by env-var(s): $METRIC_CONFIG_PATH
W|D17|T7|merge|mk=product_id,order_date|sink=DeltaMergeSink
R|T0|D18
R|T1|D19
R|T3|D20
D|D20|D21|rn|sc=customer_id,event_ts|ex=F.current_timestamp()|ws=part(customer_id)/ord(event_ts desc)
F|D21|D22|rc=rn|pr=F.col('_metadata.file_path')
P|D22|D23|rm=rn
D|D23|D24|event_id_last_event|sc=event_id
D|D23|D24|customer_id_last_event|sc=customer_id
D|D23|D24|event_type_last_event|sc=event_type
D|D23|D24|product_id_last_event|sc=product_id
D|D23|D24|session_id_last_event|sc=session_id
D|D23|D24|event_ts_last_event|sc=event_ts
D|D23|D24|ingested_ts_last_event|sc=ingested_ts
D|D23|D24|payload_last_event|sc=payload
D|D23|D24|_rescued_data_last_event|sc=_rescued_data
J|D19|D18|D25|inner|jk=customer_id=customer_id
J|D25|D24|D26|left|jk=customer_id=customer_id_last_event
A|D26|D27|by=silver/customer_profile.py|gk=customer_id,email,country_code,signup_ts|out=total_orders=count(order_id);lifetime_revenue=sum(total_amount);last_order_ts=max(order_ts);last_event_ts=max(event_ts_last_event);distinct_products_ordered=countDistinct(product_id)
W|D27|T4|overwrite|delta
R|T0|D28
O|D28|D28|utils.column_transformations.trim_string_columns|pass
D|D28|D28|currency|sc=currency|ex=F.current_timestamp()
D|D28|D28|status|sc=status|ex=F.col('_metadata.file_path')
D|D28|D28|quantity|sc=quantity|ex=F.greatest(F.col('quantity'), F.lit(0))
D|D28|D28|total_amount|sc=total_amount|ex=F.round(F.col('total_amount'), 2)
D|D28|D28|order_date|sc=order_ts|ex=F.to_date(F.col('order_ts'))
D|D28|D28|order_year|sc=order_ts|ex=F.year(F.col('order_ts'))
D|D28|D28|order_month|sc=order_ts|ex=F.month(F.col('order_ts'))
D|D28|D28|is_high_value|sc=total_amount|ex=F.col('total_amount') > F.lit(1000)
P|D28|D28|rm=_rescued_data,source_file
F|D28|D28|rc=status,quantity|pr=(F.col('status') != 'cancelled') & (F.col('quantity') > 0)
W|D28|T6|overwrite|delta

@WARN
bronze/03_ingest_events.py|opaque-call-fallback|Unregistered function 'utils.event_parsers.parse_event_payload' called with DataFrame arg; column-set change across this call is unknown.
gold/product_performance.py|dynamic-aggregation|agg list contains star-unpacked runtime variable in gold/product_performance.py:81
(assembler)|orphan-table|Table 'main.dbdemos_ecom.products_raw' appears in DDL but is never read or written by any file in this repo.
