# Benchmark: `repo_synthetic_small`

A hand-authored synthetic Databricks repository used as the primary evaluation target for
DEGraph's extractor (Phase 2) and for the LLM accuracy experiment in RQ2.

The repository is small enough to be understood in full but deliberately exercises every
schema edge type, every known extraction gap, and every registered-helper/sink pattern —
making it a complete acceptance test for the extractor before larger real-world repos are
attempted.

---

## Pipeline overview

```
bronze                         silver                          gold
──────                         ──────                          ────
orders_raw ─────────────────► orders_cleaned ────────────────►
                                                               product_performance
customers_raw ──────────────►
                              customer_profile ───────────────► customer_ltv
orders_raw ─────────────────►
events_raw ──────────────────►

(products_raw declared in DDL but never read or written — intentional orphan)
```

All tables live in catalog `main`, schema `dbdemos_ecom`.  The `database` symbol
(`main.dbdemos_ecom`) is resolved from `config.py` → `_resources/setup.py` via `%run`;
the extractor must propagate this symbol to resolve f-string table references.

---

## Directory structure

```
repo_synthetic_small/
│
├── config.py                      # catalog / db / volume_path constants
├── _resources/
│   └── setup.py                   # database = f"{catalog}.{db}" (shared via %run)
│
├── ddl/
│   └── bronze_tables.sql          # CREATE TABLE for all 4 bronze tables (incl. orphan)
│
├── utils/
│   ├── column_transformations.py  # trim_string_columns, rename_columns_with_suffix
│   ├── event_parsers.py           # parse_event_payload  (deliberately NOT registered)
│   └── sinks.py                   # DeltaMergeSink class
│
├── bronze/
│   ├── 01_ingest_orders.py        # cloudFiles stream → orders_raw
│   ├── 02_ingest_customers.ipynb  # Jupyter notebook → customers_raw
│   └── 03_ingest_events.py        # cloudFiles + opaque helper → events_raw
│
├── silver/
│   ├── orders_cleaned.py          # trim + 8 withColumns + filter → orders_cleaned
│   └── customer_profile.py        # window + suffix rename + 2 joins + agg → customer_profile
│
├── gold/
│   ├── customer_ltv.py            # spark.sql CTEs + withColumn → customer_ltv
│   └── product_performance.py     # dynamic agg + DeltaMergeSink → product_performance
│
└── .degraph/
    ├── helpers.json               # registered helper functions
    └── sinks.json                 # registered custom sink classes
```

---

## Schema features exercised

The table below maps each source file to the DEGraph edge types and extraction challenges
it is designed to exercise.  A correct extractor must produce the edge type(s) listed for
every file in the "Edge types" column.

| File | Edge types | Extraction challenge |
|---|---|---|
| `ddl/bronze_tables.sql` | — | Parses SQL DDL to populate `Table.columns`; triggers `orphan-table` warning for `products_raw` |
| `bronze/01_ingest_orders.py` | Reads (cloudFiles), Derives (×2), Writes (stream) | Resolves `cloudFiles` ExternalSource; resolves `%run` symbol for `database` f-string |
| `bronze/02_ingest_customers.ipynb` | Reads (cloudFiles), Derives (×2), Writes (stream) | Parses Jupyter `.ipynb` JSON; drops `%sql DESCRIBE` magic cell without error |
| `bronze/03_ingest_events.py` | Reads (cloudFiles), OpaqueTransform, Derives (×2), Writes (stream) | Unregistered helper `parse_event_payload` → `opaque-call-fallback` warning; `is_passthrough=false` |
| `silver/orders_cleaned.py` | Reads, OpaqueTransform (passthrough), Derives (×8), Projects (×2), Filters, Writes | Registered passthrough helper preserves column set; heterogeneous Projects + Filters chain |
| `silver/customer_profile.py` | Reads (×3), Derives (window), Filters, Projects, Derives (suffix_rename ×N), Joins (×2), Aggregates, Writes | Densest file; window spec capture; registered suffix_rename helper; anonymous intermediate node for filter+drop chain |
| `gold/customer_ltv.py` | Reads, Aggregates (SQL GROUP BY), Joins (SQL LEFT JOIN), Derives (NTILE window), Projects (drops `avg_country_ltv`), Derives (Python withColumn), Writes | `spark.sql()` block parsed by sqlglot; CTEs become DataFrameNodes; mixed SQL + Python lineage |
| `gold/product_performance.py` | Reads, Aggregates (`dynamic=True`), Writes (custom sink, `mode=merge`) | `os.environ.get` + `json.load` opaque to SafeEvaluator → `dynamic-aggregation` warning; registered `DeltaMergeSink` resolves target and mode |

---

## Intentional design decisions

These choices are deliberate.  An extractor that silently produces the "wrong" thing here
has a bug; the ground truth graph encodes the correct representation for each case.

### 1. Orphan table (`products_raw`)

`ddl/bronze_tables.sql` declares `main.dbdemos_ecom.products_raw` but no file reads or
writes it.  This exercises the `orphan-table` GraphWarning path.  Downstream consumers
of the graph should know this table exists in the schema but plays no role in the pipeline.

### 2. Unregistered helper (`parse_event_payload`)

`bronze/03_ingest_events.py` calls `utils.event_parsers.parse_event_payload(df)`.  The
function is deliberately absent from `.degraph/helpers.json`.  The extractor must fall
back to an OpaqueTransform edge with `is_passthrough=false`, emit an `opaque-call-fallback`
GraphWarning, and *not* crash.  The function actually drops `payload` and adds four
columns (`page_url`, `referrer`, `user_agent`, `cart_total`), but the extractor cannot
know this — the column delta is a known unknown.

### 3. Registered passthrough helper (`trim_string_columns`)

`silver/orders_cleaned.py` calls `utils.column_transformations.trim_string_columns(orders)`.
The function is registered in `.degraph/helpers.json` with `"kind": "passthrough"`.
The extractor must emit an OpaqueTransform edge with `is_passthrough=true` and propagate
the full input column set unchanged to the output DataFrameNode — no column delta, no warning.

### 4. Registered suffix-rename helper (`rename_columns_with_suffix`)

`silver/customer_profile.py` calls `rename_columns_with_suffix(events_latest, "_last_event")`.
Registered as `"kind": "suffix_rename"` with `"suffix_arg": 1`.  The extractor must emit
one explicit Derives edge per column in the input DataFrame, mapping `col → col_last_event`.

### 5. Dynamic aggregation list (`gold/product_performance.py`)

The `metric_columns` list is loaded from `os.environ.get("METRIC_CONFIG_PATH", ...)` +
`json.load(...)`.  Neither is resolvable by SafeEvaluator.  The aggregation loop builds
an opaque `aggregations` list that is star-unpacked into `.agg(*aggregations, ...)`.
The extractor must emit a single Aggregates edge with `dynamic=True`,
`agg_ops=["count", "<unresolved>"]`, `output_cols=["order_count", "<unresolved>"]`,
and a `dynamic-aggregation` GraphWarning.  The static `F.count("order_id")` portion
must still be captured.

### 6. Custom sink (`DeltaMergeSink`)

`gold/product_performance.py` writes via `DeltaMergeSink`, registered in
`.degraph/sinks.json` with `target_kwarg="target_table"` and `mode_kwarg="mode"`.
The extractor must resolve the `target_table=f"{database}.product_performance"` kwarg
to the FQN `main.dbdemos_ecom.product_performance`, set `mode="merge"`, populate
`sink_class="DeltaMergeSink"`, and emit a proper Writes edge — no `<unresolved>` markers.

### 7. SQL CTEs via `spark.sql()` (`gold/customer_ltv.py`)

The entire LTV computation runs inside a single `spark.sql("""WITH base AS (...) ...""")`
f-string.  The extractor must parse the SQL with sqlglot, treat each CTE as a file-local
DataFrameNode, emit Reads/Joins/Aggregates/Derives edges for the SQL-level operations,
and then stitch the resulting DataFrame into the subsequent Python `withColumn` call.

### 8. Jupyter notebook input (`bronze/02_ingest_customers.ipynb`)

One of the three bronze files is a `.ipynb` notebook exported from Databricks.  The
extractor must parse the JSON cell array, extract source lines from code cells, skip
magic cells (`%sql DESCRIBE TABLE ...`), and treat the remainder as equivalent to a
`.py` file.

---

## Associated ground-truth files

| File | Description |
|---|---|
| `data/ground_truth/repo_synthetic_small.graph.json` | Hand-authored ground-truth lineage graph. 8 Tables, 3 ExternalSources, 30 DataFrames, 18 Expressions, 57 edges, 3 GraphWarnings. This is the test oracle — the extractor's output is compared against it. |
| `data/ground_truth/repo_synthetic_small.qa.json` | 10 Q&A pairs for the LLM accuracy experiment (RQ2). Each question is answerable solely from the graph; reference answers cite specific edge IDs and node fields as evidence. |

---

## How to use this benchmark

### Extractor acceptance test (Phase 2)

Run the extractor against this directory and diff the output against the ground-truth graph:

```bash
python -m degraph extract \
  data/benchmarks/repo_synthetic_small \
  --output results/graphs/repo_synthetic_small.graph.json

python experiments/compare_graphs.py \
  data/ground_truth/repo_synthetic_small.graph.json \
  results/graphs/repo_synthetic_small.graph.json
```

A passing result means zero mismatched edges, zero missing nodes, and all three
GraphWarnings present with the correct `category` and `file` fields.

### LLM accuracy experiment (RQ2)

Feed the graph (not the source files) as context to the LLM and ask each of the 10
questions from the `.qa.json` file.  Grade each answer against the `reference_answer`
using the rubric in the `.qa.json` metadata block (`correct` / `partially_correct` /
`incorrect`).  Then repeat with raw source files as context and compare accuracy and
token counts.

---

## Token-count reference

| Context type | Approx. tokens |
|---|---|
| All source files (raw) | ~3,800 |
| Ground-truth graph JSON | ~14,000 |
| Graph — compact serialization (target) | TBD after Phase 2 |

The raw-file count is deliberately small (synthetic repo).  Real production repos that
motivated DEGraph run 500k–1M tokens; the reduction ratio at that scale is the central
claim of the paper.
