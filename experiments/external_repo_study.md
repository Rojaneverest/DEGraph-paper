# External-validity study: DEGraph extractor on real OSS PySpark repositories

**Date:** 2026-05-30. **Goal:** test whether the extractor's synthetic-benchmark
accuracy (semantic 98% on `repo_synthetic_small`) generalizes to real, non-self-
authored PySpark code. This is the external-validity evidence the paper was
missing (all three existing benchmarks are author-written).

## Method

Cloned three real OSS corpora and ran `extract_repo()` (this worktree's `src`,
install-independent) on each. Measured: (1) does it crash? (2) tables + edges by
kind, (3) coverage vs. a call-site denominator. Confirmed root causes with
minimal reproductions.

## Corpora and results

| Corpus | Files | Style | tables | edges | by kind |
|--------|-------|-------|--------|-------|---------|
| `databricks-demos/dbdemos-notebooks` → demo-retail/lakehouse-retail-c360 | 17 .py | Databricks production (DLT/SDP + plain-Spark) | 8 | 12 | reads 11, derives 1 |
| `spark-examples/pyspark-examples` | 97 .py | imperative tutorials | 2 | 17 | reads 17 |
| (control) `data/benchmarks/repo_synthetic_small` | 13 | imperative + explicit writes | 8 | 57 | all 8 edge types |

**Headline: no crash on any corpus** (robustness holds — `sqlglot` degrades
gracefully on Databricks DDL like `CREATE STREAMING TABLE`, `SET MASK`, `GRANT`).
**But transformation lineage is essentially not recovered on either real corpus**
(1 derive on dbdemos; 0 on pyspark-examples), versus 57 typed edges on the
author-written benchmark of comparable size.

## Root cause (confirmed by minimal repro)

The extractor recovers column lineage ONLY for the idiom:
*table-sourced DataFrame → **variable-assigned** method chain → explicit `.write`.*
This is exactly the style of all three synthetic benchmarks. Minimal repros (in
`_external_repos/_repro*`):

| Pattern | Real-world prevalence | Edges captured |
|---------|----------------------|----------------|
| `df = spark.read.table(...)`; `df = df.withColumn(...)`; `df.write.saveAsTable(...)` | our benchmarks | **read+2 derives+write (full)** |
| `@dp.table def f(): return spark.readStream.table(...).select(...)...` (DLT/SDP declarative) | dominant in dbdemos | **0** |
| `(spark.readStream.table(...).withColumn(...)...writeStream.table(...))` (un-assigned fluent chain) | dbdemos plain-Spark | **0** |
| `def ingest(folder,fmt,table): return spark.readStream...load(folder).writeStream.table(table)` then `ingest(...)` ×N (parameterized / inter-procedural) | dbdemos plain-Spark | **0** |
| `df = spark.createDataFrame([...])` (in-memory literal, no table source/sink) | dominant in pyspark-examples (77/97 files) | reads/writes only where real I/O exists; no table→table lineage |

So the three dominant real idioms — (1) DLT/SDP decorated functions, (2)
un-assigned fluent read→transform→write chains, (3) parameterized inter-procedural
ingestion helpers — each yield **zero** lineage. The extractor's DataFrameTracker
keys on `df = ...` assignments; none of these idioms assign.

## Honest implication

The 98% semantic accuracy reflects the imperative-assignment-with-explicit-writes
style we authored our benchmarks in; it does **not** generalize to the canonical
real Databricks corpus (dbdemos), which is DLT/SDP- and fluent-chain-heavy. The
tool's current real-world applicability is narrow: imperative, variable-assigned
PySpark with explicit table I/O. This is a first-order threat to external validity
and the #1 risk to the project, surfaced here for an explicit go/no-go on extractor
coverage work (fluent-chain support is the cheapest, highest-value fix; DLT/SDP
decorators next).

## Fix #9 — un-assigned fluent read→transform→write chains (2026-05-30)

Root fix in `chain_walker.unroll_chain`: the old two-loop unroll assumed the
shape `[trailing calls][trailing bare-attrs][base]` and **stopped at the first
mid-chain `.writeStream`/`.write` bare attribute**, truncating fluent pipelines
to `[writeStream, table]` with a `Call` base. Rewrote it as a single descent
through any alternation of `Call(Attribute)` and bare-attribute-in-FAMILY nodes
(backward-compatible: identical output on the common shape). Then added
`FileExtractor._process_read_rooted_expr` + `_emit_inline_read`: a bare
expression statement that is a full `spark.read[Stream]...load/table(...)
.<transforms>.write[Stream]...table(...)` pipeline now splits at the read loader,
emits the read, and routes the rest through `_process_chain` (transforms +
trailing write).

**Result (no regression on synthetic benchmarks — small 57 / clinical 80 /
medium 163 edges all unchanged vs committed graphs):**

| Target | before | after |
|--------|--------|-------|
| dbdemos demo-retail (whole) | 8 tables, 12 edges (11 reads, 1 derive) | **12 tables, 33 edges** (14 reads, 15 derives, 2 projects, 2 writes) |
| └ plain-spark file (fluent/imperative) | 3 edges (2 reads, 1 derive) | **22 edges** (4 reads, 14 derives, 2 projects, 2 writes) |
| └ SDP-python file (DLT declarative) | 5 reads | 5 reads (unchanged — decorator support is next) |

The gain is concentrated exactly where the fluent idiom lives (no hallucinated
edges elsewhere). **Remaining real-idiom gaps: (1) DLT/SDP `@dp.table` decorated
functions (return-expression lineage + function-name-as-output-table), (2)
parameterized inter-procedural ingestion helpers.**

## Fix #10 — DLT / Spark Declarative Pipelines decorated functions (2026-05-30)

The dominant real Databricks idiom: `@dp.table def churn_users(): return
spark.readStream.table("churn_users_bronze").select(...)`. The output table is
the *function name* (or a `name=` kwarg); the pipeline is the function's
`return` expression — neither an assignment nor a bare expression statement, so
the first two passes never saw it. Added: DLT/SDP module-alias detection in the
import pass (`from pyspark import pipelines as dp`, `import dlt`); a third pass
`_process_dlt_functions` that, for each decorated table/view function, processes
the returned read-rooted chain (reusing `_emit_read_rooted_pipeline`) or resolves
a returned df var, then emits a WritesEdge to `table:<function-name>`.

**Result (regression still clean — small 57 / clinical 80 / medium 163):**

| Target | pre-study | +Fix #9 | +Fix #10 |
|--------|-----------|---------|----------|
| dbdemos demo-retail (whole) | 12 edges (11 reads, **1 derive**, 0 writes) | 33 edges | **64 edges** (21 reads, **30 derives**, 9 writes, 2 joins, 2 projects), 16 tables |
| └ SDP-python dir (DLT) | 5 reads, 0 transforms | 5 reads | **36 edges** (12 reads, 15 derives, 7 writes, 2 joins) — writes target the function-name tables churn_users/churn_orders/churn_features/... |

**Net external-validity story:** on the canonical real Databricks corpus, two
idiom fixes took transformation-lineage recovery from ~nothing (1 derive, 0
writes across 17 files) to rich (30 derives, 9 writes) — a 5.3× edge increase —
with zero change to the synthetic-benchmark graphs.

## Fix #11 — parameterized inter-procedural ingestion helpers (partial)

Added `_process_interproc_pipeline_call`: a module-level call to a same-file
function whose body is a single returned spark-rooted pipeline
(`def ingest(folder,fmt,table): return spark.readStream...load(folder)
.writeStream.table(table)`) is now inlined at each call site by binding the
positional args to the parameters in the symbol table, so the read source and
write target resolve per call. Verified on a repro: two `ingest(...)` calls →
two distinct `ext:<folder> -> table:<name>` read/write pairs. Regression clean
(57/80/163).

**Honest boundary:** this handles the *single-return-chain* helper form. The
dbdemos `ingest_folder` uses a *two-statement* form
(`bronze_products = spark.read...load(folder); return bronze_products.writeStream
.table(table)`), where the return's base is a local variable, not `spark` — so
the dbdemos total is **unchanged at 64**. Capturing the two-statement form
requires excluding function bodies from the main AST walk and inlining them only
at call sites (otherwise in-function assignments are double-counted); that walk
refactor carries regression risk for the clinical/medium benchmarks (which may
rely on in-function processing) and is deferred as future work, documented as an
extractor limitation.

## Fix #12 — column-level impact on real code (qualify schema-less provenance)

The idiom fixes recovered the *edges*, but column-level impact/diff on real DLT
code stayed broken: DLT-derived columns resolved to **bare, un-table-qualified**
provenance (`creation_date` instead of `churn_users.creation_date`), so
`column_impact`'s seed resolver mis-bound to dead-end leaves. Root cause:
`compact._resolve_col_to_table` only attributes a column to a table when that
table's *schema is known*; intermediate DLT/SDP tables (created by the write
emitter) have no declared schema, so resolution returned None and fell back to
the bare name. Fix: when a table's schema is unknown (empty), optimistically
attribute the column to it; tables WITH a known schema keep the strict
membership check (so joins don't mis-resolve).

**Regression gate (the precious P100):** synthetic `impact_eval.py` is unchanged
— **P 100% / R 69% / F1 82%, fp=0** — because synthetic tables have schemas and
hit the strict path. **Result on dbdemos (real code):**

| Query | before | after |
|-------|--------|-------|
| `column_impact(churn_users, creation_date)` | `[]` | **`churn_features.days_since_creation`** |
| `column_impact(churn_users, last_activity_date)` | `[]` | **`churn_features.days_since_last_activity`** |
| diff of a PR renaming `creation_date` | edge diff only | **[BREAKING] flags churn_features.days_since_creation (code not updated)** |

So on the canonical real Databricks corpus, DEGraph now does table-level AND
column-level static change-impact + version-diff with breaking-change detection
— the full end-to-end story, demonstrated in `experiments/impact_demo_dbdemos.py`.
Residual: `diff_with_impact`'s *automatic* breaking-list is bounded to tables
with a declared schema (silver DLT tables have none), so the demo queries the
changed column directly; populating DLT tables' output schema would close this.

## Second hand-labeled ground truth — accuracy on REAL code (2026-05-30)

Addresses the paper's top threat to validity (accuracy rested on one self-authored
synthetic benchmark). We hand-labeled the lineage of a real third-party slice —
dbdemos retail SDP bronze/silver/gold (`01-bronze.py`, `02-silver.py`,
`03-gold.py`) — **from the source only** (not the extractor output, to avoid
circularity): 36 semantic edges + 7 tables
(`data/ground_truth/dbdemos_retail_sdp.graph.json`), scored by
`experiments/extractor_precision_dbdemos.py` with the A2 semantic method
(source-column-aware derive keys).

| edge type | GT | P | R | F1 |
|-----------|----|----|----|----|
| reads | 9 | 100% | 100% | 100% |
| writes | 7 | 100% | 100% | 100% |
| derives | 17 | 100% | 88% | 94% |
| aggregates | 2 | 100% | 0% | 0% |
| joins | 1¹ | 100% | 100% | 100% |
| **OVERALL** | **36** | **100%** | **89%** | **94%** |
| tables | 7 | 100% | 100% | 100% |

**Blind first pass: P 100% / R 89% / F1 94% on real third-party code** (vs semantic
98% on the synthetic benchmark). 100% precision = zero false positives, consistent
with the no-hallucination property holding on real code. The recall misses were two
specific, explainable categories: (1) two `derives` that alias-rename a bare `id`
column (`user_id<-id`, `order_id<-id`) — a narrow select-rename gap; (2) the two
gold `aggregates` computed in inner df-variable assignments *inside* a DLT function
body, which the DLT handler did not capture (it processed the return expression,
not inner aggregate assignments). ¹ The slice has two joins, both on
`user_id=user_id`, so they collapse to one semantic key. Single annotator (paper
author); see paper §6.2.

### Closing the two gaps (general extractor fixes → P/R/F1 = 100% on this slice)

Both gaps were general extractor limitations, not dbdemos-specific, and were fixed:
1. **Select column-rename derives.** `col("x").alias("y")` with x≠y inside a
   `.select(...)` is a real lineage edge (output y derives from x) but was skipped
   along with pure passthroughs. Now renames are emitted, pure passthroughs
   (`col("x")` / `col("x").alias("x")`) still skipped. **Generality evidence:** this
   also recovered 41 real column-rename edges in the clinical canonical projection
   (clinical 80→121 edges), with **impact-eval precision unchanged at 100%** (the new
   edges introduce no false positives) and small/medium graphs byte-identical.
2. **Read-chain trailing transforms + lowercase `groupby`.** `stats =
   spark.read.table(x).groupby(k).agg(...)` inside a Declarative-Pipeline body had
   its `groupby().agg()` dropped because the assignment read-branch returned after
   emitting the read; and `groupby` (a real lowercase PySpark alias) was absent from
   the method-family map. Both fixed; the read-branch now routes trailing transform/
   aggregate ops through the read-rooted pipeline.

Post-fix the extractor scores **P/R/F1 = 100%** on this slice (36/36 edges); the
full dbdemos retail corpus rises 64→68 edges (derives 30→32 via renames,
aggregates 0→2), with the impact+diff demo unchanged.
**Honesty note:** because these fixes were *informed by* this ground truth, the
89% is the honest *blind* real-code estimate and the 100% is post-targeted-fix; a
fully blind re-measurement would require a fresh held-out real slice (future work).
The fixes' generality is evidenced by the independent clinical-benchmark gains and
the unchanged impact precision, indicating they are real improvements rather than
test-specific tuning.

## Reproduce

```
python -c "import sys; sys.path.insert(0,'src'); from pathlib import Path; \
from degraph.extractor.assembler import extract_repo; \
print(extract_repo(Path(r'<corpus>')).model_dump_json())"
```
Corpora under `C:\Users\thapa\Desktop\Research\_external_repos\` (gitignored;
re-clone via the URLs above).
