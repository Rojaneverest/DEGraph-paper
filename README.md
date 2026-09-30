# DEGraph

A research prototype for static column lineage, change-impact analysis and lineage diff
on supported PySpark/SQL patterns. Extraction uses Python AST analysis and SQLGlot,
without running the analyzed pipelines or invoking a language model.

**Manuscript:** *What Breaks Static Column-Level Lineage in Production PySpark? A Tool
and Two Industrial Case Studies*, Rojan Raj Thapa. Research manuscript, not peer reviewed.
The manuscript is undergoing final review; a public PDF link will be added when available.

## Snapshot

This is the `degraph-2026-09-30` software snapshot, package 0.1.1. `MANIFEST.sha256` identifies the
snapshot's contents. Earlier repository revisions contain the pre-repair impact seed
resolver; use this snapshot and the commands in [REPRODUCIBILITY.md](REPRODUCIBILITY.md)
for the revised manuscript. Publication of a paper is separate from this software release.

## Reproduce the public deterministic results

```bash
python -m venv .venv
# Activate .venv using your platform's command.
python -m pip install -r requirements-reproduce.txt
python -m pip install --no-deps -e .
python experiments/regenerate_graphs.py --check
python experiments/extractor_precision.py
python experiments/impact_eval.py
python experiments/diff_eval.py
python experiments/score_cached_judges.py
python experiments/_grade_bm25_gemini.py
python -m pytest tests/ -q
```

The optional real-source regression test skips without the separately downloaded Databricks
corpus. Its evaluation command instead exits with an error if the requested corpus is missing.
See the reproduction guide for real-source and baseline comparisons.

## Evaluated scope

- Synthetic normalized edge-key recovery: 56/57 matched, with 1 false positive and 1 miss.
  This key omits some provenance payloads; it is not a full source-column accuracy metric.
- Impact: 21 selected scenarios, TP 49 / FP 0 / FN 14 (100% precision, 77.8% recall).
- Diff: 10/10 structural column changes, all six breaking/safe classifications correct;
  11/11 expected downstream persisted columns on three breaking edits.
- Cached judge agreement against reconciled labels: kappa 0.837 and 0.892.
- Gemini context grades: raw 85.8%, BM25 81.7%, graph 75.0% of available grading points.

These are sample results. DEGraph supplies neither a completeness proof nor a universal
zero-false-positive guarantee. `impact_report` distinguishes unresolved/ambiguous seeds from
resolved queries; it reports the bare-column compatibility heuristic explicitly and marks
coverage incomplete. No detected impact is not proof that a change is safe.

## Artifact layout

- `src/degraph/`: public extractor and analyses.
- `data/`: synthetic benchmarks and reference labels.
- `experiments/`: deterministic evaluators, cached-response scoring and optional model harnesses.
- `results/metrics/revision_2026-09-30/`: current public evaluation outputs.
- `results/metrics/llm_judge_impact.json`: archived, unchanged model selections and original labels.
- `results/metrics/llm_judge_impact_reconciled.json`: derived scoring against current labels,
  with label differences and hashes recorded.
- `results/graphs/`: graph fixtures, including real-code impact fixture.

Other experimental logs are historical development records; current manuscript results use
the documented snapshot and derived outputs above.

## Industrial study and third-party inputs

The two industrial pipelines and their anonymized evaluation bundles are restricted and
are **not included**. The industrial extractor variant and its original freeze point are
separate from the public benchmark implementation. Aggregate results in the manuscript do
not imply public access to the source or bundles. The optional bundle-scoring script accepts
local paths to authorized copies and never downloads or publishes them.

Databricks and baseline source repositories are not redistributed here. Obtain their pinned
revisions separately as documented. MIT licensing of DEGraph does not replace upstream terms.

## License

MIT; see `LICENSE`.
