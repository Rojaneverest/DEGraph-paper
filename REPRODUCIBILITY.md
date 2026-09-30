# Reproducing manuscript snapshot degraph-2026-09-30

This guide identifies inputs and scope. No paid model calls are part of the default path.
The software snapshot accompanies a manuscript undergoing final review. The files are
identified by `MANIFEST.sha256`; the manuscript is not included in this software release.

## Environment

Tested with Python 3.14.6. See `requirements-reproduce.txt` for pinned public-evaluation
requirements. Install in a virtual environment, then `pip install --no-deps -e .`.
The code declares Python >=3.10; other supported interpreter versions were not tested
in this revision. Paths in commands are relative to the artifact root unless stated otherwise.

## Public evaluations

```bash
python experiments/regenerate_graphs.py --check
python experiments/extractor_precision.py
python experiments/impact_eval.py
python experiments/diff_eval.py
python experiments/score_cached_judges.py
python experiments/_grade_bm25_gemini.py
python -m pytest tests/ -q
```

Graph checking ignores machine/runtime metadata but compares graph content. The three
synthetic graphs contain 57, 121 and 174 edges. Impact uses those committed graphs and
the committed Databricks impact fixture. To exercise extraction before impact, run
`python experiments/regenerate_graphs.py`, then rerun `impact_eval.py`.

`extractor_precision.py` reports strict and normalized edge keys. The latter discards
intermediate node identities and omits `source_cols` for derives; 98% is not complete
column-provenance verification. `extractor_precision_dbdemos.py` uses a distinct key
that includes source columns for derives. Neither is a full semantic equivalence test.

## Databricks source extraction

The recorded upstream snapshot is:

- Repository: https://github.com/databricks-demos/dbdemos-notebooks
- Revision: `fc48898c335df24745491f41c12a66cd0de438ca`
- Slice: `demo-retail/lakehouse-retail-c360/01-Data-ingestion/01.2-SDP-python/transformations`
- Local evaluated source fingerprints: `evaluation_inputs.json`.

Obtain the repository separately under its own license. Do not redistribute its source
under DEGraph's MIT license.

```bash
git clone https://github.com/databricks-demos/dbdemos-notebooks.git reference/dbdemos-notebooks
git -C reference/dbdemos-notebooks checkout fc48898c335df24745491f41c12a66cd0de438ca
python experiments/extractor_precision_dbdemos.py --source-dir reference/dbdemos-notebooks/demo-retail/lakehouse-retail-c360/01-Data-ingestion/01.2-SDP-python/transformations
```

For the optional real-source pytest, set `DBDEMOS_SDP` to that transformations directory.
The archived real-code impact fixture is independent of whether this source is downloaded.
Its larger graph and the 36-edge reference slice are different evaluation objects.

## Baseline comparison

`pyspark-ast-lineage` is installed from pinned source; the named PyPI release was not
available during this verification.

```bash
git clone https://github.com/richardesp/pyspark-ast-lineage.git reference/pyspark-ast-lineage
git -C reference/pyspark-ast-lineage checkout 1aaa0dabaab4b9f1a1527ccee057a7ffa5fb1b75
python -m pip install -r requirements-baseline.txt
python -m pip install --no-deps -e reference/pyspark-ast-lineage
python experiments/tool_comparison.py --baseline-example reference/pyspark-ast-lineage/examples/example_data_processing.py
```

This revision declares version 0.1.1. Its bundled example recovers seven strings and
nine detail records; the DEGraph small benchmark yields zero baseline strings.

## Cached judge agreement and context grading

`llm_judge_impact.json` is an archival response file. `score_cached_judges.py` preserves
its candidates/model selections and derives author labels from current `SCENARIOS`.
It writes a separate reconciled result with the source/label hashes and changed entries.
There are two author-label changes per judge, concerning an intermediate CTE field.
The 229 candidates per judge are fixed; this is not a newly sampled or newly judged trial.

The Gemini context script recalculates saved manual 0/1/2 grades: 103, 98 and 90 out
of 120 available points for raw, BM25 and graph. Four-family raw/graph histories are
in `_grade_test_v11.py` and `_grade_test_v12.py`; they are not a four-family BM25 study.
Running model harnesses afresh requires API credentials and optional dependencies and
can incur cost. It is unnecessary for recomputing the published cached summaries.

## Restricted industrial results

The industrial source and bundles are not public. With authorized local copies only:

```bash
python experiments/score_industrial_bundles.py --tool-src /path/to/industrial-tool/src --bundles /path/to/industrial-tool/results/eval --output /path/to/private/scores.json
```

Re-scoring requires the separately maintained industrial implementation and authorized
bundles. It does not repeat source extraction or independently validate labels. No
industrial source, bundle or aggregate evaluation output is included in this software release.

## Results and limitations

Current public outputs are under `results/metrics/revision_2026-09-30/`. The tests include
identifier-substring and qualifier ambiguity regressions. Bare-column compatibility remains
a documented heuristic. These gates cover identified risks and do not prove general safety.

## Manuscript status

The manuscript is undergoing final review and is not included in this software snapshot.
A public manuscript link will be added after that review and disclosure checks are complete.
