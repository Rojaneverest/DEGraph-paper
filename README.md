# DEGraph

Static, execution-free **column-level data lineage for the PySpark DataFrame API**, and the
change-impact and version-diff analyses built on it.

This repository is the artifact release accompanying the preprint:

> **What Breaks Static Column-Level Lineage in Production PySpark? A Tool and Two Industrial
> Case Studies.** Rojan Raj Thapa.

DEGraph parses PySpark repositories (`.py`, `.ipynb`, `.sql`) via Python's `ast` module and
`sqlglot`. **No Spark cluster, no execution, no LLM at extraction time.** It produces a typed,
column-level lineage graph over eight edge types, and answers two questions that runtime
lineage tools cannot answer before a merge:

- **Change impact.** Given a changed column, which downstream columns and tables break?
- **Lineage diff.** Given two revisions, which changes are breaking, and what is the blast radius?

## What is here

| Path | Contents |
|---|---|
| `src/degraph/` | the extractor, graph model, impact and diff analyses |
| `data/benchmarks/` | the three synthetic benchmarks |
| `data/ground_truth/` | hand-labeled ground truth and the sealed test manifest |
| `experiments/` | `impact_eval.py`, `diff_eval.py`, `extractor_precision.py`, `tool_comparison.py`, and the LLM-judge harness |
| `results/graphs/` | extracted graphs, including the committed real-code fixture so the third-party scenarios reproduce without the external repo |
| `results/metrics/` | evaluation outputs |

The preprint source is **not** mirrored here. It lives on arXiv, which is the single
canonical copy; keeping a second one in this repository only invites the two to drift.

## What is not here, and why

The paper's §5.5 external-validity study ran on **two proprietary production pipelines at an
industrial partner**. That corpus is not redistributable and is not in this repository. The
paper reports those results in anonymized form; nothing in this release depends on them, and
every number outside §5.5 reproduces from the committed synthetic and third-party material.

Third-party corpora (Databricks `dbdemos`) are not vendored. See the paper for provenance.

## Reproducing the paper's numbers

```bash
pip install -e .
python experiments/regenerate_graphs.py      # re-extract all benchmarks from src
python experiments/impact_eval.py            # 21 change scenarios
python experiments/diff_eval.py              # 6 edits, breaking vs safe
python experiments/extractor_precision.py    # extractor P/R/F1 vs hand-labeled GT
```

## Honest scope

DEGraph is **intra-procedural** and performs **no alias analysis**. Both are design choices
that buy the precision property the paper relies on, and both cost recall. The paper states
where, and §5.5 measures it on a held-out pipeline rather than asserting it does not happen.
Read §6.2 before relying on the tool.

## License

MIT. See `LICENSE`.
