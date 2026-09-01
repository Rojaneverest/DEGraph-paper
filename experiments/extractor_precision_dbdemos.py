"""Extractor precision/recall/F1 on REAL third-party code (dbdemos retail SDP).

The second hand-labeled ground truth (paper §6.2's top future-work item): unlike
`extractor_precision.py`, which scores against our own synthetic benchmark, this
scores against a hand-labeled lineage graph for a slice of *real* Databricks code
(`data/ground_truth/dbdemos_retail_sdp.graph.json` — the dbdemos retail Spark
Declarative Pipelines bronze/silver/gold transformations). It turns §5.5's
*coverage* argument into a genuine precision/recall number on code we did not write.

Method mirrors the synthetic A2 (semantic, node-name-agnostic matching) with one
refinement: derive edges are keyed on (file, output_col, source_cols) so that the
same output column produced from different sources in one file (e.g. silver
`creation_date` from `creation_date` vs from `transaction_date`) is not conflated.

Caveat (stated, not hidden): single annotator (paper author), labeled from source
only to avoid circularity. This addresses the "synthetic-only" threat, not the
"single-annotator" threat.

Run:  python experiments/extractor_precision_dbdemos.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
from degraph.extractor.assembler import extract_repo  # noqa: E402

GT_PATH = REPO / "data" / "ground_truth" / "dbdemos_retail_sdp.graph.json"
SLICE = Path(r"C:\Users\thapa\Desktop\Research\_external_repos\dbdemos-notebooks"
             r"\demo-retail\lakehouse-retail-c360\01-Data-ingestion"
             r"\01.2-SDP-python\transformations")
EDGE_KINDS = ["reads", "writes", "derives", "aggregates", "joins"]


def _sem_key(e: dict) -> str:
    """Node-name-agnostic edge key (A2 semantic + source_cols on derives)."""
    kind = e.get("kind", "?")
    file = e.get("file", "?")

    def anchor(v: str) -> str:
        v = v or ""
        return v if (v.startswith("table:") or v.startswith("ext:")) else "df"

    if kind == "reads":
        d = anchor(e.get("source", ""))
    elif kind == "writes":
        d = anchor(e.get("target", ""))
    elif kind == "derives":
        sc = ",".join(sorted(e.get("source_cols") or []))
        d = f"{e.get('output_col', '?')}<-{sc}"
    elif kind == "aggregates":
        d = ",".join(sorted(e.get("output_cols") or []))
    elif kind == "joins":
        d = ",".join(sorted("=".join(p) for p in (e.get("join_keys") or [])))
    else:
        d = ""
    return f"{kind}|{file}|{d}"


def _keys_by_kind(graph: dict) -> dict[str, set[str]]:
    by: dict[str, set[str]] = {}
    for e in graph.get("edges", []):
        by.setdefault(e.get("kind", "?"), set()).add(_sem_key(e))
    return by


def _prf(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    p = tp / (tp + fp) if (tp + fp) else 1.0
    r = tp / (tp + fn) if (tp + fn) else 1.0
    f = 2 * p * r / (p + r) if (p + r) else 0.0
    return p, r, f


def main() -> int:
    if not SLICE.exists():
        print(f"[skip] corpus not found: {SLICE}")
        return 0
    gt = json.loads(GT_PATH.read_text(encoding="utf-8"))
    ex = json.loads(extract_repo(SLICE).model_dump_json())
    gtb, exb = _keys_by_kind(gt), _keys_by_kind(ex)

    print("=== Extractor P/R/F1 vs hand-labeled REAL code (dbdemos retail SDP) ===")
    print(f"{'edge type':14s} {'GT':>4s} {'EX':>4s} {'TP':>4s} {'FP':>4s} {'FN':>4s}  "
          f"{'P':>5s} {'R':>5s} {'F1':>5s}")
    print("-" * 64)
    TP = FP = FN = 0
    for k in EDGE_KINDS:
        g, e = gtb.get(k, set()), exb.get(k, set())
        tp, fp, fn = len(g & e), len(e - g), len(g - e)
        if not (g or e):
            continue
        TP += tp; FP += fp; FN += fn
        p, r, f = _prf(tp, fp, fn)
        print(f"{k:14s} {len(g):4d} {len(e):4d} {tp:4d} {fp:4d} {fn:4d}  "
              f"{p*100:4.0f}% {r*100:4.0f}% {f*100:4.0f}%")
    print("-" * 64)
    P, R, F = _prf(TP, FP, FN)
    print(f"{'OVERALL':14s} {TP+FN:4d} {TP+FP:4d} {TP:4d} {FP:4d} {FN:4d}  "
          f"{P*100:4.0f}% {R*100:4.0f}% {F*100:4.0f}%")

    gt_t = {t["fqn"] for t in gt.get("tables", [])}
    ex_t = {t["fqn"] for t in ex.get("tables", [])}
    tp, fp, fn = len(gt_t & ex_t), len(ex_t - gt_t), len(gt_t - ex_t)
    p, r, f = _prf(tp, fp, fn)
    print(f"{'tables':14s} {len(gt_t):4d} {len(ex_t):4d} {tp:4d} {fp:4d} {fn:4d}  "
          f"{p*100:4.0f}% {r*100:4.0f}% {f*100:4.0f}%")

    # Surface the misses for the writeup.
    print("\nFalse negatives (GT edges the extractor missed):")
    for k in EDGE_KINDS:
        for key in sorted(gtb.get(k, set()) - exb.get(k, set())):
            print(f"  MISS  {key}")
    print("False positives (extracted edges not in GT):")
    for k in EDGE_KINDS:
        for key in sorted(exb.get(k, set()) - gtb.get(k, set())):
            print(f"  EXTRA {key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
