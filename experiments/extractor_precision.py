"""A2 — Extractor precision / recall / F1 per edge type, vs hand-labeled GT.

The foundation of the tool paper: recall alone is uninterpretable (emit
everything -> 100% recall, ~0% precision). This measures BOTH directions per
edge type against the hand-labeled ground-truth lineage graph.

  TP = edges in both GT and extracted   (matched)
  FP = edges extracted but not in GT    (precision loss)
  FN = edges in GT but not extracted    (recall loss)
  Precision = TP/(TP+FP)   Recall = TP/(TP+FN)   F1 = 2PR/(P+R)

Edge identity uses the same semantic key as `degraph compare` (kind + file +
endpoints, df line-numbers stripped, plus per-kind discriminators).

Run:  python experiments/extractor_precision.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
from degraph.cli import _edge_key  # reuse the canonical edge key  # noqa: E402
from degraph.extractor.assembler import extract_repo  # noqa: E402

# Benchmarks with a hand-labeled ground-truth graph.
GT = {
    "repo_synthetic_small": REPO / "data" / "ground_truth" / "repo_synthetic_small.graph.json",
}
EDGE_KINDS = ["reads", "writes", "derives", "filters", "joins",
              "aggregates", "opaque_transform", "projects"]


def _sem_key(e: dict) -> str:
    """Node-name-agnostic edge key.

    The hand-labeled GT names each intermediate DataFrame with an idealized
    step name (orders_trimmed -> orders_normalized -> ...), while the extractor
    uses the actual (reused) source variable (orders = orders.withColumn(...)).
    Same operation, same columns, different intermediate label -> a strict match
    counts these as errors. The semantic key drops df-intermediate identities,
    keeping only stable anchors (table:/ext: endpoints) + the content
    discriminator, to measure whether the *lineage* was recovered.
    """
    kind = e.get("kind", "?")
    file = e.get("file", "?")

    def anchor(v: str) -> str:
        v = v or ""
        return v if (v.startswith("table:") or v.startswith("ext:")) else "df"

    src = anchor(e.get("source") or e.get("left_source", ""))
    tgt = anchor(e.get("target", ""))
    if kind == "derives":
        d = e.get("output_col", "?")
    elif kind == "aggregates":
        d = ",".join(sorted(e.get("output_cols") or e.get("group_keys") or []))
    elif kind == "writes":
        d = f"{anchor(e.get('target',''))}:{e.get('mode','?')}"
        src = "df"
    elif kind == "opaque_transform":
        d = e.get("operator", "?")
    elif kind == "joins":
        d = ",".join(sorted("=".join(p) for p in (e.get("join_keys") or [])))
    elif kind == "filters":
        d = ",".join(sorted(e.get("referenced_cols") or []))
    elif kind == "projects":
        d = ",".join(sorted((e.get("removed_cols") or []) + (e.get("kept_cols") or [])))
    else:
        d = ""
    return f"{kind}|{file}|{src}->{tgt}|{d}"


def _keys_by_kind(graph: dict, keyfn=_edge_key) -> dict[str, set[str]]:
    by: dict[str, set[str]] = {}
    for e in graph.get("edges", []):
        by.setdefault(e.get("kind", "?"), set()).add(keyfn(e))
    return by


def _prf(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    p = tp / (tp + fp) if (tp + fp) else 1.0
    r = tp / (tp + fn) if (tp + fn) else 1.0
    f = 2 * p * r / (p + r) if (p + r) else 0.0
    return p, r, f


def main() -> int:
    for bench, gt_path in GT.items():
        gt = json.loads(gt_path.read_text(encoding="utf-8"))
        ex = json.loads(extract_repo(REPO / "data" / "benchmarks" / bench).model_dump_json())
        for label, keyfn in (("STRICT (node-name-sensitive)", _edge_key),
                             ("SEMANTIC (node-name-agnostic)", _sem_key)):
            _report(bench, gt, ex, label, keyfn)
    return 0


def _report(bench, gt, ex, label, keyfn):
        gtb = _keys_by_kind(gt, keyfn)
        exb = _keys_by_kind(ex, keyfn)

        print(f"\n=== {bench} — extractor P/R/F1 per edge type — {label} ===")
        print(f"{'edge type':18s} {'GT':>4s} {'EX':>4s} {'TP':>4s} {'FP':>4s} {'FN':>4s}  "
              f"{'P':>5s} {'R':>5s} {'F1':>5s}")
        print("-" * 70)
        TP = FP = FN = 0
        for k in EDGE_KINDS:
            g, e = gtb.get(k, set()), exb.get(k, set())
            tp, fp, fn = len(g & e), len(e - g), len(g - e)
            if not (g or e):
                continue
            TP += tp; FP += fp; FN += fn
            p, r, f = _prf(tp, fp, fn)
            print(f"{k:18s} {len(g):4d} {len(e):4d} {tp:4d} {fp:4d} {fn:4d}  "
                  f"{p*100:4.0f}% {r*100:4.0f}% {f*100:4.0f}%")
        print("-" * 70)
        P, R, F = _prf(TP, FP, FN)
        print(f"{'OVERALL (micro)':18s} {TP+FN:4d} {TP+FP:4d} {TP:4d} {FP:4d} {FN:4d}  "
              f"{P*100:4.0f}% {R*100:4.0f}% {F*100:4.0f}%")

        # table-level
        gt_t = {t["fqn"] for t in gt.get("tables", [])}
        ex_t = {t["fqn"] for t in ex.get("tables", [])}
        tp, fp, fn = len(gt_t & ex_t), len(ex_t - gt_t), len(gt_t - ex_t)
        p, r, f = _prf(tp, fp, fn)
        print(f"{'tables':18s} {len(gt_t):4d} {len(ex_t):4d} {tp:4d} {fp:4d} {fn:4d}  "
              f"{p*100:4.0f}% {r*100:4.0f}% {f*100:4.0f}%")


if __name__ == "__main__":
    sys.exit(main())
