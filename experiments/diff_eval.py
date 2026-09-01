"""Evaluate DEGraph's lineage version-diff: structural-diff accuracy + breaking-
change classification + blast-radius recall.

`diff.py` is the second half of the change-impact spine. Given the lineage graph
of a repo *before* and *after* a code edit, `diff_with_impact` reports what changed
(added/removed columns) and which removed/renamed columns BREAK downstream code
(the pre-merge / CI signal). Where `impact_eval.py` measures forward reachability
from a single column, this measures the diff end-to-end on realistic source edits.

Method. For each edit we apply a one-line source change to a temp copy of a
benchmark, re-extract both graphs (no execution), run `diff_with_impact`, and
compare to ground truth hand-derived from source. We report three things:

  1. STRUCTURAL DIFF (per edit, pooled): precision/recall of the detected
     {removed ∪ added} column set vs. the columns the edit actually changes.
  2. BREAKING CLASSIFICATION (the headline CI signal): for each edit, does the
     diff correctly flag it as breaking vs. safe? Reported as a confusion over
     edits — we want zero missed breaks AND zero false alarms on safe changes.
  3. BLAST-RADIUS RECALL (breaking edits only): of the downstream columns a
     breaking change actually affects, how many does the diff surface? This
     inherits the column-provenance resolver's recall (see impact_eval.py), so
     the misses are the same CTE-internal family; reported honestly, not as a
     separate capability claim.

Scope. Edits are on the in-repo synthetic benchmarks so the eval is fully
reproducible (no external repo, no API). The real-code (dbdemos DLT) diff is
demonstrated separately in `impact_demo_dbdemos.py`; column-level rename detection
there is bounded because DLT `select(...).alias()` provenance keys on the source
column, not the output alias (documented limitation, paper §5.5).

Run:  python experiments/diff_eval.py
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
from degraph.extractor.assembler import extract_repo  # noqa: E402
from degraph.diff import diff_with_impact  # noqa: E402

_PREFIXES = ("main.dbdemos_ecom.", "meridian.silver.", "meridian.bronze.", "meridian.")


def _short(fqn: str) -> str:
    for p in _PREFIXES:
        if fqn.startswith(p):
            return fqn[len(p):]
    return fqn


# Each edit: (label, benchmark, file, old_str, new_str, GT). GT fields:
#   removed/added  : columns the edit changes at the lineage level (table.col)
#   breaking       : True if a removed/renamed column has downstream dependents
#   blast          : full downstream set a breaking change affects (from source);
#                    empty for safe edits. Bounded by impact recall (CTE-internal
#                    derivations are the known misses).
EDITS = [
    # ---- breaking edits (3) -------------------------------------------------
    dict(
        label="E1 rename silver agg (lifetime_revenue->ltv_total)",
        bench="repo_synthetic_small", file="silver/customer_profile.py",
        old='alias("lifetime_revenue")', new='alias("ltv_total")',
        removed={"customer_profile.lifetime_revenue"},
        added={"customer_profile.ltv_total"},
        breaking=True,
        blast={
            "customer_ltv.lifetime_revenue", "customer_ltv.avg_country_ltv",
            "customer_ltv.ltv_vs_country_avg", "customer_ltv.revenue_quartile",
            "customer_ltv.ltv_tier",
        },
    ),
    dict(
        label="E5 rename silver ts (last_order_ts->most_recent_order_ts)",
        bench="repo_synthetic_small", file="silver/customer_profile.py",
        old='alias("last_order_ts")', new='alias("most_recent_order_ts")',
        removed={"customer_profile.last_order_ts"},
        added={"customer_profile.most_recent_order_ts"},
        breaking=True,
        blast={"customer_ltv.last_order_ts", "customer_ltv.days_since_last_order"},
    ),
    dict(
        label="E6 rename silver agg in MEDIUM (cross-gold blast)",
        bench="repo_synthetic_medium", file="silver/customer_profile.py",
        old='alias("lifetime_revenue")', new='alias("ltv_total")',
        removed={"customer_profile.lifetime_revenue"},
        added={"customer_profile.ltv_total"},
        breaking=True,
        blast={
            "customer_ltv.lifetime_revenue", "customer_ltv.avg_country_ltv",
            "customer_ltv.ltv_vs_country_avg", "customer_ltv.revenue_quartile",
            "customer_ltv.ltv_tier", "revenue_forecast_features.lifetime_revenue",
        },
    ),
    # ---- safe edits (3) -----------------------------------------------------
    dict(
        label="E2 drop leaf silver col (is_high_value)",
        bench="repo_synthetic_small", file="silver/orders_cleaned.py",
        old='        .withColumn("is_high_value", F.col("total_amount") > F.lit(1000))\n',
        new='',
        removed={"orders_cleaned.is_high_value"}, added=set(),
        breaking=False, blast=set(),
    ),
    dict(
        label="E3 rename leaf gold col (ltv_tier->ltv_segment)",
        bench="repo_synthetic_small", file="gold/customer_ltv.py",
        old='"ltv_tier",', new='"ltv_segment",',
        removed={"customer_ltv.ltv_tier"}, added={"customer_ltv.ltv_segment"},
        breaking=False, blast=set(),
    ),
    dict(
        label="E4 add new leaf silver col (is_low_value)",
        bench="repo_synthetic_small", file="silver/orders_cleaned.py",
        old='        .withColumn("is_high_value", F.col("total_amount") > F.lit(1000))',
        new=('        .withColumn("is_high_value", F.col("total_amount") > F.lit(1000))\n'
             '        .withColumn("is_low_value", F.col("total_amount") < F.lit(10))'),
        removed=set(), added={"orders_cleaned.is_low_value"},
        breaking=False, blast=set(),
    ),
]


def _graph(path: Path) -> dict:
    return json.loads(extract_repo(path).model_dump_json())


def _apply_diff(edit: dict) -> dict:
    src = REPO / "data" / "benchmarks" / edit["bench"]
    old = _graph(src)
    with tempfile.TemporaryDirectory() as td:
        dst = Path(td) / edit["bench"]
        shutil.copytree(src, dst)
        target = dst / edit["file"]
        text = target.read_text(encoding="utf-8")
        if edit["old"] not in text:
            raise SystemExit(f"{edit['label']}: edit anchor not found in {edit['file']}")
        target.write_text(text.replace(edit["old"], edit["new"]), encoding="utf-8")
        new = _graph(dst)
    return diff_with_impact(old, new)


def main() -> int:
    # structural diff accumulators
    s_tp = s_fp = s_fn = 0
    # breaking classification confusion (over edits)
    correct_break = correct_safe = false_alarm = missed_break = 0
    # blast-radius accumulators (breaking edits only)
    b_tp = b_fn = 0

    print(f"{'edit':52s} {'struct':>8s} {'break?':>10s} {'blast':>10s}")
    print("-" * 86)
    for e in EDITS:
        d = _apply_diff(e)
        det_removed = {_short(f"{t}.{c}") for t, cs in d["columns_removed"].items() for c in cs}
        det_added = {_short(f"{t}.{c}") for t, cs in d["columns_added"].items() for c in cs}
        det_struct = det_removed | det_added
        gt_struct = e["removed"] | e["added"]
        s_tp += len(det_struct & gt_struct)
        s_fp += len(det_struct - gt_struct)
        s_fn += len(gt_struct - det_struct)

        det_breaking = len(d["breaking_columns"]) > 0
        if e["breaking"] and det_breaking:
            correct_break += 1
            verdict = "BREAK"
        elif (not e["breaking"]) and (not det_breaking):
            correct_safe += 1
            verdict = "safe"
        elif (not e["breaking"]) and det_breaking:
            false_alarm += 1
            verdict = "FALSE ALARM"
        else:
            missed_break += 1
            verdict = "MISSED BREAK"

        # blast recall on breaking edits
        blast_str = "-"
        if e["breaking"]:
            det_blast = {_short(x) for k, vs in d["impact_of_removed"].items() for x in vs}
            tp = len(det_blast & e["blast"])
            fn = len(e["blast"] - det_blast)
            b_tp += tp
            b_fn += fn
            blast_str = f"{tp}/{tp + fn}"

        sp = len(det_struct & gt_struct)
        print(f"{e['label']:52s} {f'{sp}/{len(gt_struct)}':>8s} {verdict:>10s} {blast_str:>10s}")
        if det_struct != gt_struct:
            if det_struct - gt_struct:
                print(f"    struct FALSE POSITIVE: {sorted(det_struct - gt_struct)}")
            if gt_struct - det_struct:
                print(f"    struct missed: {sorted(gt_struct - det_struct)}")

    print("-" * 86)
    sP = s_tp / (s_tp + s_fp) if (s_tp + s_fp) else 1.0
    sR = s_tp / (s_tp + s_fn) if (s_tp + s_fn) else 1.0
    sF = 2 * sP * sR / (sP + sR) if (sP + sR) else 0.0
    print(f"STRUCTURAL DIFF (cols added/removed): P {sP*100:.0f}%  R {sR*100:.0f}%  "
          f"F1 {sF*100:.0f}%  (tp={s_tp} fp={s_fp} fn={s_fn})")
    n_break = correct_break + missed_break
    n_safe = correct_safe + false_alarm
    print(f"BREAKING CLASSIFICATION: {correct_break}/{n_break} breaks caught, "
          f"{correct_safe}/{n_safe} safe edits not false-alarmed  "
          f"(false alarms={false_alarm}, missed breaks={missed_break})")
    bR = b_tp / (b_tp + b_fn) if (b_tp + b_fn) else 1.0
    print(f"BLAST-RADIUS RECALL (breaking edits): {bR*100:.0f}%  (tp={b_tp} fn={b_fn}) "
          f"-- inherits impact recall; misses are the residual named gaps")
    return 0


if __name__ == "__main__":
    sys.exit(main())
