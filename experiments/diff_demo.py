"""End-to-end demo: edit PySpark source -> DEGraph detects the lineage change
and its downstream blast radius. Statically. No execution. No LLM.

Simulates a realistic developer change on a copy of a benchmark, re-extracts the
graph, and diffs it against the original. Demonstrates the change-impact spine
(impact.py + diff.py) on an actual code edit.

Run:  python experiments/diff_demo.py
"""
from __future__ import annotations

import json
import re
import shutil
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
from degraph.extractor.assembler import extract_repo  # noqa: E402
from degraph.diff import diff_with_impact  # noqa: E402

# A realistic breaking change: a developer renames the `lifetime_revenue`
# aggregate output in the silver customer_profile job. Gold customer_ltv still
# reads `lifetime_revenue` -> this rename breaks downstream columns.
BENCH = "repo_synthetic_small"
EDIT_FILE = "silver/customer_profile.py"
OLD = 'F.sum("total_amount").alias("lifetime_revenue")'
NEW = 'F.sum("total_amount").alias("ltv_total")'


def main() -> int:
    src = REPO / "data" / "benchmarks" / BENCH
    old_graph = extract_repo(src)  # before
    old = json.loads(old_graph.model_dump_json())

    with tempfile.TemporaryDirectory() as td:
        dst = Path(td) / BENCH
        shutil.copytree(src, dst)
        target = dst / EDIT_FILE
        text = target.read_text(encoding="utf-8")
        if OLD not in text:
            print(f"EDIT anchor not found in {EDIT_FILE}; aborting.")
            return 1
        target.write_text(text.replace(OLD, NEW), encoding="utf-8")
        new_graph = extract_repo(dst)  # after
    new = json.loads(new_graph.model_dump_json())

    print(f"CHANGE: in {BENCH}/{EDIT_FILE}")
    print(f'   rename agg output  lifetime_revenue -> ltv_total\n')

    d = diff_with_impact(old, new)

    def short(x: str) -> str:
        for p in ("main.dbdemos_ecom.", "meridian.silver.", "meridian.bronze."):
            x = x.replace(p, "")
        return x

    print(f"DEGraph detected {d['n_changes']} lineage change(s):")
    for t, cols in d["columns_removed"].items():
        print(f"   - removed columns in {short(t)}: {cols}")
    for t, cols in d["columns_added"].items():
        print(f"   + added columns in {short(t)}: {cols}")
    print(f"   edges removed: {len(d['edges_removed'])}  | edges added: {len(d['edges_added'])}")
    print()
    if d["breaking_columns"]:
        print("[BREAKING] removed/renamed columns that downstream depends on:")
        for col in d["breaking_columns"]:
            ds = [short(x) for x in d["impact_of_removed"][col]]
            print(f"   {short(col)}  ->  breaks {len(ds)} downstream column(s): {ds}")
    else:
        print("No downstream columns depend on the removed/renamed columns.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
