"""End-to-end change-impact + diff demo on REAL Databricks code (dbdemos retail).

The payoff of the external-validity extractor work: now that DEGraph recovers
column-level lineage from real dbdemos pipelines (DLT/SDP decorators + fluent
chains), we can run the two execution-free capabilities ON real code that neither
BM25 (no structure) nor runtime tools (need execution) can:

  1. STATIC CHANGE-IMPACT: given a table/column a developer is about to change,
     which downstream tables/columns are affected? (forward reachability)
  2. LINEAGE VERSION-DIFF: given before/after source, what lineage changed and
     what breaks? (the pre-merge / CI use case)

Corpus: databricks-demos/dbdemos-notebooks → demo-retail/lakehouse-retail-c360
(real Spark Declarative Pipelines medallion: bronze churn_* -> silver churn_users
/churn_orders -> gold churn_features -> churn_prediction). No code is executed;
everything is static AST + sqlglot extraction.

Run:  python experiments/impact_demo_dbdemos.py
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
from degraph.extractor.assembler import extract_repo  # noqa: E402
from degraph.impact import table_impact, column_impact, build_forward_index  # noqa: E402
from degraph.diff import graph_diff, diff_with_impact  # noqa: E402

RETAIL = Path(r"C:\Users\thapa\Desktop\Research\_external_repos\dbdemos-notebooks"
              r"\demo-retail\lakehouse-retail-c360")
SILVER = Path("01-Data-ingestion/01.2-SDP-python/transformations/02-silver.py")


def _graph(path: Path) -> dict:
    return json.loads(extract_repo(path).model_dump_json())


def main() -> int:
    if not RETAIL.exists():
        print(f"[skip] corpus not found: {RETAIL}\n  re-clone databricks-demos/dbdemos-notebooks")
        return 0

    print("=" * 72)
    print("DEGraph static change-impact + diff on REAL Databricks code (dbdemos)")
    print("=" * 72)

    g = _graph(RETAIL)
    n_tables = len({t["fqn"] for t in g["tables"]})
    n_edges = len(g["edges"])
    print(f"\nExtracted (static, no execution): {n_tables} tables, {n_edges} typed edges.")

    # ---- 1. Static change-impact (table-level reachability) --------------
    print("\n--- 1. CHANGE-IMPACT: 'I'm about to change <table>. What's downstream?' ---")
    for seed in ["churn_users", "churn_orders", "churn_users_bronze"]:
        ds = sorted(table_impact(g, seed))
        print(f"  change {seed:20s} -> affects downstream tables: {ds or '(none)'}")
    print("  (deterministic forward reachability; runtime tools would need to RUN the pipeline)")

    # ---- column-level impact (finer than table-level) --------------------
    print("\n  column-level impact (which downstream COLUMNS depend on a change):")
    for tbl, col in [("churn_users", "creation_date"),
                     ("churn_users", "last_activity_date")]:
        ds = sorted(column_impact(g, tbl, col))
        print(f"    change {tbl}.{col:18s} -> {ds or '(none recovered)'}")

    # ---- 2. Lineage version-diff + breaking-change detection -------------
    print("\n--- 2. VERSION-DIFF: simulate a PR renaming silver `creation_date` -> `signup_date` ---")
    work = REPO / "results" / "_dbdemos_diff_work"
    if work.exists():
        shutil.rmtree(work)
    shutil.copytree(RETAIL, work)
    target = work / SILVER
    src = target.read_text(encoding="utf-8")
    renamed = src.replace('alias("creation_date")', 'alias("signup_date")')
    if renamed == src:
        print("  [warn] rename target not found; source may have changed upstream.")
    target.write_text(renamed, encoding="utf-8")

    g_new = _graph(work)
    d = diff_with_impact(g, g_new)
    print(f"  lineage changes detected: {d['n_changes']}  "
          f"(+{len(d['edges_added'])} edges, -{len(d['edges_removed'])} edges)")

    def _fmt(k: tuple) -> str:  # (kind, src, tgt, cols)
        kind, _src, tgt, cols = k
        return f"{kind} -> {tgt} {list(cols)}"

    # Surface the concrete derives edges whose output column changed.
    rel = [k for k in d["edges_removed"] if "creation_date" in (k[3] or ())]
    add = [k for k in d["edges_added"] if "signup_date" in (k[3] or ())]
    for k in rel:
        print(f"    REMOVED derive: {_fmt(k)}")
    for k in add:
        print(f"    ADDED   derive: {_fmt(k)}")
    # Blast radius of the renamed column, computed on the OLD graph: the gold
    # feature churn_features.days_since_creation still references creation_date,
    # which the silver table no longer produces -> it BREAKS. This is the
    # headline pre-merge signal.
    broke = sorted(column_impact(g, "churn_users", "creation_date"))
    broke = [c for c in broke if "churn_features" in c]  # the genuine downstream
    if broke:
        print("  [BREAKING] renaming churn_users.creation_date breaks downstream "
              "columns that still reference it:")
        for c in broke:
            print(f"    -> {c}  (its code was NOT updated by this PR)")
    print("  This is the answer no runtime tool can give pre-merge and no keyword "
          "search can compute at all.")

    print("\nNote: table-level AND column-level impact + structural diff all work on")
    print("real DLT code. (diff_with_impact's automatic breaking-list is bounded to")
    print("tables with a declared schema; here we query the changed column directly.)")
    shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
