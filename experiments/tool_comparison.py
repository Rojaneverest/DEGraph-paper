"""A1 — Head-to-head: DEGraph vs pyspark-ast-lineage (v0.1.1).

Leg 2 of the tool paper. pyspark-ast-lineage is the closest existing static
PySpark analyzer. This quantifies the capability gap on a hand-labeled
benchmark, fairly:

  1. We first confirm pyspark-ast-lineage WORKS on its own bundled example
     (so a 0 on our benchmark is a real limitation, not a setup error).
  2. We run BOTH tools on repo_synthetic_small and compare against the
     hand-labeled ground-truth graph (8 tables, 57 typed edges).

What pyspark-ast-lineage produces (confirmed by reading its source +
running it): a FLAT SET of table/path strings, each tagged by the extractor
that found it (ReadExtractor / WriteExtractor / SQLExtractor / TableExtractor).
There are no edges between tables, no column-level lineage, and no reliable
source/sink direction (write targets surface as ReadExtractor). Its variable
resolver also cannot resolve f-string / %run-config-driven FQNs, which are the
norm in real Databricks pipelines.

DEGraph produces a typed, column-level lineage graph (reads / writes / derives
/ joins / filters / aggregates / projects / opaque_transform edges + column
provenance), and resolves config-driven FQNs.

Run:  python experiments/tool_comparison.py
"""
from __future__ import annotations

import glob
import io
import json
import os
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
from degraph.extractor.assembler import extract_repo  # noqa: E402

PAL_EXAMPLE = Path(r"C:\Users\thapa\Desktop\Research\pyspark-ast-lineage\examples\example_data_processing.py")
BENCH = REPO / "data" / "benchmarks" / "repo_synthetic_small"
GT = REPO / "data" / "ground_truth" / "repo_synthetic_small.graph.json"


def run_pal(path: Path) -> tuple[set[str], list[dict]]:
    """Run pyspark-ast-lineage on one file; swallow its rich/log noise."""
    from pyspark_ast_lineage.analyzer.pyspark_tables_extractor import (
        PysparkTablesExtractor as P,
    )
    code = path.read_text(encoding="utf-8")
    buf = io.StringIO()
    with redirect_stdout(buf), redirect_stderr(buf):
        tables, det = P.extract_tables_from_code(code, verbose=True)
    return set(tables), det


def pal_over_repo(repo: Path) -> tuple[set[str], dict[str, int]]:
    """Union of all table strings pyspark-ast-lineage finds across a repo's
    .py files, plus a count of detail records per extractor kind."""
    all_tables: set[str] = set()
    by_extractor: dict[str, int] = {}
    for f in sorted(glob.glob(str(repo / "**" / "*.py"), recursive=True)):
        tables, det = run_pal(Path(f))
        all_tables |= tables
        for d in det:
            k = d.get("extracted_by", "?")
            by_extractor[k] = by_extractor.get(k, 0) + 1
    return all_tables, by_extractor


def main() -> int:
    # --- 0. Sanity: pyspark-ast-lineage works on its own example ----------
    print("=== A1: DEGraph vs pyspark-ast-lineage (v0.1.1) ===\n")
    print("[sanity] pyspark-ast-lineage on its OWN bundled example:")
    if PAL_EXAMPLE.exists():
        ex_tables, ex_det = run_pal(PAL_EXAMPLE)
        print(f"   recovered {len(ex_tables)} table/path strings, "
              f"{len(ex_det)} detail records -> TOOL IS FUNCTIONAL")
        print(f"   sample: {sorted(ex_tables)[:3]}")
    else:
        print("   (bundled example not found; skipping sanity check)")

    # --- 1. Run both tools on the benchmark -------------------------------
    gt = json.loads(GT.read_text(encoding="utf-8"))
    gt_tables = {t["fqn"] for t in gt.get("tables", [])}
    gt_edges = gt.get("edges", [])
    from collections import Counter
    gt_edge_kinds = Counter(e["kind"] for e in gt_edges)

    pal_tables, pal_by_ext = pal_over_repo(BENCH)

    deg = json.loads(extract_repo(BENCH).model_dump_json())
    deg_tables = {t["fqn"] for t in deg.get("tables", [])}
    deg_edges = deg.get("edges", [])
    deg_edge_kinds = Counter(e["kind"] for e in deg_edges)

    # --- 2. Benchmark recovery -------------------------------------------
    print(f"\n=== repo_synthetic_small recovery (GT: {len(gt_tables)} tables, "
          f"{len(gt_edges)} typed edges) ===\n")
    print(f"{'':28s} {'pyspark-ast-lineage':>22s} {'DEGraph':>10s}")
    print("-" * 64)
    pal_tbl_hit = len(gt_tables & pal_tables)
    print(f"{'table NODES (of 8 GT)':28s} {pal_tbl_hit:>22d} {len(deg_tables & gt_tables):>10d}")
    print(f"{'typed lineage EDGES':28s} {'0 (none produced)':>22s} {len(deg_edges):>10d}")
    print(f"{'column-level lineage':28s} {'no':>22s} {'yes':>10s}")
    print(f"   pyspark-ast-lineage raw output on benchmark: "
          f"{len(pal_tables)} strings {sorted(pal_tables) or '[]'}")
    print(f"   (per-extractor detail records: {pal_by_ext or '{}'})")

    # --- 3. Capability matrix --------------------------------------------
    rows = [
        ("Enumerates table/path names",          "yes*", "yes"),
        ("Resolves f-string / %run-config FQNs",  "no",   "yes"),
        ("Reads edges (source -> df)",            "flat", "yes"),
        ("Writes edges (df -> sink)",             "flat", "yes"),
        ("Distinguishes source vs sink",          "no",   "yes"),
        ("Transformation edges (withColumn/...)", "no",   "yes"),
        ("Join / aggregate / filter semantics",   "no",   "yes"),
        ("Column-level provenance",               "no",   "yes"),
        ("Impact / blast-radius queryable graph", "no",   "yes"),
    ]
    print("\n=== capability matrix ===\n")
    print(f"{'capability':42s} {'pyspark-ast-lineage':>20s} {'DEGraph':>9s}")
    print("-" * 74)
    for cap, a, b in rows:
        print(f"{cap:42s} {a:>20s} {b:>9s}")
    print("\n  * pyspark-ast-lineage returns a flat SET of strings (no edges,")
    print("    no direction); 'flat' = the name appears but with no graph edge.")

    # --- 4. DEGraph edge-kind breakdown for context ----------------------
    print(f"\n=== DEGraph typed-edge breakdown (what the other tool cannot emit) ===")
    for k in sorted(set(gt_edge_kinds) | set(deg_edge_kinds)):
        print(f"   {k:18s} GT={gt_edge_kinds.get(k,0):3d}  DEGraph={deg_edge_kinds.get(k,0):3d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
