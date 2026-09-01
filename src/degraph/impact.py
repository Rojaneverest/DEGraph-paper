"""Static change-impact analysis over a DEGraph lineage graph.

The paper's spine (post-BM25 pivot): given a column (or table) that a developer
is about to change, compute the set of downstream columns/tables affected —
**statically, from source, without executing the pipeline and without an LLM.**

This is the capability neither baseline provides:
  * keyword retrieval (BM25) has no persistent structure to traverse;
  * runtime lineage tools (OpenLineage/Marquez/dbt) require executing the job.

Approach
--------
DEGraph already resolves per-column backward provenance (compact._build_column_
provenance): each written table column -> the upstream table column(s) it
derives from. We invert that DAG into forward edges (parent.col -> child.col)
and take the transitive closure from the changed column. Table-level impact is
the projection of the column set onto its tables, plus a pure reads/writes
table-reachability pass (a superset that also covers columns with incomplete
column-lineage).

Honest limitation: recall is bounded by the column-provenance resolver — some
SQL-CTE-internal derived columns (e.g. an AVG over a column inside the same
spark.sql block) may not be linked, so they can be missed. This is reported as
precision/recall in experiments/impact_eval.py rather than hidden.
"""
from __future__ import annotations

from typing import Iterable

from .compact import _build_column_provenance


def _col_id(table: str, col: str) -> str:
    return f"{table}.{col}"


def build_forward_index(graph: dict) -> dict[str, set[str]]:
    """Invert backward column-provenance into forward edges: src.col -> {child.col}."""
    prov = _build_column_provenance(graph)
    fwd: dict[str, set[str]] = {}
    for tbl, cols in prov.items():
        for col, info in cols.items():
            child = _col_id(tbl, col)
            for src in (info.get("from") or []):
                fwd.setdefault(src, set()).add(child)
    return fwd


def column_impact(graph: dict, table: str, column: str,
                  _fwd: dict[str, set[str]] | None = None) -> set[str]:
    """Forward transitive closure: all `table.col` downstream of `table.column`.

    `table` may be a bare name (e.g. 'orders_raw') or a fully-qualified FQN; we
    match on suffix so callers need not know the catalog/schema prefix.
    """
    fwd = _fwd if _fwd is not None else build_forward_index(graph)
    nodes = _all_nodes(fwd)
    # Resolve seed key(s). Provenance sources appear at varying granularity:
    #   * table-qualified  "main.db.orders_raw.total_amount"  (resolved to source)
    #   * bare file-local  "claim_status_hdr"                 (suffix-renamed / SQL CTE)
    # Prefer table-qualified matches; if none, fall back to bare-name matches so
    # single-file / suffix-renamed pipelines (e.g. clinical) still seed.
    qualified = [k for k in nodes if k.endswith(f".{column}") and table in k]
    if qualified:
        seeds = qualified
    else:
        seeds = [k for k in nodes if k == column or k.endswith(f".{column}")]
    seen: set[str] = set()
    stack: list[str] = list(seeds)
    while stack:
        n = stack.pop()
        for child in fwd.get(n, ()):  # type: ignore[union-attr]
            if child not in seen:
                seen.add(child)
                stack.append(child)
    return seen


def _all_nodes(fwd: dict[str, set[str]]) -> set[str]:
    nodes = set(fwd.keys())
    for vs in fwd.values():
        nodes |= vs
    return nodes


def table_impact(graph: dict, table: str) -> set[str]:
    """All downstream TABLES reachable from `table` via reads->writes flow.

    A coarse but always-available superset: file reads table A and writes table
    B  =>  A feeds B. Column-incomplete lineage still shows up here.
    """
    # producer: table written by a df; that df's file also reads tables.
    # Build table->table edges: for each write(df->Tout), every table read into
    # that file's df-chain feeds Tout. We approximate via file attribution.
    reads_by_file: dict[str, set[str]] = {}
    writes_by_file: dict[str, set[str]] = {}
    for e in graph.get("edges", []):
        f = e.get("file", "")
        if e.get("kind") == "reads":
            src = e.get("source", "")
            if src.startswith("table:"):
                reads_by_file.setdefault(f, set()).add(src.removeprefix("table:"))
        elif e.get("kind") == "writes":
            tgt = e.get("target", "")
            if tgt.startswith("table:"):
                writes_by_file.setdefault(f, set()).add(tgt.removeprefix("table:"))
    edges: dict[str, set[str]] = {}
    for f, outs in writes_by_file.items():
        for src in reads_by_file.get(f, ()):
            for out in outs:
                edges.setdefault(src, set()).add(out)
    # transitive closure from any FQN matching `table`
    seeds = [t for t in (set(edges) | {x for v in edges.values() for x in v})
             if t == table or t.endswith(f".{table}")]
    seen: set[str] = set()
    stack = list(seeds)
    while stack:
        n = stack.pop()
        for c in edges.get(n, ()):
            if c not in seen:
                seen.add(c)
                stack.append(c)
    return seen


def impact_report(graph: dict, table: str, column: str | None = None) -> dict:
    """Structured impact report for a CLI / programmatic use."""
    if column:
        cols = sorted(column_impact(graph, table, column))
        tables = sorted({c.rsplit(".", 1)[0] for c in cols})
        return {"target": f"{table}.{column}", "impacted_columns": cols,
                "impacted_tables": tables}
    return {"target": table, "impacted_tables": sorted(table_impact(graph, table))}
