"""Lineage-graph version diff: what changed between two DEGraph graphs.

The second half of the static change-impact story (the first is impact.py).
Given the lineage graph of a repo *before* and *after* a code change, report
exactly what changed at the lineage level — added/removed tables and columns,
added/removed/retargeted lineage edges — and (combined with impact.py) the
downstream blast radius of those changes.

This is what makes the lineage a *versionable artifact*: a keyword index (BM25)
has nothing to diff, and runtime lineage tools would need to re-execute both
versions. DEGraph diffs two static graphs produced from source.

Used for: CI / pre-merge review ("this PR changes the lineage of N downstream
columns"), drift detection, and the paper's change-impact evaluation.
"""
from __future__ import annotations

from .compact import _build_column_provenance
from .impact import build_forward_index, column_impact


def _columns_by_table(graph: dict) -> dict[str, set[str]]:
    prov = _build_column_provenance(graph)
    return {t: set(cols.keys()) for t, cols in prov.items()}


def _edge_key(e: dict) -> tuple:
    """Canonical identity of a lineage edge (kind + endpoints + key columns).

    Node ids embed file:line which churn across unrelated edits, so we key on
    the semantic shape: kind, source/target table-or-df *name*, and the column
    fields. Two graphs' edges with the same key are 'the same edge'.
    """
    def node(v: str) -> str:
        v = v or ""
        if v.startswith("table:"):
            return v
        # df:<file>:<line>:<var>  -> drop the volatile line number
        parts = v.split(":")
        return ":".join(parts[:2] + parts[3:]) if len(parts) >= 4 else v
    kind = e.get("kind", "")
    src = node(e.get("source") or e.get("left_source", ""))
    tgt = node(e.get("target", ""))
    cols = tuple(sorted(
        (e.get("source_cols") or [])
        + ([e.get("output_col")] if e.get("output_col") else [])
        + (e.get("output_cols") or [])
    ))
    return (kind, src, tgt, cols)


def graph_diff(old: dict, new: dict) -> dict:
    """Structured lineage diff between two graphs."""
    old_cols = _columns_by_table(old)
    new_cols = _columns_by_table(new)
    old_tables, new_tables = set(old_cols), set(new_cols)

    tables_added = sorted(new_tables - old_tables)
    tables_removed = sorted(old_tables - new_tables)

    cols_added: dict[str, list[str]] = {}
    cols_removed: dict[str, list[str]] = {}
    for t in sorted(old_tables & new_tables):
        a = new_cols[t] - old_cols[t]
        r = old_cols[t] - new_cols[t]
        if a:
            cols_added[t] = sorted(a)
        if r:
            cols_removed[t] = sorted(r)

    old_edges = {_edge_key(e) for e in old.get("edges", [])}
    new_edges = {_edge_key(e) for e in new.get("edges", [])}
    edges_added = sorted(new_edges - old_edges)
    edges_removed = sorted(old_edges - new_edges)

    return {
        "tables_added": tables_added,
        "tables_removed": tables_removed,
        "columns_added": cols_added,
        "columns_removed": cols_removed,
        "edges_added": edges_added,
        "edges_removed": edges_removed,
        "n_changes": (len(tables_added) + len(tables_removed)
                      + sum(len(v) for v in cols_added.values())
                      + sum(len(v) for v in cols_removed.values())
                      + len(edges_added) + len(edges_removed)),
    }


def diff_with_impact(old: dict, new: dict) -> dict:
    """Diff + downstream impact (in the NEW graph) of every changed column.

    For a PR/commit, this answers: 'what lineage changed, and which downstream
    columns are affected by those changes?'
    """
    d = graph_diff(old, new)
    fwd_new = build_forward_index(new)
    fwd_old = build_forward_index(old)

    # ADDED columns: their downstream in the NEW graph (usually empty for a
    # fresh column, non-empty if it feeds further derivations).
    added_impact: dict[str, list[str]] = {}
    for t, cols in d["columns_added"].items():
        for c in cols:
            ds = sorted(column_impact(new, t, c, _fwd=fwd_new))
            if ds:
                added_impact[f"{t}.{c}"] = ds

    # REMOVED/renamed columns: what depended on them in the OLD graph — i.e. the
    # columns this change BREAKS. This is the headline CI signal.
    removed_impact: dict[str, list[str]] = {}
    for t, cols in d["columns_removed"].items():
        for c in cols:
            ds = sorted(column_impact(old, t, c, _fwd=fwd_old))
            removed_impact[f"{t}.{c}"] = ds  # may be [] if nothing depended on it

    d["impact_of_added"] = added_impact
    d["impact_of_removed"] = removed_impact
    d["breaking_columns"] = sorted(k for k, v in removed_impact.items() if v)
    return d
