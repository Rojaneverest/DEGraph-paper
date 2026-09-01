"""Compact-graph serialization for LLM consumption.

Takes a full ``Graph`` dict (as produced by ``extract_repo().model_dump()``)
and returns a stripped-down dict optimized for direct paste into an LLM
context window. Drops internal IDs, linenos, and expression-tree bodies;
keeps tables (with inline per-column ``column_provenance``) and edge
summaries with impact-analysis-relevant fields.

This module was originally embedded in ``experiments/token_count.py``; it
lives here so the CLI (``degraph context``) and any downstream code can
import it without depending on the experiments folder.
"""

from __future__ import annotations

from typing import Optional


# ---------------------------------------------------------------------------
# Legend — semantic notes prepended to every compact graph so an LLM reading
# the JSON knows how to interpret each edge type and where to look for
# reverse-lineage answers.
# ---------------------------------------------------------------------------

LEGEND = {
    "schema_role": "DEGraph compact data-lineage graph. Each table lists its output columns; edges describe how a DataFrame was built.",
    "edge_semantics": {
        "reads":            "table -> dataframe; projected_cols = columns selected at read",
        "writes":           "dataframe -> table; mode/format/merge_keys describe sink",
        "derives":          "computes ONE new column (output_col) from source_cols; window_spec for window funcs; rule_logic=[{cond,value},...] lists the ordered when()/otherwise() branches for conditional expressions; struct_fields=[{field,kind,value,source_cols,rule_logic},...] enumerates per-field provenance for array(struct(...))/struct(...) outputs (kind='lit'|'col'|'expr'); lit_null_fields lists field names that are simple lit(None) placeholders (collapsed for brevity)",
        "filters":          "row restriction; referenced_cols = which columns the predicate touches (DOES NOT contribute values)",
        "projects":         "pure column-set restriction; kept_cols / removed_cols",
        "joins":            "two dataframes -> one; join_keys are MATCHING columns, NOT value contributors to the output. If right_projected_cols is present, ONLY those columns from right_src propagate into target (e.g. df.join(other.select('a','b'), ...)).",
        "aggregates":       "groupBy + agg; agg_outputs reads positionally as 'output_col = op(input_col)'; group_keys are output dimensions, not value contributors",
        "opaque_transform": "imported helper call; is_passthrough=true means column set unchanged",
    },
    "named_column_logic_hint": "named_column_logic contains Python variables that hold PySpark when()/otherwise() classification chains but are NOT directly assigned via withColumn(). Read these to understand conditional business rules (e.g. claim subtype buckets, TOB code ranges). Each entry is an ordered list of {cond, value} pairs; the last entry with cond='otherwise' is the fallback branch.",
    "predicate_dicts_hint": "predicate_dicts maps Python dict-variable names (e.g. 'conds') to {label: predicate_text}. When the SAME dict is referenced by multiple derives edges via subscript (e.g. when(conds['BH'], 'BH').when(...) and array(when(conds[n], n) for n in conds)), both columns share the same predicate set. Cond text in rule_logic is auto-resolved from this dict, so 'conds[\"BH\"]' appears as the actual predicate expression. Use this to answer 'do columns X and Y share classification logic?' questions: if both rule_logic chains cite the same predicate texts, the answer is yes.",
    "reverse_lineage_hint": "To trace 'which input columns contribute VALUES to output column X of table T': look up T.column_provenance[X] FIRST. The 'from' field lists the exact upstream column(s) and 'role' tells you the relationship (passthrough/derived/aggregate/group_key/opaque/unknown). Recurse on those upstream columns via their own table's column_provenance. Only fall back to edge topology if provenance is empty. IGNORE join_keys and group_keys when answering value-contribution questions — they describe how rows are matched, not where values come from.",
    "table_entry_fields": "Each table entry has: fqn, written_by, read_by, columns (the output schema), column_provenance (per-column role + upstream sources). column_provenance is the AUTHORITATIVE source for reverse-lineage questions.",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _norm_id(node_id: str) -> str:
    """Strip lineno segment from DataFrame node IDs for readability."""
    if not node_id.startswith("df:"):
        return node_id
    parts = node_id.split(":")
    if len(parts) == 4 and parts[2].isdigit():
        return f"{parts[1]}:{parts[3]}"
    return node_id.replace("df:", "")


def _resolve_col_to_table(
    df_id: str,
    col_name: str,
    edges_by_target: dict[str, list[dict]],
    table_columns: dict[str, set[str]] | None = None,
    visited: set | None = None,
) -> str | None:
    """Walk back from a DataFrame node to find the table+column that ultimately
    supplies this value. Returns ``"<table_fqn>.<col>"`` or ``None``.

    ``table_columns`` is an optional ``{table_fqn: {col_names}}`` map used to
    verify that the column actually exists in the upstream table — important
    for joins where the column could be on either side.
    """
    if visited is None:
        visited = set()
    # Key the cycle-guard on (node, column), not node alone: the same DataFrame
    # node is legitimately walked for different columns (e.g. both sides of a join
    # feed back into the same base CTE for different fields). Keying on node alone
    # blocked resolving a second column through an already-visited node.
    if not df_id or (df_id, col_name) in visited:
        return None
    visited.add((df_id, col_name))
    in_edges = edges_by_target.get(df_id, [])
    for e in reversed(in_edges):
        k = e["kind"]
        if k == "reads":
            src = e.get("source", "")
            if src.startswith("table:"):
                fqn = src.removeprefix("table:")
                projected = e.get("projected_cols") or []
                if projected:
                    if col_name in projected:
                        return f"{fqn}.{col_name}"
                elif table_columns is not None:
                    cols = table_columns.get(fqn)
                    if cols and col_name in cols:
                        return f"{fqn}.{col_name}"
                    elif not cols:
                        # Schema unknown (e.g. an intermediate DLT/SDP table whose
                        # columns we never learned). Optimistically attribute the
                        # column to this table — far better than dropping to a bare,
                        # un-table-qualified name, which breaks downstream column-
                        # level impact. Tables WITH a known schema keep the strict
                        # membership check above (so joins don't mis-resolve).
                        return f"{fqn}.{col_name}"
                else:
                    return f"{fqn}.{col_name}"
        elif k == "derives":
            if e.get("output_col") == col_name:
                for src_col in e.get("source_cols") or []:
                    up = _resolve_col_to_table(e.get("source", ""), src_col, edges_by_target, table_columns, visited)
                    if up:
                        return up
                return None
            up = _resolve_col_to_table(e.get("source", ""), col_name, edges_by_target, table_columns, visited)
            if up:
                return up
        elif k in ("filters", "projects"):
            up = _resolve_col_to_table(e.get("source", ""), col_name, edges_by_target, table_columns, visited)
            if up:
                return up
        elif k == "opaque_transform" and e.get("is_passthrough"):
            up = _resolve_col_to_table(e.get("source", ""), col_name, edges_by_target, table_columns, visited)
            if up:
                return up
        elif k == "joins":
            for side in ("left_source", "right_source"):
                src = e.get(side, "")
                if not src or "unresolved" in src:
                    continue
                up = _resolve_col_to_table(src, col_name, edges_by_target, table_columns, visited)
                if up:
                    return up
        elif k == "aggregates":
            outs = e.get("output_cols") or []
            ins = e.get("agg_inputs") or []
            gks = e.get("group_keys") or []
            if col_name in outs:
                idx = outs.index(col_name)
                inp = ins[idx] if idx < len(ins) else ""
                if inp and inp != "<unresolved>":
                    up = _resolve_col_to_table(e.get("source", ""), inp, edges_by_target, table_columns, visited)
                    if up:
                        return up
            elif col_name in gks:
                up = _resolve_col_to_table(e.get("source", ""), col_name, edges_by_target, table_columns, visited)
                if up:
                    return up
    return None


def _build_column_provenance(graph: dict) -> dict[str, dict]:
    """For each table, build a {col_name: {role, from}} provenance map."""
    edges_by_target: dict[str, list[dict]] = {}
    for e in graph.get("edges", []):
        tgt = e.get("target")
        if tgt:
            edges_by_target.setdefault(tgt, []).append(e)

    table_columns: dict[str, set[str]] = {
        t["fqn"]: {c["name"] for c in (t.get("columns") or [])}
        for t in graph.get("tables", [])
    }

    def _ref_for(nid: str) -> str:
        if not nid:
            return "?"
        if nid.startswith("table:"):
            return nid.removeprefix("table:")
        return _norm_id(nid)

    provenance: dict[str, dict] = {}

    for t in graph.get("tables", []):
        cols = [c["name"] for c in (t.get("columns") or [])]
        if not cols:
            continue
        write_edges = [
            e for e in graph.get("edges", [])
            if e.get("kind") == "writes" and e.get("target") == t["id"]
        ]
        if not write_edges:
            continue
        we = write_edges[0]
        per_col: dict[str, dict] = {c: {"role": "unknown", "from": []} for c in cols}
        visited: set[str] = set()

        def _walk(node_id: str, pending: set[str]) -> None:
            if not pending or not node_id or node_id in visited:
                return
            visited.add(node_id)
            in_edges = edges_by_target.get(node_id, [])
            for e in reversed(in_edges):
                if not pending:
                    return
                k = e["kind"]
                if k == "derives":
                    out_col = e.get("output_col")
                    if out_col in pending:
                        src_id = e.get("source", "")
                        resolved_from: list[str] = []
                        for sc in e.get("source_cols") or []:
                            up = _resolve_col_to_table(src_id, sc, edges_by_target, table_columns)
                            resolved_from.append(up if up else sc)
                        per_col[out_col] = {
                            "role": "derived",
                            "from": resolved_from,
                        }
                        if e.get("window_spec"):
                            per_col[out_col]["window"] = e["window_spec"]
                        pending.discard(out_col)
                elif k == "aggregates":
                    gks = e.get("group_keys") or []
                    ops = e.get("agg_ops") or []
                    ins = e.get("agg_inputs") or []
                    outs = e.get("output_cols") or []
                    src_id = e.get("source", "")
                    for col in list(pending):
                        if col in gks:
                            up = _resolve_col_to_table(src_id, col, edges_by_target, table_columns)
                            per_col[col] = {
                                "role": "group_key",
                                "from": [up] if up else [],
                            }
                            pending.discard(col)
                        elif col in outs:
                            idx = outs.index(col)
                            op = ops[idx] if idx < len(ops) else "?"
                            inp = ins[idx] if idx < len(ins) else ""
                            resolved: list[str] = []
                            if inp and inp != "<unresolved>":
                                up = _resolve_col_to_table(src_id, inp, edges_by_target, table_columns)
                                resolved = [up] if up else [inp]
                            per_col[col] = {
                                "role": "aggregate",
                                "op": op,
                                "from": resolved,
                            }
                            pending.discard(col)
                elif k == "reads":
                    src_ref = _ref_for(e.get("source", ""))
                    projected = e.get("projected_cols") or []
                    for col in list(pending):
                        if not projected or col in projected:
                            per_col[col] = {"role": "passthrough", "from": [f"{src_ref}.{col}"]}
                            pending.discard(col)
                elif k == "opaque_transform":
                    if not e.get("is_passthrough"):
                        op_name = e.get("operator", "?")
                        for col in list(pending):
                            per_col[col] = {"role": "opaque", "from": [f"{op_name}(...)"]}
                            pending.discard(col)

            if not pending:
                return
            for e in reversed(in_edges):
                if not pending:
                    return
                k = e["kind"]
                if k in ("derives", "filters", "projects", "reads"):
                    src = e.get("source")
                    if src:
                        _walk(src, pending)
                elif k == "opaque_transform" and e.get("is_passthrough"):
                    src = e.get("source")
                    if src:
                        _walk(src, pending)
                elif k == "joins":
                    left = e.get("left_source")
                    right = e.get("right_source")
                    if left:
                        _walk(left, pending)
                    if pending and right:
                        _walk(right, pending)

        _walk(we.get("source", ""), set(cols))

        # ★ P11 fix: strip informationally-empty entries.
        # Columns resolved only to role="opaque" or role="unknown" carry no
        # actionable provenance — they all say the same thing (opaque transform
        # applied, or we couldn't trace it).  On real-world pipelines with
        # hundreds of columns that pass through a MatchSchema / MatchSink step,
        # keeping them inflates the compact graph past the raw-source token
        # count.  Drop them here; a missing entry is semantically equivalent to
        # "unknown" and costs zero tokens.
        per_col = {
            col: info
            for col, info in per_col.items()
            if info.get("role") not in ("unknown", "opaque")
        }
        if per_col:
            provenance[t["fqn"]] = per_col

    return provenance


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_compact_graph(graph: dict) -> dict:
    """Return a stripped graph keeping cross-file-relevant fields.

    Adds: ``legend`` (semantic notes), per-table inline ``column_provenance``.
    Aggregates payload uses a positional ``output_col = op(input_col)`` map.
    """
    provenance = _build_column_provenance(graph)

    compact_tables = []
    for t in graph.get("tables", []):
        fqn = t["fqn"]
        entry = {
            "fqn": fqn,
            "written_by": t.get("written_by", []),
            "read_by": t.get("read_by", []),
        }
        if t.get("columns"):
            entry["columns"] = [c["name"] for c in t["columns"]]
        tbl_prov = provenance.get(fqn)
        if tbl_prov:
            entry["column_provenance"] = tbl_prov
        compact_tables.append(entry)

    compact_edges = []
    for e in graph.get("edges", []):
        kind = e["kind"]
        src = _norm_id(e.get("source") or e.get("left_source", "?"))
        tgt = _norm_id(e.get("target", "?"))
        entry: dict = {"kind": kind, "file": e.get("file", ""), "src": src, "tgt": tgt}

        if kind == "reads":
            entry["streaming"] = e.get("streaming", False)
            if e.get("projected_cols"):
                entry["projected_cols"] = e["projected_cols"]
        elif kind == "writes":
            entry["mode"] = e.get("mode", "")
            entry["format"] = e.get("format", "")
            entry["streaming"] = e.get("streaming", False)
            if e.get("sink_class"):
                entry["sink_class"] = e["sink_class"]
            if e.get("merge_keys"):
                entry["merge_keys"] = e["merge_keys"]
            if e.get("sink_kwargs"):
                entry["sink_kwargs"] = e["sink_kwargs"]
            if e.get("partition_cols"):
                entry["partition_cols"] = e["partition_cols"]
        elif kind == "derives":
            entry["output_col"] = e.get("output_col", "")
            if e.get("source_cols"):
                entry["source_cols"] = e["source_cols"]
            if e.get("window_spec"):
                entry["window_spec"] = e["window_spec"]
            if e.get("dynamic"):
                entry["dynamic"] = True
            if e.get("dynamic_note"):
                entry["dynamic_note"] = e["dynamic_note"]
            if e.get("rule_logic"):
                entry["rule_logic"] = e["rule_logic"]
            # ★ Array<struct> field provenance.  Collapse lit-None placeholder
            # fields into a single `lit_null_fields` list (just names) to keep
            # the token budget bounded — a wide struct field can have dozens of
            # NULL placeholders that add no information once you know they're
            # NULL.  Keep `col`, `expr`, and non-None `lit` entries in full.
            sf = e.get("struct_fields")
            if sf:
                lit_null_names: list[str] = []
                kept_fields: list[dict] = []
                for f in sf:
                    if f.get("kind") == "lit" and f.get("value") in ("None", None):
                        lit_null_names.append(str(f.get("field", "")))
                    else:
                        # Drop empty source_cols / null values to reduce noise.
                        compact_f = {"field": f.get("field"), "kind": f.get("kind")}
                        if f.get("value"):
                            compact_f["value"] = f["value"]
                        if f.get("source_cols"):
                            compact_f["source_cols"] = f["source_cols"]
                        if f.get("rule_logic"):
                            compact_f["rule_logic"] = f["rule_logic"]
                        kept_fields.append(compact_f)
                if kept_fields:
                    entry["struct_fields"] = kept_fields
                if lit_null_names:
                    # If the list is very long, swap to a count to bound tokens.
                    if len(lit_null_names) > 30:
                        entry["lit_null_field_count"] = len(lit_null_names)
                    else:
                        entry["lit_null_fields"] = lit_null_names
        elif kind == "filters":
            if e.get("referenced_cols"):
                entry["referenced_cols"] = e["referenced_cols"]
        elif kind == "projects":
            if e.get("removed_cols"):
                entry["removed_cols"] = e["removed_cols"]
            elif e.get("kept_cols"):
                kept = e["kept_cols"]
                if kept == ["*"]:
                    entry["pass_through"] = True
                else:
                    entry["kept_cols"] = kept
        elif kind == "joins":
            entry["join_type"] = e.get("join_type", "")
            if e.get("join_keys"):
                entry["join_keys"] = e["join_keys"]
            right = _norm_id(e.get("right_source", ""))
            if right:
                entry["right_src"] = right
            if e.get("right_projected_cols"):
                entry["right_projected_cols"] = e["right_projected_cols"]
            if e.get("left_projected_cols"):
                entry["left_projected_cols"] = e["left_projected_cols"]
        elif kind == "aggregates":
            gks = e.get("group_keys", [])
            ops = e.get("agg_ops", [])
            ins = e.get("agg_inputs", [])
            outs = e.get("output_cols", [])
            entry["group_keys"] = gks
            agg_outputs: dict[str, str] = {}
            for i, out_col in enumerate(outs):
                op = ops[i] if i < len(ops) else "?"
                inp = ins[i] if i < len(ins) else ""
                agg_outputs[out_col] = f"{op}({inp})" if inp else f"{op}()"
            entry["agg_outputs"] = agg_outputs
            if e.get("dynamic"):
                entry["dynamic"] = True
            if e.get("dynamic_note"):
                entry["dynamic_note"] = e["dynamic_note"]
        elif kind == "opaque_transform":
            entry["operator"] = e.get("operator", "")
            entry["is_passthrough"] = e.get("is_passthrough", False)
            if e.get("opaque_kind") and e["opaque_kind"] != "unknown":
                entry["opaque_kind"] = e["opaque_kind"]

        compact_edges.append(entry)

    compact_warnings = [
        {"file": w.get("file", ""), "category": w.get("category", ""), "message": w.get("message", "")}
        for w in graph.get("warnings", [])
    ]

    # Named when()-chain variables that never appear directly in withColumn()
    # (e.g. passed into helper functions or used inside a select() alias list).
    # Surface them verbatim so LLMs can read the classification rules directly.
    named_column_logic = graph.get("column_rules") or {}

    # ★ Q3: shared-predicate dicts (e.g. `conds = {"BH": <expr>, ...}` used
    # by both `claim_subcategory` (first-match when-chain) and
    # `multiple_claim_subcategories` (array-of-matches accumulator)).
    predicate_dicts = graph.get("predicate_dicts") or {}

    result: dict = {
        "legend": LEGEND,
        "tables": compact_tables,
        "edges": compact_edges,
        "warnings": compact_warnings,
    }
    if named_column_logic:
        result["named_column_logic"] = named_column_logic
    if predicate_dicts:
        result["predicate_dicts"] = predicate_dicts
    return result
