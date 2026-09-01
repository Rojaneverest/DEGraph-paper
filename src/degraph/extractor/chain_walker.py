"""ChainWalker — unroll and classify PySpark method chains.

Given a terminal ``ast.Call`` node that ends a chain like::

    df.withColumn("a", ...).withColumn("b", ...).filter(...).drop("x")

``unroll_chain()`` returns::

    (Name("df"), [ChainOp("withColumn",...), ChainOp("withColumn",...),
                  ChainOp("filter",...), ChainOp("drop",...)])

``group_by_family()`` then groups consecutive ops of the same semantic type so
the file extractor knows when to create intermediate nodes and when to share
a single source/target pair across multiple edges.

Rules (mirroring the ground-truth conventions):
  - A **single Python assignment** may produce multiple edge types (e.g.
    ``.filter().drop()``).  Each type boundary gets a new anonymous
    intermediate ``DataFrameNode``; all ops in the same group share source/target.
  - ``groupBy().agg(...)`` spans two method calls but is a *single* Aggregates
    edge — they are merged into one group.
  - Write chains (``.write.format().mode().saveAsTable()``) are a single group
    consumed by the WritesExtractor; they never produce DataFrame nodes.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Edge-family classification
# ---------------------------------------------------------------------------

#: Maps PySpark method name → semantic edge family.
FAMILY: dict[str, str] = {
    # Derives (column-level transformations)
    "withColumn":         "derives",
    "withColumnRenamed":  "derives",
    "select":             "derives",
    "selectExpr":         "derives",
    "toDF":               "derives",
    # Projects (column-set restriction, no new expressions)
    "drop":               "projects",
    # Filters (row restriction)
    "filter":             "filters",
    "where":              "filters",
    # Joins
    "join":               "joins",
    "crossJoin":          "joins",
    # Aggregates (two-method: groupBy → agg)
    "groupBy":            "agg_start",
    "groupby":            "agg_start",  # PySpark lowercase alias (real, used in dbdemos)
    "agg":                "agg_end",
    # Convenience aggregation shortcuts on GroupedData
    "count":              "agg_end",
    "sum":                "agg_end",
    "avg":                "agg_end",
    "mean":               "agg_end",
    "max":                "agg_end",
    "min":                "agg_end",
    # Write chain: everything between .write/.writeStream and saveAsTable/table
    "write":              "write_chain",
    "writeStream":        "write_chain",
    "format":             "write_chain",
    "mode":               "write_chain",
    "option":             "write_chain",
    "options":            "write_chain",
    "partitionBy":        "write_chain",
    "saveAsTable":        "write_chain",
    "save":               "write_chain",
    "table":              "write_chain",
    "insertInto":         "write_chain",
    "start":              "write_chain",
    # Read chain starters (base of chain is spark, not a DataFrame var)
    "read":               "read_chain",
    "readStream":         "read_chain",
    "load":               "read_chain",
}

#: Families that represent a transformation producing a new DataFrame.
TRANSFORM_FAMILIES = {"derives", "projects", "filters", "joins", "aggregates"}


# ---------------------------------------------------------------------------
# ChainOp dataclass
# ---------------------------------------------------------------------------


@dataclass
class ChainOp:
    """One method call in a PySpark chain."""

    method: str
    args: list[ast.expr] = field(default_factory=list)
    keywords: list[ast.keyword] = field(default_factory=list)
    lineno: int = 0
    family: str = "unknown"

    # Convenience helpers for callers

    def get_kwarg(self, name: str) -> ast.expr | None:
        """Return the value of keyword argument *name*, or None."""
        for kw in self.keywords:
            if kw.arg == name:
                return kw.value
        return None

    def positional(self, idx: int) -> ast.expr | None:
        """Return positional argument *idx* (0-based), or None."""
        return self.args[idx] if idx < len(self.args) else None


# ---------------------------------------------------------------------------
# unroll_chain
# ---------------------------------------------------------------------------


def unroll_chain(node: ast.expr) -> tuple[ast.expr, list[ChainOp]]:
    """Unroll a method chain into ``(base_node, [ops_in_call_order])``.

    Walks the nested ``ast.Call(func=Attribute(...))`` structure from the
    outermost call inward, collecting each method and its args, then reverses
    to return ops in source-code order (left to right).

    The *base_node* is the first expression in the chain — typically a
    ``ast.Name`` (variable reference) or ``ast.Call`` (e.g. ``spark.table``).
    If the chain has no method calls at all, base is the original node and ops
    is empty.

    Example::

        orders.withColumn("a", F.lit(1)).filter(F.col("a") > 0)
        → (Name("orders"), [ChainOp("withColumn",...), ChainOp("filter",...)])
    """
    ops: list[ChainOp] = []
    current = node

    # Single descent that handles any alternation of method calls and bare
    # property accesses (.write/.writeStream/.read/.readStream). The earlier
    # two-loop version assumed the shape ``[trailing calls][trailing bare-attrs]
    # [base]`` and stopped at the first bare attribute, which truncated
    # un-assigned fluent pipelines like
    # ``spark.readStream.table(x).withColumn(...).writeStream.table(y)`` (a
    # ``.writeStream`` Attribute sits *between* two Calls). Descending through
    # both node kinds in one loop captures the full chain. For the common
    # ``[calls][bare-attrs][base]`` shape the result is identical.
    while True:
        if isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
            method = current.func.attr
            ops.append(ChainOp(
                method=method,
                args=list(current.args),
                keywords=list(current.keywords),
                lineno=getattr(current, "lineno", 0),
                family=FAMILY.get(method, "unknown"),
            ))
            current = current.func.value
        elif isinstance(current, ast.Attribute) and current.attr in FAMILY:
            method = current.attr
            ops.append(ChainOp(
                method=method,
                args=[],
                keywords=[],
                lineno=getattr(current, "lineno", 0),
                family=FAMILY.get(method, "unknown"),
            ))
            current = current.value
        else:
            break

    ops.reverse()  # restore call order
    return current, ops


# ---------------------------------------------------------------------------
# group_by_family
# ---------------------------------------------------------------------------


def group_by_family(ops: list[ChainOp]) -> list[list[ChainOp]]:
    """Group consecutive ops into semantic groups.

    Rules:
    - Consecutive ops of the same ``derives`` / ``projects`` / ``filters``
      family are merged into one group.
    - ``joins`` are never merged (each join has its own unique right-source).
    - ``agg_start`` (groupBy) + ``agg_end`` (agg/sum/...) are merged into a
      single ``aggregates`` group.
    - All ``write_chain`` ops are merged into one group.
    - ``read_chain`` ops are merged into one group.
    - ``unknown`` ops remain separate singletons.

    The caller creates an intermediate DataFrameNode at every group boundary
    (except for the last group, which uses the assignment's target node).
    """
    if not ops:
        return []

    groups: list[list[ChainOp]] = []
    current_group: list[ChainOp] = [ops[0]]
    current_fam = _canonical_family(ops[0].family)

    for op in ops[1:]:
        fam = _canonical_family(op.family)

        # agg_start was opened; any agg_end closes it into "aggregates"
        if current_fam == "agg_start" and op.family == "agg_end":
            current_group.append(op)
            current_fam = "aggregates"
            continue

        # Merge same-family ops (except joins — each is independent)
        mergeable = fam == current_fam and fam not in ("joins", "unknown", "agg_start")
        if mergeable:
            current_group.append(op)
        else:
            groups.append(current_group)
            current_group = [op]
            current_fam = fam

    groups.append(current_group)
    return groups


def _canonical_family(raw: str) -> str:
    """Normalise raw family string for grouping decisions."""
    if raw in ("agg_end",):
        return "aggregates"
    return raw


# ---------------------------------------------------------------------------
# Helpers used by FileExtractor
# ---------------------------------------------------------------------------


def is_write_chain(ops: list[ChainOp]) -> bool:
    """True when the chain terminates with a write operation."""
    return any(op.family == "write_chain" for op in ops)


def is_read_chain(ops: list[ChainOp]) -> bool:
    """True when the chain is a spark.read/readStream chain."""
    return any(op.family == "read_chain" for op in ops)


def find_write_op(ops: list[ChainOp]) -> ChainOp | None:
    """Return the terminal write operation (saveAsTable / table / save / etc.)."""
    terminals = {"saveAsTable", "table", "insertInto", "save", "start"}
    for op in reversed(ops):
        if op.method in terminals:
            return op
    return None
