"""DEGraph schema — pydantic models for the data-semantic lineage graph.

This module is the **executable specification** for the schema described in:
- `paper/outline.md §3` (node types, edge types, assembly model)
- `dev/methodology.md §3` (locked-in decisions: forward-chain tracker, DDL-required,
  single-file JSON output, cell-aware notebook preprocessor, opaque-call handling,
  custom-sink handling)
- `dev/schema-audit.md` (gaps A-E identified against real PySpark code)
- `dev/feasibility-claim-pipeline.md` (gaps F-1 and F-2 from the synthetic
  claim-pipeline dry run)

Every node and edge type defined here corresponds to a row in the tables of
`outline.md §3.3`. If the schema changes, this file changes first; the paper
mirrors it.

Design choices encoded here:

- **String IDs, not nested objects.** Node IDs are strings (e.g.
  ``"table:bronze.health.medical_claim"`` for Tables; ``"df:<file>:<lineno>"``
  for DataFrames). Edges reference IDs, not the nodes themselves. This keeps
  the serialized JSON small and LLM-readable, and decouples node/edge growth.
- **Expressions are interned.** Every transformation expression (the body of
  ``withColumn``, ``filter``, ``agg``, etc.) is stored once in the
  ``Graph.expressions`` table keyed by ``expr_id``. Edges reference the id.
  Adapted from CodexGraph's storage trick (see `paper/notes/deep_dive_findings.md`).
- **No silent drops.** Every limitation (dynamic column lists, opaque calls,
  unrecognized sinks) emits either an ``<unresolved>`` marker, a
  ``dynamic=True`` flag, or a ``GraphWarning``. The LLM consuming the graph
  can see and reason about what is missing. This is the central guarantee.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = "0.1.0"
"""Version of the DEGraph JSON schema. Bumped on any breaking change to
node/edge shape. Independent of the package version."""

UNRESOLVED = "<unresolved>"
"""Sentinel string used wherever a value could not be determined statically.
Per `outline.md §4.5`, this is the *graceful degradation* marker; it is never
silently dropped. LLMs reading the graph treat this token as an explicit
'unknown' rather than an absent field."""


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class NodeKind(str, Enum):
    """Discriminator for the four node types in the graph."""

    TABLE = "table"
    EXTERNAL_SOURCE = "external_source"
    DATAFRAME = "dataframe"
    EXPRESSION = "expression"


class EdgeKind(str, Enum):
    """Discriminator for the eight edge types in the graph.

    Order mirrors `outline.md §3.3`'s edge table.
    """

    READS = "reads"
    WRITES = "writes"
    DERIVES = "derives"
    FILTERS = "filters"
    PROJECTS = "projects"
    JOINS = "joins"
    AGGREGATES = "aggregates"
    OPAQUE_TRANSFORM = "opaque_transform"


class WriteMode(str, Enum):
    """PySpark write modes. Used by `WritesEdge.mode`."""

    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"
    IGNORE = "ignore"
    ERROR = "error"  # aka errorifexists
    UNRESOLVED = "unresolved"


class JoinType(str, Enum):
    """PySpark / SQL join types. Used by `JoinsEdge.join_type`."""

    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"
    LEFT_SEMI = "left_semi"
    LEFT_ANTI = "left_anti"
    UNRESOLVED = "unresolved"


class OpaqueKind(str, Enum):
    """Classification of an `OpaqueTransform` edge.

    Per Decision 3.7 in `dev/methodology.md`, opaque calls fall into one of
    these buckets. `PASSTHROUGH` is for registered helpers known to preserve
    the column set (e.g. ``trim_string_columns``); `UNKNOWN` is the default
    fallback for any unregistered imported function call.
    """

    PASSTHROUGH = "passthrough"
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Sub-models (composed by nodes and edges below)
# ---------------------------------------------------------------------------


class Column(BaseModel):
    """A column on a `Table` or `ExternalSource`.

    Schemas are sourced from DDL per Decision 3.3 — if a source table's DDL is
    not supplied, parsing of the referencing file errors rather than producing
    a Column-less Table.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    dtype: str = Field(
        default=UNRESOLVED,
        description="Spark SQL type string (e.g. 'string', 'int', 'decimal(18,2)'). "
        "Defaults to <unresolved> when DDL is incomplete.",
    )
    nullable: Optional[bool] = None
    comment: Optional[str] = None


class WindowSpec(BaseModel):
    """A windowing specification attached to a `DerivesEdge` (Gap B).

    Per `outline.md §3.3`, window functions are folded into ``Derives`` rather
    than getting their own edge type. The ``window_spec`` records the
    partition keys, order keys, and frame so that impact analysis can answer
    "if column X is renamed, does this window break?"
    """

    model_config = ConfigDict(extra="forbid")

    partition_cols: list[str] = Field(default_factory=list)
    order_cols: list[str] = Field(
        default_factory=list,
        description="Order keys, optionally suffixed with ' asc' or ' desc'.",
    )
    frame: Optional[str] = Field(
        default=None,
        description="Frame spec (e.g. 'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW'). "
        "Often None for row_number/rank, populated for sliding-window aggregates.",
    )


# ---------------------------------------------------------------------------
# Node models
# ---------------------------------------------------------------------------


class _NodeBase(BaseModel):
    """Common fields for all node types. Not instantiated directly."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(
        description="Globally unique node ID. Conventions: "
        "'table:<fqn>' for Table, 'ext:<location>' for ExternalSource, "
        "'df:<file>:<lineno>[:<var>]' for DataFrame, 'expr:<hash>' for Expression."
    )


class Table(_NodeBase):
    """A named, persisted dataset that one file writes and one or more files read.

    The keystone node of the cross-file graph (per `outline.md §3.2`). Tables
    are merged across files by ``fqn`` during repo-level assembly.
    """

    kind: Literal[NodeKind.TABLE] = NodeKind.TABLE
    fqn: str = Field(
        description="Fully-qualified table name, e.g. 'silver.sales.order_enriched'. "
        "May contain <unresolved> segments if SafeEvaluator could not resolve a variable."
    )
    format: Optional[str] = Field(default=None, description="e.g. 'delta', 'parquet', 'iceberg'.")
    location: Optional[str] = Field(
        default=None,
        description="Storage path for external tables; None for catalog-managed tables.",
    )
    columns: list[Column] = Field(default_factory=list)
    partition_cols: list[str] = Field(default_factory=list)
    written_by: list[str] = Field(
        default_factory=list,
        description="File paths of files containing a Writes edge to this Table.",
    )
    read_by: list[str] = Field(
        default_factory=list,
        description="File paths of files containing a Reads edge from this Table.",
    )


class ExternalSource(_NodeBase):
    """A data source outside the repo's control: a file path, REST endpoint, "
    JDBC URL, etc. Read by some file but never written by it.
    """

    kind: Literal[NodeKind.EXTERNAL_SOURCE] = NodeKind.EXTERNAL_SOURCE
    format: Optional[str] = Field(
        default=None, description="e.g. 'cloudFiles', 'parquet', 'csv', 'json', 'jdbc'."
    )
    location: str = Field(
        description="Source location: file path, S3 URI, ADLS URI, JDBC URL, REST endpoint."
    )
    columns: list[Column] = Field(default_factory=list)
    read_by: list[str] = Field(default_factory=list)


class DataFrameNode(_NodeBase):
    """A file-local intermediate dataset: a Python ``DataFrame`` variable or an
    unnamed CTE inside a SQL string.

    Named ``DataFrameNode`` rather than ``DataFrame`` to disambiguate from
    pyspark's class — DEGraph does not import pyspark, but contributors
    reading code that does will appreciate the distinction.

    Per Decision 3.2 (forward-chain tracker), reassigning the same variable
    name advances its symbol-table head but does NOT delete the old node — the
    old node remains in the graph, connected to the new node by an edge.
    """

    kind: Literal[NodeKind.DATAFRAME] = NodeKind.DATAFRAME
    file: str = Field(description="File path relative to the repo root.")
    var_name: Optional[str] = Field(
        default=None,
        description="Python variable name, e.g. 'claim_df'. None for unnamed SQL CTEs.",
    )
    lineno: Optional[int] = Field(
        default=None,
        description="Line number in the source file where this DataFrame was produced.",
    )
    columns: list[str] = Field(
        default_factory=list,
        description="Best-effort column-set at this point in the chain. May be incomplete "
        "for DataFrames downstream of dynamic-pivot or opaque-helper edges.",
    )
    derived_from: list[str] = Field(
        default_factory=list,
        description="IDs of parent nodes (Table, ExternalSource, or DataFrameNode) "
        "that this DataFrame is derived from. Multi-parent for joins/unions.",
    )


class Expression(_NodeBase):
    """An interned transformation expression: the body of a ``withColumn``,
    ``filter``, ``agg``, ``when``, etc.

    Stored once in `Graph.expressions` and referenced by ``expr_id`` from
    edges. Borrowed from CodexGraph's indexed-reference trick to keep the
    graph compact even when the same expression appears many times.
    """

    kind: Literal[NodeKind.EXPRESSION] = NodeKind.EXPRESSION
    text: str = Field(
        description="Source text of the expression as it appeared in the file. "
        "Trimmed and normalized; not necessarily evaluable code."
    )
    referenced_cols: list[str] = Field(
        default_factory=list,
        description="Column names referenced anywhere in the expression. "
        "May include <unresolved> entries for symbolic accesses DEGraph could not resolve.",
    )
    udf: bool = Field(
        default=False,
        description="True if the expression applies a user-defined function whose body "
        "DEGraph cannot analyze (see `outline.md §4.5`).",
    )


Node = Annotated[
    Union[Table, ExternalSource, DataFrameNode, Expression],
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# Edge models
# ---------------------------------------------------------------------------


class _EdgeBase(BaseModel):
    """Common fields for all edge types. Not instantiated directly."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(description="Globally unique edge ID.")
    file: str = Field(description="File path where this edge originates.")
    lineno: Optional[int] = Field(default=None, description="Line number in the source file.")


class ReadsEdge(_EdgeBase):
    """``Table → DataFrame`` or ``ExternalSource → DataFrame``.

    Pattern matches: ``spark.read.table()``, ``spark.table()``,
    ``spark.read.format(X).load()``, ``spark.read.{parquet,csv,json,orc}``,
    ``spark.readStream.*``, SQL ``FROM`` and ``JOIN`` clauses.
    """

    kind: Literal[EdgeKind.READS] = EdgeKind.READS
    source: str = Field(description="Node ID of the source Table or ExternalSource.")
    target: str = Field(description="Node ID of the destination DataFrame.")
    projected_cols: list[str] = Field(
        default_factory=list,
        description="Columns selected at read time (when `.select()` is applied to the read).",
    )
    streaming: bool = Field(default=False, description="True for `spark.readStream.*`.")


class WritesEdge(_EdgeBase):
    """``DataFrame → Table``.

    Pattern matches: ``df.write.saveAsTable()``, ``df.write.format(X).save()``,
    ``df.write.{parquet,csv,json}``, ``df.write.insertInto()``,
    ``df.writeStream.table()``, SQL ``INSERT INTO``, ``MERGE INTO``,
    ``CREATE TABLE AS SELECT``, plus custom sinks resolved via the sink registry
    (Decision 3.8 in `dev/methodology.md`).
    """

    kind: Literal[EdgeKind.WRITES] = EdgeKind.WRITES
    source: str = Field(description="Node ID of the source DataFrame.")
    target: str = Field(description="Node ID of the destination Table.")
    mode: WriteMode = WriteMode.UNRESOLVED
    partition_cols: list[str] = Field(default_factory=list)
    format: Optional[str] = Field(default=None, description="e.g. 'delta', 'parquet'.")
    streaming: bool = Field(default=False, description="True for `df.writeStream.*`.")
    sink_class: Optional[str] = Field(
        default=None,
        description="When this Writes edge originated from a custom sink class "
        "(via the sink registry or warn-and-record fallback), this records the "
        "qualified class name. None for native df.write.* writes.",
    )
    merge_keys: list[str] = Field(
        default_factory=list,
        description="For mode='merge' writes, the columns used as merge keys "
        "(e.g. for SCD-Type-1 upserts via Delta MERGE INTO). Extracted from "
        "the sink registry's `merge_keys_kwarg`. Empty for non-merge writes.",
    )
    sink_kwargs: dict[str, str] = Field(
        default_factory=dict,
        description="Additional keyword arguments passed to a custom sink class "
        "(beyond target_table / mode / merge_keys). Captures flags like "
        "enable_leap_column=True, strict=True, audit_table='...' that affect "
        "sink behaviour but aren't in the core schema. Each value is stored as "
        "its ast.unparse() text so the LLM can read literal True/False/strings.",
    )


class DerivesEdge(_EdgeBase):
    """``DataFrame → DataFrame``, one edge per output column.

    Pattern matches: ``withColumn``, ``select``, ``selectExpr``, column renames,
    SQL ``SELECT expr AS col``, window functions via ``.over(Window...)``.

    Window functions populate ``window_spec`` (Gap B in `dev/schema-audit.md`).
    Loop-built or comprehension-built output column lists set ``dynamic=True``
    (Gap E).
    """

    kind: Literal[EdgeKind.DERIVES] = EdgeKind.DERIVES
    source: str
    target: str
    output_col: str = Field(
        description="Name of the column produced by this derivation. May be "
        "<unresolved> when dynamic=True and enumeration failed.",
    )
    source_cols: list[str] = Field(
        default_factory=list,
        description="Columns of `source` referenced in the expression.",
    )
    expr_id: Optional[str] = Field(
        default=None, description="Node ID of the interned Expression."
    )
    window_spec: Optional[WindowSpec] = None
    dynamic: bool = Field(
        default=False,
        description="True when the output_col enumeration could not be resolved statically "
        "(e.g., loop-built CASE WHEN expressions). Source columns may still be "
        "statically known from the loop body's f-string templates.",
    )
    dynamic_note: Optional[str] = Field(
        default=None,
        description="Human-readable explanation of why `dynamic=True`. Included in the "
        "graph so an LLM consumer can reason about the gap.",
    )
    rule_logic: Optional[list[dict[str, str]]] = Field(
        default=None,
        description="For when()-chain derivations: ordered list of "
        "{\"cond\": <predicate text>, \"value\": <result text>} dicts extracted "
        "from the F.when(...).when(...).otherwise(...) chain. "
        "The last entry has cond=\"otherwise\" for the fallback branch. "
        "None for non-conditional expressions.",
    )

    # ★ Array<struct> field provenance.  For derivations of the form
    # ``withColumn(col, array(struct(<f1>.alias("a"), <f2>.alias("b"), ...)))``
    # or bare ``struct(...)``, record per-field metadata so LLMs can answer
    # "which struct field comes from which source column / literal?" without
    # parsing the original expression.  Each entry is:
    #   {"field": str, "kind": "lit"|"col"|"expr",
    #    "value": Optional[str],            # literal text or short expr summary
    #    "source_cols": list[str],          # cols referenced by this field
    #    "rule_logic": Optional[list[dict]] # if the field is a when() chain
    #   }
    struct_fields: Optional[list[dict[str, object]]] = Field(
        default=None,
        description="Per-field provenance for array(struct(...)) / struct(...) "
        "derivations: ordered list of {field, kind, value, source_cols, "
        "rule_logic} dicts. None for non-struct outputs.",
    )
    column_var: Optional[str] = Field(
        default=None,
        description="When the withColumn expression is a named Python Column "
        "variable (i.e. a bare Name node in _column_exprs), this stores the "
        "variable name. Enables the compact DSL to emit cv=<var> so the LLM "
        "can trace which @RULES entry drives this output column.",
    )


class FiltersEdge(_EdgeBase):
    """``DataFrame → DataFrame``.

    Pattern matches: ``filter``, ``where``, SQL ``WHERE``, SQL ``HAVING``.
    """

    kind: Literal[EdgeKind.FILTERS] = EdgeKind.FILTERS
    source: str
    target: str
    predicate_id: Optional[str] = Field(
        default=None, description="Node ID of the interned predicate Expression."
    )
    referenced_cols: list[str] = Field(default_factory=list)


class ProjectsEdge(_EdgeBase):
    """``DataFrame → DataFrame``. Pure column-set restriction (Gap A).

    Pattern matches: ``df.drop("col1", "col2", ...)``, ``df.drop(col("x"))``,
    ``df.select("col1", "col2", ...)`` when no expressions are computed.

    Structurally distinct from `DerivesEdge` (which adds columns) and
    `FiltersEdge` (which restricts rows). A dedicated edge type makes
    "which file dropped column X?" trivially answerable.
    """

    kind: Literal[EdgeKind.PROJECTS] = EdgeKind.PROJECTS
    source: str
    target: str
    removed_cols: list[str] = Field(default_factory=list)
    kept_cols: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _exactly_one_of_removed_or_kept(self) -> ProjectsEdge:
        if bool(self.removed_cols) == bool(self.kept_cols):
            raise ValueError(
                "ProjectsEdge must specify exactly one of `removed_cols` (for drop) "
                "or `kept_cols` (for select-by-name)."
            )
        return self


class JoinsEdge(_EdgeBase):
    """``(DataFrame, DataFrame) → DataFrame``.

    Two source DataFrames join to produce a single target. Per the audit at
    `dev/feasibility-claim-pipeline.md §1.13`, the node visitor must recognize
    both ``F.col("name")``/``col("name")`` and bracket-indexing ``df["name"]``
    when parsing the join predicate.

    Pattern matches: ``df1.join(df2, predicate, "join_type")``, ``crossJoin``,
    SQL ``JOIN``, SQL ``JOIN ... USING (...)``.
    """

    kind: Literal[EdgeKind.JOINS] = EdgeKind.JOINS
    left_source: str = Field(description="Node ID of the left DataFrame.")
    right_source: str = Field(description="Node ID of the right DataFrame.")
    target: str = Field(description="Node ID of the resulting DataFrame.")
    join_type: JoinType = JoinType.INNER
    join_keys: list[tuple[str, str]] = Field(
        default_factory=list,
        description="Pairs of (left_col, right_col) extracted from the predicate. "
        "For USING(...) syntax, the same column name appears in both positions.",
    )
    on_expr_id: Optional[str] = Field(
        default=None, description="Node ID of the interned predicate Expression."
    )
    left_projected_cols: list[str] = Field(
        default_factory=list,
        description="If the left side is an inline projection (e.g. "
        "``orders.select('order_id','customer_id').join(...)``), the columns "
        "selected before the join. Empty means 'use all columns of left_source'.",
    )
    right_projected_cols: list[str] = Field(
        default_factory=list,
        description="If the right side is an inline projection (e.g. "
        "``timeline.join(orders.select('order_id','customer_id'), ...)``), the "
        "columns selected before the join. Empty means 'use all columns of "
        "right_source'. Captures the narrowing-projection that makes "
        "join-chain column-propagation accurate.",
    )


class AggregatesEdge(_EdgeBase):
    """``DataFrame → DataFrame``.

    Pattern matches: ``groupBy().agg()``, ``groupBy().sum/count/avg/...``, SQL
    ``GROUP BY``.

    Loop-built or comprehension-built agg lists (``*aggregations`` star-unpacked
    from a runtime-built list) set ``dynamic=True`` (Gap E in
    `dev/schema-audit.md`).
    """

    kind: Literal[EdgeKind.AGGREGATES] = EdgeKind.AGGREGATES
    source: str
    target: str
    group_keys: list[str] = Field(default_factory=list)
    agg_ops: list[str] = Field(
        default_factory=list,
        description="Aggregation function names: ['sum', 'count', 'avg', 'collect_list', ...].",
    )
    agg_inputs: list[str] = Field(
        default_factory=list,
        description="Positionally paired with agg_ops/output_cols: the input column "
        "name each aggregate consumes. E.g. agg_ops=['sum'], agg_inputs=['total_amount'], "
        "output_cols=['lifetime_revenue'] reads as 'lifetime_revenue = sum(total_amount)'. "
        "May contain <unresolved> for star-unpacked dynamic aggs or count(*) calls.",
    )
    output_cols: list[str] = Field(default_factory=list)
    dynamic: bool = False
    dynamic_note: Optional[str] = None


class OpaqueTransformEdge(_EdgeBase):
    """``DataFrame → DataFrame``. Records an imported function/method call that
    returns a DataFrame, when DEGraph cannot analyze the body (Decision 3.7).

    Pattern matches: ``df = imported_helper(df, ...)``,
    ``df = ImportedClass(df, ...).method()``.

    The default behavior (``opaque_kind=UNKNOWN``) preserves DataFrame identity
    through the call site so downstream operations don't disconnect from the
    lineage chain. Helpers registered in ``<repo>/.degraph/helpers.json`` get
    semantically-rich edges (typically multiple ``DerivesEdge``s for renames,
    or this edge with ``opaque_kind=PASSTHROUGH`` for trim-like ops).
    """

    kind: Literal[EdgeKind.OPAQUE_TRANSFORM] = EdgeKind.OPAQUE_TRANSFORM
    source: str
    target: str
    operator: str = Field(
        description="Qualified name of the imported function or method, e.g. "
        "'utils.column_transformations.trim_string_columns' or "
        "'health_pipeline.stages.MatchSchema.transform'.",
    )
    opaque_kind: OpaqueKind = OpaqueKind.UNKNOWN
    is_passthrough: bool = Field(
        default=False,
        description="True if the registered helper preserves the column set (no rename, "
        "no add, no drop). Implies opaque_kind=PASSTHROUGH.",
    )


Edge = Annotated[
    Union[
        ReadsEdge,
        WritesEdge,
        DerivesEdge,
        FiltersEdge,
        ProjectsEdge,
        JoinsEdge,
        AggregatesEdge,
        OpaqueTransformEdge,
    ],
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# Top-level container
# ---------------------------------------------------------------------------


class GraphWarning(BaseModel):
    """A non-fatal issue surfaced during extraction.

    Per the methodology's central guarantee — "no silent drops" — every limit
    encountered during parsing is recorded here so an LLM consuming the graph
    can see and reason about it.

    Examples:
    - "Unrecognized sink class `MergeSink` at line 894; emitted heuristic Writes edge."
    - "%run target '_resources/00-setup.py' not found in repo; variables remain <unresolved>."
    - "DLT decorator @dlt.table at line 12; file skipped, no lineage extracted."
    """

    model_config = ConfigDict(extra="forbid")

    file: str
    lineno: Optional[int] = None
    category: str = Field(
        description="Short tag for grouping warnings: 'unresolved-table-name', "
        "'unregistered-sink', 'dlt-skipped', 'opaque-call-fallback', '%run-not-found', etc."
    )
    message: str


class GraphMetadata(BaseModel):
    """Metadata about the extraction run that produced this graph."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = SCHEMA_VERSION
    extractor_version: str = Field(
        default="dev",
        description="Version of the DEGraph extractor that produced this graph.",
    )
    repo_root: Optional[str] = None
    files_parsed: int = 0
    files_skipped: int = 0
    extraction_seconds: Optional[float] = None


class Graph(BaseModel):
    """The DEGraph output for a single repo (single-file JSON mode, Decision 3.5).

    Layout matches `outline.md §3.5`. Nodes are split by type for compactness
    and direct indexing; edges are a single typed list discriminated on
    ``kind``; expressions are interned in their own dict.
    """

    model_config = ConfigDict(extra="forbid")

    metadata: GraphMetadata = Field(default_factory=GraphMetadata)

    # Nodes, split by type for compact serialization and O(1) FQN lookup.
    tables: list[Table] = Field(default_factory=list)
    external_sources: list[ExternalSource] = Field(default_factory=list)
    dataframes: list[DataFrameNode] = Field(default_factory=list)
    expressions: list[Expression] = Field(default_factory=list)

    # Edges in a single typed list.
    edges: list[Edge] = Field(default_factory=list)

    # Non-fatal extraction issues.
    warnings: list[GraphWarning] = Field(default_factory=list)

    # Named Column-typed Python variables that hold when() classification chains
    # but are never the direct second argument of a withColumn() call (e.g. they
    # are passed into helper functions like _ext(), or aliased inside a select()
    # list). Without this field their conditional logic would be invisible to an
    # LLM reading the compact graph.
    #
    # Maps ``var_name`` → ordered ``[{"cond": <predicate>, "value": <result>}, ...]``
    # as produced by ``_extract_when_logic``.
    column_rules: dict[str, list[dict[str, str]]] = Field(
        default_factory=dict,
        description="Named Column-expression variables (not directly in withColumn) "
        "whose bodies are when()/otherwise() chains. Keys are Python variable names "
        "(e.g. 'orig_claim_subtype_expr'); values are ordered [{cond, value}] rule lists.",
    )

    # ★ Q3 fix: shared-predicate dicts.
    # When a Python dict variable holds Column-typed predicates referenced by
    # subscript from multiple withColumn() calls (e.g. ``conds = {"BH": <expr>,
    # ...}`` used by both ``claim_subcategory`` and
    # ``multiple_claim_subcategories``), record the dict here so the compact
    # graph can surface the shared origin once instead of duplicating it.
    #
    # Maps ``var_name`` → ``{label: predicate_text, ...}`` where
    # ``predicate_text`` is ``ast.unparse(<expr>)`` truncated to a sensible
    # length.  Both direct dict literals and same-file FunctionDef-returning-
    # dict shapes are detected.
    predicate_dicts: dict[str, dict[str, str]] = Field(
        default_factory=dict,
        description="Python dict variables holding shared classification predicates "
        "(label -> predicate text). Referenced by Subscript from multiple derives "
        "edges; surfaces the shared structure that links e.g. a first-match when-chain "
        "to an array-of-matches accumulator.",
    )

    # ---- Convenience accessors ----

    def all_node_ids(self) -> set[str]:
        """Return the IDs of every node in the graph. Used by validators and
        cross-reference checks."""
        return (
            {t.id for t in self.tables}
            | {e.id for e in self.external_sources}
            | {d.id for d in self.dataframes}
            | {x.id for x in self.expressions}
        )

    def find_table(self, fqn: str) -> Optional[Table]:
        """Look up a Table by its fully-qualified name. Returns None if not found."""
        for t in self.tables:
            if t.fqn == fqn:
                return t
        return None

    # ---- Validators ----

    @field_validator("tables", "external_sources", "dataframes", "expressions")
    @classmethod
    def _unique_node_ids_within_kind(cls, v: list) -> list:
        """Each node list must have unique IDs within its own kind."""
        ids = [n.id for n in v]
        if len(ids) != len(set(ids)):
            dups = sorted({x for x in ids if ids.count(x) > 1})
            raise ValueError(f"Duplicate node IDs within kind: {dups}")
        return v

    @model_validator(mode="after")
    def _edges_reference_existing_nodes(self) -> Graph:
        """Every edge endpoint and expr_id must reference a node that exists.

        Skipped in two cases:
        1. The endpoint string starts with `<unresolved>` — explicit allowed marker.
        2. The graph is empty (e.g. freshly constructed for piecewise loading).
        """
        if not self.edges:
            return self

        known = self.all_node_ids()

        def _check(eid: str, where: str) -> None:
            if eid.startswith(UNRESOLVED):
                return
            if eid not in known:
                raise ValueError(
                    f"Edge references unknown node id '{eid}' at {where}. "
                    f"Either the referenced node is missing or the id is malformed."
                )

        for e in self.edges:
            if isinstance(e, JoinsEdge):
                _check(e.left_source, f"JoinsEdge[{e.id}].left_source")
                _check(e.right_source, f"JoinsEdge[{e.id}].right_source")
                _check(e.target, f"JoinsEdge[{e.id}].target")
                if e.on_expr_id is not None:
                    _check(e.on_expr_id, f"JoinsEdge[{e.id}].on_expr_id")
            else:
                _check(e.source, f"{type(e).__name__}[{e.id}].source")
                _check(e.target, f"{type(e).__name__}[{e.id}].target")
                expr_attr = getattr(e, "expr_id", None) or getattr(e, "predicate_id", None)
                if expr_attr is not None:
                    _check(expr_attr, f"{type(e).__name__}[{e.id}].expr_ref")

        return self
