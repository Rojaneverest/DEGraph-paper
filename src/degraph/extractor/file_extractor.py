"""FileExtractor — extract lineage subgraph from a single source file.

Handles ``.py`` (Databricks notebook exports), ``.ipynb`` (Jupyter notebooks),
and ``.sql`` files (DDL / DML).

For each Python file the extractor:
  1. Preprocesses the source (strips magic comments, extracts ``%run`` paths).
  2. Seeds a ``SymbolTable`` from the ``%run`` targets (config variables).
  3. Parses the clean source with ``ast.parse()``.
  4. Walks top-level statements looking for DataFrame assignments and writes.
  5. For each assignment: unrolls the RHS method chain, emits edges.
  6. For each write expression: emits a ``WritesEdge``.

For SQL files the extractor uses sqlglot to parse DDL statements and populate
``Table.columns`` — no DataFrame edges are emitted from SQL.

Returns ``FileSubgraph`` which the assembler merges across files.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from degraph.graph import (
    UNRESOLVED,
    AggregatesEdge,
    DataFrameNode,
    DerivesEdge,
    Edge,
    Expression,
    ExternalSource,
    FiltersEdge,
    GraphWarning,
    JoinType,
    JoinsEdge,
    OpaqueKind,
    OpaqueTransformEdge,
    ProjectsEdge,
    ReadsEdge,
    Table,
    Column,
    WindowSpec,
    WritesEdge,
    WriteMode,
)
from degraph.extractor.chain_walker import (
    ChainOp,
    find_write_op,
    group_by_family,
    is_read_chain,
    is_write_chain,
    unroll_chain,
)
from degraph.extractor.df_tracker import DataFrameTracker
from degraph.extractor.notebook import PreparedSource, prepare
from degraph.extractor.registry import HelperRegistry, SinkRegistry
from degraph.extractor.safe_eval import SafeEvaluator, SymbolTable


# ---------------------------------------------------------------------------
# FileSubgraph — result of extracting one file
# ---------------------------------------------------------------------------


@dataclass
class FileSubgraph:
    """All nodes and edges emitted from a single source file."""

    rel_path: str
    """File path relative to the repo root."""

    dataframes: list[DataFrameNode] = field(default_factory=list)
    tables_referenced: list[Table] = field(default_factory=list)
    """Partial Table stubs (fqn + written_by/read_by); merged later."""

    external_sources_referenced: list[ExternalSource] = field(default_factory=list)
    """ExternalSource stubs (file paths, JDBC URLs); merged later."""

    expressions: list[Expression] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)
    warnings: list[GraphWarning] = field(default_factory=list)
    column_rules: dict[str, list[dict[str, str]]] = field(default_factory=dict)
    """Named Column-expression variables whose bodies are when() chains.
    Populated in FileExtractor.extract() by scanning _column_exprs.
    Merged across files by the assembler into Graph.column_rules."""

    predicate_dicts: dict[str, dict[str, str]] = field(default_factory=dict)
    """★ Q3: Python dict variables holding Column-typed predicates,
    referenced by Subscript from multiple withColumn() / column_rule chains.
    Values are unparsed AST text.  Merged into Graph.predicate_dicts."""


# ---------------------------------------------------------------------------
# FileExtractor
# ---------------------------------------------------------------------------


class FileExtractor:
    """Extract a lineage subgraph from one file.

    Parameters
    ----------
    path:
        Absolute path to the file.
    repo_root:
        Absolute path to the repo root (used to compute relative paths and
        to resolve ``%run`` targets).
    symbol_table:
        Pre-seeded symbol table (e.g. from ``%run ../_resources/setup``).
        The extractor adds any top-level assignments it can resolve.
    helper_registry / sink_registry:
        Loaded from ``.degraph/helpers.json`` / ``sinks.json``.
    """

    def __init__(
        self,
        path: Path,
        repo_root: Path,
        symbol_table: SymbolTable,
        helper_registry: HelperRegistry,
        sink_registry: SinkRegistry,
        table_schema: Optional[dict[str, list[str]]] = None,
    ) -> None:
        self.path = path
        self.repo_root = repo_root
        self.rel_path = path.relative_to(repo_root).as_posix()
        self.symbols = symbol_table
        self.helpers = helper_registry
        self.sinks = sink_registry

        self.tracker = DataFrameTracker()
        self._subgraph = FileSubgraph(rel_path=self.rel_path)
        self._expr_cache: dict[str, Expression] = {}
        self._expr_counter = 0
        self._edge_counter = 0
        # var_name → kwargs dict for DeltaMergeSink-style patterns
        self._pending_sinks: dict[str, dict] = {}
        # FQN → column name list for column-set propagation (from DDL)
        self._table_schema: dict[str, list[str]] = table_schema or {}
        # bare_name → fully-qualified "module.name" for unregistered opaque call detection
        self._imported_fqn: dict[str, str] = {}
        # cell_start_lines and cell_global_indices for .ipynb cell-prefix DF IDs
        self._cell_start_lines: list[int] = []
        self._cell_global_indices: list[int] = []
        # var_name → AST of a PySpark Column-typed variable (e.g. assignments
        # like `prof_subtype = F.when(...).when(...).otherwise(...)`). These
        # variables never appear inside the DataFrameTracker because they bind
        # a Column expression, not a DataFrame. When such a variable is later
        # referenced inside a .withColumn() / filter() / select() expression,
        # we expand it back to its original AST so column references buried
        # inside the variable's body are credited correctly on the edge's
        # source_cols / referenced_cols list.
        self._column_exprs: dict[str, ast.expr] = {}
        # P9: var_name -> WindowSpec for assignments like
        # `w = Window.partitionBy("a","b").orderBy(F.col("c").desc_nulls_last())`.
        # Resolved on derives edges whose expr contains `.over(<var_name>)`.
        self._window_specs: dict[str, WindowSpec] = {}
        # Fix #1: var_name -> list of AST exprs appended to it in module-level
        # for-loops.  Detected in _seed_symbols_from_source.  Used by
        # _extract_array_struct_fields to handle array(*var) patterns where the
        # struct schema lives inside the loop body (e.g. array(*diag_structs)).
        self._list_vars: dict[str, list[ast.expr]] = {}
        # ★ Q3 fix: var_name -> {label: ast.expr} for Python dict variables
        # holding Column-typed predicates.  Detected for two shapes:
        #   1. ``conds = {"BH": <col_expr>, ...}``  — direct dict literal.
        #   2. ``conds = build_conds(...)`` where build_conds is defined in the
        #      same file and ends with ``return <ast.Dict>``.
        # Referenced via Subscript in _extract_when_logic (and indirectly
        # surfaced via Graph.predicate_dicts in compact.py).
        self._predicate_dicts: dict[str, dict[str, ast.expr]] = {}
        # Same-file FunctionDef registry (name -> FunctionDef) for the
        # function-call inliner above.  Populated by _seed_symbols_from_source.
        self._function_defs: dict[str, ast.FunctionDef] = {}
        # ★ P17: env-var names referenced via os.environ.get() in this file.
        # Used to enrich dynamic_note on AggregatesEdges so the LLM can see
        # which config source controls the runtime agg list.
        self._environ_gets: list[str] = []

    # ------------------------------------------------------------------ #
    # Node ID helpers                                                       #
    # ------------------------------------------------------------------ #

    def _node_prefix(self, lineno: int) -> str:
        """Return the lineno string (or cell<N> for .ipynb) for use in DF node IDs.

        For .py files this is just str(lineno).
        For .ipynb files it maps the line in the joined source back to the
        global cell index (1-based, counting all cell types) so that node IDs
        read as ``cell5:customers_landing`` rather than raw line numbers.
        """
        if not self._cell_start_lines:
            return str(lineno)
        # Binary search: find the last cell whose start line <= lineno
        idx = len(self._cell_start_lines) - 1
        for i, start in enumerate(self._cell_start_lines):
            if start > lineno:
                idx = i - 1
                break
        global_idx = self._cell_global_indices[max(idx, 0)]
        return f"cell{global_idx}"

    # ------------------------------------------------------------------ #
    # Public entry point                                                   #
    # ------------------------------------------------------------------ #

    def extract(self) -> FileSubgraph:
        """Run extraction and return the subgraph."""
        if self.path.suffix == ".sql":
            self._extract_sql()
            return self._subgraph

        prepared = prepare(self.path)
        self._cell_start_lines = prepared.cell_start_lines
        self._cell_global_indices = prepared.cell_global_indices
        # Add any top-level assignments discoverable in this file's source
        self._seed_symbols_from_source(prepared.source)
        self._process_prepared(prepared)
        # Flush all DataFrameNodes registered in the tracker to the subgraph
        self._subgraph.dataframes.extend(self.tracker.all_nodes())
        # P6: scan the Column-expression registry for any variable whose body
        # is (or expands to) a when()/otherwise() chain.  These carry the key
        # classification rules (claim subtype buckets, TOB code ranges, etc.)
        # but are often never passed directly as the second arg of withColumn().
        # Instead they land inside select()-list aliases or helper function calls,
        # making them invisible to rule_logic extraction in _emit_derives_op.
        # We surface them here so the compact graph can include a dedicated
        # "named_column_logic" section that an LLM can read directly.
        for var_name, expr_ast in self._column_exprs.items():
            rules = _extract_when_logic(
                expr_ast, self._column_exprs,
                predicate_dicts=self._predicate_dicts,
            )
            if not rules:
                # ★ Q3 v2: also handle the `array(*[when(d[k], lit(k)) for k
                # in d])` comprehension pattern used to build "all matches"
                # arrays from a predicate dict. Without this fallback, the
                # link between the shared dict and downstream columns is
                # invisible, breaking shared-predicate questions on
                # real-world pipelines that use this pattern.
                rules = _extract_array_of_whens_logic(
                    expr_ast, self._predicate_dicts,
                )
            if rules:
                self._subgraph.column_rules[var_name] = rules

        # ★ Q3: also export predicate_dicts so the compact graph can surface
        # them.  Values are unparsed AST text, truncated to 200 chars.
        for var_name, mapping in self._predicate_dicts.items():
            self._subgraph.predicate_dicts[var_name] = {
                label: ast.unparse(expr)[:200]
                for label, expr in mapping.items()
            }

        return self._subgraph

    # ------------------------------------------------------------------ #
    # Symbol table seeding                                                 #
    # ------------------------------------------------------------------ #

    def _seed_symbols_from_source(self, source: str) -> None:
        try:
            tree = ast.parse(source, filename=self.rel_path)
            self.symbols.update_from_ast(tree)
            # ★ Q3 fix: register top-level FunctionDef bodies so the predicate-
            # dict detector can inline ``conds = build_conds(...)`` patterns.
            for node in tree.body:
                if isinstance(node, ast.FunctionDef):
                    self._function_defs[node.name] = node
            # ★ P17: scan for os.environ.get("<VAR>") calls to capture config
            # paths that drive dynamic aggregation lists at runtime.
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "get"
                    and isinstance(node.func.value, ast.Attribute)
                    and node.func.value.attr == "environ"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)
                ):
                    env_name = node.args[0].value
                    if env_name not in self._environ_gets:
                        self._environ_gets.append(env_name)
            # Fix #1: detect `var = []` + for-loop `var.append(expr)` at module
            # top-level so array(*var) patterns resolve to struct field schemas.
            for stmt in tree.body:
                if (
                    isinstance(stmt, ast.Assign)
                    and len(stmt.targets) == 1
                    and isinstance(stmt.targets[0], ast.Name)
                    and isinstance(stmt.value, ast.List)
                    and not stmt.value.elts
                ):
                    self._list_vars[stmt.targets[0].id] = []
                elif isinstance(stmt, ast.For):
                    for body_stmt in stmt.body:
                        if (
                            isinstance(body_stmt, ast.Expr)
                            and isinstance(body_stmt.value, ast.Call)
                        ):
                            call = body_stmt.value
                            if (
                                isinstance(call.func, ast.Attribute)
                                and call.func.attr == "append"
                                and isinstance(call.func.value, ast.Name)
                                and call.func.value.id in self._list_vars
                                and call.args
                            ):
                                self._list_vars[call.func.value.id].append(call.args[0])
        except SyntaxError:
            pass

    # ------------------------------------------------------------------ #
    # Q3 — predicate-dict detection                                       #
    # ------------------------------------------------------------------ #

    def _try_extract_predicate_dict(
        self, value: ast.expr
    ) -> Optional[dict[str, ast.expr]]:
        """If *value* is (or resolves to) a dict literal whose keys are string
        constants and whose values look like PySpark Column expressions,
        return ``{label: <value_ast>, ...}``.  Otherwise return ``None``.

        Two shapes are supported:
          1. Direct ``ast.Dict`` literal at the assignment RHS.
          2. ``Call(func=Name(fn), args=...)`` where ``fn`` is a same-file
             FunctionDef whose final ``Return`` is an ``ast.Dict``.

        Function-call shape (2) handles the real-world pattern
        ``conds = build_conditions(...)`` where the
        function returns a dict whose values are bare ``Name`` references to
        local Column-typed variables (``cond_a``, ``cond_b``, ...).  In
        that case we walk the function body to resolve those names to their
        local Column-expression assignments.
        """
        dict_ast: Optional[ast.Dict] = None
        local_col_vars: dict[str, ast.expr] = {}

        if isinstance(value, ast.Dict):
            dict_ast = value
        elif (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id in self._function_defs
        ):
            fn = self._function_defs[value.func.id]
            # First pass: register local Column-typed variables in the function
            # body so the dict values (which are usually bare Names) can be
            # resolved to their underlying expressions.
            for stmt in fn.body:
                if (
                    isinstance(stmt, ast.Assign)
                    and len(stmt.targets) == 1
                    and isinstance(stmt.targets[0], ast.Name)
                    and _looks_like_column_expr(stmt.value, self._column_exprs)
                ):
                    local_col_vars[stmt.targets[0].id] = stmt.value
            # Find the LAST return statement at the function-body level whose
            # value is a Dict literal.  We don't recurse into nested scopes.
            for stmt in reversed(fn.body):
                if isinstance(stmt, ast.Return):
                    if isinstance(stmt.value, ast.Dict):
                        dict_ast = stmt.value
                    break

        if dict_ast is None:
            return None

        # The effective Column-expr namespace = module-level _column_exprs
        # plus the locally discovered function-body Column variables.
        col_ns = {**self._column_exprs, **local_col_vars}

        result: dict[str, ast.expr] = {}
        for k, v in zip(dict_ast.keys, dict_ast.values):
            if (
                isinstance(k, ast.Constant)
                and isinstance(k.value, str)
                and v is not None
            ):
                # Resolve bare-Name dict values via the local Column-var map
                # so the registered AST is the actual predicate expression.
                resolved_v: ast.expr = v
                if isinstance(v, ast.Name) and v.id in local_col_vars:
                    resolved_v = local_col_vars[v.id]
                if _looks_like_column_expr(resolved_v, col_ns):
                    result[k.value] = resolved_v

        return result if result else None

    # ------------------------------------------------------------------ #
    # Python AST processing                                                #
    # ------------------------------------------------------------------ #

    def _process_prepared(self, prepared: PreparedSource) -> None:
        try:
            tree = ast.parse(prepared.source, filename=self.rel_path)
        except SyntaxError as e:
            self._warn(0, "syntax-error", f"ast.parse failed: {e}")
            return

        # First pass: collect imported names for opaque-call detection.
        # _imported_fqn maps bare_name → "module.name" so we can emit FQN in OpaqueTransform.
        # Also detect DLT / Spark-Declarative-Pipelines module aliases so decorated
        # table/view functions can be recognised (@dp.table / @dlt.table).
        self._dlt_aliases: set[str] = set()
        for stmt in tree.body:
            if isinstance(stmt, ast.ImportFrom):
                module = stmt.module or ""
                for alias in stmt.names:
                    bare = alias.asname if alias.asname else alias.name
                    # Keep the original module.name as FQN; asname is just an alias, FQN stays
                    fqn = f"{module}.{alias.name}" if module and not alias.asname else bare
                    self._imported_fqn[bare] = fqn
                    # from pyspark import pipelines as dp  /  import dlt
                    if alias.name in ("pipelines", "dlt") or module.endswith("pipelines"):
                        self._dlt_aliases.add(bare)
            elif isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    bare = alias.asname if alias.asname else alias.name.split(".")[0]
                    self._imported_fqn[bare] = bare
                    if alias.name in ("dlt",) or alias.name.endswith("pipelines"):
                        self._dlt_aliases.add(bare)

        # Second pass: extract lineage
        for stmt in ast.walk(tree):
            if isinstance(stmt, ast.Assign):
                self._process_assign(stmt)
            elif isinstance(stmt, ast.Expr):
                self._process_expr_stmt(stmt)

        # Third pass: DLT / Spark Declarative Pipelines decorated functions, whose
        # output table is the function name and whose body returns the pipeline.
        if self._dlt_aliases:
            self._process_dlt_functions(tree)

    def _process_assign(self, stmt: ast.Assign) -> None:
        """Handle ``varname = <expr>``."""
        if len(stmt.targets) != 1:
            return
        target = stmt.targets[0]
        if not isinstance(target, ast.Name):
            return
        var_name = target.id
        lineno = stmt.lineno

        value = stmt.value
        base, ops = unroll_chain(value)

        # ---- Spark read: spark.table(...) / spark.read.* ------------------
        if is_read_chain(ops) or self._is_spark_table_call(value) or _is_spark_read_base(base):
            # If the read chain continues into transforms (e.g.
            # `stats = spark.read.table(x).groupby(k).agg(...)` inside a
            # Declarative-Pipeline body), the bare `_emit_reads` would drop the
            # trailing aggregate/transform ops. Route those through the read-rooted
            # pipeline so the aggregate/derive edges are captured and assigned.
            sp = self._split_read_chain(base, ops)
            if sp is not None and sp[1]:
                final_id = self._emit_read_rooted_pipeline(value, lineno, var_name=var_name)
                if final_id:
                    return
            node = self._emit_reads(var_name, lineno, base, ops, value)
            if node:
                self.tracker.assign(var_name, node)
            return

        # ---- spark.sql("SELECT ... FROM ...") ----------------------------
        if self._is_spark_sql_call(value):
            node = self._emit_spark_sql(var_name, lineno, value)
            if node:
                self.tracker.assign(var_name, node)
            return

        # ---- Registered sink instantiation (DeltaMergeSink pattern) --------
        if self._try_sink_instantiation(var_name, lineno, value):
            return

        # ---- Registered helper call (trim / suffix rename) ---------------
        helper_result = self._try_helper_call(var_name, lineno, value)
        if helper_result is not None:
            self.tracker.assign(var_name, helper_result)
            return

        # P9: detect Window.partitionBy(...).orderBy(...) assignments BEFORE the
        # opaque-call branch (which would otherwise grab Window.partitionBy as an
        # unrecognised imported call). Stash the resolved WindowSpec so later
        # derives edges whose expression uses .over(<var_name>) can attach the
        # partition/order keys.
        if isinstance(base, ast.Name) and base.id == "Window" and ops:
            spec = self._extract_window_spec_from_ops(ops)
            if spec is not None:
                self._window_specs[var_name] = spec
            return

        # ★ Q3 fix: detect predicate-dict assignments BEFORE the opaque/chain
        # branches.  Two shapes:
        #   1. ``conds = {"BH": <col_expr>, ...}``  — direct dict literal.
        #   2. ``conds = same_file_fn(args)`` where same_file_fn is defined in
        #      the current file and ends with ``return <ast.Dict>``.
        # In both cases the dict's values must be Column-typed; keys must be
        # string constants.  Registered in self._predicate_dicts for later
        # subscript resolution in _extract_when_logic.
        pred_dict = self._try_extract_predicate_dict(value)
        if pred_dict is not None:
            self._predicate_dicts[var_name] = pred_dict
            # Fall through — the variable may also still be useful for
            # downstream tracking; in practice these dicts aren't DataFrames.
            return

        # ---- Unregistered imported function call (opaque) ----------------
        opaque_result = self._try_opaque_call(var_name, lineno, value)
        if opaque_result is not None:
            self.tracker.assign(var_name, opaque_result)
            return

        # ---- DataFrame method chain --------------------------------------
        if not ops:
            # Not a DataFrame chain. Before giving up, see if the RHS is a
            # PySpark Column expression (e.g. `prof = F.when(...)`) and stash
            # it for later expansion when referenced from withColumn/select.
            if _looks_like_column_expr(value, self._column_exprs):
                self._column_exprs[var_name] = value
            return

        # P4: detect  spark.table("fqn").filter(...).select(...).groupBy(...).agg(...)
        # After unroll_chain the base is Name("spark") and ops[0].method == "table".
        # None of the earlier read-detection conditions fire because the full value
        # is not spark.table() directly and FAMILY classifies "table" as write_chain.
        # Emit an anonymous ReadsEdge for the catalog table, then hand the remaining
        # ops to _process_chain so filter/select/groupBy/agg are fully captured.
        if (
            isinstance(base, ast.Name) and base.id == "spark"
            and ops and ops[0].method == "table"
        ):
            anon_src_id = self._emit_inline_spark_table_read(ops[0], lineno)
            remaining = ops[1:]
            if remaining:
                self._process_chain(var_name, lineno, anon_src_id, remaining)
            else:
                # No downstream transforms — give the anonymous read node the var name
                read_node = self.tracker.get_node_by_id(anon_src_id)
                named = DataFrameNode(
                    id=f"df:{self.rel_path}:{self._node_prefix(lineno)}:{var_name}",
                    file=self.rel_path,
                    var_name=var_name,
                    lineno=lineno,
                    columns=list(read_node.columns) if read_node else [],
                )
                self.tracker.assign(var_name, named)
            return

        # Resolve base to a known DataFrame
        source_id = self._resolve_base_to_id(base)
        if source_id is None:
            # Column-expr fallback: RHS is a method chain on a Column, not a DataFrame
            # (e.g. `bill_int = bill.cast("int")`).
            if _looks_like_column_expr(value, self._column_exprs):
                self._column_exprs[var_name] = value
            return

        self._process_chain(var_name, lineno, source_id, ops)

    def _process_expr_stmt(self, stmt: ast.Expr) -> None:
        """Handle bare expression statements — mostly write calls."""
        value = stmt.value
        if not isinstance(value, ast.Call):
            return

        # sink.run(df)  pattern for registered sinks (sink pre-assigned to var)
        if (
            isinstance(value.func, ast.Attribute)
            and value.func.attr == "run"
            and isinstance(value.func.value, ast.Name)
        ):
            sink_var = value.func.value.id
            if sink_var in self._pending_sinks:
                df_arg = value.args[0] if value.args else None
                df_id = self._resolve_base_to_id(df_arg) if df_arg else None
                if df_id:
                    self._emit_sink_writes(sink_var, df_id, stmt.lineno)
                return

        # Inline pattern: SinkClass(target_table=..., ...).save(df)  or .run(df)
        # — the sink is constructed and immediately invoked in one expression.
        # No intermediate variable assignment, so _try_sink_instantiation never
        # fires; we have to recognise it here at the call site.
        if self._try_inline_sink_call(value, stmt.lineno):
            return

        # Parameterized inter-procedural ingestion helper:
        #   def ingest(folder,fmt,table): return spark.readStream...load(folder)
        #       .writeStream.table(table)
        #   ingest("/vol/orders","json","orders_bronze")
        if self._process_interproc_pipeline_call(value, stmt.lineno):
            return

        # Un-assigned read-rooted pipeline:
        #   (spark.readStream.table(...).withColumn(...)...writeStream.table(...))
        # The whole read->transform->write pipeline is one bare expression with no
        # df = assignment. Dominant in real Databricks plain-Spark code.
        if self._process_read_rooted_expr(value, stmt.lineno):
            return

        # Direct write chains:  (df.write.format(...).saveAsTable(...))
        base, ops = unroll_chain(value)
        if is_write_chain(ops):
            df_source_id = self._resolve_base_to_id(base)
            if df_source_id:
                self._emit_writes_from_chain(df_source_id, ops, stmt.lineno)

    # ------------------------------------------------------------------ #
    # Chain processing (the core loop)                                    #
    # ------------------------------------------------------------------ #

    def _process_chain(
        self,
        var_name: str,
        lineno: int,
        source_id: str,
        ops: list[ChainOp],
    ) -> None:
        """Process a method chain and emit edges.

        Creates the final target DataFrameNode and, where the chain has
        operations of different types, intermediate anonymous nodes at each
        type boundary.
        """
        # Separate write ops from transform ops
        transform_ops = [op for op in ops if op.family not in ("write_chain", "unknown")]
        write_ops = [op for op in ops if op.family == "write_chain"]

        if not transform_ops and not write_ops:
            return

        # Build the final (assignment) target node
        final_node = DataFrameNode(
            id=f"df:{self.rel_path}:{self._node_prefix(lineno)}:{var_name}",
            file=self.rel_path,
            var_name=var_name,
            lineno=lineno,
        )
        self.tracker.assign(var_name, final_node)

        if not transform_ops:
            # Pure write chain — just emit writes, no DataFrame produced
            self._emit_writes_from_chain(source_id, write_ops, lineno)
            return

        groups = group_by_family(transform_ops)

        current_source = source_id
        for g_idx, group in enumerate(groups):
            is_last = g_idx == len(groups) - 1
            group_family = group[0].family
            # Normalise agg families
            if group_family in ("agg_start", "agg_end"):
                group_family = "aggregates"

            if is_last:
                current_target = final_node.id
            else:
                anon_id = self.tracker.make_anon_id(self.rel_path, self._node_prefix(group[0].lineno))
                anon_node = DataFrameNode(
                    id=anon_id,
                    file=self.rel_path,
                    var_name=None,
                    lineno=group[0].lineno,
                )
                self.tracker.register(anon_node)
                current_target = anon_id

            # Emit edges for this group
            self._emit_group(group_family, group, current_source, current_target)
            self._propagate_cols(group_family, group, current_source, current_target)
            current_source = current_target

        # Emit any trailing write chain
        if write_ops:
            self._emit_writes_from_chain(current_source, write_ops, lineno)

    def _emit_group(
        self,
        family: str,
        group: list[ChainOp],
        source_id: str,
        target_id: str,
    ) -> None:
        """Dispatch to the per-family edge emitter."""
        if family == "derives":
            for op in group:
                self._emit_derives_op(op, source_id, target_id)
        elif family == "projects":
            self._emit_projects(group, source_id, target_id)
        elif family == "filters":
            self._emit_filters(group[0], source_id, target_id)
        elif family == "joins":
            # Each join is independent; caller should have created separate groups
            for op in group:
                self._emit_join(op, source_id, target_id)
        elif family == "aggregates":
            self._emit_aggregates(group, source_id, target_id)

    def _propagate_cols(
        self,
        family: str,
        group: list[ChainOp],
        source_id: str,
        target_id: str,
    ) -> None:
        """Forward-propagate column sets through schema-preserving transforms.

        Populates the target DataFrameNode.columns in-place. Best-effort:
        produces an empty list if input is unknown.
        """
        source_node = self.tracker.get_node_by_id(source_id)
        target_node = self.tracker.get_node_by_id(target_id)
        if not target_node:
            return

        ev = SafeEvaluator(self.symbols.symbols)
        cols = list(source_node.columns) if (source_node and source_node.columns) else []

        if family == "filters":
            target_node.columns = cols

        elif family == "projects":
            if not cols:
                return
            removed = set()
            for op in group:
                for arg in op.args:
                    removed.add(ev.resolve(arg))
            target_node.columns = [c for c in cols if c not in removed]

        elif family == "derives":
            for op in group:
                if op.method == "withColumn":
                    new_col = ev.resolve(op.positional(0)) if op.positional(0) else None
                    if new_col and new_col not in cols:
                        cols = cols + [new_col]
                elif op.method == "withColumnRenamed":
                    old_col = ev.resolve(op.positional(0)) if op.positional(0) else None
                    new_col = ev.resolve(op.positional(1)) if op.positional(1) else None
                    if new_col:
                        cols = [new_col if c == old_col else c for c in cols]
                        if new_col not in cols:
                            cols = cols + [new_col]
            target_node.columns = cols

        elif family == "joins":
            # Best-effort union of left + right columns minus the right-side
            # duplicate of any single-key (string or USING-style) join column.
            #
            # If the right side is an inline ``.select(...)`` projection (e.g.
            # ``timeline.join(orders.select('order_id','customer_id'), ...)``)
            # the static positional columns are honoured instead of the full
            # right-source column list. Without this, every column of the right
            # source spuriously propagates into the join target and downstream
            # writes — over-counting the silver/gold table schema.
            for op in group:
                right_arg = op.positional(0)
                right_id = self._resolve_base_to_id(right_arg) if right_arg else None
                inline_proj = _extract_inline_select_cols(right_arg)
                if inline_proj:
                    right_cols = list(inline_proj)
                else:
                    right_node = self.tracker.get_node_by_id(right_id) if right_id else None
                    right_cols = list(right_node.columns) if (right_node and right_node.columns) else []
                on_arg = op.get_kwarg("on") or op.positional(1)
                using_keys: set[str] = set()
                if isinstance(on_arg, ast.Constant) and isinstance(on_arg.value, str):
                    using_keys.add(on_arg.value)
                elif isinstance(on_arg, ast.List):
                    for elt in on_arg.elts:
                        if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                            using_keys.add(elt.value)
                for c in right_cols:
                    if c in using_keys:
                        continue
                    if c not in cols:
                        cols.append(c)
            target_node.columns = cols

        elif family == "aggregates":
            # Output schema = group_keys + agg output cols (already computed by emitter)
            group_keys: list[str] = []
            output_cols: list[str] = []
            for op in group:
                if op.method == "groupBy":
                    for arg in op.args:
                        group_keys.append(ev.resolve(arg))
                elif op.method == "agg":
                    for arg in op.args:
                        if isinstance(arg, ast.Starred):
                            output_cols.append(UNRESOLVED)
                        else:
                            _, _, alias = _parse_agg_call(arg, ev)
                            output_cols.append(alias)
            target_node.columns = [
                c for c in group_keys + output_cols if c
            ]

    # ------------------------------------------------------------------ #
    # Reads                                                                #
    # ------------------------------------------------------------------ #

    def _is_spark_table_call(self, node: ast.expr) -> bool:
        """True for  spark.table(...)."""
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "table"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "spark"
        )

    def _is_spark_sql_call(self, node: ast.expr) -> bool:
        """True for  spark.sql(...)."""
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "sql"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "spark"
        )

    def _emit_spark_sql(
        self,
        var_name: str,
        lineno: int,
        value: ast.Call,
    ) -> Optional[DataFrameNode]:
        """Parse spark.sql() WITH...SELECT and emit CTE lineage edges."""
        try:
            import sqlglot
            import sqlglot.expressions as exp
        except ImportError:
            return DataFrameNode(
                id=f"df:{self.rel_path}:{self._node_prefix(lineno)}:{var_name}",
                file=self.rel_path,
                var_name=var_name,
                lineno=lineno,
            )

        if not value.args:
            return None
        ev = SafeEvaluator(self.symbols.symbols)
        sql_str = ev.resolve(value.args[0])
        if not isinstance(sql_str, str) or not sql_str.strip():
            return DataFrameNode(
                id=f"df:{self.rel_path}:{self._node_prefix(lineno)}:{var_name}",
                file=self.rel_path,
                var_name=var_name,
                lineno=lineno,
            )

        try:
            statements = sqlglot.parse(sql_str.strip(), dialect="spark")
            parsed = statements[0] if statements else None
        except Exception:
            parsed = None

        output_node = DataFrameNode(
            id=f"df:{self.rel_path}:{self._node_prefix(lineno)}:{var_name}",
            file=self.rel_path,
            var_name=var_name,
            lineno=lineno,
        )

        if parsed is None:
            return output_node

        with_clause = parsed.find(exp.With)
        if not with_clause:
            return output_node

        # cte_name → final DataFrameNode for that CTE
        cte_nodes: dict[str, DataFrameNode] = {}
        # monotonic counter so every CTE node gets a unique lineno-offset ID
        _cte_seq = [0]

        def _cte_lineno() -> int:
            _cte_seq[0] += 1
            return lineno + _cte_seq[0]

        def _make_cte_node(node_name: str) -> DataFrameNode:
            nl = _cte_lineno()
            n = DataFrameNode(
                id=f"df:{self.rel_path}:{self._node_prefix(nl)}:{node_name}",
                file=self.rel_path,
                var_name=node_name,
                lineno=nl,
            )
            self.tracker.register(n)
            return n

        for cte_expr in with_clause.expressions:
            cte_name = cte_expr.alias
            select = cte_expr.this

            from_clause = select.find(exp.From)
            from_tbl = from_clause.find(exp.Table) if from_clause else None
            joins = list(select.find_all(exp.Join))
            group = select.find(exp.Group)

            # SELECT-list parsing:
            #  - bare Column → projected_cols entry (passthrough column from source)
            #  - Alias of an aggregate → output of aggregation (handled below)
            #  - Alias of an expression with a Window → derived window col
            #  - Alias of a non-aggregate expression → derived col
            projected_cols: list[str] = []
            computed_cols: list[tuple[str, Optional[exp.Window], Any]] = []
            for sel_item in select.selects:
                if isinstance(sel_item, exp.Column):
                    projected_cols.append(sel_item.name)
                elif isinstance(sel_item, exp.Alias) and isinstance(sel_item.this, exp.Column):
                    # `SELECT col AS alias` — projects col through under a new name.
                    # Record the alias name as the surviving output column.
                    projected_cols.append(sel_item.alias)
                elif isinstance(sel_item, exp.Alias):
                    computed_cols.append((sel_item.alias, sel_item.this.find(exp.Window), sel_item.this))

            cte_node = _make_cte_node(f"{cte_name}_cte")

            if from_tbl is None:
                cte_nodes[cte_name] = cte_node
                continue

            from_name = from_tbl.name

            # FROM a catalog table (not a CTE)
            if from_name not in cte_nodes:
                fqn_parts = [p for p in [from_tbl.catalog, from_tbl.db, from_tbl.name] if p]
                fqn = ".".join(fqn_parts) if fqn_parts else from_name
                source_id = f"table:{fqn}"
                table_stub = Table(id=source_id, fqn=fqn, read_by=[self.rel_path])
                self._subgraph.tables_referenced.append(table_stub)
                self._subgraph.edges.append(ReadsEdge(
                    id=self._next_edge_id("r"),
                    file=self.rel_path,
                    lineno=lineno,
                    source=source_id,
                    target=cte_node.id,
                    projected_cols=projected_cols,
                    streaming=False,
                ))
                # Fix #2(a): process computed_cols (e.g. window functions) for
                # catalog-table CTEs. Previously these were silently dropped here.
                if computed_cols:
                    import sqlglot.expressions as _sg_exp_a
                    for _ccol_name, _ccol_win, _ccol_expr in computed_cols:
                        _cwin_spec: Optional[WindowSpec] = None
                        _cwin_src: list[str] = []
                        if _ccol_win is not None:
                            _c_order = _ccol_win.args.get("order")
                            _c_order_cols: list[str] = []
                            if _c_order:
                                for _cord_item in _c_order.expressions:
                                    _ccol_e = _cord_item.this
                                    _ccol_s = _ccol_e.name if isinstance(_ccol_e, _sg_exp_a.Column) else str(_ccol_e)
                                    _cdesc = bool(_cord_item.args.get("desc"))
                                    _c_order_cols.append(f"{_ccol_s} desc" if _cdesc else _ccol_s)
                            _c_part_by = _ccol_win.args.get("partition_by")
                            _c_part_cols: list[str] = []
                            if _c_part_by:
                                # partition_by is a plain Python list of sqlglot Column nodes
                                _pb_list = _c_part_by if isinstance(_c_part_by, list) else getattr(_c_part_by, "expressions", [_c_part_by])
                                for _pb in _pb_list:
                                    _c_part_cols.append(_pb.name if isinstance(_pb, _sg_exp_a.Column) else str(_pb))
                            _cwin_spec = WindowSpec(partition_cols=_c_part_cols, order_cols=_c_order_cols, frame=None)
                            _cwin_src = list(_c_part_cols)
                            for _oc in _c_order_cols:
                                _b = _oc.split()[0]
                                if _b not in _cwin_src:
                                    _cwin_src.append(_b)
                        else:
                            # non-window computed col: source cols = referenced cols
                            _cwin_src = _sqlglot_expr_cols(_ccol_expr)
                        self._subgraph.edges.append(DerivesEdge(
                            id=self._next_edge_id("d"),
                            file=self.rel_path,
                            lineno=lineno,
                            source=cte_node.id,
                            target=cte_node.id,
                            output_col=_ccol_name,
                            source_cols=_cwin_src,
                            window_spec=_cwin_spec,
                        ))
                # Seed the CTE node's column set from the SELECT list so downstream
                # propagation can walk through it.
                cte_node.columns = list(projected_cols) + [cn for cn, *_ in computed_cols if cn]
                cte_nodes[cte_name] = cte_node
                continue

            from_source_id = cte_nodes[from_name].id

            # Aggregation CTE: GROUP BY, no JOINs
            if group and not joins:
                gkeys = [col.name for col in group.find_all(exp.Column)]
                agg_ops_list: list[str] = []
                agg_inputs_list: list[str] = []
                output_cols_list: list[str] = []
                for sel_item in select.selects:
                    if isinstance(sel_item, exp.Alias):
                        output_cols_list.append(sel_item.alias)
                        agg_ops_list.append(type(sel_item.this).__name__.lower())
                        # Extract first column reference inside the aggregate
                        inner_cols = [c.name for c in sel_item.this.find_all(exp.Column)]
                        agg_inputs_list.append(inner_cols[0] if inner_cols else UNRESOLVED)
                self._subgraph.edges.append(AggregatesEdge(
                    id=self._next_edge_id("a"),
                    file=self.rel_path,
                    lineno=lineno,
                    source=from_source_id,
                    target=cte_node.id,
                    group_keys=gkeys,
                    agg_ops=agg_ops_list,
                    agg_inputs=agg_inputs_list,
                    output_cols=output_cols_list,
                ))
                # Output columns = group_keys + aggregate output columns
                cte_node.columns = list(gkeys) + [
                    c for c in output_cols_list if c and c != UNRESOLVED
                ]
                cte_nodes[cte_name] = cte_node
                continue

            # JOIN CTE: create intermediate join node then emit derives
            if joins:
                join_node = _make_cte_node(f"{cte_name}_joined")

                first_join = joins[0]
                right_tbl = first_join.find(exp.Table)
                right_name = right_tbl.name if right_tbl else None
                right_source_id = (
                    cte_nodes[right_name].id if right_name and right_name in cte_nodes
                    else UNRESOLVED
                )

                jside = str(first_join.args.get("side") or "").lower()
                join_type = _parse_join_type(jside or "inner")

                jusing = first_join.args.get("using") or []
                join_keys: list[list[str]] = [[str(c), str(c)] for c in jusing]

                # ★ P13: also extract the ON clause (USING-only was the prior bug).
                # `from_tbl.alias` is the LEFT table alias (e.g. "clm"); the
                # JOIN side's alias (e.g. "c") is on `right_tbl.alias`.
                on_expr = first_join.args.get("on")
                if on_expr is not None:
                    left_alias = (getattr(from_tbl, "alias", None) or from_name) if from_tbl else None
                    right_alias = (getattr(right_tbl, "alias", None) or right_name) if right_tbl else None
                    on_keys = _extract_sqlglot_join_keys(on_expr, left_alias, right_alias)
                    for lk, rk in on_keys:
                        join_keys.append([lk, rk])

                self._subgraph.edges.append(JoinsEdge(
                    id=self._next_edge_id("j"),
                    file=self.rel_path,
                    lineno=lineno,
                    left_source=from_source_id,
                    right_source=right_source_id,
                    target=join_node.id,
                    join_type=join_type,
                    join_keys=join_keys,
                ))

                # Compose the join_node's column set from the two sides' columns.
                left_cols = list(cte_nodes[from_name].columns) if from_name in cte_nodes else []
                right_cols = (
                    list(cte_nodes[right_name].columns)
                    if right_name and right_name in cte_nodes else []
                )
                # USING(key) coalesces the key column; otherwise both sides' cols carry.
                using_set = {str(c) for c in jusing}
                merged = list(left_cols)
                for c in right_cols:
                    if c in using_set:
                        continue
                    if c not in merged:
                        merged.append(c)
                join_node.columns = merged

                for col_name, window_node, col_expr in computed_cols:
                    window_spec: Optional[WindowSpec] = None
                    win_source_cols: list[str] = []
                    if window_node is not None:
                        import sqlglot.expressions as _sg_exp
                        order = window_node.args.get("order")
                        order_cols: list[str] = []
                        if order:
                            for ord_item in order.expressions:
                                # ★ P16 fix: strip SQL table alias from col refs
                                # (e.g. "b.lifetime_revenue" → "lifetime_revenue")
                                # and detect DESC via Ordered.args["desc"] rather
                                # than type name "Desc" (which sqlglot doesn't use).
                                col_expr = ord_item.this
                                if isinstance(col_expr, _sg_exp.Column):
                                    col_str = col_expr.name  # bare col name, no alias
                                else:
                                    col_str = str(col_expr)
                                desc = bool(ord_item.args.get("desc"))
                                order_cols.append(f"{col_str} desc" if desc else col_str)
                        # Fix #5: extract PARTITION BY cols from sqlglot window node.
                        # window_node.args["partition_by"] is a plain Python list of Column nodes.
                        part_by = window_node.args.get("partition_by")
                        partition_cols: list[str] = []
                        if part_by:
                            _pb_items = part_by if isinstance(part_by, list) else getattr(part_by, "expressions", [part_by])
                            for pb_expr in _pb_items:
                                if isinstance(pb_expr, _sg_exp.Column):
                                    partition_cols.append(pb_expr.name)
                                else:
                                    partition_cols.append(str(pb_expr))
                        window_spec = WindowSpec(partition_cols=partition_cols, order_cols=order_cols, frame=None)
                        # Fix #5: source_cols = PARTITION BY ∪ ORDER BY bare names.
                        win_source_cols = list(partition_cols)
                        for _oc in order_cols:
                            _bare = _oc.split()[0]
                            if _bare not in win_source_cols:
                                win_source_cols.append(_bare)
                    else:
                        # non-window computed col: source cols = referenced cols
                        win_source_cols = _sqlglot_expr_cols(col_expr)
                    self._subgraph.edges.append(DerivesEdge(
                        id=self._next_edge_id("d"),
                        file=self.rel_path,
                        lineno=lineno,
                        source=join_node.id,
                        target=cte_node.id,
                        output_col=col_name,
                        source_cols=win_source_cols,
                        window_spec=window_spec,
                    ))

                # Fix #2(b): if GROUP BY is present alongside a JOIN, emit an
                # AggregatesEdge so the group keys and aggregate functions are
                # visible in the DSL (previously only the join was captured).
                if group:
                    _gkeys = [c.name for c in group.find_all(exp.Column)]
                    _agg_ops: list[str] = []
                    _agg_inputs: list[str] = []
                    _agg_out: list[str] = []
                    for _sel_item in select.selects:
                        if (
                            isinstance(_sel_item, exp.Alias)
                            and _sel_item.this.find(exp.AggFunc)
                        ):
                            _agg_out.append(_sel_item.alias)
                            _agg_ops.append(type(_sel_item.this).__name__.lower())
                            _inner_cols = [c.name for c in _sel_item.this.find_all(exp.Column)]
                            _agg_inputs.append(_inner_cols[0] if _inner_cols else UNRESOLVED)
                    if _gkeys or _agg_out:
                        self._subgraph.edges.append(AggregatesEdge(
                            id=self._next_edge_id("a"),
                            file=self.rel_path,
                            lineno=lineno,
                            source=join_node.id,
                            target=cte_node.id,
                            group_keys=_gkeys,
                            agg_ops=_agg_ops,
                            agg_inputs=_agg_inputs,
                            output_cols=_agg_out,
                        ))
                        # Narrow cte_node columns to group keys + aggregate outputs
                        cte_node.columns = _gkeys + [c for c in _agg_out if c and c != UNRESOLVED]
                        cte_nodes[cte_name] = cte_node
                        continue

                # cte_node output cols: projected_cols (passthrough) + new derived cols
                cte_node.columns = list(projected_cols) + [
                    cn for cn, *_ in computed_cols if cn
                ]
                cte_nodes[cte_name] = cte_node
                continue

            # SELECT without join or group — straight passthrough/projection
            if projected_cols or computed_cols:
                cte_node.columns = list(projected_cols) + [
                    cn for cn, *_ in computed_cols if cn
                ]
            else:
                # Fall back to source columns (SELECT * FROM cte)
                src_node = cte_nodes.get(from_name)
                if src_node and src_node.columns:
                    cte_node.columns = list(src_node.columns)

            cte_nodes[cte_name] = cte_node

        # ★ P13: Outer SELECT joins.  The CTE chain only handles joins INSIDE
        # CTEs; the outer SELECT (after ``WITH ... AS (...)``) can also have
        # its own ``LEFT JOIN ... ON ...`` clauses against bronze tables.
        # A real-world benchmark's result-assembly CTE has 4 such joins —
        # none of them surfaced as JoinsEdges before this branch.
        outer_select = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
        outer_joins = list(outer_select.find_all(exp.Join)) if outer_select else []
        # Filter to joins that are direct children of the outer SELECT (not
        # buried inside CTE expressions, which were already processed above).
        cte_join_ids = set()
        for cte_expr in with_clause.expressions:
            for j in cte_expr.this.find_all(exp.Join):
                cte_join_ids.add(id(j))
        outer_joins = [j for j in outer_joins if id(j) not in cte_join_ids]

        outer_from = parsed.find(exp.From)
        outer_from_tbl: Optional["exp.Table"] = None
        outer_left_source_id: Optional[str] = None
        outer_left_alias: Optional[str] = None
        if outer_from:
            outer_from_tbl = outer_from.find(exp.Table)
            if outer_from_tbl:
                outer_left_alias = (
                    getattr(outer_from_tbl, "alias", None) or outer_from_tbl.name
                )
                if outer_from_tbl.name in cte_nodes:
                    outer_left_source_id = cte_nodes[outer_from_tbl.name].id
                else:
                    # Outer FROM is a catalog table — register it as a Table
                    # stub and create a ReadsEdge so the JoinsEdges can hang
                    # off something real.
                    fqn_parts = [p for p in [outer_from_tbl.catalog, outer_from_tbl.db, outer_from_tbl.name] if p]
                    fqn = ".".join(fqn_parts) if fqn_parts else outer_from_tbl.name
                    outer_left_source_id = f"table:{fqn}"
                    table_stub = Table(id=outer_left_source_id, fqn=fqn, read_by=[self.rel_path])
                    self._subgraph.tables_referenced.append(table_stub)
                    self._subgraph.edges.append(ReadsEdge(
                        id=self._next_edge_id("r"),
                        file=self.rel_path,
                        lineno=lineno,
                        source=outer_left_source_id,
                        target=output_node.id,
                        projected_cols=[],
                        streaming=False,
                    ))

        # Emit a JoinsEdge per outer join.
        prev_target_id = outer_left_source_id
        for j in outer_joins:
            right_tbl = j.find(exp.Table)
            right_name = right_tbl.name if right_tbl else None
            right_alias = (
                (getattr(right_tbl, "alias", None) or right_name)
                if right_tbl else None
            )
            # Resolve right source: CTE node, or register a Table stub.
            if right_name and right_name in cte_nodes:
                right_source_id = cte_nodes[right_name].id
            elif right_tbl is not None:
                r_fqn_parts = [p for p in [right_tbl.catalog, right_tbl.db, right_tbl.name] if p]
                r_fqn = ".".join(r_fqn_parts) if r_fqn_parts else right_tbl.name
                right_source_id = f"table:{r_fqn}"
                # Register the table stub if not already referenced.
                if not any(t.id == right_source_id for t in self._subgraph.tables_referenced):
                    self._subgraph.tables_referenced.append(
                        Table(id=right_source_id, fqn=r_fqn, read_by=[self.rel_path])
                    )
            else:
                right_source_id = UNRESOLVED

            jside = str(j.args.get("side") or "").lower()
            jkind = str(j.args.get("kind") or "").lower()
            join_type = _parse_join_type(jside or jkind or "inner")

            jusing = j.args.get("using") or []
            j_keys: list[list[str]] = [[str(c), str(c)] for c in jusing]
            on_expr = j.args.get("on")
            if on_expr is not None:
                on_keys = _extract_sqlglot_join_keys(on_expr, outer_left_alias, right_alias)
                for lk, rk in on_keys:
                    j_keys.append([lk, rk])

            # Each join chains off the previous one's output: build a small
            # intermediate node so the join chain is linear (left,right -> tgt).
            j_target = _make_cte_node(f"join_{len(outer_joins)}")
            self._subgraph.edges.append(JoinsEdge(
                id=self._next_edge_id("j"),
                file=self.rel_path,
                lineno=lineno,
                left_source=prev_target_id or UNRESOLVED,
                right_source=right_source_id,
                target=j_target.id,
                join_type=join_type,
                join_keys=j_keys,
            ))
            prev_target_id = j_target.id

        # If outer joins existed, the output_node's "logical source" is the
        # final join target rather than the original outer FROM.  But the
        # downstream Projects edge from the CTE-output to output_node still
        # makes sense; we leave it in place for backward compat.

        if outer_from:
            outer_tbl = outer_from.find(exp.Table)
            if outer_tbl and outer_tbl.name in cte_nodes:
                source_cte = cte_nodes[outer_tbl.name]
                # Determine kept/removed columns from the outer SELECT.
                # Expand "*" to the source CTE's known column set when available
                # so downstream consumers see the actual columns flowing through.
                outer_selects = list(parsed.selects)
                is_star = any(isinstance(s, exp.Star) for s in outer_selects)
                if is_star and source_cte.columns:
                    kept = list(source_cte.columns)
                elif is_star:
                    kept = ["*"]
                else:
                    kept = [s.alias or str(s) for s in outer_selects]
                self._subgraph.edges.append(ProjectsEdge(
                    id=self._next_edge_id("p"),
                    file=self.rel_path,
                    lineno=lineno,
                    source=source_cte.id,
                    target=output_node.id,
                    removed_cols=[],
                    kept_cols=kept,
                ))
                # Propagate column set to the output DataFrame node
                output_node.columns = [c for c in kept if c != "*"]

                # Computed columns in the OUTER SELECT (after the WITH) — e.g.
                # `inv.on_hand / NULLIF(ds.sold,0) AS turnover_rate` — were not
                # given a DerivesEdge, so their column lineage was lost (the
                # ProjectsEdge only carries names). Emit one DerivesEdge per
                # aliased non-passthrough column, sourced from the final outer
                # join target (so the resolver can reach both join sides), with
                # source_cols extracted from the expression. General rule; the
                # resolver already chains the rest. (Pure `col AS alias` renames
                # also get a derive carrying the single source column.)
                _oderive_src = prev_target_id or source_cte.id
                for _s in outer_selects:
                    if not isinstance(_s, exp.Alias):
                        continue
                    _inner = _s.this
                    if isinstance(_inner, exp.Column):
                        _oscols = [_inner.name]
                    else:
                        _oscols = _sqlglot_expr_cols(_inner)
                    if not _oscols:
                        continue
                    self._subgraph.edges.append(DerivesEdge(
                        id=self._next_edge_id("d"),
                        file=self.rel_path,
                        lineno=lineno,
                        source=_oderive_src,
                        target=output_node.id,
                        output_col=_s.alias,
                        source_cols=_oscols,
                    ))

        return output_node

    def _emit_reads(
        self,
        var_name: str,
        lineno: int,
        base: ast.expr,
        ops: list[ChainOp],
        original_node: ast.expr,
    ) -> Optional[DataFrameNode]:
        """Emit a ReadsEdge and create the target DataFrameNode.

        If the resolved location looks like a file path (starts with ``/``,
        ``s3://``, ``abfss://``, ``dbfs:``, ``wasbs://``) it is registered as
        an ``ExternalSource``; otherwise as a catalog ``Table``.
        """
        location, streaming = self._resolve_read_fqn(base, ops, original_node)

        # Distinguish ExternalSource (file path) from Table (catalog FQN)
        if _looks_like_path(location):
            source_id = f"ext:{location}"
            ext_source = ExternalSource(
                id=source_id,
                location=location,
                format=self._detect_format(ops),
                read_by=[self.rel_path],
            )
            self._subgraph.external_sources_referenced.append(ext_source)
        else:
            source_id = f"table:{location}"
            table_stub = Table(id=source_id, fqn=location, read_by=[self.rel_path])
            self._subgraph.tables_referenced.append(table_stub)

        df_node = DataFrameNode(
            id=f"df:{self.rel_path}:{self._node_prefix(lineno)}:{var_name}",
            file=self.rel_path,
            var_name=var_name,
            lineno=lineno,
            columns=list(self._table_schema.get(location, [])),
        )

        edge_id = self._next_edge_id("r")
        self._subgraph.edges.append(ReadsEdge(
            id=edge_id,
            file=self.rel_path,
            lineno=lineno,
            source=source_id,
            target=df_node.id,
            streaming=streaming,
        ))
        return df_node

    def _detect_format(self, ops: list[ChainOp]) -> Optional[str]:
        """Extract format string from a read chain's .format() call."""
        ev = SafeEvaluator(self.symbols.symbols)
        for op in ops:
            if op.method == "format":
                return ev.resolve(op.positional(0)) if op.positional(0) else None
        return None

    def _resolve_read_fqn(
        self,
        base: ast.expr,
        ops: list[ChainOp],
        original_node: ast.expr,
    ) -> tuple[str, bool]:
        """Return (fqn_or_path, streaming) for a read chain."""
        ev = SafeEvaluator(self.symbols.symbols)

        # spark.table(fqn)
        if self._is_spark_table_call(original_node) and isinstance(original_node, ast.Call):
            fqn_arg = original_node.args[0] if original_node.args else None
            fqn = ev.resolve(fqn_arg) if fqn_arg else UNRESOLVED
            return fqn, False

        # Detect streaming from base: spark.readStream.xxx
        streaming = (
            isinstance(base, ast.Attribute) and base.attr == "readStream"
        )

        # Walk ops for load() or table()
        for op in ops:
            if op.method in ("load", "table", "insertInto"):
                fqn_arg = op.positional(0)
                return ev.resolve(fqn_arg) if fqn_arg else UNRESOLVED, streaming

        return UNRESOLVED, streaming

    # ------------------------------------------------------------------ #
    # Derives                                                              #
    # ------------------------------------------------------------------ #

    def _extract_window_spec_from_ops(self, ops: list[ChainOp]) -> Optional[WindowSpec]:
        """Resolve a Window.partitionBy/orderBy/... op chain into a WindowSpec.

        Recognised ops:
          - partitionBy(*cols) -> partition_cols
          - orderBy / sortBy(*cols) -> order_cols (with ' asc' / ' desc' suffix
            inferred from .asc()/.desc()/.asc_nulls_last()/.desc_nulls_last())
          - rowsBetween / rangeBetween(start, end) -> frame text

        Returns None if no partition / order / frame is recovered.
        """
        ev = SafeEvaluator(self.symbols.symbols)
        partition_cols: list[str] = []
        order_cols: list[str] = []
        frame: Optional[str] = None
        for op in ops:
            m = op.method
            if m == "partitionBy":
                for a in op.args:
                    val = ev.resolve(a)
                    if val and val != UNRESOLVED:
                        partition_cols.append(str(val))
                    else:
                        ref = self._col_ref_text(a)
                        if ref:
                            partition_cols.append(ref)
            elif m in ("orderBy", "sortBy"):
                for a in op.args:
                    order_cols.append(self._order_key_text(a, ev))
            elif m in ("rowsBetween", "rangeBetween"):
                kind = "ROWS" if m == "rowsBetween" else "RANGE"
                lo = ev.resolve(op.positional(0)) if op.positional(0) is not None else "?"
                hi = ev.resolve(op.positional(1)) if op.positional(1) is not None else "?"
                frame = f"{kind} BETWEEN {lo} AND {hi}"
        if not partition_cols and not order_cols and not frame:
            return None
        return WindowSpec(partition_cols=partition_cols, order_cols=order_cols, frame=frame)

    def _col_ref_text(self, node: ast.expr) -> Optional[str]:
        """Extract a column name from col('x') / F.col('x') / 'x', or unparse."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Call):
            fname = None
            if isinstance(node.func, ast.Name):
                fname = node.func.id
            elif isinstance(node.func, ast.Attribute):
                fname = node.func.attr
            if fname == "col" and node.args:
                first = node.args[0]
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    return first.value
        try:
            return ast.unparse(node)
        except Exception:
            return None

    def _order_key_text(self, node: ast.expr, ev: SafeEvaluator) -> str:
        """Return an order-key string with full ordering suffix.

        Captures the full PySpark ordering directive so impact-analysis
        questions about tie-break semantics (e.g. "which value wins among
        {P,S,B}?") can be answered without re-reading the source. Suffixes
        emitted (one of):
          - ``' asc'`` / ``' desc'``                       (no NULL handling)
          - ``' asc_nulls_first'`` / ``' asc_nulls_last'``
          - ``' desc_nulls_first'`` / ``' desc_nulls_last'``

        Note: bare ``.asc()`` defaults to ``asc_nulls_first`` and bare
        ``.desc()`` defaults to ``desc_nulls_last`` in Spark — we emit the
        explicit form only when the source code uses one of the explicit
        ``*_nulls_*`` variants; otherwise we emit just ``asc``/``desc``.
        """
        suffix = ""
        inner = node
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            if attr in ("asc", "asc_nulls_first", "asc_nulls_last",
                        "desc", "desc_nulls_first", "desc_nulls_last"):
                suffix = f" {attr}"
                inner = node.func.value
        ref = self._col_ref_text(inner)
        if not ref:
            val = ev.resolve(inner) if inner is not None else None
            ref = str(val) if val and val != UNRESOLVED else "?"
        return f"{ref}{suffix}"

    def _resolve_over_window(self, expr_arg) -> Optional[WindowSpec]:
        """Walk expr_arg for `.over(<name>)` and look <name> up in _window_specs."""
        if not self._window_specs or expr_arg is None:
            return None
        for sub in ast.walk(expr_arg):
            if not isinstance(sub, ast.Call):
                continue
            func = sub.func
            if not isinstance(func, ast.Attribute) or func.attr != "over" or not sub.args:
                continue
            arg0 = sub.args[0]
            if isinstance(arg0, ast.Name) and arg0.id in self._window_specs:
                return self._window_specs[arg0.id]
        return None

    def _extract_array_struct_fields(
        self,
        expr_arg: ast.expr,
    ) -> Optional[list[dict]]:
        """★ Array<struct> provenance: walk an
        ``array(struct(<f1>.alias("a"), <f2>.alias("b"), ...))`` or bare
        ``struct(...)`` expression and return per-field metadata.

        Returns ``None`` for non-struct shapes; caller falls back to the flat
        ``source_cols`` extraction path.

        Each entry:
          * ``field``: alias text (or ``"<unaliased_N>"`` if no .alias()).
          * ``kind``: ``"lit"`` for ``lit(...)`` constants, ``"col"`` for a
            single column reference, ``"expr"`` for everything else.
          * ``value``: literal text (lit) or short unparsed summary (expr) /
            None for col.
          * ``source_cols``: column refs found inside the field expression.
          * ``rule_logic``: when-chain ``[{cond, value}, ...]`` if applicable.
        """
        if not isinstance(expr_arg, ast.Call):
            return None

        # Detect the outer shape: array(struct(...)) OR struct(...).
        target_call: Optional[ast.Call] = None
        inner = expr_arg
        # Unwrap a single-arg array(...) wrapper, if present.
        fname: Optional[str] = None
        if isinstance(inner.func, ast.Name):
            fname = inner.func.id
        elif isinstance(inner.func, ast.Attribute):
            fname = inner.func.attr
        if fname == "array":
            if (
                inner.args
                and not any(isinstance(a, ast.Starred) for a in inner.args)
                and isinstance(inner.args[0], ast.Call)
            ):
                # array(struct1[, struct2, ...]) — use first struct arg as representative.
                # Fix #1: handles multi-struct arrays (e.g. identifiers col with 6 elements).
                inner = inner.args[0]
                fname = None
                if isinstance(inner.func, ast.Name):
                    fname = inner.func.id
                elif isinstance(inner.func, ast.Attribute):
                    fname = inner.func.attr
            elif (
                len(inner.args) == 1
                and isinstance(inner.args[0], ast.Starred)
                and isinstance(inner.args[0].value, ast.Name)
            ):
                # array(*var) — Fix #1: look up list variable built by for-loop appends.
                var_name = inner.args[0].value.id
                elems = self._list_vars.get(var_name, [])
                if elems:
                    # Find first struct(...) call anywhere inside the first element.
                    # Handles when(cond, struct(...)) shapes.
                    representative: Optional[ast.Call] = None
                    for sub in ast.walk(elems[0]):
                        if isinstance(sub, ast.Call):
                            sf = None
                            if isinstance(sub.func, ast.Name):
                                sf = sub.func.id
                            elif isinstance(sub.func, ast.Attribute):
                                sf = sub.func.attr
                            if sf == "struct":
                                representative = sub
                                break
                    if representative is not None:
                        inner = representative
                        fname = "struct"
        if fname == "struct":
            target_call = inner
        if target_call is None:
            return None

        def _strip_cast(node: ast.expr) -> ast.expr:
            """Peel off .cast(...).alias() and similar inner-method chains
            *except* the outermost .alias(), which is handled separately."""
            cur = node
            while (
                isinstance(cur, ast.Call)
                and isinstance(cur.func, ast.Attribute)
                and cur.func.attr == "cast"
            ):
                cur = cur.func.value
            return cur

        def _classify(node: ast.expr) -> dict:
            """Classify the inner (post-alias-strip, post-cast-strip) node."""
            # lit(X) — literal placeholder or string value.
            if isinstance(node, ast.Call):
                cf = None
                if isinstance(node.func, ast.Name):
                    cf = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    cf = node.func.attr
                if cf == "lit" and node.args:
                    val_arg = node.args[0]
                    try:
                        return {
                            "kind": "lit",
                            "value": ast.unparse(val_arg),
                            "source_cols": [],
                        }
                    except Exception:
                        return {"kind": "lit", "value": None, "source_cols": []}
                if cf == "col" and node.args:
                    first = node.args[0]
                    if isinstance(first, ast.Constant) and isinstance(first.value, str):
                        return {
                            "kind": "col",
                            "value": None,
                            "source_cols": [first.value],
                        }
            # Everything else: classify as expr.  If it happens to be a
            # when()-chain (or contains one nested inside concat_ws/coalesce/
            # cast/etc), attach rule_logic.
            try:
                expr_text = ast.unparse(node)
            except Exception:
                expr_text = "<?>"
            # ★ Q7 fix: bump value truncation from 80 -> 250 chars. Real-world
            # struct fields like `policy_id = concat_ws('_', coalesce(member,
            # 'unknown'), coalesce(when(prod=='A','A1').when(...).otherwise(
            # 'unknown'), 'unknown'))` exceed 80 chars and lose the product
            # mapping. 250 chars captures the structural shape; nested
            # when-chains still get their own rule_logic block below.
            if len(expr_text) > 250:
                expr_text = expr_text[:247] + "..."
            cols = self._collect_col_refs(node)
            entry: dict = {
                "kind": "expr",
                "value": expr_text,
                "source_cols": cols,
            }
            # First try treating the WHOLE node as a when-chain.
            rl = _extract_when_logic(
                node, self._column_exprs,
                predicate_dicts=self._predicate_dicts,
            )
            # ★ Q7 fix: if the outer node isn't a when-chain, but is a
            # wrapper like concat_ws/coalesce/cast that holds a when-chain
            # in one of its arguments, walk and surface the inner chain.
            # This is what makes `policy_id` answerable — its
            # when().when().otherwise() product mapping lives inside the
            # SECOND coalesce arg of a concat_ws.
            if rl is None and isinstance(node, ast.Call):
                for sub in ast.walk(node):
                    if sub is node:
                        continue
                    inner_rl = _extract_when_logic(
                        sub, self._column_exprs,
                        predicate_dicts=self._predicate_dicts,
                    )
                    if inner_rl:
                        rl = inner_rl
                        break
            if rl:
                entry["rule_logic"] = rl
            return entry

        result: list[dict] = []
        for i, arg in enumerate(target_call.args):
            # Strip the outermost .alias("<name>") to get the field name.
            field_name: str = f"<unaliased_{i}>"
            inner_node: ast.expr = arg
            if (
                isinstance(arg, ast.Call)
                and isinstance(arg.func, ast.Attribute)
                and arg.func.attr == "alias"
                and arg.args
                and isinstance(arg.args[0], ast.Constant)
                and isinstance(arg.args[0].value, str)
            ):
                field_name = arg.args[0].value
                inner_node = arg.func.value

            inner_node = _strip_cast(inner_node)
            classified = _classify(inner_node)
            classified["field"] = field_name
            # Reorder keys for readability in JSON.
            result.append({
                "field": classified["field"],
                "kind": classified["kind"],
                "value": classified.get("value"),
                "source_cols": classified.get("source_cols", []),
                **({"rule_logic": classified["rule_logic"]} if "rule_logic" in classified else {}),
            })

        return result if result else None

    def _emit_derives_op(
        self,
        op: ChainOp,
        source_id: str,
        target_id: str,
    ) -> None:
        """Emit one DerivesEdge for a withColumn / withColumnRenamed / select op."""
        ev = SafeEvaluator(self.symbols.symbols)

        if op.method == "withColumn":
            col_name = ev.resolve(op.positional(0)) if op.positional(0) else UNRESOLVED
            expr_arg = op.positional(1)
            expr_node = self._intern_expr(expr_arg)
            # Lift the column references off the Expression onto the edge so
            # the compact graph sees which input columns feed `col_name`.
            # The expansion-aware collector walks through any registered
            # Column-typed Python variables referenced in the expression.
            source_cols = list(expr_node.referenced_cols) if expr_node else []
            if not source_cols and expr_arg is not None:
                source_cols = self._collect_col_refs(expr_arg)
            # ★ Q3 v3: F.expr("...") string-arg column-ref resolution. A
            # real-world pipeline uses ``F.expr("filter(multi_raw, x -> x is not null)")``
            # to filter the array-of-predicates. The SQL string mentions
            # ``multi_raw`` but the AST visitor sees only a string literal —
            # so source_cols stays empty and the link from
            # ``multiple_claim_subcategories`` back to its predicate-driven
            # source is lost. Scan the SQL string for identifiers that match
            # known column-typed Python variables and add them as source cols.
            expr_string_refs = _extract_column_refs_from_expr_string(
                expr_arg, self._column_exprs
            )
            if expr_string_refs:
                # de-dupe preserving order
                existing = set(source_cols)
                for ref in expr_string_refs:
                    if ref not in existing:
                        source_cols.append(ref)
                        existing.add(ref)
            # Extract rule logic from when() chains (P6).  Works whether
            # expr_arg is the literal when()-chain AST *or* a Name reference
            # to a Column-typed variable holding the chain.
            rule_logic = _extract_when_logic(
                expr_arg, self._column_exprs,
                predicate_dicts=self._predicate_dicts,
            ) if expr_arg is not None else None
            # ★ Q3 v3: if no rule_logic on this edge but one of the F.expr
            # string refs has rule_logic in column_rules, surface it here so
            # downstream columns inherit the shared-predicate chain.
            if rule_logic is None and expr_string_refs:
                for ref in expr_string_refs:
                    ref_expr = self._column_exprs.get(ref)
                    if ref_expr is None:
                        continue
                    inherited = _extract_when_logic(
                        ref_expr, self._column_exprs,
                        predicate_dicts=self._predicate_dicts,
                    ) or _extract_array_of_whens_logic(
                        ref_expr, self._predicate_dicts,
                    )
                    if inherited:
                        rule_logic = inherited
                        break
            # P9: resolve `.over(<window_var>)` against the per-file Window registry
            window_spec = self._resolve_over_window(expr_arg) if expr_arg is not None else None
            # Fix #5: when a window function is found, source_cols = PARTITION BY ∪ ORDER BY
            # (bare names, stripped of " asc"/" desc") ∪ any operand cols already found.
            # This replaces the opaque "all cols in scope" that _collect_col_refs returns
            # for a .over(w) expression, and ensures held-out window questions get correct
            # provenance (e.g. HQ5-small: ntile → revenue_quartile → lifetime_revenue).
            if window_spec is not None:
                win_src: list[str] = list(window_spec.partition_cols)
                for _oc in window_spec.order_cols:
                    _bare = _oc.split()[0]
                    if _bare not in win_src:
                        win_src.append(_bare)
                for _c in source_cols:
                    if _c not in win_src:
                        win_src.append(_c)
                source_cols = win_src
            # ★ Array<struct> field provenance.
            struct_fields = self._extract_array_struct_fields(expr_arg) if expr_arg is not None else None
            # ★ column_var: record which named Column variable drives this output col.
            # Two patterns:
            #   (a) bare Name: withColumn('x', my_var) → column_var = 'my_var'
            #   (b) F.expr string: withColumn('x', F.expr('filter(my_var, ...)'))
            #       → column_var = 'my_var' (the first column_exprs ref in the string)
            # This lets the compact DSL emit cv=<var> so the model can trace
            # @RULES entry → output column without naming-confusion.
            import ast as _ast
            column_var = None
            if expr_arg is not None:
                if isinstance(expr_arg, _ast.Name) and expr_arg.id in self._column_exprs:
                    column_var = expr_arg.id
                elif (
                    isinstance(expr_arg, _ast.Call)
                    and isinstance(getattr(expr_arg, "func", None), _ast.Attribute)
                    and expr_arg.func.attr == "expr"  # type: ignore[union-attr]
                    and isinstance(getattr(expr_arg, "args", [None])[0], _ast.Constant)
                    and isinstance(expr_arg.args[0].value, str)
                ):
                    # F.expr("filter(my_var, x -> ...)") — look for first column_exprs ref
                    import re
                    sql_str = expr_arg.args[0].value
                    for m in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", sql_str):
                        if m.group(1) in self._column_exprs:
                            column_var = m.group(1)
                            break
            edge_id = self._next_edge_id("d")
            self._subgraph.edges.append(DerivesEdge(
                id=edge_id,
                file=self.rel_path,
                lineno=op.lineno,
                source=source_id,
                target=target_id,
                output_col=col_name,
                source_cols=source_cols,
                expr_id=expr_node.id if expr_node else None,
                rule_logic=rule_logic,
                window_spec=window_spec,
                struct_fields=struct_fields,
                column_var=column_var,
            ))

        elif op.method == "withColumnRenamed":
            old_col = ev.resolve(op.positional(0)) if op.positional(0) else UNRESOLVED
            new_col = ev.resolve(op.positional(1)) if op.positional(1) else UNRESOLVED
            expr_id_val = self._intern_expr_text(f"rename({old_col} → {new_col})", [old_col])
            edge_id = self._next_edge_id("d")
            self._subgraph.edges.append(DerivesEdge(
                id=edge_id,
                file=self.rel_path,
                lineno=op.lineno,
                source=source_id,
                target=target_id,
                output_col=new_col,
                source_cols=[old_col],
                expr_id=expr_id_val,
            ))

        elif op.method == "select":
            # Fix #8: capture NEW columns introduced by a `.select(...)` that are
            # not plain column passthroughs/renames — i.e. literals (F.lit),
            # runtime functions (F.current_timestamp()), or computed expressions,
            # each written as `<expr>.alias("name")`. Bare `col("x")` and
            # `col("x").alias("y")` passthroughs are intentionally NOT emitted
            # (that would add one edge per projected column — e.g. 85 for the
            # clinical canonical projection — with no lineage value). The v1.1
            # sealed test showed these audit/ETL columns (etl_loaded_at,
            # etl_source_job, source_system) were dropped entirely (clinical
            # TQ11: all 4 DSL answers scored 0). Generalizable: any select that
            # mints a constant/derived column now surfaces it.
            for sel in op.args:
                if not (
                    isinstance(sel, ast.Call)
                    and isinstance(sel.func, ast.Attribute)
                    and sel.func.attr == "alias"
                    and sel.args
                ):
                    continue
                alias_name = ev.resolve(sel.args[0]) if sel.args[0] else UNRESOLVED
                if not alias_name or alias_name == UNRESOLVED:
                    continue
                receiver = sel.func.value  # the expression being aliased
                # Plain column reference: col("x").alias("y").
                if (
                    isinstance(receiver, ast.Call)
                    and _extract_func_name(receiver.func).split(".")[-1] == "col"
                ):
                    col_arg = receiver.args[0] if receiver.args else None
                    src_col = ev.resolve(col_arg) if col_arg is not None else None
                    # Pure passthrough col("x").alias("x") -> no edge (emitting one
                    # per projected column adds noise without lineage value). But a
                    # RENAME col("id").alias("user_id") is a real lineage edge —
                    # output user_id derives from id — and must be emitted (the
                    # dbdemos silver selects rename id->user_id / id->order_id).
                    if src_col and src_col != UNRESOLVED and src_col != alias_name:
                        self._subgraph.edges.append(DerivesEdge(
                            id=self._next_edge_id("d"),
                            file=self.rel_path,
                            lineno=op.lineno,
                            source=source_id,
                            target=target_id,
                            output_col=alias_name,
                            source_cols=[src_col],
                            expr_id=self._intern_expr_text(
                                f"rename({src_col} -> {alias_name})", [src_col]),
                        ))
                    continue
                sel_expr_node = self._intern_expr(receiver)
                sel_src = list(sel_expr_node.referenced_cols) if sel_expr_node else []
                if not sel_src:
                    sel_src = self._collect_col_refs(receiver)
                self._subgraph.edges.append(DerivesEdge(
                    id=self._next_edge_id("d"),
                    file=self.rel_path,
                    lineno=op.lineno,
                    source=source_id,
                    target=target_id,
                    output_col=alias_name,
                    source_cols=sel_src,
                    expr_id=sel_expr_node.id if sel_expr_node else None,
                ))

    # ------------------------------------------------------------------ #
    # Projects                                                             #
    # ------------------------------------------------------------------ #

    def _emit_projects(
        self,
        group: list[ChainOp],
        source_id: str,
        target_id: str,
    ) -> None:
        ev = SafeEvaluator(self.symbols.symbols)
        removed: list[str] = []
        for op in group:
            for arg in op.args:
                removed.append(ev.resolve(arg))
        lineno = group[0].lineno
        edge_id = self._next_edge_id("p")
        self._subgraph.edges.append(ProjectsEdge(
            id=edge_id,
            file=self.rel_path,
            lineno=lineno,
            source=source_id,
            target=target_id,
            removed_cols=removed,
        ))

    # ------------------------------------------------------------------ #
    # Filters                                                              #
    # ------------------------------------------------------------------ #

    def _emit_filters(
        self,
        op: ChainOp,
        source_id: str,
        target_id: str,
    ) -> None:
        pred_arg = op.positional(0)
        expr_node = self._intern_expr(pred_arg)
        # Lift referenced_cols off the Expression so downstream consumers
        # (compact graph, LLM context) can see WHICH columns the filter touches
        # without needing to chase the expr_id back to its Expression node.
        refs = list(expr_node.referenced_cols) if expr_node else []
        edge_id = self._next_edge_id("f")
        self._subgraph.edges.append(FiltersEdge(
            id=edge_id,
            file=self.rel_path,
            lineno=op.lineno,
            source=source_id,
            target=target_id,
            predicate_id=expr_node.id if expr_node else None,
            referenced_cols=refs,
        ))

    # ------------------------------------------------------------------ #
    # Joins                                                                #
    # ------------------------------------------------------------------ #

    def _emit_join(
        self,
        op: ChainOp,
        left_source: str,
        target_id: str,
    ) -> None:
        """Emit a JoinsEdge.

        The left source is the DataFrame the ``.join()`` is called on.
        The right source is the first positional argument.
        """
        ev = SafeEvaluator(self.symbols.symbols)

        # Right source: first positional arg (another DataFrame variable)
        right_arg = op.positional(0)
        right_id = self._resolve_base_to_id(right_arg) if right_arg else None
        if right_id is None:
            right_id = UNRESOLVED

        # Extract inline projection if right arg is df.select("col1", "col2", ...)
        # This captures the narrowing projection so column-propagation only carries
        # the selected columns into the join target (not all columns of right_source).
        right_projected_cols = _extract_inline_select_cols(right_arg)

        # Join condition: second positional or `on=` kwarg
        on_arg = op.get_kwarg("on") or op.positional(1)

        # Join type: third positional or `how=` kwarg
        how_arg = op.get_kwarg("how") or op.positional(2)
        how_str = ev.resolve(how_arg) if how_arg else "inner"
        join_type = _parse_join_type(how_str)

        # Parse join keys from on= condition
        join_keys = _extract_join_keys(on_arg)

        edge_id = self._next_edge_id("j")
        self._subgraph.edges.append(JoinsEdge(
            id=edge_id,
            file=self.rel_path,
            lineno=op.lineno,
            left_source=left_source,
            right_source=right_id,
            target=target_id,
            join_type=join_type,
            join_keys=join_keys,
            right_projected_cols=right_projected_cols,
        ))

    # ------------------------------------------------------------------ #
    # Aggregates                                                           #
    # ------------------------------------------------------------------ #

    def _emit_aggregates(
        self,
        group: list[ChainOp],
        source_id: str,
        target_id: str,
    ) -> None:
        """Emit an AggregatesEdge from a groupBy().agg() pair."""
        ev = SafeEvaluator(self.symbols.symbols)

        group_keys: list[str] = []
        agg_ops: list[str] = []
        agg_inputs: list[str] = []
        output_cols: list[str] = []
        dynamic = False
        dynamic_note: Optional[str] = None

        for op in group:
            if op.method == "groupBy":
                for arg in op.args:
                    group_keys.append(ev.resolve(arg))

            elif op.method == "agg":
                for arg in op.args:
                    # Star-unpack: *aggregations → dynamic
                    if isinstance(arg, ast.Starred):
                        dynamic = True
                        # ★ P17: include any os.environ.get() env-var names found
                        # in this file so the LLM can identify the config source.
                        if self._environ_gets:
                            env_hint = "; controlled by env-var(s): " + ", ".join(
                                f"${v}" for v in self._environ_gets
                            )
                        else:
                            env_hint = ""
                        dynamic_note = (
                            "agg list star-unpacked from a runtime-built variable; "
                            f"column list cannot be enumerated statically{env_hint}"
                        )
                        agg_ops.append(UNRESOLVED)
                        agg_inputs.append(UNRESOLVED)
                        output_cols.append(UNRESOLVED)
                    else:
                        fn, inp, alias = _parse_agg_call(arg, ev)
                        agg_ops.append(fn)
                        agg_inputs.append(inp)
                        output_cols.append(alias)

        lineno = group[0].lineno
        edge_id = self._next_edge_id("a")
        self._subgraph.edges.append(AggregatesEdge(
            id=edge_id,
            file=self.rel_path,
            lineno=lineno,
            source=source_id,
            target=target_id,
            group_keys=group_keys,
            agg_ops=agg_ops,
            agg_inputs=agg_inputs,
            output_cols=output_cols,
            dynamic=dynamic,
            dynamic_note=dynamic_note if dynamic else None,
        ))

        if dynamic:
            self._warn(
                lineno,
                "dynamic-aggregation",
                f"agg list contains star-unpacked runtime variable in {self.rel_path}:{lineno}",
            )

    # ------------------------------------------------------------------ #
    # Writes                                                               #
    # ------------------------------------------------------------------ #

    def _emit_writes_from_chain(
        self,
        source_df_id: str,
        ops: list[ChainOp],
        lineno: int,
    ) -> None:
        """Emit a WritesEdge from a write chain."""
        ev = SafeEvaluator(self.symbols.symbols)
        write_op = find_write_op(ops)
        if write_op is None:
            return

        # Resolve target FQN
        fqn_arg = write_op.positional(0)
        fqn = ev.resolve(fqn_arg) if fqn_arg else UNRESOLVED

        # Resolve mode
        mode = WriteMode.UNRESOLVED
        for op in ops:
            if op.method == "mode":
                mode_str = ev.resolve(op.positional(0)) if op.positional(0) else UNRESOLVED
                mode = _parse_write_mode(mode_str)

        # Resolve format
        fmt: Optional[str] = None
        for op in ops:
            if op.method == "format":
                fmt = ev.resolve(op.positional(0)) if op.positional(0) else None

        streaming = any(op.method == "writeStream" for op in ops)
        # Streaming writes have no explicit .mode() call; the semantic default is append
        if streaming and mode == WriteMode.UNRESOLVED:
            mode = WriteMode.APPEND

        # Fix #7: capture .partitionBy(*cols) on the write chain. Mirrors the
        # Window.partitionBy extraction. Without this the Writes edge silently
        # drops the table's physical partitioning (the v1.1 test set showed the
        # DSL Write edge had no partition info — clinical/medium partitionBy Qs).
        partition_cols: list[str] = []
        for op in ops:
            if op.method == "partitionBy":
                for a in op.args:
                    val = ev.resolve(a)
                    if val and val != UNRESOLVED:
                        partition_cols.append(str(val))
                    else:
                        ref = self._col_ref_text(a)
                        if ref:
                            partition_cols.append(ref)

        table_id = f"table:{fqn}"
        table_stub = Table(id=table_id, fqn=fqn, written_by=[self.rel_path])
        self._subgraph.tables_referenced.append(table_stub)

        edge_id = self._next_edge_id("w")
        self._subgraph.edges.append(WritesEdge(
            id=edge_id,
            file=self.rel_path,
            lineno=lineno,
            source=source_df_id,
            target=table_id,
            mode=mode,
            format=fmt,
            streaming=streaming,
            partition_cols=partition_cols,
        ))

    # ------------------------------------------------------------------ #
    # Registered helper calls                                              #
    # ------------------------------------------------------------------ #

    def _try_helper_call(
        self,
        var_name: str,
        lineno: int,
        value: ast.expr,
    ) -> Optional[DataFrameNode]:
        """Check if value is a call to a registered helper; emit edge if so."""
        if not isinstance(value, ast.Call):
            return None

        func_name = _extract_func_name(value.func)
        spec = self.helpers.get(func_name)
        if spec is None:
            return None

        # Resolve the source DataFrame (first positional arg)
        df_arg = value.args[0] if value.args else None
        source_id = self._resolve_base_to_id(df_arg) if df_arg else None
        if source_id is None:
            return None

        target_node = DataFrameNode(
            id=f"df:{self.rel_path}:{self._node_prefix(lineno)}:{var_name}",
            file=self.rel_path,
            var_name=var_name,
            lineno=lineno,
        )

        # Always propagate input columns onto the result node so downstream
        # transforms see the full column set. Passthrough preserves names
        # verbatim; suffix_rename remaps them below.
        source_node_lookup = self.tracker.get_node_by_id(source_id)
        if source_node_lookup and source_node_lookup.columns:
            if spec.kind == "passthrough":
                target_node.columns = list(source_node_lookup.columns)

        if spec.kind == "passthrough":
            edge_id = self._next_edge_id("o")
            self._subgraph.edges.append(OpaqueTransformEdge(
                id=edge_id,
                file=self.rel_path,
                lineno=lineno,
                source=source_id,
                target=target_node.id,
                operator=spec.qualified_name,
                opaque_kind=OpaqueKind.PASSTHROUGH,
                is_passthrough=True,
            ))

        elif spec.kind == "suffix_rename":
            ev = SafeEvaluator(self.symbols.symbols)
            suffix_idx = spec.suffix_arg if spec.suffix_arg is not None else 1
            suffix_arg = value.args[suffix_idx] if suffix_idx < len(value.args) else None
            suffix = ev.resolve(suffix_arg) if suffix_arg else UNRESOLVED

            # Look up source columns from the source DataFrameNode
            source_node = self.tracker.get_node_by_id(source_id)
            cols = source_node.columns if source_node else []
            renamed_cols: list[str] = []
            for col in cols:
                out_col = f"{col}{suffix}" if not suffix.startswith(UNRESOLVED) else UNRESOLVED
                renamed_cols.append(out_col)
                edge_id = self._next_edge_id("d")
                self._subgraph.edges.append(DerivesEdge(
                    id=edge_id,
                    file=self.rel_path,
                    lineno=lineno,
                    source=source_id,
                    target=target_node.id,
                    output_col=out_col,
                    source_cols=[col],
                ))
            target_node.columns = renamed_cols

        return target_node

    # ------------------------------------------------------------------ #
    # Unregistered imported function calls (opaque fallback)              #
    # ------------------------------------------------------------------ #

    def _try_opaque_call(
        self,
        var_name: str,
        lineno: int,
        value: ast.expr,
    ) -> Optional[DataFrameNode]:
        """Detect unregistered imported-function calls that return DataFrames.

        Only fires for module-qualified imports like
        ``utils.event_parsers.parse_event_payload(df)`` — NOT for DataFrame
        method calls like ``df.join(...)`` or ``df.withColumn(...)``.

        Also handles the **builder-method pattern** ``ClassName(...).apply(df)``
        / ``Stage(cfg).transform(df)`` where ``ClassName`` is imported and the
        method's first argument is a known DataFrame. Without this, custom
        pipeline stages (e.g. ``MatchSchema(ref_table_name=table).apply(df)``)
        produce no edge and leave the target variable invisible to downstream
        sink/save calls.
        """
        if not isinstance(value, ast.Call):
            return None

        # ---- Builder pattern: ClassName(...).method(df) ---------------------
        if (
            isinstance(value.func, ast.Attribute)
            and isinstance(value.func.value, ast.Call)
            and isinstance(value.func.value.func, ast.Name)
        ):
            class_name = value.func.value.func.id
            method = value.func.attr
            if class_name in self._imported_fqn:
                # Some pipeline stages take config first and the DataFrame
                # second (e.g. ``MatchSchema(ref_table_name=t).apply(df)`` —
                # one arg only — but also ``apply_dq_rules(rules, df)``).
                # Scan every positional arg for a known DataFrame.
                source_id = self._first_df_arg(value.args)
                if source_id is not None:
                    target_node = DataFrameNode(
                        id=f"df:{self.rel_path}:{self._node_prefix(lineno)}:{var_name}",
                        file=self.rel_path,
                        var_name=var_name,
                        lineno=lineno,
                    )
                    # Conservative: propagate input columns through the stage.
                    # Domain-specific stages like MatchSchema or ApplyContract
                    # typically preserve the column set; if a future stage
                    # rewrites columns we'd misreport. Register as passthrough
                    # so the silver table inherits the upstream column list.
                    source_node = self.tracker.get_node_by_id(source_id)
                    if source_node and source_node.columns:
                        target_node.columns = list(source_node.columns)

                    operator = f"{self._imported_fqn.get(class_name, class_name)}.{method}"
                    edge_id = self._next_edge_id("o")
                    self._subgraph.edges.append(OpaqueTransformEdge(
                        id=edge_id,
                        file=self.rel_path,
                        lineno=lineno,
                        source=source_id,
                        target=target_node.id,
                        operator=operator,
                        opaque_kind=OpaqueKind.PASSTHROUGH,
                        is_passthrough=True,
                    ))
                    return target_node

        func_name = _extract_func_name(value.func)
        if not func_name:
            return None

        # Qualified call: utils.event_parsers.parse_event_payload(df)
        is_qualified = "." in func_name
        # Bare imported name: parse_event_payload(df)
        is_imported_bare = (not is_qualified) and (func_name in self._imported_fqn)

        if not is_qualified and not is_imported_bare:
            return None  # looks like a built-in or local def — skip

        # If the root of a qualified call is a known DataFrame variable, it's a
        # method call (df.join, df.filter, etc.) — not an opaque import call.
        if is_qualified:
            root_name = func_name.split(".")[0]
            if self.tracker.is_known(root_name):
                return None

        # Find a DataFrame in any positional arg slot. Some helpers take the
        # DataFrame second, e.g. ``apply_dq_rules(rules_str, df)``; restricting
        # to args[0] would miss the lineage entirely.
        source_id = self._first_df_arg(value.args)
        if source_id is None:
            return None

        target_node = DataFrameNode(
            id=f"df:{self.rel_path}:{self._node_prefix(lineno)}:{var_name}",
            file=self.rel_path,
            var_name=var_name,
            lineno=lineno,
        )

        # Resolve the operator to a fully-qualified name when imported bare
        operator = self._imported_fqn.get(func_name, func_name) if not is_qualified else func_name

        edge_id = self._next_edge_id("o")
        self._subgraph.edges.append(OpaqueTransformEdge(
            id=edge_id,
            file=self.rel_path,
            lineno=lineno,
            source=source_id,
            target=target_node.id,
            operator=operator,
            opaque_kind=OpaqueKind.UNKNOWN,
            is_passthrough=False,
        ))

        self._warn(
            lineno,
            "opaque-call-fallback",
            f"Unregistered function '{operator}' called with DataFrame arg; "
            f"column-set change across this call is unknown.",
        )

        return target_node

    # ------------------------------------------------------------------ #
    # Sink patterns (DeltaMergeSink)                                       #
    # ------------------------------------------------------------------ #

    def _try_sink_instantiation(
        self,
        var_name: str,
        lineno: int,
        value: ast.expr,
    ) -> bool:
        """Detect  sink = SinkClass(target_table=..., mode=...) patterns.

        If *value* is a call to a registered sink class, record the kwargs
        in ``_pending_sinks`` and return True.  Returns False otherwise.
        """
        if not isinstance(value, ast.Call):
            return False

        # The function must be a simple Name (not a method call)
        if not isinstance(value.func, ast.Name):
            return False

        class_name = value.func.id
        spec = self.sinks.get(class_name)
        if spec is None:
            return False

        ev = SafeEvaluator(self.symbols.symbols)

        # Extract target table and mode from kwargs
        kwargs: dict[str, str] = {}
        extra_sink_kwargs: dict[str, str] = {}
        for kw in value.keywords:
            if kw.arg == spec.target_kwarg:
                kwargs["target"] = ev.resolve(kw.value)
            elif kw.arg == spec.mode_kwarg:
                kwargs["mode"] = ev.resolve(kw.value)
            elif spec.merge_keys_kwarg and kw.arg == spec.merge_keys_kwarg:
                # merge_keys is a list literal — keep as list, not comma-string
                if isinstance(kw.value, ast.List):
                    kwargs["merge_keys"] = [
                        ev.resolve(elt) for elt in kw.value.elts
                    ]
            elif kw.arg:
                # ★ Q6 fix: surface other sink kwargs (e.g. enable_leap_column,
                # strict, audit_table) so the model can describe non-core sink
                # behaviour. Stored as unparse text to keep literals readable.
                try:
                    extra_sink_kwargs[kw.arg] = ast.unparse(kw.value)
                except Exception:
                    extra_sink_kwargs[kw.arg] = "<?>"

        kwargs.setdefault("target", UNRESOLVED)
        kwargs.setdefault("mode", spec.default_mode)
        kwargs["sink_class"] = class_name
        kwargs["sink_kwargs"] = extra_sink_kwargs

        self._pending_sinks[var_name] = kwargs
        return True

    def _emit_sink_writes(
        self,
        sink_var: str,
        df_source_id: str,
        lineno: int,
    ) -> None:
        """Emit a WritesEdge for a pending sink.run(df) call."""
        info = self._pending_sinks.get(sink_var)
        if info is None:
            return

        fqn = info.get("target", UNRESOLVED)
        mode_str = info.get("mode", UNRESOLVED)
        sink_class = info.get("sink_class")
        merge_keys = info.get("merge_keys") or []
        sink_kwargs = info.get("sink_kwargs") or {}

        table_id = f"table:{fqn}"
        table_stub = Table(id=table_id, fqn=fqn, written_by=[self.rel_path])
        self._subgraph.tables_referenced.append(table_stub)

        edge_id = self._next_edge_id("w")
        self._subgraph.edges.append(WritesEdge(
            id=edge_id,
            file=self.rel_path,
            lineno=lineno,
            source=df_source_id,
            target=table_id,
            mode=_parse_write_mode(mode_str),
            sink_class=sink_class,
            merge_keys=merge_keys,
            sink_kwargs=sink_kwargs,
        ))

    def _try_inline_sink_call(self, value: ast.Call, lineno: int) -> bool:
        """Detect ``SinkClass(target_table=..., ...).save(df)`` or ``.run(df)``.

        Some pipelines never assign the sink to a variable and instead chain
        the constructor and the invocation in one expression:

            MergeSink(target_table=table, key_columns=["id"], ...).save(silver_df)

        Without this hook, the chain walker treats the leading
        ``MergeSink(...)`` as the chain base, fails to resolve it to a
        DataFrame, and emits nothing — so the target table never appears in
        the graph. Here we recognise the receiver as a registered sink
        constructor, pull the target/mode/merge-keys out of its kwargs, and
        emit a ``WritesEdge`` directly.
        """
        if not isinstance(value.func, ast.Attribute):
            return False
        method = value.func.attr
        if method not in ("save", "run"):
            return False
        # Receiver must be a constructor call SinkClass(...)
        receiver = value.func.value
        if not isinstance(receiver, ast.Call):
            return False
        if not isinstance(receiver.func, ast.Name):
            return False
        class_name = receiver.func.id
        spec = self.sinks.get(class_name)
        if spec is None:
            return False

        ev = SafeEvaluator(self.symbols.symbols)

        # Pull configuration out of the constructor's kwargs
        fqn = UNRESOLVED
        mode_str = spec.default_mode
        merge_keys: list[str] = []
        sink_kwargs: dict[str, str] = {}
        for kw in receiver.keywords:
            if kw.arg == spec.target_kwarg:
                fqn = ev.resolve(kw.value)
            elif kw.arg == spec.mode_kwarg:
                mode_str = ev.resolve(kw.value)
            elif spec.merge_keys_kwarg and kw.arg == spec.merge_keys_kwarg:
                if isinstance(kw.value, ast.List):
                    merge_keys = [ev.resolve(elt) for elt in kw.value.elts]
            elif kw.arg:
                # ★ Q6 fix: capture other sink kwargs as readable text.
                try:
                    sink_kwargs[kw.arg] = ast.unparse(kw.value)
                except Exception:
                    sink_kwargs[kw.arg] = "<?>"

        # Resolve the DataFrame argument
        df_arg = value.args[0] if value.args else None
        if df_arg is None:
            return False
        df_source_id = self._resolve_base_to_id(df_arg)
        if df_source_id is None:
            return False

        table_id = f"table:{fqn}"
        table_stub = Table(id=table_id, fqn=fqn, written_by=[self.rel_path])
        self._subgraph.tables_referenced.append(table_stub)

        edge_id = self._next_edge_id("w")
        self._subgraph.edges.append(WritesEdge(
            id=edge_id,
            file=self.rel_path,
            lineno=lineno,
            source=df_source_id,
            target=table_id,
            mode=_parse_write_mode(mode_str),
            sink_class=class_name,
            merge_keys=merge_keys,
            sink_kwargs=sink_kwargs,
        ))
        return True

    def _emit_inline_spark_table_read(
        self,
        table_op: "ChainOp",
        lineno: int,
    ) -> str:
        """Emit an anonymous DataFrameNode + ReadsEdge for ``spark.table("fqn")``
        when it appears as the first op of a longer transform chain (P4).

        *table_op* is the ``ChainOp`` produced by ``unroll_chain`` for the
        ``spark.table("fqn")`` method call — i.e. ``ops[0]`` for assignments
        whose base resolves to ``Name("spark")`` with ``ops[0].method=="table"``.

        Creates an anonymous DataFrameNode that holds the raw table read and
        registers a ``ReadsEdge`` from the catalog table to that node.  Returns
        the anonymous node's ID so the caller can feed it as ``source_id`` into
        ``_process_chain`` for the remaining transform ops.
        """
        ev = SafeEvaluator(self.symbols.symbols)
        fqn_arg = table_op.positional(0)
        fqn = ev.resolve(fqn_arg) if fqn_arg else UNRESOLVED

        source_table_id = f"table:{fqn}"
        table_stub = Table(id=source_table_id, fqn=fqn, read_by=[self.rel_path])
        self._subgraph.tables_referenced.append(table_stub)

        anon_id = self.tracker.make_anon_id(self.rel_path, self._node_prefix(lineno))
        anon_node = DataFrameNode(
            id=anon_id,
            file=self.rel_path,
            var_name=None,
            lineno=lineno,
            columns=list(self._table_schema.get(fqn, [])),
        )
        self.tracker.register(anon_node)

        edge_id = self._next_edge_id("r")
        self._subgraph.edges.append(ReadsEdge(
            id=edge_id,
            file=self.rel_path,
            lineno=lineno,
            source=source_table_id,
            target=anon_node.id,
            streaming=False,
        ))
        return anon_node.id

    #: Methods that terminate a read chain (the loader call), as opposed to
    #: read *configuration* ops (.format/.option/.schema). After unroll_chain a
    #: read-rooted chain looks like read|readStream, format*, option*, <loader>.
    _READ_LOADER_METHODS = frozenset({
        "load", "table", "csv", "json", "parquet", "orc",
        "jdbc", "text", "avro", "delta",
    })

    def _emit_inline_read(
        self,
        base: ast.expr,
        read_ops: list["ChainOp"],
        lineno: int,
    ) -> Optional[str]:
        """Emit an anonymous read source node + ReadsEdge for the read portion
        of a chain (``spark.read.../spark.readStream...`` up to and including the
        loader call). Generalises ``_emit_inline_spark_table_read`` to file-path
        loaders (``.load``/``.csv``/``.parquet``/...) and streaming.

        Returns the anonymous node id so the caller can feed it as ``source_id``
        into ``_process_chain`` for the remaining transform/write ops, or None if
        no loader/location is recoverable.
        """
        ev = SafeEvaluator(self.symbols.symbols)
        streaming = any(op.method == "readStream" for op in read_ops)

        # Locate the loader op and resolve its location argument.
        location: Optional[str] = None
        for op in read_ops:
            if op.method in self._READ_LOADER_METHODS:
                loc_arg = op.positional(0)
                location = ev.resolve(loc_arg) if loc_arg is not None else UNRESOLVED
                break
        if location is None:
            return None

        if _looks_like_path(location):
            source_id = f"ext:{location}"
            self._subgraph.external_sources_referenced.append(ExternalSource(
                id=source_id,
                location=location,
                format=self._detect_format(read_ops),
                read_by=[self.rel_path],
            ))
            cols: list[str] = []
        else:
            source_id = f"table:{location}"
            self._subgraph.tables_referenced.append(
                Table(id=source_id, fqn=location, read_by=[self.rel_path]))
            cols = list(self._table_schema.get(location, []))

        anon_id = self.tracker.make_anon_id(self.rel_path, self._node_prefix(lineno))
        self.tracker.register(DataFrameNode(
            id=anon_id, file=self.rel_path, var_name=None, lineno=lineno, columns=cols))
        self._subgraph.edges.append(ReadsEdge(
            id=self._next_edge_id("r"),
            file=self.rel_path,
            lineno=lineno,
            source=source_id,
            target=anon_id,
            streaming=streaming,
        ))
        return anon_id

    def _split_read_chain(
        self, base: ast.expr, ops: list["ChainOp"],
    ) -> Optional[tuple[list["ChainOp"], list["ChainOp"]]]:
        """For a chain rooted at ``spark`` whose head is a read, return
        ``(read_ops, rest_ops)`` split at the read loader; else None. ``rest_ops``
        are the trailing transform/write operations (possibly empty)."""
        if not (isinstance(base, ast.Name) and base.id == "spark") or not ops:
            return None
        if ops[0].method == "table":
            split = 1  # spark.table("fqn").<...>
        elif ops[0].method in ("read", "readStream"):
            split = None
            for i in range(1, len(ops)):
                if ops[i].method in self._READ_LOADER_METHODS:
                    split = i + 1
                    break
            if split is None:
                return None
        else:
            return None
        return ops[:split], ops[split:]

    def _emit_read_rooted_pipeline(
        self, value: ast.expr, lineno: int, var_name: Optional[str] = None,
    ) -> Optional[str]:
        """Emit a read-rooted chain (``spark.read[Stream]...load/table(...)
        .<transforms>[.write...]``) and return the id of the final DataFrame node
        produced (or None if not a read-rooted chain). Splits at the read loader,
        emits the read, and delegates the remaining transform/write ops to
        ``_process_chain``. ``var_name`` names the final node when the chain is the
        RHS of an assignment (e.g. ``stats = spark.read.table(x).groupby(...).agg(...)``
        inside a Declarative-Pipeline function body); otherwise a synthetic name is
        used for an un-assigned fluent pipeline.
        """
        base, ops = unroll_chain(value)
        sp = self._split_read_chain(base, ops)
        if sp is None:
            return None
        read_ops, rest = sp
        source_id = self._emit_inline_read(base, read_ops, lineno)
        if source_id is None:
            return None
        if not rest:
            return source_id
        vn = var_name or f"__pipeline_{lineno}"
        self._process_chain(vn, lineno, source_id, rest)
        return f"df:{self.rel_path}:{self._node_prefix(lineno)}:{vn}"

    def _process_read_rooted_expr(self, value: ast.expr, lineno: int) -> bool:
        """Bare-expression-statement wrapper around ``_emit_read_rooted_pipeline``
        for un-assigned fluent pipelines
        ``(spark.readStream.table(...).withColumn(...)...writeStream.table(...))``
        — the dominant idiom in real Databricks plain-Spark code. Returns True if
        handled.
        """
        return self._emit_read_rooted_pipeline(value, lineno) is not None

    def _pipeline_function_return(self, fn: ast.FunctionDef) -> Optional[ast.expr]:
        """If same-file function *fn*'s last value-return is a spark-rooted
        pipeline chain, return that expression; else None."""
        ret = next((s for s in reversed(fn.body)
                    if isinstance(s, ast.Return) and s.value is not None), None)
        if ret is None:
            return None
        base, ops = unroll_chain(ret.value)
        if isinstance(base, ast.Name) and base.id == "spark" and ops:
            return ret.value
        return None

    def _process_interproc_pipeline_call(self, value: ast.expr, lineno: int) -> bool:
        """Handle a call to a same-file *parameterized ingestion helper*:
        ``def ingest(folder, fmt, table): return spark.readStream...load(folder)
        .writeStream.table(table)`` invoked as ``ingest("/vol/orders","json",
        "orders_bronze")`` (optionally chained ``.awaitTermination()``). Common in
        real Databricks plain-Spark code; missed by every other pass because the
        pipeline is inside a function and its read/write locations are parameters.

        Binds the call's positional args to the function's parameters in the
        symbol table, then processes the function's returned pipeline so the read
        source and write target resolve to the per-call-site values. Returns True
        if handled.
        """
        # Locate the call to a same-file function (bare or with trailing ops).
        call: Optional[ast.Call] = None
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Name):
            call = value
        else:
            base, _ = unroll_chain(value)
            if isinstance(base, ast.Call) and isinstance(base.func, ast.Name):
                call = base
        if call is None or not isinstance(call.func, ast.Name):
            return False
        fn = self._function_defs.get(call.func.id)
        if fn is None:
            return False
        ret_val = self._pipeline_function_return(fn)
        if ret_val is None:
            return False

        # Bind positional params → resolved arg values, then process the pipeline
        # with those bindings visible to the SafeEvaluator (restore afterward).
        ev = SafeEvaluator(self.symbols.symbols)
        bindings: dict[str, object] = {}
        for param, arg in zip(fn.args.args, call.args):
            val = ev.resolve(arg)
            if val is not None and val != UNRESOLVED:
                bindings[param.arg] = val
        saved = dict(self.symbols.symbols)
        try:
            self.symbols.symbols.update(bindings)
            produced = self._emit_read_rooted_pipeline(ret_val, lineno)
        finally:
            self.symbols.symbols.clear()
            self.symbols.symbols.update(saved)
        return produced is not None

    #: Decorator attributes that mark a Spark Declarative Pipelines / Delta Live
    #: Tables materialization (the decorated function's name is the output table).
    _DLT_DECORATORS = frozenset({
        "table", "view", "materialized_view", "create_table",
        "create_streaming_table", "create_materialized_view",
    })

    def _dlt_output_table(self, fn: ast.FunctionDef) -> Optional[str]:
        """If *fn* is decorated with a DLT/SDP table/view decorator from a known
        pipelines alias (``dlt`` / ``dp`` / ``pipelines``), return the output
        table name (the decorator's ``name=`` kwarg if present, else the function
        name). Otherwise None.
        """
        for dec in fn.decorator_list:
            call = dec if isinstance(dec, ast.Call) else None
            attr = dec.func if isinstance(dec, ast.Call) else dec
            if not (isinstance(attr, ast.Attribute) and attr.attr in self._DLT_DECORATORS):
                continue
            base = attr.value
            if not (isinstance(base, ast.Name) and base.id in self._dlt_aliases):
                continue
            # name= kwarg overrides the function name
            if call is not None:
                for kw in call.keywords:
                    if kw.arg == "name":
                        ev = SafeEvaluator(self.symbols.symbols)
                        resolved = ev.resolve(kw.value)
                        if resolved and resolved != UNRESOLVED:
                            return str(resolved)
            return fn.name
        return None

    def _process_dlt_functions(self, tree: ast.Module) -> None:
        """Emit lineage for Spark Declarative Pipelines / DLT functions:
        ``@dp.table def silver(): return spark.readStream.table(...).select(...)``.
        The output table is the function name (or ``name=`` kwarg); the pipeline
        is the function's ``return`` expression. Without this, the dominant real
        Databricks idiom yields zero transformation lineage.
        """
        for fn in ast.walk(tree):
            if not isinstance(fn, ast.FunctionDef):
                continue
            out_name = self._dlt_output_table(fn)
            if out_name is None:
                continue
            # Use the last top-level return with a value.
            ret = next((s for s in reversed(fn.body)
                        if isinstance(s, ast.Return) and s.value is not None), None)
            if ret is None:
                continue
            lineno = getattr(ret, "lineno", fn.lineno)
            # Resolve the returned DataFrame: either a read-rooted chain or a var.
            final_id = self._emit_read_rooted_pipeline(ret.value, lineno)
            if final_id is None:
                final_id = self._resolve_base_to_id(ret.value)
            if final_id is None:
                continue
            streaming = "readStream" in ast.dump(ret.value)
            table_id = f"table:{out_name}"
            self._subgraph.tables_referenced.append(
                Table(id=table_id, fqn=out_name, written_by=[self.rel_path]))
            self._subgraph.edges.append(WritesEdge(
                id=self._next_edge_id("w"),
                file=self.rel_path,
                lineno=lineno,
                source=final_id,
                target=table_id,
                mode=WriteMode.APPEND if streaming else WriteMode.UNRESOLVED,
                format=None,
                streaming=streaming,
                partition_cols=[],
            ))

    # ------------------------------------------------------------------ #
    # SQL DDL (populate Table.columns)                                    #
    # ------------------------------------------------------------------ #

    def _extract_sql(self) -> None:
        """Parse a .sql file and extract CREATE TABLE column definitions."""
        try:
            import sqlglot
        except ImportError:
            self._warn(0, "missing-dependency", "sqlglot not installed; SQL parsing skipped")
            return

        sql = self.path.read_text(encoding="utf-8")
        try:
            statements = sqlglot.parse(sql)
        except Exception as e:
            self._warn(0, "sql-parse-error", f"sqlglot failed: {e}")
            return

        for stmt in statements:
            if stmt is None:
                continue
            # CREATE TABLE
            if stmt.key == "create":
                self._handle_create_table(stmt)

    def _handle_create_table(self, stmt: object) -> None:
        """Extract a Table stub from a CREATE TABLE statement."""
        try:
            import sqlglot.expressions as exp
            if not isinstance(stmt, exp.Create):
                return
            table_expr = stmt.find(exp.Table)
            if table_expr is None:
                return

            # Build FQN
            parts = []
            for attr in ("catalog", "db", "name"):
                val = getattr(table_expr, attr, None)
                if val:
                    parts.append(str(val))
            fqn = ".".join(parts) if parts else UNRESOLVED

            # Extract columns
            cols: list[Column] = []
            schema = stmt.find(exp.Schema)
            if schema:
                for col_def in schema.find_all(exp.ColumnDef):
                    col_name = col_def.name
                    dtype_expr = col_def.find(exp.DataType)
                    dtype = str(dtype_expr) if dtype_expr else UNRESOLVED
                    cols.append(Column(name=col_name, dtype=dtype.lower()))

            table_id = f"table:{fqn}"
            stub = Table(id=table_id, fqn=fqn, columns=cols)
            self._subgraph.tables_referenced.append(stub)

        except Exception as e:
            self._warn(0, "sql-parse-error", f"Error processing CREATE TABLE: {e}")

    # ------------------------------------------------------------------ #
    # Expression interning                                                 #
    # ------------------------------------------------------------------ #

    def _collect_col_refs(self, node: ast.expr) -> list[str]:
        """Like ``_extract_col_refs`` but transparently expands any
        Column-typed Python variables previously registered in
        ``self._column_exprs``.

        Worked example: given the source

            prof_subtype = F.when(F.col("any_line_is_pcp") == 1, ...)
            new_claim_subtype_expr = F.when(rev_is_er, ...).when(bill_missing, prof_subtype)
            df.withColumn("subtype", new_claim_subtype_expr)

        the bare module-level extractor sees only the literal AST of the
        ``withColumn`` value — a ``Name("new_claim_subtype_expr")`` — and
        finds zero column references. This method follows that Name back to
        its registered AST (the full ``when().when().otherwise()`` body),
        recursively expanding any further variable references it discovers
        (``rev_is_er``, ``bill_missing``, ``prof_subtype``), and unions every
        column name found anywhere in the transitive closure.
        """
        direct = _extract_col_refs(node)
        expanded: list[str] = list(direct)
        seen_vars: set[str] = set()

        def _walk(n: ast.expr) -> None:
            for sub in ast.walk(n):
                if not isinstance(sub, ast.Name):
                    continue
                vid = sub.id
                if vid in seen_vars or vid not in self._column_exprs:
                    continue
                seen_vars.add(vid)
                sub_ast = self._column_exprs[vid]
                for r in _extract_col_refs(sub_ast):
                    if r not in expanded:
                        expanded.append(r)
                _walk(sub_ast)

        _walk(node)
        return expanded

    def _intern_expr(self, node: Optional[ast.expr]) -> Optional[Expression]:
        """Intern an AST expression node; return the Expression object or None."""
        if node is None:
            return None
        text = ast.unparse(node)
        return self._get_or_create_expr(text, self._collect_col_refs(node))

    def _intern_expr_text(self, text: str, refs: list[str]) -> str:
        """Intern by text string; return the expr_id (str)."""
        return self._get_or_create_expr(text, refs).id

    def _get_or_create_expr(self, text: str, refs: list[str]) -> Expression:
        """Return existing Expression for *text*, or create and register a new one."""
        if text in self._expr_cache:
            return self._expr_cache[text]
        self._expr_counter += 1
        expr = Expression(
            id=f"expr:e{self._expr_counter:02d}",
            text=text,
            referenced_cols=refs,
        )
        self._expr_cache[text] = expr
        self._subgraph.expressions.append(expr)
        return expr

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _first_df_arg(self, args: list[ast.expr]) -> Optional[str]:
        """Return the DataFrame-node ID of the first positional arg that
        resolves to a tracked DataFrame, or ``None`` if no arg does.

        Used by ``_try_opaque_call`` to handle helpers where the DataFrame is
        not the first positional argument (e.g. ``grammar.apply_dq_rules(rules,
        df)`` puts the rule string at position 0 and the DataFrame at 1).
        """
        for arg in args:
            sid = self._resolve_base_to_id(arg)
            if sid is not None:
                return sid
        return None

    def _resolve_base_to_id(self, node: Optional[ast.expr]) -> Optional[str]:
        """Resolve an AST node to a DataFrameNode ID via the tracker.

        Handles:
        - ``Name("orders")``  → direct tracker lookup
        - ``Attribute(Name("orders"), "write")``  → strip attribute, recurse
          (covers ``df.write``, ``df.writeStream``, ``df.groupBy``, etc.)
        - ``Call(Attribute(Name("orders"), "select"), ...)``  → inline method
          chain on a known DataFrame (e.g. ``orders_df.select("a","b")`` used
          as a join argument). Resolve to the receiver's ID so the upstream
          DataFrame is preserved as the lineage source even when the inline
          projection is not assigned to a variable. Without this, joins like
          ``timeline.join(orders_df.select("order_id","customer_id"), ...)``
          emit ``right_source=<unresolved>``.
        """
        if node is None:
            return None
        if isinstance(node, ast.Name):
            return self.tracker.get_id(node.id)
        if isinstance(node, ast.Attribute):
            # Strip any chained attribute access and resolve the root object.
            # This handles df.write, df.writeStream, df.select etc. where the
            # DataFrame is the leftmost object, not the accessor.
            return self._resolve_base_to_id(node.value)
        if isinstance(node, ast.Call):
            # spark.table(...) → not a variable reference
            if self._is_spark_table_call(node):
                return None
            # Method-chain call on something: recurse into the receiver.
            # For ``orders_df.select("order_id", "customer_id")`` the func is
            # ``Attribute(value=Name("orders_df"), attr="select")`` and we want
            # ``orders_df``'s ID. For unrelated calls (e.g. ``F.col(...)``,
            # ``helper(df)``) the receiver chain bottoms out at a non-tracker
            # name and we still return None — safe.
            if isinstance(node.func, ast.Attribute):
                return self._resolve_base_to_id(node.func.value)
        return None

    def _next_edge_id(self, prefix: str) -> str:
        self._edge_counter += 1
        return f"e:{prefix}:{self.rel_path.replace('/', ':').replace('.py','').replace('.ipynb','')}:{self._edge_counter:03d}"

    def _warn(self, lineno: int, category: str, message: str) -> None:
        self._subgraph.warnings.append(GraphWarning(
            file=self.rel_path,
            lineno=lineno or None,
            category=category,
            message=message,
        ))


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


_PATH_PREFIXES = ("/", "s3://", "s3a://", "abfss://", "abfs://", "wasbs://",
                  "dbfs:/", "gs://", "hdfs://")


def _looks_like_path(location: str) -> bool:
    """True when *location* looks like a file-system path rather than a catalog FQN."""
    if location.startswith(UNRESOLVED):
        return False
    return any(location.startswith(p) for p in _PATH_PREFIXES)


def _is_spark_read_base(base: ast.expr) -> bool:
    """True when base is ``spark.read`` or ``spark.readStream`` (attribute, not call)."""
    return (
        isinstance(base, ast.Attribute)
        and base.attr in ("read", "readStream")
        and isinstance(base.value, ast.Name)
        and base.value.id == "spark"
    )


def _extract_func_name(func_node: ast.expr) -> str:
    """Extract qualified function name from a Call's func node."""
    if isinstance(func_node, ast.Name):
        return func_node.id
    if isinstance(func_node, ast.Attribute):
        prefix = _extract_func_name(func_node.value)
        return f"{prefix}.{func_node.attr}" if prefix else func_node.attr
    return ""


# Names that, when called bare or as ``F.<name>(...)``, return a
# PySpark ``Column`` rather than a ``DataFrame``. Used by the
# Column-variable detector — see ``_looks_like_column_expr``.
_COLUMN_BUILDER_FUNCS: frozenset[str] = frozenset({
    # Construction / projection
    "col", "column", "lit", "expr", "when",
    # Containers / structuring
    "array", "struct", "map_from_arrays", "map_from_entries",
    "create_map", "named_struct",
    # String / numeric / cast helpers commonly used standalone
    "concat", "concat_ws", "coalesce", "trim", "ltrim", "rtrim",
    "lower", "upper", "regexp_replace", "regexp_extract", "substring",
    "substr", "instr", "length", "split", "lpad", "rpad", "translate",
    "format_string", "format_number",
    "abs", "ceil", "floor", "round", "bround", "sqrt", "pow", "exp",
    "log", "log10", "log2", "sin", "cos", "tan", "asin", "acos", "atan",
    # Date / time
    "to_date", "to_timestamp", "current_date", "current_timestamp",
    "year", "month", "dayofmonth", "dayofweek", "dayofyear", "hour",
    "minute", "second", "date_format", "date_add", "date_sub",
    "datediff", "months_between", "from_unixtime", "unix_timestamp",
    "add_months", "last_day", "next_day", "trunc",
    # Conditional / null
    "nullif", "isnull", "isnan", "ifnull", "nvl", "nvl2", "greatest",
    "least",
    # Window / collection
    "row_number", "rank", "dense_rank", "percent_rank", "ntile",
    "cume_dist", "lag", "lead", "first", "last",
    "collect_list", "collect_set", "array_distinct", "array_sort",
    "array_union", "array_intersect", "array_except", "array_contains",
    "array_position", "flatten", "size", "explode", "explode_outer",
    "posexplode", "sequence",
    "transform", "filter", "exists", "forall", "element_at",
    "map_keys", "map_values", "map_entries", "map_filter",
    # Hashes / encoding
    "md5", "sha1", "sha2", "crc32", "hash", "xxhash64",
    "encode", "decode", "base64", "unbase64", "soundex",
    # Aggregates that also appear standalone in expressions
    "sum", "avg", "mean", "min", "max", "count", "countDistinct",
    "approx_count_distinct", "stddev", "variance", "skewness",
    "kurtosis", "sumDistinct",
})

# Method names invoked on a ``Column`` to derive another ``Column``.
# Used to detect chain assignments like ``bill_int = bill.cast("int")``
# where the receiver itself is already a Column-typed variable.
_COLUMN_METHOD_NAMES: frozenset[str] = frozenset({
    "cast", "alias", "name",
    "between", "isin", "isNull", "isNotNull",
    "like", "rlike", "ilike", "contains", "startswith", "endswith",
    "asc", "desc",
    "asc_nulls_last", "desc_nulls_last", "asc_nulls_first", "desc_nulls_first",
    "otherwise", "when",
    "bitwiseAND", "bitwiseOR", "bitwiseXOR",
    "getField", "getItem", "substr", "eqNullSafe",
})


def _looks_like_column_expr(
    node: ast.expr,
    known_column_vars: dict[str, ast.expr],
) -> bool:
    """Heuristically detect whether *node* evaluates to a PySpark ``Column``.

    Used to decide whether to register an assignment's RHS in the
    ``_column_exprs`` registry. False positives here are harmless (the entry
    just sits unused); false negatives mean column references buried inside
    the variable will be invisible on downstream ``derives`` edges.

    Recognised shapes:

      * ``F.<fn>(...)`` or bare ``<fn>(...)`` where ``<fn>`` is a known
        Column-builder (``col``, ``when``, ``lit``, ``array``, ``struct``,
        ``concat``, ``trim``, ``regexp_replace``, etc.).
      * ``<expr>.<method>(...)`` where ``<method>`` is a Column method
        (``cast``, ``alias``, ``between``, ``isin``, ``isNull``, ``like``,
        ``otherwise``, ...) — including chains rooted at a previously
        registered Column variable.
      * Comparison / boolean / arithmetic expressions whose operands include
        any of the above (e.g. ``col("a") == "x"``, ``bill.isNull() | ...``).
      * Bare references to a name already in *known_column_vars*.
    """
    # Direct reference to a known Column-typed variable
    if isinstance(node, ast.Name) and node.id in known_column_vars:
        return True

    # Compound expressions: any operand being a Column propagates upwards
    if isinstance(node, ast.BoolOp):
        return any(_looks_like_column_expr(v, known_column_vars) for v in node.values)
    if isinstance(node, ast.BinOp):
        return (
            _looks_like_column_expr(node.left, known_column_vars)
            or _looks_like_column_expr(node.right, known_column_vars)
        )
    if isinstance(node, ast.UnaryOp):
        return _looks_like_column_expr(node.operand, known_column_vars)
    if isinstance(node, ast.Compare):
        if _looks_like_column_expr(node.left, known_column_vars):
            return True
        return any(_looks_like_column_expr(c, known_column_vars) for c in node.comparators)

    if isinstance(node, ast.Call):
        func = node.func
        # F.<fn>(...) — attribute call where attr is a builder name
        if isinstance(func, ast.Attribute):
            if func.attr in _COLUMN_BUILDER_FUNCS:
                return True
            if func.attr in _COLUMN_METHOD_NAMES:
                # Column method on something; if the receiver is itself a
                # Column expression, the result is too.
                return _looks_like_column_expr(func.value, known_column_vars)
        # bare <fn>(...) where <fn> is imported from pyspark.sql.functions
        if isinstance(func, ast.Name) and func.id in _COLUMN_BUILDER_FUNCS:
            return True

    return False


def _extract_column_refs_from_expr_string(
    node: Optional[ast.expr],
    column_exprs: dict[str, ast.expr],
) -> list[str]:
    """★ Q3 v3: pull column-variable references out of ``F.expr("...")`` /
    ``expr("...")`` string literals.

    The SQL string passed to ``F.expr`` mentions column names directly
    (no ``col(...)`` wrapper). For static analysis we treat any identifier
    in the string that matches a key in *column_exprs* as a source-column
    reference. This recovers the link from e.g.

        F.expr("filter(multi_raw, x -> x is not null)")

    back to ``multi_raw`` — the Column-typed Python variable holding the
    array-of-predicates that drives the downstream column.

    Returns the list in source-order, deduplicated.
    """
    import re

    if node is None or not isinstance(node, ast.Call):
        return []
    fname: Optional[str] = None
    if isinstance(node.func, ast.Name):
        fname = node.func.id
    elif isinstance(node.func, ast.Attribute):
        fname = node.func.attr
    if fname != "expr" or not node.args:
        return []
    first = node.args[0]
    if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
        return []
    sql_text = first.value
    # Conservative identifier match — only bareword names, no quoted ids.
    # Use word boundaries so substrings of longer identifiers don't match.
    candidates: list[str] = []
    seen: set[str] = set()
    for m in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", sql_text):
        ident = m.group(1)
        if ident in column_exprs and ident not in seen:
            candidates.append(ident)
            seen.add(ident)
    return candidates


def _extract_array_of_whens_logic(
    node: ast.expr,
    predicate_dicts: Optional[dict[str, dict[str, ast.expr]]],
) -> Optional[list[dict[str, str]]]:
    """★ Q3 v2: detect the ``F.array(*[F.when(d[k], lit(k)) for k in d])``
    comprehension pattern and return synthetic rule_logic linking every
    predicate in *d* to its emitted value.

    This is a real-world `multi_raw` shape:

        multi_raw = F.array(*[
            F.when(conds[name], F.lit(name)) for name in conds.keys()
        ])

    Without this, the connection between `conds` (the predicate dict) and
    the downstream array column built from it is invisible — every
    predicate ID appears in the chain that consumes `conds[name]` but no
    rule_logic surfaces. After this fix, the downstream column's
    ``column_rules`` entry will list every (predicate, value) pair so
    @PUSE can cross-reference it back to other shared-predicate columns.

    Returns ``None`` if *node* is not recognisable as the comprehension
    shape OR if the dict isn't in *predicate_dicts*.
    """
    if predicate_dicts is None:
        return None
    # Outer must be a Call F.array(*<gen>) or array(*<gen>)
    if not isinstance(node, ast.Call):
        return None
    fname: Optional[str] = None
    if isinstance(node.func, ast.Name):
        fname = node.func.id
    elif isinstance(node.func, ast.Attribute):
        fname = node.func.attr
    if fname != "array":
        return None
    # Must be array(*<gen>) — exactly one Starred arg holding a GeneratorExp
    if len(node.args) != 1 or not isinstance(node.args[0], ast.Starred):
        return None
    gen = node.args[0].value
    if not isinstance(gen, (ast.GeneratorExp, ast.ListComp)):
        return None
    # The element must be a F.when(<dict>[<key_var>], <val_expr>) call
    elt = gen.elt
    if not isinstance(elt, ast.Call):
        return None
    elt_fname: Optional[str] = None
    if isinstance(elt.func, ast.Name):
        elt_fname = elt.func.id
    elif isinstance(elt.func, ast.Attribute):
        elt_fname = elt.func.attr
    if elt_fname != "when" or len(elt.args) < 2:
        return None
    cond_arg = elt.args[0]
    val_arg = elt.args[1]
    # cond must be Subscript(value=Name(dict_var), slice=Name(loop_var))
    if not isinstance(cond_arg, ast.Subscript):
        return None
    if not isinstance(cond_arg.value, ast.Name):
        return None
    dict_var = cond_arg.value.id
    if dict_var not in predicate_dicts:
        return None
    mapping = predicate_dicts[dict_var]
    # Build synthetic rules: one (cond, value) pair per key in the dict.
    # We unparse each predicate AST so the text matches what @PREDS holds,
    # which is what @PUSE keys off — so the cross-reference works.
    rules: list[dict[str, str]] = []
    for label, pred_expr in mapping.items():
        try:
            cond_text = ast.unparse(pred_expr)
        except Exception:
            cond_text = "?"
        # Value: try to substitute the loop variable for the label so the
        # value text matches what would have been emitted at runtime.
        # If val_arg is `F.lit(name)` with name being the loop var, emit
        # `F.lit('<label>')`; otherwise just unparse val_arg.
        try:
            val_text = ast.unparse(val_arg)
        except Exception:
            val_text = "?"
        # Heuristic substitution: replace the loop var name with the actual
        # label when it appears in val_text. This keeps the value text
        # informative (e.g. `lit('Behavioral Health')`) rather than the
        # opaque `lit(name)`.
        if isinstance(gen.generators[0].target, ast.Name):
            loop_var = gen.generators[0].target.id
            val_text = val_text.replace(loop_var, repr(label))
        rules.append({"cond": cond_text, "value": val_text})
    return rules if rules else None


def _resolve_predicate_subscripts(
    node: ast.expr,
    predicate_dicts: Optional[dict[str, dict[str, ast.expr]]],
) -> ast.expr:
    """★ Q3: rewrite ``Name(var)[Constant(label)]`` references to predicate
    dicts so that ``ast.unparse`` produces the actual predicate text rather
    than ``conds['BH']``.  Returns a modified copy (or the original if no
    rewrites apply).
    """
    if not predicate_dicts:
        return node

    class _Rewriter(ast.NodeTransformer):
        def visit_Subscript(self, n: ast.Subscript) -> ast.AST:
            self.generic_visit(n)
            if isinstance(n.value, ast.Name) and n.value.id in predicate_dicts:
                # py3.9+ uses ast.Constant for the slice
                slc = n.slice
                if isinstance(slc, ast.Constant) and isinstance(slc.value, str):
                    label = slc.value
                    dct = predicate_dicts[n.value.id]
                    if label in dct:
                        return ast.copy_location(dct[label], n)
            return n

    try:
        return _Rewriter().visit(ast.parse(ast.unparse(node), mode="eval").body)
    except Exception:
        return node


def _extract_when_logic(
    node: ast.expr,
    column_exprs: dict[str, ast.expr],
    _seen: Optional[set[str]] = None,
    predicate_dicts: Optional[dict[str, dict[str, ast.expr]]] = None,
) -> Optional[list[dict[str, str]]]:
    """Walk a ``F.when(...).when(...).otherwise(...)`` chain and return an
    ordered list of ``{"cond": <predicate>, "value": <result>}`` dicts.

    The last entry has ``cond="otherwise"`` for the fallback branch (if
    present).  Returns ``None`` if *node* is not recognisable as a
    ``when()`` chain.

    Transparent variable expansion: if *node* is a ``Name`` that points to
    a registered ``_column_exprs`` entry, the expansion is followed (with
    cycle-guard via ``_seen``).

    ★ Q3: if *predicate_dicts* is provided, ``conds['BH']``-style subscript
    references inside ``cond`` expressions are resolved to the actual
    predicate text from the dict before being unparsed.
    """
    if _seen is None:
        _seen = set()

    # Expand a Column-typed variable reference (e.g. `new_claim_subtype_expr`)
    if isinstance(node, ast.Name):
        if node.id in _seen or node.id not in column_exprs:
            return None
        _seen = _seen | {node.id}
        return _extract_when_logic(
            column_exprs[node.id], column_exprs, _seen, predicate_dicts
        )

    if not isinstance(node, ast.Call):
        return None
    if not isinstance(node.func, ast.Attribute):
        return None

    def _cond_text(arg: ast.expr) -> str:
        # ★ Q3: rewrite subscript references to predicate dicts first.
        resolved = _resolve_predicate_subscripts(arg, predicate_dicts)
        return ast.unparse(resolved)

    # Walk the chain from outermost (.otherwise / .when) down to the
    # root F.when() call, collecting rules in reverse order.
    rules: list[dict[str, str]] = []
    current: ast.expr = node

    while isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
        method = current.func.attr
        if method == "otherwise":
            val_node = current.args[0] if current.args else None
            rules.append({
                "cond": "otherwise",
                "value": ast.unparse(val_node) if val_node is not None else "",
            })
            current = current.func.value
        elif method == "when" and len(current.args) >= 2:
            rules.append({
                "cond": _cond_text(current.args[0]),
                "value": ast.unparse(current.args[1]),
            })
            current = current.func.value
        else:
            # Some unrelated method — abort; not a pure when() chain
            break

    # ★ P12 fix: bare `when(cond, val)` root call.
    # When a chain starts with a bare `when(...)` import (no `F.` prefix),
    # its AST func is `Name("when")` — not an Attribute — so the while loop
    # exits without capturing it.  Detect and append it here.
    if isinstance(current, ast.Call) and isinstance(current.func, ast.Name):
        if current.func.id == "when" and len(current.args) >= 2:
            rules.append({
                "cond": _cond_text(current.args[0]),
                "value": ast.unparse(current.args[1]),
            })

    if not rules:
        return None

    # Rules were collected outermost-first; reverse to get logical order
    # (first branch first, "otherwise" last).
    rules.reverse()
    return rules


def _extract_col_refs(node: ast.expr) -> list[str]:
    """Heuristically extract column name strings from an expression AST.

    Recognises three reference shapes:

      * ``F.col("name")`` or ``alias.col("name")``        — attribute call
      * ``col("name")``                                    — bare call (from
        ``from pyspark.sql.functions import col``)
      * ``df["name"]``                                     — subscript

    The bare-call form is critical for codebases that ``from pyspark.sql
    .functions import *`` at the top of the file: without it, every
    ``when(col("X") == ...)`` predicate vanishes from the source-column list
    on its ``derives`` edge.
    """
    refs: list[str] = []
    for n in ast.walk(node):
        # F.col("name") or alias.col("name")
        if (
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "col"
            and n.args
            and isinstance(n.args[0], ast.Constant)
            and isinstance(n.args[0].value, str)
        ):
            refs.append(n.args[0].value)
        # bare col("name")
        elif (
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and n.func.id == "col"
            and n.args
            and isinstance(n.args[0], ast.Constant)
            and isinstance(n.args[0].value, str)
        ):
            refs.append(n.args[0].value)
        # df["col_name"]
        elif (
            isinstance(n, ast.Subscript)
            and isinstance(n.slice, ast.Constant)
            and isinstance(n.slice.value, str)
        ):
            refs.append(n.slice.value)
    return list(dict.fromkeys(refs))  # deduplicate preserving order


def _parse_join_type(how: str) -> JoinType:
    mapping = {
        "inner": JoinType.INNER,
        "left": JoinType.LEFT,
        "left_outer": JoinType.LEFT,
        "right": JoinType.RIGHT,
        "right_outer": JoinType.RIGHT,
        "full": JoinType.FULL,
        "full_outer": JoinType.FULL,
        "outer": JoinType.FULL,
        "cross": JoinType.CROSS,
        "left_semi": JoinType.LEFT_SEMI,
        "left_anti": JoinType.LEFT_ANTI,
    }
    return mapping.get(how.lower(), JoinType.UNRESOLVED)


def _parse_write_mode(mode: str) -> WriteMode:
    mapping = {
        "overwrite": WriteMode.OVERWRITE,
        "append": WriteMode.APPEND,
        "merge": WriteMode.MERGE,
        "ignore": WriteMode.IGNORE,
        "error": WriteMode.ERROR,
        "errorifexists": WriteMode.ERROR,
    }
    return mapping.get(mode.lower(), WriteMode.UNRESOLVED)


def _extract_inline_select_cols(arg: Optional[ast.expr]) -> list[str]:
    """If *arg* is ``<something>.select("col1", "col2", ...)``, return those col names.

    Returns ``[]`` for any other AST shape — meaning "no inline projection
    detected; downstream should fall back to the source's full column list."

    Handles only the static positional-string form. Dynamic forms like
    ``df.select(*cols)`` or ``df.select(F.col("x"))`` return ``[]`` (caller
    falls back to full source columns, which is the conservative choice).
    """
    if not isinstance(arg, ast.Call):
        return []
    if not isinstance(arg.func, ast.Attribute):
        return []
    if arg.func.attr != "select":
        return []
    cols: list[str] = []
    for a in arg.args:
        if isinstance(a, ast.Constant) and isinstance(a.value, str):
            cols.append(a.value)
        else:
            # Non-string arg (e.g. F.col(...), *list_expr) — bail; caller falls
            # back to the source's full column list rather than emit a partial
            # projection that could mislead downstream consumers.
            return []
    return cols


def _sqlglot_expr_cols(expr) -> list[str]:
    """Deduped bare column names referenced anywhere in a sqlglot expression.

    Table aliases are stripped via ``.name`` (``ca.avg_country_ltv`` ->
    ``avg_country_ltv``). General by construction: works for arithmetic,
    function calls (DATEDIFF / coalesce / concat_ws / cast / NULLIF ...),
    CASE expressions, etc. This is the same extraction already used for
    aggregate inputs and window ORDER BY keys; it lets a computed SQL column
    carry its real source columns so downstream column-level impact resolves
    through ``spark.sql`` blocks. References no benchmark-specific names.
    """
    seen: list[str] = []
    try:
        import sqlglot.expressions as _e
        for c in expr.find_all(_e.Column):
            n = c.name
            if n and n not in seen:
                seen.append(n)
    except Exception:
        pass
    return seen


def _extract_sqlglot_join_keys(
    on_expr,
    left_alias: Optional[str],
    right_alias: Optional[str],
) -> list[tuple[str, str]]:
    """★ P13: extract join key pairs from a sqlglot ``ON`` clause AST.

    Walks ``And(EQ(Column, Column), ...)`` chains of arbitrary depth and emits
    ``(left_col, right_col)`` tuples.  Uses the table alias (e.g. ``c`` vs
    ``clm``) to decide which side of each equality belongs to the left/right
    table; falls back to AST order if neither side carries a recognised alias.

    Returns ``[]`` for unsupported predicate shapes (OR, inequalities,
    function-wrapped keys).  Wrapped in try/except to survive sqlglot AST
    drift across versions.
    """
    if on_expr is None:
        return []
    try:
        import sqlglot.expressions as exp
    except Exception:
        return []

    def _walk(node) -> list[tuple[str, str]]:
        # Unwrap parentheses transparently.
        if isinstance(node, exp.Paren):
            return _walk(node.this)
        # AND-chain: concatenate both sides.
        if isinstance(node, exp.And):
            return _walk(node.left) + _walk(node.right)
        # Equality on two Columns.
        if isinstance(node, exp.EQ):
            l, r = node.left, node.right
            if isinstance(l, exp.Column) and isinstance(r, exp.Column):
                l_tbl = l.table or ""
                r_tbl = r.table or ""
                # Decide which side is which based on table alias.
                if left_alias and r_tbl == left_alias:
                    # The right-hand side of the AST is actually the LEFT table.
                    return [(r.name, l.name)]
                if right_alias and l_tbl == right_alias:
                    # The left-hand side of the AST is actually the RIGHT table — swap.
                    return [(r.name, l.name)]
                # Otherwise trust AST order: (left_ast, right_ast).
                return [(l.name, r.name)]
        return []

    try:
        return _walk(on_expr)
    except Exception:
        return []


def _extract_join_keys(on_arg: Optional[ast.expr]) -> list[tuple[str, str]]:
    """Best-effort extraction of join key pairs from the on= argument."""
    if on_arg is None:
        return []

    # on="customer_id"  (single string key — same column name on both sides)
    if isinstance(on_arg, ast.Constant) and isinstance(on_arg.value, str):
        return [(on_arg.value, on_arg.value)]

    # on=["key1", "key2"]
    if isinstance(on_arg, ast.List):
        keys = []
        for elt in on_arg.elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                keys.append((elt.value, elt.value))
        return keys

    # on=df1["col"] == df2["col"]  or  F.col("col") == F.col("col")
    if isinstance(on_arg, ast.Compare) and on_arg.comparators:
        left_col = _col_from_expr(on_arg.left)
        right_col = _col_from_expr(on_arg.comparators[0])
        if left_col and right_col:
            return [(left_col, right_col)]

    return []


def _col_from_expr(node: ast.expr) -> Optional[str]:
    """Extract column name from F.col("x") / df["x"] / Name."""
    # F.col("x") or col("x")
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "col"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    ):
        return node.args[0].value
    # df["x"]
    if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
        return node.slice.value
    # bare Name
    if isinstance(node, ast.Name):
        return node.id
    return None


def _parse_agg_call(
    node: ast.expr,
    ev: SafeEvaluator,
) -> tuple[str, str, str]:
    """Parse a single agg argument like F.sum("col").alias("total").
    Returns (agg_func_name, agg_input_col, output_col_name).

    agg_input_col is the column the aggregate consumes, e.g. 'total_amount'
    for F.sum('total_amount'). Returns UNRESOLVED when the input cannot be
    resolved (e.g., count('*') or a complex expression argument).
    """
    alias_name = UNRESOLVED
    inner = node

    # Strip .alias(...)
    if (
        isinstance(inner, ast.Call)
        and isinstance(inner.func, ast.Attribute)
        and inner.func.attr == "alias"
        and inner.args
    ):
        alias_name = ev.resolve(inner.args[0])
        inner = inner.func.value

    # F.sum("col") / F.count("col") / etc.
    func_name = UNRESOLVED
    agg_input = UNRESOLVED
    if isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute):
        func_name = inner.func.attr
        if inner.args:
            # First positional arg is the aggregated column. Resolve string
            # literals, F.col("x"), df["x"], or bare names.
            arg0 = inner.args[0]
            resolved = ev.resolve(arg0)
            if isinstance(resolved, str) and resolved and resolved != UNRESOLVED:
                agg_input = resolved
            else:
                col_ref = _col_from_expr(arg0)
                if col_ref:
                    agg_input = col_ref
                else:
                    # Fallback: walk the expr for any column reference
                    refs = _extract_col_refs(arg0)
                    if refs:
                        agg_input = refs[0]

    return func_name, agg_input, alias_name
