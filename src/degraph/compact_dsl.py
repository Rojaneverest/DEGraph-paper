"""DEGraph DSL — a line-based, token-efficient serialization of the compact graph.

Motivation
----------
The JSON-based ``build_compact_graph`` keeps the structure faithful to the
extractor schema, but JSON syntax overhead (quotes, brackets, repeated keys),
full-FQN repetition, and per-occurrence predicate text inflate the token
count. On a real-world benchmark we measured only 6% reduction vs raw source.

This module emits the same information as a compact text DSL with three
compression layers:

  1. **Symbol interning** — tables get short IDs (``T0``, ``T1``, …),
     dataframes get ``D0``, ``D1``, …. Used as references throughout.
  2. **Predicate interning** — every distinct predicate text seen in
     ``rule_logic`` / ``predicate_dicts`` is assigned an ID ``X0``, ``X1``, …
     so a 200-char predicate appears once in the ``@PREDICATES`` section and
     is referenced by ID everywhere else.
  3. **Line-based records** — one entity per line with a single-letter type
     prefix and pipe-separated fields. No JSON brackets, no repeated keys.

A compact legend at the top teaches the LLM how to read the format in
~250 tokens (vs ~1000 for the JSON LEGEND dict).

Public API
----------
``build_compact_dsl(graph: dict) -> str``
    Takes the same input as ``build_compact_graph`` (the full ``Graph`` dict
    from ``extract_repo().model_dump()``) and returns a DSL string.
"""

from __future__ import annotations

from typing import Optional

from .compact import _build_column_provenance, _norm_id


# ---------------------------------------------------------------------------
# Legend — compact teaching prompt for the LLM. Targets ~250 tokens.
# ---------------------------------------------------------------------------

LEGEND = """\
# DEGraph DSL v1 — data-lineage graph in compact line format.
# Read each line as ONE record. Sections marked by @HEADERS.
#
# Symbols:
#   T<n>  table        (defined in @TABLES; reference elsewhere)
#   D<n>  dataframe    (file-local intermediate; reference in edges)
#   X<n>  predicate    (defined in @PREDICATES; referenced as X<n> in rule_logic)
#
# Sections (in order):
#   @TABLES    one per line: T<n>|<fqn>|<col1,col2,...>|by=<writer_file>|rb=<reader_file1,...>
#              by= the file that PRODUCES (writes/creates) this table — its WRITER/PRODUCER
#              rb= the file(s) that CONSUME this table as input — its READERS/CONSUMERS
#              IMPORTANT: by= ≠ consumer; rb= ≠ producer. by= is the source/writer; rb= is the sink/reader.
#   @PROV      column provenance:  <Tn>.<col>=<role>:<src1>,<src2>[;op=<f>][;win=<spec>]
#              roles: P=passthrough  D=derived  A=aggregate  G=group_key
#              sources are either "<Tn>.<col>" or bare column names (file-local)
#   @PREDS     X<n>=<predicate text>
#   @RULES    named when-chain Python vars.  Two shapes:
#              FIRST-MATCH (has else): <var>=X1=>v;X2=>v;else=>v
#                — returns the FIRST matching value; if nothing fires returns else-value (often NULL).
#              ARRAY-ACC (no else):   <var>=X1=>v;X2=>v;X3=>v
#                — ALL matching predicates fire; results collected into an array;
#                  if nothing fires the array is EMPTY [] (NOT null).
#              To distinguish: look for `else=>` at the end of the rule chain.
#              [→col] suffix on a rule means that rule's result is stored directly in col
#              (confirmed by a cv= annotation on the D-edge in @EDGES).
#   @PDICTS   shared predicate dicts:  <var>|<label>=<X<n> or text>
#   @PUSE     predicate cross-reference:  X<n>:<rule_or_edge>,<rule_or_edge>,...
#              lists every named rule + derives edge that references X<n>.
#              If two entries share X<n>, they implement IDENTICAL predicate logic —
#              when X<n> fires, ALL listed columns/rules respond to the same condition.
#              CRITICAL: this means first-match columns (else in @RULES) and
#              array-accumulator columns (no else in @RULES) that share an X<n>
#              can NEVER produce conflicting NULL-vs-non-empty states:
#              if no X<n> fires → first-match = NULL AND array-acc = [] (both empty).
#   @EDGES    one edge per line, code-prefixed:
#     R|<Tn>|<Dn>[|pc=c1,c2][|stream]                  reads table->df
#     W|<Dn>|<Tn>|<mode>[|<fmt>][|mk=k1,k2][|pc=p1,p2][|sink=<class>][|kw=k=v,...] writes df->table
#          sink= custom sink class (e.g. MergeSink) used instead of df.write.saveAsTable
#          kw=   extra constructor kwargs for the sink (e.g. enable_leap_column=True)
#     D|<Dn>|<Dn>|<out>[|sc=c1,c2][|ex=<expr>][|cv=<var>][|rl=X1=>v;...;else=>v][|ws=part(...)/ord(...)][|sf=...][|nf=f1,f2]
#                                                       derives one new column
#                  ex= source expression text (truncated to 80 chars) for non-when-chain derives.
#                      Gives the LLM the actual PySpark/SQL expression rather than just the col list.
#                  cv= named Python Column variable from @RULES used as the expression.
#                      Trace: @RULES entry <var> → this output column.
#                  sf = struct field list (array(struct(...)) outputs).
#                       format: <field>:<kind>=<value> separated by ';'
#                       kinds: L=literal C=column E=expr
#                       For E with nested when-chain, value is:
#                         <expr-text>~~rl=<X1=>v;X2=>v;else=>v>
#                       where ~~rl= delimits the inner conditional rules.
#                  nf = lit-null placeholder field names (collapsed)
#     F|<Dn>|<Dn>[|rc=c1,c2][|pr=<pred>]               filters df->df (row restrict)
#          pr= predicate expression text (truncated to 80 chars); gives the actual filter condition.
#     P|<Dn>|<Dn>[|rm=c1,c2][|kp=c1,c2|*]              projects df->df (col restrict)
#     J|<Dn>|<Dn>|<Dn>|<type>[|jk=l1=r1,l2=r2][|rpc=...][|lpc=...]
#                                                       join (left,right -> target)
#     A|<Dn>|<Dn>[|by=<file>]|gk=k1,k2|out=col=op(in);col=op(in)[|dyn=<note>]   aggregates
#          by= the file this aggregation is performed IN (disambiguates same-shaped GROUP BYs across files)
#          gk= aggregate GROUP key — NOT the same as a window part() partition key
#          dyn= agg column list cannot be enumerated statically (runtime variable); note explains why
#     O|<Dn>|<Dn>|<operator>[|pass]                    opaque helper transform
#          pass = column-set passthrough (output has SAME columns as input;
#                 values may change but NO columns are added or removed;
#                 removing a |pass transform NEVER changes the column set)
#   @WARN     <file>|<category>|<message>
#
# Reverse-lineage rule: to find "which input columns supply VALUES to T<n>.col",
# read @PROV first. If T<n>.col is absent from @PROV, fall back to @EDGES.
# Join keys (jk=) and group keys (gk=) describe row-matching, NOT value sources.
#
# Parsing note: '|' is the FIELD separator AND a legitimate Python operator
# (bitwise/logical OR) inside predicate text. Predicate text always appears
# AFTER a fixed prefix (X<n>=, var=, label=, rl=, ws=, sf=) and runs to end-of-
# line. Split each record on '|' from the LEFT for the first few structural
# fields, then treat the remainder as a single body. Pipes inside that body
# are part of the expression, not new fields.
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_ROLE_SHORT = {
    "passthrough": "P",
    "derived": "D",
    "aggregate": "A",
    "group_key": "G",
}


def _esc(s: str) -> str:
    """Escape only newlines for framing safety.

    We deliberately do NOT escape ``|`` even though it's the field separator.
    PySpark uses ``|`` as the bitwise/logical OR in predicate expressions and
    the LLM reads it correctly when left intact. Records that hold predicate
    text (``@PREDS``, ``@RULES``, ``@PDICTS``, ``rl=`` rule values) always
    place the predicate text *after* a fixed-position ``=`` or final field,
    so a parser can split on the first few ``|`` separators and treat the
    remainder as the predicate body. The LEGEND documents this convention.
    """
    if not s:
        return ""
    return s.replace("\n", " ").replace("\r", "")


class _SymTab:
    """Symbol table for interning table FQNs, dataframe IDs, and predicates.

    Each get-or-assign returns a short ID string. Defined separately for
    tables/dataframes/predicates so namespaces don't collide.
    """

    def __init__(self, prefix: str) -> None:
        self._prefix = prefix
        self._map: dict[str, str] = {}
        self._order: list[str] = []

    def intern(self, key: str) -> str:
        if key in self._map:
            return self._map[key]
        sid = f"{self._prefix}{len(self._order)}"
        self._map[key] = sid
        self._order.append(key)
        return sid

    def maybe(self, key: str) -> Optional[str]:
        return self._map.get(key)

    def items(self) -> list[tuple[str, str]]:
        return [(self._map[k], k) for k in self._order]


# ---------------------------------------------------------------------------
# Provenance encoder
# ---------------------------------------------------------------------------


def _encode_prov_source(
    src_text: str,
    table_sym: _SymTab,
) -> str:
    """Convert a provenance source string to its compact form.

    Inputs look like ``"bronze.health.medical_claim.claim_id"`` (a
    fully-qualified table column). If the prefix matches a known table FQN,
    rewrite as ``"T0.claim_id"``. Otherwise leave as-is (could be a file-local
    column name).
    """
    if not src_text or src_text == "?":
        return src_text or "?"
    # Try the longest matching prefix to avoid splitting "schema.col" wrong
    # when a table is named "schema". We iterate over known FQNs.
    for fqn, sid in [(k, v) for v, k in [(s, f) for s, f in table_sym.items()]]:
        # _SymTab.items() returns (sid, fqn); we want longest fqn first.
        pass
    # Simpler: get sorted FQNs by length desc.
    fqns_by_len = sorted(
        (fqn for _sid, fqn in table_sym.items()),
        key=len,
        reverse=True,
    )
    for fqn in fqns_by_len:
        if src_text.startswith(fqn + "."):
            sid = table_sym.maybe(fqn)
            return f"{sid}.{src_text[len(fqn) + 1:]}"
    return src_text


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_compact_dsl(graph: dict) -> str:
    """Serialize a Graph dict to the DEGraph DSL text format."""
    provenance = _build_column_provenance(graph)

    # Pass 1 — assign IDs to tables.
    table_sym = _SymTab("T")
    for t in graph.get("tables", []):
        table_sym.intern(t["fqn"])

    # Pass 2 — assign IDs to dataframes. We use the normalized id
    # (file:lineno) as the key so cross-file refs collapse cleanly.
    df_sym = _SymTab("D")
    for e in graph.get("edges", []):
        for slot in ("source", "target", "left_source", "right_source"):
            v = e.get(slot)
            if not v:
                continue
            if v.startswith("table:"):
                continue
            df_sym.intern(_norm_id(v))

    # Pass 3 — predicate interning. Walk all rule_logic and predicate_dicts
    # entries; assign IDs to distinct predicate texts.
    pred_sym = _SymTab("X")

    def _intern_rules(rules: list[dict]) -> None:
        for r in rules or []:
            cond = (r.get("cond") or "").strip()
            if cond and cond != "otherwise":
                pred_sym.intern(cond)

    for e in graph.get("edges", []):
        if e.get("kind") != "derives":
            continue
        _intern_rules(e.get("rule_logic") or [])
        for f in e.get("struct_fields") or []:
            _intern_rules(f.get("rule_logic") or [])
    for rules in (graph.get("column_rules") or {}).values():
        _intern_rules(rules)
    for mapping in (graph.get("predicate_dicts") or {}).values():
        for text in mapping.values():
            t = (text or "").strip()
            if t:
                pred_sym.intern(t)

    # ---- Emit ----
    out: list[str] = [LEGEND.rstrip(), ""]

    # @TABLES
    out.append("@TABLES")
    for sid, fqn in table_sym.items():
        t = next((tt for tt in graph.get("tables", []) if tt["fqn"] == fqn), None)
        if t is None:
            continue
        cols = ",".join(c["name"] for c in (t.get("columns") or []))
        wb = ",".join(t.get("written_by") or [])
        rb = ",".join(t.get("read_by") or [])
        out.append(f"{sid}|{fqn}|{cols}|by={wb}|rb={rb}")
    out.append("")

    # @PROV — only nonempty provenance entries (P11 already filtered opaque/unknown)
    # Fix #8 cleanup: the column-provenance walker can surface intermediate /
    # SQL-CTE-alias columns that are NOT real output columns of the written
    # table (e.g. `pp.claim_id`, `aps.any_pcp_flag` from a `SELECT pp.col ...`
    # CTE, or internal pivot join-keys like `claim_id_diag` that the final
    # projection drops). These mislead the LLM and inflate tokens. Filter them:
    #   (a) any name containing "." — a table-qualifier/alias prefix, never a
    #       valid top-level output column name; and
    #   (b) names that are pure join keys renamed for the cascade (they appear
    #       as a JoinsEdge key but are never an output/derived column).
    _all_join_keys: set[str] = set()
    _produced_cols: set[str] = set()
    _rename_outputs: set[str] = set()
    _expr_text = {x.get("id"): (x.get("text") or "") for x in graph.get("expressions", [])}
    for _e in graph.get("edges", []):
        k = _e.get("kind")
        if k in ("derives", "aggregates"):
            for _c in ([_e.get("output_col")] + (_e.get("output_cols") or [])):
                if _c:
                    _produced_cols.add(_c)
        if k == "derives" and _e.get("output_col"):
            # withColumnRenamed edges intern their expr as "rename(old -> new)".
            if _expr_text.get(_e.get("expr_id"), "").startswith("rename("):
                _rename_outputs.add(_e["output_col"])
        if k == "joins":
            for _pair in (_e.get("join_keys") or []):
                for _side in _pair:
                    if _side:
                        _all_join_keys.add(_side)

    def _is_junk_prov_col(c: str) -> bool:
        # (a) alias/qualifier prefix (pp.claim_id, aps.flag) — never a real
        #     top-level column; (b) a RENAMED key used only for the join cascade
        #     (claim_id_diag, customer_id_last_event, …) — a rename-output that is
        #     also a join key is internal plumbing the final projection drops.
        # NOTE: do NOT filter plain join keys — real read columns like customer_id
        # / order_id are join keys yet are genuine columns with valid provenance.
        return ("." in c) or (c in _rename_outputs and c in _all_join_keys)

    prov_lines: list[str] = []
    for fqn, per_col in provenance.items():
        tid = table_sym.maybe(fqn)
        if tid is None:
            continue
        for col, info in per_col.items():
            if _is_junk_prov_col(col):
                continue
            role = info.get("role", "")
            role_short = _ROLE_SHORT.get(role, role)
            from_list = info.get("from") or []
            encoded_from = ",".join(_encode_prov_source(s, table_sym) for s in from_list)
            extras = []
            if info.get("op"):
                extras.append(f"op={info['op']}")
            if info.get("window"):
                ws = info["window"]
                ws_short = _encode_window(ws)
                extras.append(f"win={ws_short}")
            extra_str = (";" + ";".join(extras)) if extras else ""
            prov_lines.append(f"{tid}.{col}={role_short}:{encoded_from}{extra_str}")
    if prov_lines:
        out.append("@PROV")
        out.extend(prov_lines)
        out.append("")

    # @PREDS
    if pred_sym.items():
        out.append("@PREDS")
        for sid, text in pred_sym.items():
            out.append(f"{sid}={_esc(text)}")
        out.append("")

    # Build cv_to_outputs early (needed by both @RULES annotations and @PUSE).
    # Maps: named column_rule var → list of output columns it drives (via cv= edges).
    cv_to_outputs: dict[str, list[str]] = {}
    for _e in graph.get("edges", []):
        if _e.get("kind") != "derives":
            continue
        _cv = _e.get("column_var")
        if _cv:
            _out = _e.get("output_col") or "?"
            cv_to_outputs.setdefault(_cv, []).append(_out)
    # Deduplicate (same cv= might appear on multiple edges).
    cv_to_outputs = {k: list(dict.fromkeys(v)) for k, v in cv_to_outputs.items()}

    # @RULES — named when-chain Python vars (not in any withColumn).
    # Each rule shows [→out1,out2] if it drives named output column(s) via cv= edges,
    # or nothing if it is an intermediate helper consumed inside another expression.
    column_rules = graph.get("column_rules") or {}
    if column_rules:
        out.append("@RULES")
        for var, rules in column_rules.items():
            rule_str = f"{var}={_encode_rule_chain(rules, pred_sym)}"
            driven = cv_to_outputs.get(var, [])
            if driven:
                # Filter out self-references (var == output col) to keep the annotation focused
                ext = [c for c in driven if c != var]
                if ext:
                    rule_str += f"  [→{','.join(ext)}]"
            out.append(rule_str)
        out.append("")

    # @PDICTS — shared predicate dicts.
    pdicts = graph.get("predicate_dicts") or {}
    if pdicts:
        out.append("@PDICTS")
        for var, mapping in pdicts.items():
            for label, text in mapping.items():
                t = (text or "").strip()
                pid = pred_sym.maybe(t)
                ref = pid if pid else _esc(t)
                out.append(f"{var}|{label}={ref}")
        out.append("")

    # ★ Q3 fix — @PUSE: predicate cross-reference.
    # For every X<n>, list the named rules and derives-edge output columns
    # that reference it. This makes "do these two columns share classification
    # logic?" answerable by checking whether they appear on the same X<n>
    # line. Without this, the model has to scan all rule_logic chains in
    # @EDGES and @RULES to discover the shared structure — easy to miss.
    if pred_sym.items():
        usage: dict[str, list[str]] = {sid: [] for sid, _ in pred_sym.items()}

        def _record_rules(rules: list[dict], owner: str) -> None:
            seen_here: set[str] = set()
            for r in rules or []:
                cond = (r.get("cond") or "").strip()
                if cond == "otherwise" or not cond:
                    continue
                pid = pred_sym.maybe(cond)
                if pid and pid not in seen_here:
                    usage[pid].append(owner)
                    seen_here.add(pid)

        # Edges → use output_col (e.g. claim_subcategory, multiple_claim_subcategories)
        for e in graph.get("edges", []):
            if e.get("kind") != "derives":
                continue
            owner = e.get("output_col") or "?"
            _record_rules(e.get("rule_logic") or [], owner)
            for f in e.get("struct_fields") or []:
                if f.get("rule_logic"):
                    _record_rules(f["rule_logic"], f"{owner}.{f.get('field', '?')}")
        # Named rules — also credit their cv= output columns
        for var, rules in (graph.get("column_rules") or {}).items():
            outputs = cv_to_outputs.get(var, [])
            # Use the var name itself first; then the output columns it drives
            _record_rules(rules, var)
            for out_col in outputs:
                _record_rules(rules, out_col)

        used_lines = [
            f"{sid}:{','.join(dict.fromkeys(owners))}"
            for sid, owners in usage.items()
            if owners
        ]
        if used_lines:
            out.append("@PUSE")
            out.extend(used_lines)
            out.append("")

    # Build expression lookup for ex= and pr= attributes (Fix #3 / Fix #4).
    expr_lookup: dict[str, str] = {
        n["id"]: (n.get("text") or "")
        for n in graph.get("expressions", [])
        if n.get("id")
    }

    # @EDGES — one record per edge, code-prefixed.
    out.append("@EDGES")
    for e in graph.get("edges", []):
        line = _encode_edge(e, table_sym, df_sym, pred_sym, expr_lookup)
        if line:
            out.append(line)
    out.append("")

    # @WARN
    warns = graph.get("warnings") or []
    if warns:
        out.append("@WARN")
        for w in warns:
            out.append(
                f"{_esc(w.get('file', ''))}|{_esc(w.get('category', ''))}|{_esc(w.get('message', ''))}"
            )
        out.append("")

    return "\n".join(out).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Edge encoding
# ---------------------------------------------------------------------------


def _df_ref(v: str, df_sym: _SymTab, table_sym: _SymTab) -> str:
    if not v:
        return "?"
    if v.startswith("table:"):
        fqn = v.removeprefix("table:")
        return table_sym.maybe(fqn) or fqn
    return df_sym.maybe(_norm_id(v)) or _norm_id(v)


def _encode_window(ws: dict) -> str:
    parts = []
    if ws.get("partition_cols"):
        parts.append(f"part({','.join(ws['partition_cols'])})")
    if ws.get("order_cols"):
        parts.append(f"ord({','.join(ws['order_cols'])})")
    if ws.get("frame"):
        parts.append(f"frame({ws['frame']})")
    return "/".join(parts) if parts else "-"


def _encode_rule_chain(rules: list[dict], pred_sym: _SymTab) -> str:
    """Emit a when-chain as ``X1=>'val';X2=>'val';else=>'val'``."""
    items: list[str] = []
    for r in rules or []:
        cond = (r.get("cond") or "").strip()
        val = _esc(str(r.get("value", "")))
        if cond == "otherwise":
            items.append(f"else=>{val}")
        else:
            pid = pred_sym.maybe(cond)
            ref = pid if pid else _esc(cond)
            items.append(f"{ref}=>{val}")
    return ";".join(items)


def _encode_struct_fields(sf: list[dict], pred_sym: _SymTab) -> tuple[str, list[str]]:
    """Return (sf_string, null_field_names). Collapses lit(None) placeholders.

    sf_string format: ``field:K=value`` separated by ``;`` where K is
    L/C/E (literal/column/expr).

    For expr-kind fields that contain a nested ``when()`` chain (e.g.
    ``concat_ws('_', ..., when(...).otherwise(...))``), emit BOTH the
    truncated full-expression text AND the rule chain, separated by ``~~rl=``:
        field:E=<truncated expr text>~~rl=<X1=>v;X2=>v;else=>v>
    so the model can see both the structural wrapper (concat_ws, coalesce,
    etc.) AND the inner classification logic.
    """
    kept: list[str] = []
    null_names: list[str] = []
    for f in sf or []:
        kind = f.get("kind", "")
        field = f.get("field", "")
        value = f.get("value")
        if kind == "lit" and value in ("None", None):
            null_names.append(str(field))
            continue
        kshort = {"lit": "L", "col": "C", "expr": "E"}.get(kind, kind[:1].upper() or "?")
        if kind == "col":
            srcs = f.get("source_cols") or []
            v = ",".join(srcs) if srcs else (value or "")
        elif kind == "expr" and f.get("rule_logic"):
            rl_text = _encode_rule_chain(f["rule_logic"], pred_sym)
            val_text = _esc(str(value or ""))
            if val_text:
                # Always emit both: the truncated full-expression text (so the
                # model sees the outer wrapper like concat_ws / coalesce) AND
                # the inner rule chain (so it sees the conditional logic).
                # Cap the value at 250 chars to keep tokens bounded; the rule
                # chain is the authoritative short form of the inner logic.
                if len(val_text) > 250:
                    val_text = val_text[:247] + "..."
                v = f"{val_text}~~rl={rl_text}"
            else:
                v = rl_text
        else:
            v = _esc(str(value or ""))
        kept.append(f"{field}:{kshort}={v}")
    return ";".join(kept), null_names


def _encode_edge(
    e: dict,
    table_sym: _SymTab,
    df_sym: _SymTab,
    pred_sym: _SymTab,
    expr_lookup: Optional[dict[str, str]] = None,
) -> str:
    kind = e.get("kind", "")
    src = _df_ref(e.get("source") or e.get("left_source", ""), df_sym, table_sym)
    tgt = _df_ref(e.get("target", ""), df_sym, table_sym)

    if kind == "reads":
        parts = [f"R", src, tgt]
        if e.get("projected_cols"):
            parts.append("pc=" + ",".join(e["projected_cols"]))
        if e.get("streaming"):
            parts.append("stream")
        return "|".join(parts)

    if kind == "writes":
        parts = [f"W", src, tgt, e.get("mode", "")]
        if e.get("format"):
            parts.append(e["format"])
        if e.get("merge_keys"):
            parts.append("mk=" + ",".join(e["merge_keys"]))
        if e.get("partition_cols"):
            parts.append("pc=" + ",".join(e["partition_cols"]))
        if e.get("streaming"):
            parts.append("stream")
        if e.get("sink_class"):
            parts.append("sink=" + e["sink_class"])
        if e.get("sink_kwargs"):
            # ★ Q6 fix: surface non-core sink kwargs (enable_leap_column=True,
            # strict=True, etc.) so the model can describe full sink behaviour
            # beyond the merge keys.
            kv = ";".join(f"{k}={v}" for k, v in e["sink_kwargs"].items())
            parts.append("kw=" + kv)
        return "|".join(parts)

    if kind == "derives":
        parts = [f"D", src, tgt, e.get("output_col", "?")]
        if e.get("source_cols"):
            parts.append("sc=" + ",".join(e["source_cols"]))
        # Fix #3: emit expression text when there is no rule_logic / struct_fields.
        # Gives the LLM the actual derivation expression (coalesce, concat_ws, etc.)
        # rather than only source column names.
        if (
            not e.get("rule_logic")
            and not e.get("struct_fields")
            and not e.get("column_var")
            and e.get("expr_id")
            and expr_lookup is not None
        ):
            expr_text = expr_lookup.get(e["expr_id"], "")
            if expr_text:
                if len(expr_text) > 80:
                    expr_text = expr_text[:77] + "..."
                parts.append("ex=" + _esc(expr_text))
        if e.get("column_var"):
            parts.append("cv=" + e["column_var"])
        if e.get("rule_logic"):
            parts.append("rl=" + _encode_rule_chain(e["rule_logic"], pred_sym))
        if e.get("window_spec"):
            parts.append("ws=" + _encode_window(e["window_spec"]))
        sf = e.get("struct_fields") or []
        if sf:
            sf_str, null_names = _encode_struct_fields(sf, pred_sym)
            if sf_str:
                parts.append("sf=" + sf_str)
            if null_names:
                if len(null_names) > 30:
                    parts.append(f"nfn={len(null_names)}")
                else:
                    parts.append("nf=" + ",".join(null_names))
        if e.get("dynamic"):
            parts.append("dyn")
        return "|".join(parts)

    if kind == "filters":
        parts = [f"F", src, tgt]
        if e.get("referenced_cols"):
            parts.append("rc=" + ",".join(e["referenced_cols"]))
        # Fix #4: emit predicate text for filter edges.
        # Gives the LLM the actual filter condition rather than only referenced cols.
        if e.get("predicate_id") and expr_lookup is not None:
            pred_text = expr_lookup.get(e["predicate_id"], "")
            if pred_text:
                if len(pred_text) > 80:
                    pred_text = pred_text[:77] + "..."
                parts.append("pr=" + _esc(pred_text))
        return "|".join(parts)

    if kind == "projects":
        parts = [f"P", src, tgt]
        if e.get("removed_cols"):
            parts.append("rm=" + ",".join(e["removed_cols"]))
        elif e.get("kept_cols"):
            kept = e["kept_cols"]
            if kept == ["*"]:
                parts.append("kp=*")
            else:
                parts.append("kp=" + ",".join(kept))
        return "|".join(parts)

    if kind == "joins":
        left = src
        right = _df_ref(e.get("right_source", ""), df_sym, table_sym)
        parts = [f"J", left, right, tgt, e.get("join_type", "inner")]
        if e.get("join_keys"):
            jk = ",".join(f"{l}={r}" for l, r in e["join_keys"])
            parts.append("jk=" + jk)
        if e.get("right_projected_cols"):
            parts.append("rpc=" + ",".join(e["right_projected_cols"]))
        if e.get("left_projected_cols"):
            parts.append("lpc=" + ",".join(e["left_projected_cols"]))
        return "|".join(parts)

    if kind == "aggregates":
        parts = [f"A", src, tgt]
        # Fix #6: per-file attribution on aggregate edges. The src/tgt DataFrame
        # nodes collapse to opaque D<n> symbols with no file legend, so two
        # aggregations in different files (e.g. silver/customer_profile.py vs
        # gold/customer_ltv.py) are otherwise indistinguishable — the model
        # confuses which file each GROUP BY lives in. by=<file> resolves that.
        if e.get("file"):
            parts.append("by=" + e["file"])
        if e.get("group_keys"):
            parts.append("gk=" + ",".join(e["group_keys"]))
        outs = e.get("output_cols") or []
        ops = e.get("agg_ops") or []
        ins = e.get("agg_inputs") or []
        if outs:
            pairs = []
            for i, out_col in enumerate(outs):
                op = ops[i] if i < len(ops) else "?"
                inp = ins[i] if i < len(ins) else ""
                pairs.append(f"{out_col}={op}({inp})" if inp else f"{out_col}={op}()")
            parts.append("out=" + ";".join(pairs))
        if e.get("dynamic"):
            note = _esc(e.get("dynamic_note") or "")
            parts.append(f"dyn={note}" if note else "dyn")
        return "|".join(parts)

    if kind == "opaque_transform":
        parts = [f"O", src, tgt, e.get("operator", "?")]
        if e.get("is_passthrough"):
            parts.append("pass")
        return "|".join(parts)

    return ""
