"""SafeEvaluator — statically resolve Python AST expressions to string values.

Used to turn f-strings like ``f"{database}.orders_raw"`` into the literal
string ``"main.dbdemos_ecom.orders_raw"`` given a symbol table populated from
``%run`` targets and top-level assignments.

Only handles the subset of Python that appears in Databricks config files:
  - String literals
  - Variable references  (``catalog``, ``db``, ``database``)
  - F-strings           (``f"{catalog}.{db}"``)
  - String concatenation (``"a" + "b"``)

Everything else returns ``UNRESOLVED`` — no exceptions are raised to callers.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

from degraph.graph import UNRESOLVED


# ---------------------------------------------------------------------------
# SymbolTable
# ---------------------------------------------------------------------------


@dataclass
class SymbolTable:
    """Flat dict of ``variable_name → string_value``.

    Seeded by loading ``%run`` target files and any config module imported via
    ``%run ../_resources/setup``.  The evaluator walks the seeded module's
    top-level assignments and records every one it can statically resolve.
    """

    symbols: dict[str, str] = field(default_factory=dict)

    def update_from_ast(self, tree: ast.Module) -> None:
        """Walk a parsed AST module, recording resolvable string assignments.

        Only top-level ``name = expr`` assignments are processed (not tuple
        unpacking, augmented assignment, or annotated assignment).  The
        evaluator is bootstrapped from the *current* symbol table so that
        chained definitions like::

            catalog = "main"
            db      = "dbdemos_ecom"
            database = f"{catalog}.{db}"

        resolve correctly in a single pass.
        """
        evaluator = SafeEvaluator(self.symbols)
        for stmt in tree.body:
            if not isinstance(stmt, ast.Assign):
                continue
            if len(stmt.targets) != 1:
                continue
            target = stmt.targets[0]
            if not isinstance(target, ast.Name):
                continue
            resolved = evaluator.resolve(stmt.value)
            if not resolved.startswith(UNRESOLVED):
                self.symbols[target.id] = resolved

    def update_from_file(self, path: Path) -> None:
        """Parse a ``.py`` file and load its resolvable assignments."""
        try:
            source = path.read_text(encoding="utf-8")
        except OSError:
            return
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError:
            return
        self.update_from_ast(tree)

    def get(self, name: str, default: str = UNRESOLVED) -> str:
        return self.symbols.get(name, default)

    def __contains__(self, name: str) -> bool:
        return name in self.symbols


# ---------------------------------------------------------------------------
# SafeEvaluator
# ---------------------------------------------------------------------------


class SafeEvaluator:
    """Evaluates AST expression nodes to string values without executing code.

    Returns ``UNRESOLVED`` (never raises) for anything it cannot resolve.
    """

    def __init__(self, symbols: dict[str, str]):
        self.symbols = symbols

    def resolve(self, node: ast.expr | None) -> str:
        """Try to evaluate *node* to a string.  Returns ``UNRESOLVED`` on failure."""
        if node is None:
            return UNRESOLVED
        try:
            return self._eval(node)
        except _CannotEvaluate:
            return UNRESOLVED

    # ------------------------------------------------------------------
    # Internal evaluation (raises _CannotEvaluate on failure)
    # ------------------------------------------------------------------

    def _eval(self, node: ast.expr) -> str:  # noqa: PLR0911
        # --- String / bytes / numeric literal ---
        if isinstance(node, ast.Constant):
            if isinstance(node.value, str):
                return node.value
            raise _CannotEvaluate(f"Non-string constant: {node.value!r}")

        # --- Variable reference ---
        if isinstance(node, ast.Name):
            if node.id in self.symbols:
                return self.symbols[node.id]
            raise _CannotEvaluate(f"Unknown variable: {node.id!r}")

        # --- F-string: ast.JoinedStr ---
        if isinstance(node, ast.JoinedStr):
            parts: list[str] = []
            for part in node.values:
                if isinstance(part, ast.Constant):
                    parts.append(str(part.value))
                elif isinstance(part, ast.FormattedValue):
                    # Ignore format_spec / conversion for our purposes
                    parts.append(self._eval(part.value))
                else:
                    raise _CannotEvaluate(f"Unhandled JoinedStr part: {type(part).__name__}")
            return "".join(parts)

        # --- String concatenation: "a" + var ---
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return self._eval(node.left) + self._eval(node.right)

        # --- Attribute access: module.ATTR ---
        if isinstance(node, ast.Attribute):
            # Try resolving the object first; if it's a known symbol, key with dot
            try:
                obj_str = self._eval(node.value)
                key = f"{obj_str}.{node.attr}"
                if key in self.symbols:
                    return self.symbols[key]
            except _CannotEvaluate:
                pass
            # Also try bare `obj_id.attr` when obj is a Name
            if isinstance(node.value, ast.Name):
                key = f"{node.value.id}.{node.attr}"
                if key in self.symbols:
                    return self.symbols[key]
            raise _CannotEvaluate(f"Unresolved attribute access")

        raise _CannotEvaluate(f"Unhandled AST node type: {type(node).__name__}")


class _CannotEvaluate(Exception):
    """Internal sentinel — never escapes SafeEvaluator."""
