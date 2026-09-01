"""DataFrameTracker — forward-chain variable tracking (Decision 3.2).

Every Python assignment that binds a variable to a DataFrame expression
creates a *new* ``DataFrameNode``.  When the same variable name is reused
(e.g. ``orders = trim_string_columns(orders)``), the old node is *not*
deleted — it stays in the graph, connected to the new node by an edge.

The tracker maps each variable name to its **most-recently-assigned** node ID
so that subsequent reads of that variable name resolve to the correct node.

This is per-file state; the assembler creates a fresh tracker for each file.
"""

from __future__ import annotations

from degraph.graph import DataFrameNode, UNRESOLVED


class DataFrameTracker:
    """Map variable names to their current ``DataFrameNode`` for one file."""

    def __init__(self) -> None:
        # node_id → DataFrameNode
        self._nodes: dict[str, DataFrameNode] = {}
        # var_name → currently-active node_id
        self._current: dict[str, str] = {}
        # monotonic counter for generating anonymous node IDs
        self._anon_counter: int = 0

    # ------------------------------------------------------------------ #
    # Registration                                                         #
    # ------------------------------------------------------------------ #

    def register(self, node: DataFrameNode) -> None:
        """Store a ``DataFrameNode`` without binding it to any variable name.

        Use this for anonymous intermediate nodes (SQL CTEs, filter+drop
        chain intermediates) where no Python variable name is assigned.
        """
        self._nodes[node.id] = node

    def assign(self, var_name: str, node: DataFrameNode) -> None:
        """Bind *var_name* to *node* and register the node.

        If *var_name* was previously bound to a different node, that old node
        remains in the graph; only the current-pointer advances.
        """
        self._nodes[node.id] = node
        self._current[var_name] = node.id

    # ------------------------------------------------------------------ #
    # Lookup                                                               #
    # ------------------------------------------------------------------ #

    def get_id(self, var_name: str) -> str | None:
        """Return the current node ID for *var_name*, or ``None``."""
        return self._current.get(var_name)

    def get_node(self, var_name: str) -> DataFrameNode | None:
        """Return the current ``DataFrameNode`` for *var_name*, or ``None``."""
        node_id = self._current.get(var_name)
        return self._nodes.get(node_id) if node_id else None

    def get_node_by_id(self, node_id: str) -> DataFrameNode | None:
        """Look up a node by its ID regardless of variable binding."""
        return self._nodes.get(node_id)

    def is_known(self, var_name: str) -> bool:
        """Return ``True`` if *var_name* is currently bound to a DataFrame."""
        return var_name in self._current

    # ------------------------------------------------------------------ #
    # Anonymous node helpers                                               #
    # ------------------------------------------------------------------ #

    def make_anon_id(self, file: str, prefix: str) -> str:
        """Generate a unique anonymous DataFrameNode ID."""
        self._anon_counter += 1
        return f"df:{file}:{prefix}:_anon{self._anon_counter}"

    # ------------------------------------------------------------------ #
    # Output                                                               #
    # ------------------------------------------------------------------ #

    def all_nodes(self) -> list[DataFrameNode]:
        """Return every registered ``DataFrameNode`` (assigned + anonymous)."""
        return list(self._nodes.values())
