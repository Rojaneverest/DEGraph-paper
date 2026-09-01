"""DEGraph — Static Data Lineage Graph Extraction for LLM Context Optimization.

Top-level package. The schema lives in `degraph.graph`; everything else
(extractors, evaluators, serializers) imports from there.
"""

from degraph.graph import (
    # Enums
    WriteMode,
    JoinType,
    NodeKind,
    EdgeKind,
    OpaqueKind,
    # Sub-models
    Column,
    WindowSpec,
    # Node models
    Table,
    ExternalSource,
    DataFrameNode,
    Expression,
    # Edge models
    ReadsEdge,
    WritesEdge,
    DerivesEdge,
    FiltersEdge,
    ProjectsEdge,
    JoinsEdge,
    AggregatesEdge,
    OpaqueTransformEdge,
    Edge,
    # Container
    Graph,
    GraphMetadata,
    GraphWarning,
    # Constants
    UNRESOLVED,
    SCHEMA_VERSION,
)

__version__ = "0.1.0"

__all__ = [
    "WriteMode",
    "JoinType",
    "NodeKind",
    "EdgeKind",
    "OpaqueKind",
    "Column",
    "WindowSpec",
    "Table",
    "ExternalSource",
    "DataFrameNode",
    "Expression",
    "ReadsEdge",
    "WritesEdge",
    "DerivesEdge",
    "FiltersEdge",
    "ProjectsEdge",
    "JoinsEdge",
    "AggregatesEdge",
    "OpaqueTransformEdge",
    "Edge",
    "Graph",
    "GraphMetadata",
    "GraphWarning",
    "UNRESOLVED",
    "SCHEMA_VERSION",
    "__version__",
]
