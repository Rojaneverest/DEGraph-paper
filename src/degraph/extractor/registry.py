"""HelperRegistry + SinkRegistry — load ``.degraph/helpers.json`` and ``sinks.json``.

These registries let the extractor handle custom/imported functions that would
otherwise produce opaque edges:

* **HelperRegistry** — maps function names to their semantic kind
  (``passthrough`` or ``suffix_rename``), so the extractor can emit precise
  edges instead of falling back to ``OpaqueTransform``.

* **SinkRegistry** — maps custom sink class names to the kwargs that carry the
  target table FQN and write mode, so the extractor can resolve them to proper
  ``Writes`` edges with ``sink_class`` populated.

Both registries are optional (empty registries are valid; files that aren't
registered fall back to the default heuristics).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Specs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HelperSpec:
    """Describes a registered helper function."""

    func_name: str
    """Short (unqualified) function name, e.g. ``trim_string_columns``."""

    module: str
    """Module path, e.g. ``utils.column_transformations``."""

    kind: str
    """Semantic kind:
      - ``"passthrough"``   — preserves column set (trim, cast-all-strings, etc.)
      - ``"suffix_rename"`` — renames every column by appending a suffix
    """

    suffix_arg: Optional[int] = None
    """For ``suffix_rename``: 0-based positional index of the suffix string arg."""

    @property
    def qualified_name(self) -> str:
        return f"{self.module}.{self.func_name}"


@dataclass(frozen=True)
class SinkSpec:
    """Describes a registered custom sink class."""

    class_name: str
    """Short class name, e.g. ``DeltaMergeSink``."""

    module: str
    """Module path, e.g. ``utils.sinks``."""

    target_kwarg: str
    """Name of the constructor kwarg that holds the target table FQN."""

    mode_kwarg: str
    """Name of the constructor kwarg that holds the write mode."""

    merge_keys_kwarg: Optional[str] = None
    """Name of the constructor kwarg that holds the merge key list (for MERGE mode)."""

    default_mode: str = "overwrite"
    """Mode to use when ``mode_kwarg`` is absent."""


# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------


class HelperRegistry:
    """Index of registered helper functions loaded from ``.degraph/helpers.json``.

    The JSON schema is::

        {
            "<func_name>": {
                "module":     "utils.column_transformations",
                "kind":       "passthrough" | "suffix_rename",
                "suffix_arg": 1          // optional; only for suffix_rename
            },
            ...
        }
    """

    def __init__(self, degraph_dir: Optional[Path] = None):
        # Maps both short name and qualified name → HelperSpec
        self._by_name: dict[str, HelperSpec] = {}
        if degraph_dir is not None:
            self._load(degraph_dir / "helpers.json")

    def _load(self, path: Path) -> None:
        if not path.exists():
            return
        data: dict = json.loads(path.read_text(encoding="utf-8"))
        for func_name, attrs in data.items():
            # Skip JSON metadata keys like "$comment"
            if not isinstance(attrs, dict):
                continue
            spec = HelperSpec(
                func_name=func_name,
                module=attrs["module"],
                kind=attrs["kind"],
                suffix_arg=attrs.get("suffix_arg"),
            )
            self._by_name[func_name] = spec
            self._by_name[spec.qualified_name] = spec

    def get(self, name: str) -> Optional[HelperSpec]:
        """Look up by short name *or* fully-qualified name."""
        return self._by_name.get(name)

    def is_registered(self, name: str) -> bool:
        return name in self._by_name


class SinkRegistry:
    """Index of registered sink classes loaded from ``.degraph/sinks.json``.

    The JSON schema is::

        {
            "<ClassName>": {
                "module":           "utils.sinks",
                "target_kwarg":     "target_table",
                "mode_kwarg":       "mode",
                "merge_keys_kwarg": "merge_keys",   // optional
                "default_mode":     "merge"
            },
            ...
        }
    """

    def __init__(self, degraph_dir: Optional[Path] = None):
        self._by_class: dict[str, SinkSpec] = {}
        if degraph_dir is not None:
            self._load(degraph_dir / "sinks.json")

    def _load(self, path: Path) -> None:
        if not path.exists():
            return
        data: dict = json.loads(path.read_text(encoding="utf-8"))
        for class_name, attrs in data.items():
            if not isinstance(attrs, dict):
                continue
            spec = SinkSpec(
                class_name=class_name,
                module=attrs["module"],
                target_kwarg=attrs["target_kwarg"],
                mode_kwarg=attrs["mode_kwarg"],
                merge_keys_kwarg=attrs.get("merge_keys_kwarg"),
                default_mode=attrs.get("default_mode", "overwrite"),
            )
            self._by_class[class_name] = spec

    def get(self, class_name: str) -> Optional[SinkSpec]:
        return self._by_class.get(class_name)

    def is_registered(self, class_name: str) -> bool:
        return class_name in self._by_class
