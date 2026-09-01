"""ScopeConfig — filter which files DEGraph processes within a repo.

Real-world data-engineering monorepos contain hundreds of subdirectories,
most of which are irrelevant to any single lineage question (tests, vendor
code, dashboards, infrastructure-as-code, sample notebooks, etc.). Pointing
the extractor at the repo root would waste time, inflate the graph with
noise, and risk hitting OpaqueTransform fallbacks for code patterns the
extractor was never designed to handle.

``ScopeConfig`` solves this with three independent filters applied during
file discovery:

  - ``scope_dirs`` — only walk these subdirectories (paths relative to the
    repo root). If empty, the entire repo is walked. Pruning happens at the
    walk level, not post-hoc, so unscoped subtrees are never touched.
  - ``include`` — glob patterns (relative-path); a file must match at least
    one to be kept. If empty, all walked files are candidates.
  - ``exclude`` — glob patterns (relative-path); any match drops the file.
    Always applied last, so excludes win over includes.

Patterns use ``fnmatch``-style globs with ``**`` supported via the ``**/``
prefix idiom (``**/*.py``, ``**/tests/**``). All matching is done against
the POSIX-style relative path from the repo root.

Loading order:

  1. Defaults (empty filters → walk everything).
  2. ``<repo_root>/.degraph/scope.json`` if present.
  3. ``--config <path>`` CLI flag if supplied.
  4. Inline ``--include``/``--exclude``/``--scope`` CLI flags (append to
     whatever was loaded; explicit later wins).

Keeping the config in source control (``.degraph/scope.json`` in the
corporate repo, not in DEGraph itself) means every collaborator extracts
the same subgraph.
"""

from __future__ import annotations

import fnmatch
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class ScopeConfig:
    """Filter configuration for which files DEGraph processes."""

    scope_dirs: list[str] = field(default_factory=list)
    """Subdirectories (relative to repo root) to walk. Empty = walk all."""

    include: list[str] = field(default_factory=list)
    """fnmatch-style globs; file must match at least one. Empty = accept all walked files."""

    exclude: list[str] = field(default_factory=list)
    """fnmatch-style globs; matching files are dropped. Always applied last."""

    # ------------------------------------------------------------------ #
    # Loading                                                              #
    # ------------------------------------------------------------------ #

    @classmethod
    def empty(cls) -> ScopeConfig:
        """Return a permissive config that walks the entire repo."""
        return cls()

    @classmethod
    def from_dict(cls, data: dict) -> ScopeConfig:
        """Build from a parsed JSON dict.

        Accepted keys: ``scope_dirs``, ``include``, ``exclude``. Unknown
        keys are silently ignored (forward-compatible).
        """
        return cls(
            scope_dirs=list(data.get("scope_dirs") or []),
            include=list(data.get("include") or []),
            exclude=list(data.get("exclude") or []),
        )

    @classmethod
    def from_file(cls, path: Path) -> ScopeConfig:
        """Load from a JSON file."""
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls.from_dict(data)

    @classmethod
    def auto_load(cls, repo_root: Path) -> ScopeConfig:
        """Load ``<repo_root>/.degraph/scope.json`` if it exists; else empty."""
        candidate = repo_root / ".degraph" / "scope.json"
        if candidate.exists():
            return cls.from_file(candidate)
        return cls.empty()

    # ------------------------------------------------------------------ #
    # Merging                                                              #
    # ------------------------------------------------------------------ #

    def merge(self, other: ScopeConfig) -> ScopeConfig:
        """Append ``other``'s patterns to this config; return a new instance.

        Used for layering CLI flags on top of a file-loaded config. The
        union semantics matches user intent: adding ``--include "**/*.sql"``
        on the command line should extend, not replace, the file's includes.
        """
        return ScopeConfig(
            scope_dirs=list(dict.fromkeys(self.scope_dirs + other.scope_dirs)),
            include=list(dict.fromkeys(self.include + other.include)),
            exclude=list(dict.fromkeys(self.exclude + other.exclude)),
        )

    # ------------------------------------------------------------------ #
    # Matching                                                             #
    # ------------------------------------------------------------------ #

    def matches(self, rel_path: str) -> bool:
        """Return True if a file at ``rel_path`` (POSIX, relative to repo root)
        passes the include/exclude filters.

        The ``scope_dirs`` filter is NOT checked here — it is applied at the
        walk level so unscoped subtrees are never enumerated. This method
        is for the include/exclude pass that runs on each candidate file.
        """
        # Exclude wins over include.
        for pat in self.exclude:
            if _glob_match(rel_path, pat):
                return False
        if not self.include:
            return True
        for pat in self.include:
            if _glob_match(rel_path, pat):
                return True
        return False

    # ------------------------------------------------------------------ #
    # Summary (for CLI output)                                             #
    # ------------------------------------------------------------------ #

    def summary(self) -> str:
        """Return a one-line human-readable description."""
        parts = []
        if self.scope_dirs:
            parts.append(f"scope_dirs={self.scope_dirs}")
        if self.include:
            parts.append(f"include={self.include}")
        if self.exclude:
            parts.append(f"exclude={self.exclude}")
        if not parts:
            return "no filters (walk entire repo)"
        return "; ".join(parts)


# ---------------------------------------------------------------------------
# Glob matching with ** support
# ---------------------------------------------------------------------------


def _glob_match(rel_path: str, pattern: str) -> bool:
    """Match a POSIX rel path against a glob pattern that may contain ``**``.

    ``fnmatch.fnmatch`` does not treat ``**`` as "any number of directories";
    it treats each ``*`` as "any chars except slash" only when the OS is
    POSIX-y, and otherwise as "any chars including slash". To get
    cross-platform deterministic behavior we expand ``**`` ourselves.

    Supported patterns:
      ``**/*.py``         — any .py file at any depth
      ``bronze/**/*.py``  — any .py file under bronze/ at any depth
      ``**/tests/**``     — anything under any tests/ directory
      ``*.py``            — .py files at the repo root only
      ``bronze/*.py``     — .py files directly inside bronze/
    """
    # Normalize path separators just in case.
    rel_path = rel_path.replace("\\", "/")
    pattern = pattern.replace("\\", "/")

    # Build a regex from the glob so ** works correctly.
    import re

    # Translate the pattern. We do this segment by segment.
    parts = pattern.split("/")
    regex_parts: list[str] = []
    for i, part in enumerate(parts):
        if part == "**":
            # Match zero or more path segments. We emit a single non-greedy
            # ".*" and let the surrounding "/" separators be optional.
            regex_parts.append(".*")
        else:
            # Translate single-segment glob ("*" and "?") to regex chars,
            # but "*" here means "any chars except /" within one segment.
            seg = ""
            for ch in part:
                if ch == "*":
                    seg += "[^/]*"
                elif ch == "?":
                    seg += "[^/]"
                else:
                    seg += re.escape(ch)
            regex_parts.append(seg)

    # Join with "/" then fix up the three "**" boundary cases so ** correctly
    # means "zero or more directories":
    #   - Leading  "**/foo"  must match "foo" (no parent dir)  → wrap in (?:.*/)?
    #   - Trailing "foo/**"  must match "foo" (no child path)  → wrap in (?:/.*)?
    #   - Middle   "foo/**/bar" must match "foo/bar"           → (?:/.*/|/)
    regex_str = "/".join(regex_parts)
    if regex_str.startswith(".*/"):
        regex_str = "(?:.*/)?" + regex_str[3:]
    if regex_str.endswith("/.*"):
        regex_str = regex_str[:-3] + "(?:/.*)?"
    # Handle a bare "**" pattern (entire pattern is just ".*" after segment translation)
    if regex_str == ".*":
        regex_str = ".*"
    regex_str = regex_str.replace("/.*/", "(?:/.*/|/)")
    regex_str = "^" + regex_str + "$"

    return re.match(regex_str, rel_path) is not None
