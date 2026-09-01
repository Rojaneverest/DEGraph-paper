"""NotebookPreprocessor — convert Databricks notebooks to clean Python source.

Handles two formats:
  - ``.py`` Databricks notebook exports (``# COMMAND ----------`` separators,
    ``# MAGIC `` line prefix, ``%run`` / ``%md`` / ``%sql`` magics)
  - ``.ipynb`` Jupyter notebooks (JSON cell array, code/markdown cell types)

The output is a ``PreparedSource`` whose ``.source`` can be fed directly to
``ast.parse()``.  ``%run`` targets are extracted separately so the file
extractor can seed the symbol table before parsing.

Line-number fidelity for ``.py`` files: blank lines are inserted in place of
stripped cells so that ``ast`` line numbers match the original file's lines.
For ``.ipynb`` files, cell-based numbering is used (``cell<N>`` prefix).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAGIC_PREFIX = "# MAGIC "
_MAGIC_BARE = "# MAGIC"
_CELL_SEP = "# COMMAND ----------"

# Magic directives that indicate a cell whose content should be skipped
_SKIP_DIRECTIVES = ("%md", "%sql", "%sh", "%r", "%scala", "%fs", "%pip")

# Magic directive for %run
_RUN_DIRECTIVE = "%run"


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------


@dataclass
class PreparedSource:
    """Preprocessed source ready for ``ast.parse()``."""

    source: str
    """Combined Python source text (blank lines in place of stripped cells)."""

    run_targets: list[str] = field(default_factory=list)
    """`%run` paths in encounter order (relative to the notebook's directory)."""

    is_ipynb: bool = False
    """True when the source came from a ``.ipynb`` file."""

    cell_start_lines: list[int] = field(default_factory=list)
    """For ``.ipynb``: 1-based line number in ``source`` where each kept cell starts."""

    cell_global_indices: list[int] = field(default_factory=list)
    """For ``.ipynb``: 1-based global cell index (counting all cells, incl. markdown/skipped)
    for each kept code cell, parallel to ``cell_start_lines``."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def prepare(path: Path) -> PreparedSource:
    """Dispatch to the correct preprocessor based on file extension."""
    if path.suffix == ".ipynb":
        return prepare_ipynb(path)
    return prepare_py(path)


def prepare_py(path: Path) -> PreparedSource:
    """Prepare a Databricks ``.py`` notebook export.

    Cells are separated by ``# COMMAND ----------`` lines.  Magic lines begin
    with ``# MAGIC ``.  Skipped cells are replaced with an equal number of
    blank lines to preserve original file line numbers.
    """
    raw = path.read_text(encoding="utf-8")
    lines = raw.splitlines()

    run_targets: list[str] = []
    output_lines: list[str] = []

    # Split into raw cells
    cells: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        if line.rstrip() == _CELL_SEP:
            cells.append(current)
            current = []
        else:
            current.append(line)
    cells.append(current)

    for cell in cells:
        magic_lines = [l for l in cell if l.startswith(_MAGIC_PREFIX) or l.rstrip() == _MAGIC_BARE]
        code_lines = [l for l in cell if not l.startswith(_MAGIC_PREFIX) and l.rstrip() != _MAGIC_BARE]

        if magic_lines:
            # Recover content by stripping the prefix
            content: list[str] = []
            for l in magic_lines:
                if l.rstrip() == _MAGIC_BARE:
                    content.append("")
                else:
                    content.append(l[len(_MAGIC_PREFIX):])

            directive = content[0].strip() if content else ""

            if directive.startswith(_RUN_DIRECTIVE):
                target = directive[len(_RUN_DIRECTIVE):].strip()
                run_targets.append(target)
                # Blank out this cell to preserve line offsets
                output_lines.extend([""] * len(cell))
                continue

            if any(directive.startswith(d) for d in _SKIP_DIRECTIVES):
                output_lines.extend([""] * len(cell))
                continue

            # Otherwise preserve the magic content (e.g. inline SQL run via magic)
            output_lines.extend(content)

        else:
            # Pure Python cell — keep as-is (include the COMMAND separator line
            # as a blank so line numbers stay aligned)
            output_lines.extend(code_lines if code_lines else [""] * len(cell))

    return PreparedSource(
        source="\n".join(output_lines),
        run_targets=run_targets,
        is_ipynb=False,
    )


def prepare_ipynb(path: Path) -> PreparedSource:
    """Prepare a Jupyter ``.ipynb`` notebook.

    Code cells are concatenated with a blank-line separator.  Markdown/raw
    cells are skipped.  ``%run`` cells are extracted and skipped.

    **Cell numbering**: because ``.ipynb`` cells don't map cleanly to file
    line numbers, DataFrameNode IDs for this format use ``cell<N>:<lineno>``
    where ``N`` is the 1-based index of the code cell in the notebook and
    ``lineno`` is the line within that cell.  The ``cell_start_lines`` list
    records where each kept cell begins in the joined source for the extractor
    to use.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    cells = data.get("cells", [])

    run_targets: list[str] = []
    parts: list[str] = []
    cell_start_lines: list[int] = []
    cell_global_indices: list[int] = []
    current_line = 1  # 1-based

    for global_idx, cell in enumerate(cells, start=1):
        if cell.get("cell_type") != "code":
            continue

        raw_source = "".join(cell.get("source", []))
        cell_lines = raw_source.splitlines()

        if not cell_lines:
            continue

        # Strip Databricks-style `# MAGIC ` prefixes if present
        clean_lines: list[str] = []
        for l in cell_lines:
            if l.startswith(_MAGIC_PREFIX):
                clean_lines.append(l[len(_MAGIC_PREFIX):])
            else:
                clean_lines.append(l)

        # Determine the leading directive (first non-empty line)
        directive = next((l.strip() for l in clean_lines if l.strip()), "")

        if directive.startswith(_RUN_DIRECTIVE):
            target = directive[len(_RUN_DIRECTIVE):].strip()
            run_targets.append(target)
            continue

        if any(directive.startswith(d) for d in _SKIP_DIRECTIVES):
            continue

        cell_start_lines.append(current_line)
        cell_global_indices.append(global_idx)
        parts.append("\n".join(clean_lines))
        current_line += len(clean_lines) + 1  # +1 for the blank separator between parts

    return PreparedSource(
        source="\n\n".join(parts),
        run_targets=run_targets,
        is_ipynb=True,
        cell_start_lines=cell_start_lines,
        cell_global_indices=cell_global_indices,
    )
