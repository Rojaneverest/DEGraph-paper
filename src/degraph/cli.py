"""DEGraph CLI.

Commands:

    degraph extract <repo_dir>    Run the static extractor and write the
                                  full Graph JSON.
    degraph context <repo_dir>    Run the extractor + build the compact
                                  graph + wrap it in a paste-ready prompt
                                  for an in-editor LLM (Cursor, Copilot,
                                  Claude Code, etc.).
    degraph compare <gt> <ex>     Compare an extracted graph against a
                                  ground-truth graph.

All three commands accept the same set of ``--include`` / ``--exclude``
/ ``--scope`` / ``--config`` flags for filtering which files are
processed. See ``docs/CORPORATE_USAGE.md`` for the recommended workflow
on a large monorepo where only a subset of folders is relevant.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import click

from degraph.compact import build_compact_graph
from degraph.extractor.assembler import extract_repo
from degraph.extractor.scope import ScopeConfig


# ---------------------------------------------------------------------------
# Shared options
# ---------------------------------------------------------------------------


def _scope_options(f):
    """Decorator that adds --include / --exclude / --scope / --config flags."""
    f = click.option(
        "--config",
        "config_path",
        default=None,
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        help="Path to a scope JSON config file. Overrides .degraph/scope.json auto-discovery.",
    )(f)
    f = click.option(
        "--include-from",
        "include_from",
        default=None,
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        help="File with one include-glob per line (lines starting with '#' are comments).",
    )(f)
    f = click.option(
        "--scope",
        "scope_dirs",
        multiple=True,
        help="Subdirectory (relative to REPO_DIR) to restrict the walk to. Repeatable.",
    )(f)
    f = click.option(
        "--exclude",
        "exclude_patterns",
        multiple=True,
        help="Glob pattern to exclude (e.g. '**/tests/**'). Repeatable.",
    )(f)
    f = click.option(
        "--include",
        "include_patterns",
        multiple=True,
        help="Glob pattern to include (e.g. '**/*.py'). Repeatable.",
    )(f)
    return f


def _build_scope(
    repo_dir: Path,
    config_path: Optional[Path],
    scope_dirs: tuple[str, ...],
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
    include_from: Optional[Path],
) -> ScopeConfig:
    """Layer scope sources in the documented precedence order.

    1. Explicit ``--config`` if provided; else ``<repo>/.degraph/scope.json``
       if it exists; else empty.
    2. ``--include-from`` file contents appended to includes.
    3. CLI ``--include``, ``--exclude``, ``--scope`` flags appended.

    The "append" semantics are intentional: a corporate user can commit a
    baseline scope file and then pass ``--include`` flags ad hoc to widen
    a specific extraction without editing the committed config.
    """
    if config_path is not None:
        base = ScopeConfig.from_file(config_path)
    else:
        base = ScopeConfig.auto_load(repo_dir)

    cli_overlay = ScopeConfig(
        scope_dirs=list(scope_dirs),
        include=list(include_patterns),
        exclude=list(exclude_patterns),
    )

    if include_from is not None:
        extra_includes: list[str] = []
        for raw in include_from.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            extra_includes.append(line)
        cli_overlay = cli_overlay.merge(ScopeConfig(include=extra_includes))

    return base.merge(cli_overlay)


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
def main() -> None:
    """DEGraph — Static Data Lineage Graph Extraction for LLM Context Optimization."""


# ---------------------------------------------------------------------------
# extract
# ---------------------------------------------------------------------------


@main.command("extract")
@click.argument("repo_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--output", "-o",
    default=None,
    type=click.Path(path_type=Path),
    help="Output path for the full graph JSON. Use '-' for stdout. Defaults to stdout.",
)
@click.option(
    "--pretty",
    is_flag=True,
    default=False,
    help="Pretty-print the JSON output (indent=2).",
)
@click.option(
    "--validate/--no-validate",
    default=True,
    help="Validate the graph against the pydantic schema before writing (default: on).",
)
@_scope_options
def extract_command(
    repo_dir: Path,
    output: Path | None,
    pretty: bool,
    validate: bool,
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
    scope_dirs: tuple[str, ...],
    include_from: Path | None,
    config_path: Path | None,
) -> None:
    """Extract a DEGraph lineage graph from REPO_DIR.

    Writes the full Graph JSON to OUTPUT (or stdout). Use ``degraph context``
    to produce a paste-ready compact-graph prompt for an in-editor LLM.
    """
    repo_dir = repo_dir.resolve()
    scope = _build_scope(
        repo_dir, config_path, scope_dirs, include_patterns, exclude_patterns, include_from,
    )

    click.echo(f"[degraph] Extracting {repo_dir}", err=True)
    click.echo(f"[degraph] Scope: {scope.summary()}", err=True)

    graph = extract_repo(repo_dir, scope=scope)

    click.echo(
        f"[degraph] Done: {graph.metadata.files_parsed} files parsed, "
        f"{graph.metadata.files_skipped} skipped, "
        f"{len(graph.tables)} tables, "
        f"{len(graph.dataframes)} dataframes, "
        f"{len(graph.edges)} edges, "
        f"{len(graph.warnings)} warnings  "
        f"({graph.metadata.extraction_seconds:.2f}s)",
        err=True,
    )

    indent = 2 if pretty else None
    graph_json = graph.model_dump_json(indent=indent)

    if output is None or str(output) == "-":
        sys.stdout.write(graph_json)
        if indent:
            sys.stdout.write("\n")
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(graph_json, encoding="utf-8")
        click.echo(f"[degraph] Written to {output}", err=True)


# ---------------------------------------------------------------------------
# context — produce a paste-ready prompt for an in-editor LLM
# ---------------------------------------------------------------------------


CURSOR_PROMPT_HEADER = """\
# DEGraph Context Bundle

You are answering data lineage / impact-analysis questions about a PySpark
codebase. The block below is a **compact data-semantic graph** extracted
statically from a scoped subset of the repository.

**How to read this graph (read this BEFORE answering):**

- `tables`: each entry is a persisted dataset (Delta / Parquet / catalog
  table). The `columns` field lists its output schema. The
  `column_provenance` field is **the authoritative answer** to
  "where does column X come from": for each column it gives a `role`
  (passthrough / derived / aggregate / group_key / opaque / unknown) and
  a `from` list of upstream `<table>.<col>` references.
- `edges`: typed transformations between datasets. Read the `legend` inside
  the JSON for per-edge-type semantics. Critical: `join_keys` and
  `group_keys` describe how rows are *matched*, not where values come
  from — never list them as "contributors" to a value column.
- `warnings`: machine-readable list of extraction blind spots (dynamic
  column lists, unresolved variable names, opaque imported helpers). If
  asked about pipeline gaps or coverage, this is the right place to look.

**When answering:**

1. For "which tables read/write X?" → check `tables[X].written_by` /
   `read_by` directly.
2. For "where does column X of table Y come from?" → check
   `tables[Y].column_provenance[X]` first; only fall back to edge
   topology if provenance is empty.
3. For "what does the pipeline not know?" → enumerate `warnings`.
4. If the graph genuinely lacks the information needed, say so — do not
   invent table/column names that don't appear in the graph.

---

## Compact graph

```json
"""

CURSOR_PROMPT_FOOTER_FMT = """\
```

---

## Question

{question}
"""


@main.command("context")
@click.argument("repo_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--output", "-o",
    default=None,
    type=click.Path(path_type=Path),
    help="Output path for the bundle. Defaults to <repo>/degraph_context.md. Use '-' for stdout.",
)
@click.option(
    "--question", "-q",
    default=None,
    help="Pre-fill the question section. Omit to leave a TODO placeholder.",
)
@click.option(
    "--minify/--pretty",
    default=True,
    help="Minify the embedded JSON to save tokens (default: on). Use --pretty for readability.",
)
@click.option(
    "--graph-only",
    is_flag=True,
    default=False,
    help="Write just the compact-graph JSON without the prompt scaffold (for piping).",
)
@_scope_options
def context_command(
    repo_dir: Path,
    output: Path | None,
    question: str | None,
    minify: bool,
    graph_only: bool,
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
    scope_dirs: tuple[str, ...],
    include_from: Path | None,
    config_path: Path | None,
) -> None:
    """Build a paste-ready LLM context bundle for REPO_DIR.

    The output is a single Markdown file containing (a) a brief instruction
    block explaining how to read the graph, (b) the compact-graph JSON, and
    (c) a placeholder for the user's question. Paste the whole file into
    Cursor's composer, GitHub Copilot Chat, or any other in-editor LLM that
    accepts a long context block. No API keys or network access required.

    Combine with ``--scope`` / ``--include`` / ``--exclude`` to extract just
    the part of a large monorepo that's relevant to the current question.
    """
    repo_dir = repo_dir.resolve()
    scope = _build_scope(
        repo_dir, config_path, scope_dirs, include_patterns, exclude_patterns, include_from,
    )

    click.echo(f"[degraph] Extracting {repo_dir}", err=True)
    click.echo(f"[degraph] Scope: {scope.summary()}", err=True)

    graph = extract_repo(repo_dir, scope=scope)
    graph_dict = graph.model_dump(mode="json")

    click.echo(
        f"[degraph] Extracted: {len(graph.tables)} tables, "
        f"{len(graph.edges)} edges, {len(graph.warnings)} warnings "
        f"({graph.metadata.extraction_seconds:.2f}s)",
        err=True,
    )

    compact = build_compact_graph(graph_dict)
    if minify:
        compact_json = json.dumps(compact, separators=(",", ":"))
    else:
        compact_json = json.dumps(compact, indent=2)

    if graph_only:
        bundle = compact_json
    else:
        q = question if question else "<replace this with your data-lineage / impact-analysis question>"
        bundle = CURSOR_PROMPT_HEADER + compact_json + CURSOR_PROMPT_FOOTER_FMT.format(question=q)

    if output is None:
        output = repo_dir / "degraph_context.md"

    if str(output) == "-":
        sys.stdout.write(bundle)
        if not bundle.endswith("\n"):
            sys.stdout.write("\n")
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(bundle, encoding="utf-8")
        # Approximate token count: ~4 chars/token for English+JSON mix
        approx_tokens = len(bundle) // 4
        click.echo(
            f"[degraph] Bundle written to {output} ({len(bundle):,} chars, "
            f"~{approx_tokens:,} tokens est.)",
            err=True,
        )


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------


@main.command("compare")
@click.argument("ground_truth", type=click.Path(exists=True, path_type=Path))
@click.argument("extracted", type=click.Path(exists=True, path_type=Path))
@click.option("--strict", is_flag=True, help="Fail on any difference (exit code 1).")
def compare_command(ground_truth: Path, extracted: Path, strict: bool) -> None:
    """Compare EXTRACTED graph against GROUND_TRUTH.

    Prints a summary of matches, missing edges, and extra edges.
    Exits with code 1 if ``--strict`` and any differences are found.
    """
    gt = json.loads(ground_truth.read_text(encoding="utf-8"))
    ex = json.loads(extracted.read_text(encoding="utf-8"))

    gt_edges = {_edge_key(e) for e in gt.get("edges", [])}
    ex_edges = {_edge_key(e) for e in ex.get("edges", [])}

    missing = gt_edges - ex_edges
    extra = ex_edges - gt_edges
    matched = gt_edges & ex_edges

    click.echo(f"Edges matched : {len(matched)}")
    click.echo(f"Missing       : {len(missing)}")
    click.echo(f"Extra         : {len(extra)}")

    if missing:
        click.echo("\nMissing edges (in ground truth but not in extracted):")
        for k in sorted(missing):
            click.echo(f"  - {k}")

    if extra:
        click.echo("\nExtra edges (in extracted but not in ground truth):")
        for k in sorted(extra):
            click.echo(f"  + {k}")

    gt_tables = {t["fqn"] for t in gt.get("tables", [])}
    ex_tables = {t["fqn"] for t in ex.get("tables", [])}
    missing_tables = gt_tables - ex_tables
    extra_tables = ex_tables - gt_tables

    click.echo(f"\nTables matched: {len(gt_tables & ex_tables)}")
    if missing_tables:
        click.echo(f"Missing tables: {sorted(missing_tables)}")
    if extra_tables:
        click.echo(f"Extra tables  : {sorted(extra_tables)}")

    if strict and (missing or extra or missing_tables or extra_tables):
        raise SystemExit(1)


def _norm_id(node_id: str) -> str:
    """Strip the lineno segment from a df: ID so GT and extracted can be compared."""
    if not node_id.startswith("df:"):
        return node_id
    parts = node_id.split(":")
    if len(parts) == 4 and parts[2].isdigit():
        return f"df:{parts[1]}:{parts[3]}"
    return node_id


def _edge_key(edge: dict) -> str:
    """Build a comparable key for an edge from its semantic properties."""
    kind = edge.get("kind", "?")
    file = edge.get("file", "?")
    src = _norm_id(edge.get("source") or edge.get("left_source", "?"))
    tgt = _norm_id(edge.get("target", "?"))
    extra = ""
    if kind == "derives":
        extra = f":{edge.get('output_col','?')}"
    elif kind == "aggregates":
        extra = f":{','.join(edge.get('group_keys', []))}"
    elif kind == "writes":
        extra = f":{edge.get('mode','?')}"
    elif kind == "opaque_transform":
        extra = f":{edge.get('operator','?')}"
    return f"{kind}|{file}|{src}->{tgt}{extra}"


if __name__ == "__main__":
    main()
