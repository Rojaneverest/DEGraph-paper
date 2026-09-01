"""token_count.py - measure token counts for raw source vs DEGraph serializations.

Usage:
    python experiments/token_count.py

Outputs a table comparing:
  - raw source files (all .py / .ipynb / .sql in the benchmark)
  - full graph JSON  (current results/graphs output)
  - compact graph    (stripped to tables + edge summaries only)
  - ultra-compact    (tables list + one-line-per-edge)

The compact-graph builder lives in ``src/degraph/compact.py`` so the CLI and
this script share one implementation; this script is a measurement+reporter
wrapper.

Requires: tiktoken  (pip install tiktoken)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from degraph.compact import build_compact_graph, _norm_id  # noqa: E402
from degraph.compact_dsl import build_compact_dsl  # noqa: E402

try:
    import tiktoken
    _enc = tiktoken.get_encoding("cl100k_base")
    def count_tokens(text: str) -> int:
        return len(_enc.encode(text))
except ImportError:
    print("[warn] tiktoken not installed; using char/4 approximation", file=sys.stderr)
    def count_tokens(text: str) -> int:
        return max(1, len(text) // 4)


DEFAULT_BENCHMARK = "repo_synthetic_small"
BENCHMARK_NAME = DEFAULT_BENCHMARK
BENCHMARK_DIR = REPO_ROOT / "data" / "benchmarks" / BENCHMARK_NAME
GRAPH_PATH    = REPO_ROOT / "results" / "graphs" / f"{BENCHMARK_NAME}.graph.json"


def count_raw_source(benchmark_dir: Path) -> dict:
    extensions = {".py", ".ipynb", ".sql"}
    files = {}
    for p in sorted(benchmark_dir.rglob("*")):
        if p.suffix not in extensions or not p.is_file():
            continue
        rel = p.relative_to(benchmark_dir).as_posix()
        text = p.read_text(encoding="utf-8", errors="ignore")
        files[rel] = count_tokens(text)
    return files


def build_ultra_compact(graph: dict) -> str:
    lines: list[str] = []
    lines.append("# TABLES")
    for t in graph.get("tables", []):
        fqn = t["fqn"]
        cols = " ".join(c["name"] for c in (t.get("columns") or []))
        written = ",".join(t.get("written_by") or [])
        read = ",".join(t.get("read_by") or [])
        lines.append(f"TABLE {fqn}  cols=[{cols}]  written_by={written or '-'}  read_by={read or '-'}")
    lines.append("")
    lines.append("# LINEAGE EDGES")
    for e in graph.get("edges", []):
        kind = e["kind"]
        src = _norm_id(e.get("source") or e.get("left_source", "?"))
        tgt = _norm_id(e.get("target", "?"))
        file = e.get("file", "")
        extra = ""
        if kind == "writes":
            extra = f" mode={e.get('mode','')} fmt={e.get('format','')}"
            if e.get("merge_keys"):
                extra += f" merge_keys={e['merge_keys']}"
        elif kind == "derives":
            extra = f" col={e.get('output_col','')}"
            if e.get("window_spec"):
                ws = e["window_spec"]
                extra += f" window(order={ws.get('order_cols',[])})"
        elif kind == "joins":
            right = _norm_id(e.get("right_source", ""))
            extra = f" right={right} type={e.get('join_type','')} keys={e.get('join_keys',[])}"
        elif kind == "aggregates":
            extra = f" groupby={e.get('group_keys',[])} ops={e.get('agg_ops',[])} out={e.get('output_cols',[])}"
        elif kind == "opaque_transform":
            extra = f" op={e.get('operator','')}"
        elif kind == "projects":
            extra = f" drop={e.get('removed_cols',[])}"
        lines.append(f"{kind.upper():18s} {file}: {src} -> {tgt}{extra}")
    lines.append("")
    lines.append("# WARNINGS")
    for w in graph.get("warnings", []):
        lines.append(f"WARN [{w.get('category','')}] {w.get('file','')}: {w.get('message','')}")
    return "\n".join(lines)


def main() -> None:
    import argparse
    global BENCHMARK_NAME, BENCHMARK_DIR, GRAPH_PATH
    parser = argparse.ArgumentParser(description="DEGraph token-count report")
    parser.add_argument("--benchmark", default=DEFAULT_BENCHMARK)
    args = parser.parse_args()
    BENCHMARK_NAME = args.benchmark
    BENCHMARK_DIR = REPO_ROOT / "data" / "benchmarks" / BENCHMARK_NAME
    GRAPH_PATH    = REPO_ROOT / "results" / "graphs" / f"{BENCHMARK_NAME}.graph.json"

    raw_files = count_raw_source(BENCHMARK_DIR)
    total_raw = sum(raw_files.values())

    graph = json.load(GRAPH_PATH.open())
    full_pretty   = json.dumps(graph, indent=2)
    full_minified = json.dumps(graph, separators=(",", ":"))
    full_tokens   = count_tokens(full_pretty)
    mini_tokens   = count_tokens(full_minified)

    compact = build_compact_graph(graph)
    compact_pretty   = json.dumps(compact, indent=2)
    compact_minified = json.dumps(compact, separators=(",", ":"))
    compact_tokens   = count_tokens(compact_pretty)
    compact_mini_tok = count_tokens(compact_minified)

    ultra_text   = build_ultra_compact(graph)
    ultra_tokens = count_tokens(ultra_text)

    dsl_text   = build_compact_dsl(graph)
    dsl_tokens = count_tokens(dsl_text)

    print("=" * 70)
    print(f"DEGraph token-count report - {BENCHMARK_NAME}")
    print("=" * 70)
    print()
    print("RAW SOURCE FILES")
    print(f"  {'File':<48} {'tokens':>7}")
    print(f"  {'-'*48} {'-'*7}")
    for rel, tok in raw_files.items():
        print(f"  {rel:<48} {tok:>7,}")
    print(f"  {'TOTAL':<48} {total_raw:>7,}")
    print()
    print("GRAPH SERIALIZATIONS")
    print(f"  {'Format':<38} {'tokens':>7}  {'vs raw':>9}")
    print(f"  {'-'*38} {'-'*7}  {'-'*9}")
    def ratio(n): return f"{n/total_raw*100:.1f}%"
    print(f"  {'Full JSON (pretty-printed)':<38} {full_tokens:>7,}  {ratio(full_tokens):>9}")
    print(f"  {'Full JSON (minified)':<38} {mini_tokens:>7,}  {ratio(mini_tokens):>9}")
    print(f"  {'Compact JSON (pretty-printed)':<38} {compact_tokens:>7,}  {ratio(compact_tokens):>9}")
    print(f"  {'Compact JSON (minified)':<38} {compact_mini_tok:>7,}  {ratio(compact_mini_tok):>9}")
    print(f"  {'Ultra-compact text':<38} {ultra_tokens:>7,}  {ratio(ultra_tokens):>9}")
    print(f"  {'Compact DSL (v1)':<38} {dsl_tokens:>7,}  {ratio(dsl_tokens):>9}")
    print()
    print("COMPACT GRAPH STRUCTURE")
    print(f"  Tables:   {len(compact['tables'])}")
    print(f"  Edges:    {len(compact['edges'])}")
    print(f"  Warnings: {len(compact['warnings'])}")
    if compact.get("named_column_logic"):
        print(f"  Named column-rule vars: {len(compact['named_column_logic'])}")
    rl_edges = sum(1 for e in compact['edges'] if e.get('rule_logic'))
    if rl_edges:
        print(f"  Derives edges w/ rule_logic: {rl_edges}")
    print()
    print("SAVINGS (compact JSON vs raw source)")
    saving = total_raw - compact_tokens
    print(f"  Tokens saved:  {saving:,}  ({saving/total_raw*100:.1f}% reduction)")
    print(f"  Context ratio: 1 graph token : {total_raw/compact_tokens:.1f} raw-source tokens")

    out_dir = REPO_ROOT / "results" / "graphs"
    (out_dir / f"{BENCHMARK_NAME}.compact.json").write_text(compact_pretty, encoding="utf-8")
    (out_dir / f"{BENCHMARK_NAME}.ultracompact.txt").write_text(ultra_text, encoding="utf-8")
    (out_dir / f"{BENCHMARK_NAME}.compact.dsl").write_text(dsl_text, encoding="utf-8")
    print()
    print(f"Compact JSON saved to: results/graphs/{BENCHMARK_NAME}.compact.json")
    print(f"Ultra-compact saved to: results/graphs/{BENCHMARK_NAME}.ultracompact.txt")
    print(f"Compact DSL  saved to: results/graphs/{BENCHMARK_NAME}.compact.dsl")


if __name__ == "__main__":
    main()
