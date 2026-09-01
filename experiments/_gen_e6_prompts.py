"""Generate the E6 manual-prompt sheet from current artifacts.

Reads the benchmark source files, the Q&A questions, and the current compact
graph, and writes a fresh `e6_v2_claude_pro_prompts.md` ready to paste into
two Claude Pro chats (one RAW, one GRAPH).

Run:
    python experiments/_gen_e6_prompts.py
"""

from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BENCH = REPO / "data" / "benchmarks" / "repo_synthetic_small"
QA = REPO / "data" / "ground_truth" / "repo_synthetic_small.qa.json"
COMPACT = REPO / "results" / "graphs" / "repo_synthetic_small.compact.json"
OUT = REPO / "experiments" / "e6_v2_claude_pro_prompts.md"

SYSTEM_PROMPT = (
    "You are an expert data engineer. You will be given context about a PySpark "
    "repository (either as source code or as a DEGraph lineage graph) and asked "
    "impact-analysis questions. Answer precisely and concisely based solely on the "
    "provided context. If the context does not contain enough information to answer "
    "fully, say so explicitly and explain what is missing rather than guessing."
)


def collect_raw() -> str:
    exts = {".py", ".ipynb", ".sql"}
    out = []
    for p in sorted(BENCH.rglob("*")):
        if p.suffix not in exts or not p.is_file():
            continue
        rel = p.relative_to(BENCH).as_posix()
        out.append(f"### FILE: {rel}\n{p.read_text(encoding='utf-8', errors='ignore')}\n")
    return "\n".join(out)


def main() -> None:
    qa = json.loads(QA.read_text(encoding="utf-8"))
    compact = json.loads(COMPACT.read_text(encoding="utf-8"))
    compact_text = json.dumps(compact, indent=2)
    raw_text = collect_raw()
    questions = qa["questions"]

    lines: list[str] = []
    lines.append("# E6 v2 — Claude Pro Manual Experiment Prompts (post-graph-fix)")
    lines.append("# repo_synthetic_small | 10 questions | RAW + GRAPH modes")
    lines.append("#")
    lines.append("# CHAT A = RAW mode  -> paste STEP A-1 once, then each A-Q block")
    lines.append("# CHAT B = GRAPH mode -> paste STEP B-1 once, then each B-Q block")
    lines.append("# IMPORTANT: use two fresh Claude chats; do NOT reuse a single chat.")
    lines.append("")
    lines.append("=" * 80)
    lines.append("CHAT A - RAW MODE")
    lines.append("=" * 80)
    lines.append("")
    lines.append("~~~ STEP A-1: Paste this FIRST in a new Claude chat (do this once) ~~~")
    lines.append("")
    lines.append(SYSTEM_PROMPT)
    lines.append("")
    lines.append("# DEGraph Benchmark - repo_synthetic_small")
    lines.append("# The following is the source code of this PySpark repository.")
    lines.append("")
    lines.append(raw_text)
    lines.append("")
    lines.append("~~~ END OF STEP A-1 ~~~")
    lines.append("")
    for q in questions:
        lines.append(f"--- STEP A-{q['id']} ({q['id']} | {q['category']} | {q['difficulty']}) ---")
        lines.append("")
        lines.append(f"Question: {q['question']}")
        lines.append("")
    lines.append("")
    lines.append("=" * 80)
    lines.append("CHAT B - GRAPH MODE")
    lines.append("=" * 80)
    lines.append("")
    lines.append("~~~ STEP B-1: Paste this FIRST in a new Claude chat (do this once) ~~~")
    lines.append("")
    lines.append(SYSTEM_PROMPT)
    lines.append("")
    lines.append("# DEGraph Benchmark - repo_synthetic_small")
    lines.append("# The following is the DEGraph compact lineage graph for this PySpark repository.")
    lines.append("# Use it to answer questions about data lineage, column provenance, and impact.")
    lines.append("# The `legend` block at the top explains the schema.")
    lines.append("")
    lines.append(compact_text)
    lines.append("")
    lines.append("~~~ END OF STEP B-1 ~~~")
    lines.append("")
    for q in questions:
        lines.append(f"--- STEP B-{q['id']} ({q['id']} | {q['category']} | {q['difficulty']}) ---")
        lines.append("")
        lines.append(f"Question: {q['question']}")
        lines.append("")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT}")
    print(f"  RAW context chars:   {len(raw_text):,}")
    print(f"  GRAPH context chars: {len(compact_text):,}")


if __name__ == "__main__":
    main()
