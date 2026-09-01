"""Regenerate all benchmark graph.json + compact.dsl artifacts from THIS
worktree's `src/degraph` — never the globally pip-installed `degraph`.

WHY THIS EXISTS
---------------
`degraph` is installed editable (`pip install -e`) pointing at ONE worktree.
Running `python -m degraph.cli extract` from a *different* worktree silently
uses that other worktree's (possibly stale) extractor — which is exactly how the
committed graphs drifted from the `degraph extract` CLI output (the install
pointed at a sibling worktree whose extractor lacked the window-resolution and
partitionBy code). This script forces the local source onto sys.path FIRST, so
the artifacts are always generated from the code in *this* checkout.

Run:
    python experiments/regenerate_graphs.py            # all benchmarks
    python experiments/regenerate_graphs.py --check     # regenerate to temp + diff, don't write

After running, regenerate the DSLs (token_count.py already pins local src):
    for b in repo_synthetic_small silver_clinical_claims repo_synthetic_medium:
        python experiments/token_count.py --benchmark $b
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))  # ← local source wins over any global install

from degraph.extractor.assembler import extract_repo  # noqa: E402

BENCHMARKS = [
    "repo_synthetic_small",
    "silver_clinical_claims",
    "repo_synthetic_medium",
]


def _dump(graph) -> str:
    """Match the committed graph.json formatting (indent=2, ascii-escaped)."""
    obj = json.loads(graph.model_dump_json())
    return json.dumps(obj, indent=2, ensure_ascii=True)  # no trailing newline (matches CLI write_text)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true",
                    help="Do not overwrite; report whether output would change.")
    args = ap.parse_args()

    print(f"Using degraph from: {Path(extract_repo.__module__.replace('.', '/'))} "
          f"(src={REPO / 'src'})")
    import os
    os.chdir(REPO)  # so a relative repo_dir keeps repo_root relative (matches committed)
    changed = 0
    for b in BENCHMARKS:
        repo_dir = Path("data") / "benchmarks" / b  # relative on purpose
        out = REPO / "results" / "graphs" / f"{b}.graph.json"
        graph = extract_repo(repo_dir)
        new_text = _dump(graph)
        old_text = out.read_text(encoding="utf-8") if out.exists() else ""
        # compare ignoring the volatile extraction_seconds line
        def _strip(t: str) -> str:
            return "\n".join(l for l in t.splitlines() if "extraction_seconds" not in l)
        if _strip(new_text) == _strip(old_text):
            print(f"  {b:26s} unchanged ({len(graph.edges)} edges)")
            continue
        changed += 1
        if args.check:
            print(f"  {b:26s} WOULD CHANGE ({len(graph.edges)} edges)")
        else:
            out.write_text(new_text, encoding="utf-8", newline="\n")  # LF, matches committed
            print(f"  {b:26s} rewritten ({len(graph.edges)} edges)")
    if args.check and changed:
        print(f"\n{changed} graph(s) would change. Run without --check to apply, "
              f"then regenerate DSLs via token_count.py.")
        return 1
    print("\nDone. Now regenerate DSLs: "
          "for b in <benchmarks>: python experiments/token_count.py --benchmark $b")
    return 0


if __name__ == "__main__":
    sys.exit(main())
