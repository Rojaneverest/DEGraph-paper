"""T3 — BM25 token-matched retrieval baseline (the third context mode).

Motivation (publication_roadmap.md T3): every reviewer asks "is the *graph
structure* doing the work, or just the smaller token budget?" This baseline
controls for token budget: for each question we BM25-retrieve raw-source chunks
up to (approximately) the SAME token count as the DEGraph DSL for that
benchmark, then run the identical models/prompt/grading. If DSL still beats
token-matched BM25, the structure — not merely compression — is what helps.

BM25 is tool-independent (it reads raw source, never the extractor output), so a
single run is valid across tool versions.

Design
------
* Chunk each source file at a natural granularity: Databricks `# COMMAND ----`
  cells, else .ipynb cells, else top-level def/class blocks; any chunk over
  ~MAX_CHUNK_TOKENS is sub-split into line windows.
* Build one BM25Okapi index per benchmark over those chunks.
* Per question: rank chunks by BM25, greedily concatenate (with FILE headers)
  until adding the next chunk would exceed the DSL token budget for that
  benchmark (= token count of results/graphs/<bench>.compact.dsl).
* Call the model with the same SYSTEM_PROMPT + make_prompt as RAW/DSL modes.
* Emit a run_qa_experiment-compatible JSON (context_mode="bm25_retrieval") so the
  existing manual-grading pipeline + _grade scripts work unchanged.

Usage
-----
  python experiments/bm25_baseline.py --benchmark repo_synthetic_small \
      --qa-file data/ground_truth/repo_synthetic_small.qa.test.json \
      --backend openrouter --model google/gemini-2.0-flash-001
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "experiments"))

# Reuse the harness's model call, prompt, system prompt, and path resolver.
import run_qa_experiment as H  # noqa: E402
from rank_bm25 import BM25Okapi  # noqa: E402

try:
    import tiktoken
    _ENC = tiktoken.get_encoding("cl100k_base")
    def toks(s: str) -> int:
        return len(_ENC.encode(s))
except Exception:  # pragma: no cover
    def toks(s: str) -> int:
        return len(s) // 4

MET = REPO / "results" / "metrics"
MAX_CHUNK_TOKENS = 220       # sub-split chunks larger than this
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _tokenize(text: str) -> list[str]:
    """Identifier-preserving lowercase tokenizer for BM25 over code text."""
    return [w.lower() for w in _WORD.findall(text)]


def _sub_split(text: str) -> list[str]:
    """Split an over-long chunk into <=MAX_CHUNK_TOKENS line windows."""
    lines = text.splitlines()
    out, buf = [], []
    for ln in lines:
        buf.append(ln)
        if toks("\n".join(buf)) >= MAX_CHUNK_TOKENS:
            out.append("\n".join(buf))
            buf = []
    if buf:
        out.append("\n".join(buf))
    return out or [text]


def _chunk_file(path: Path, rel: str) -> list[tuple[str, str]]:
    """Return [(file_rel, chunk_text), ...] for one source file."""
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return []
    pieces: list[str] = []
    if path.suffix == ".ipynb":
        try:
            nb = json.loads(raw)
            for cell in nb.get("cells", []):
                src = "".join(cell.get("source", []))
                if src.strip():
                    pieces.append(src)
        except Exception:
            pieces = [raw]
    elif "# COMMAND ----------" in raw:
        pieces = [c for c in raw.split("# COMMAND ----------") if c.strip()]
    else:
        # plain module: split on top-level def/class, keep a leading header chunk
        parts = re.split(r"\n(?=(?:def |class )\w)", raw)
        pieces = [p for p in parts if p.strip()]
    # enforce max chunk size
    chunks: list[tuple[str, str]] = []
    for p in pieces:
        for sub in (_sub_split(p) if toks(p) > MAX_CHUNK_TOKENS else [p]):
            chunks.append((rel, sub.strip()))
    return chunks


def build_chunks(bench_dir: Path) -> list[tuple[str, str]]:
    exts = {".py", ".ipynb", ".sql"}
    chunks: list[tuple[str, str]] = []
    for p in sorted(bench_dir.rglob("*")):
        if p.is_file() and p.suffix in exts:
            chunks.extend(_chunk_file(p, p.relative_to(bench_dir).as_posix()))
    return chunks


def dsl_budget(bench_name: str) -> int:
    dsl = REPO / "results" / "graphs" / f"{bench_name}.compact.dsl"
    return toks(dsl.read_text(encoding="utf-8")) if dsl.exists() else 6000


def retrieve(bm25, chunks, question: str, budget: int) -> tuple[str, int]:
    """Greedy top-ranked concatenation up to `budget` tokens. Returns (context, ntoks)."""
    scores = bm25.get_scores(_tokenize(question))
    order = sorted(range(len(chunks)), key=lambda i: scores[i], reverse=True)
    picked: list[str] = []
    used = 0
    for i in order:
        rel, txt = chunks[i]
        block = f"### FILE: {rel}\n{txt}"
        bt = toks(block) + 2
        if used + bt > budget and picked:
            break
        picked.append(block)
        used += bt
        if used >= budget:
            break
    header = (
        "# The following are the most relevant source-code chunks retrieved for "
        "this question (BM25, token-budget-matched to the DEGraph DSL).\n"
    )
    return header + "\n\n".join(picked), used


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--benchmark", required=True)
    ap.add_argument("--qa-file", required=True)
    ap.add_argument("--backend", default="openrouter")
    ap.add_argument("--model", required=True)
    ap.add_argument("--budget", type=int, default=None,
                    help="Token budget; default = the benchmark's DSL token count.")
    args = ap.parse_args()

    bench_dir = REPO / "data" / "benchmarks" / args.benchmark
    qa_path = REPO / args.qa_file if not Path(args.qa_file).is_absolute() else Path(args.qa_file)
    qa = json.loads(qa_path.read_text(encoding="utf-8"))
    questions = qa["questions"]

    chunks = build_chunks(bench_dir)
    corpus = [_tokenize(f"{rel} {txt}") for rel, txt in chunks]
    bm25 = BM25Okapi(corpus)
    budget = args.budget or dsl_budget(args.benchmark)

    print(f"\n{'='*66}\n  BM25 baseline  |  {args.backend} / {args.model}")
    print(f"  benchmark: {args.benchmark}  | chunks: {len(chunks)}  | budget: ~{budget:,} tok (DSL-matched)")
    print(f"  questions: {len(questions)}\n{'='*66}\n")

    sys_tokens = toks(H.SYSTEM_PROMPT)
    results = []
    for i, q in enumerate(questions, 1):
        ctx, ctx_tok = retrieve(bm25, chunks, q["question"], budget)
        prompt = H.make_prompt(ctx, q["question"])
        ptoks = sys_tokens + toks(prompt)
        print(f"[{i}/{len(questions)}] {q['id']} ({q.get('difficulty','?')}) ctx~{ctx_tok}tok")
        try:
            ans, elapsed = H.call_openrouter(args.model, H.SYSTEM_PROMPT, prompt)
            print(f"         answered in {elapsed:.1f}s")
        except Exception as e:
            ans, elapsed = f"[ERROR: {e}]", 0.0
            print(f"         ERROR: {e}")
        results.append({
            "id": q["id"], "category": q.get("category", ""),
            "difficulty": q.get("difficulty", "?"), "question": q["question"],
            "context_mode": "bm25_retrieval", "context_tokens": ctx_tok,
            "prompt_tokens": ptoks, "reference_answer": q["reference_answer"],
            "model_answer": ans, "elapsed_s": round(elapsed, 2),
            "score": None, "notes": "",
        })

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = args.model.replace("/", "-")
    split = "_test" if ".test." in qa_path.name else ("_heldout" if "heldout" in qa_path.name else "")
    out = MET / f"{safe}_{ts}{split}_bm25.json"
    out.write_text(json.dumps({
        "runs": [{
            "meta": {"backend": args.backend, "model": args.model,
                     "context_mode": "bm25_retrieval", "context_tokens": budget,
                     "budget_source": "dsl", "benchmark": args.benchmark,
                     "timestamp": ts},
            "results": results,
        }]
    }, indent=2), encoding="utf-8")
    errs = sum(1 for r in results if r["model_answer"].startswith("[ERROR"))
    print(f"\nWrote {out.name}  | errors: {errs}/{len(results)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
