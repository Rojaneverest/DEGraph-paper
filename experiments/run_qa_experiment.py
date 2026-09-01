"""run_qa_experiment.py — run the DEGraph Q&A accuracy experiment.

Compares two context modes for each question:
  (A) RAW   — all source files concatenated as plain text
  (B) GRAPH — compact DEGraph JSON serialization

Supported backends (configured via --backend):
  ollama     Local Ollama server or LM Studio (OpenAI-compatible /v1/chat/completions)
             Default URL: http://localhost:11434 (LM Studio: http://127.0.0.1:1234)
             Models: gemma3:4b, gemma2:2b, llama3.1:8b, google/gemma-4-e2b, etc.
  anthropic  Anthropic API (requires ANTHROPIC_API_KEY env var)
             Models: claude-haiku-3-5, claude-3-5-sonnet-20241022, etc.
  gemini     Google AI Studio (requires GOOGLE_API_KEY env var)
             Free tier: 1500 req/day on Flash, 50/day on Pro (as of 2025).
             Models: gemini-2.5-flash, gemini-2.5-pro, gemini-1.5-flash, etc.
  openrouter OpenRouter unified API (requires OPENROUTER_API_KEY env var)
             Pay-as-you-go. Models: google/gemini-2.0-flash, anthropic/claude-haiku-3-5,
             meta-llama/llama-3.3-70b-instruct, etc. See openrouter.ai/models for pricing.

Usage examples:
  # Local Gemma 3 4B via Ollama
  python experiments/run_qa_experiment.py --backend ollama --model gemma3:4b

  # gemma-4-e2b via LM Studio (E1 setup)
  python experiments/run_qa_experiment.py --backend ollama --model google/gemma-4-e2b \
      --ollama-url http://127.0.0.1:1234

  # Claude Haiku for reference
  python experiments/run_qa_experiment.py --backend anthropic --model claude-haiku-3-5

  # Gemini 2.5 Flash (free tier ceiling reference)
  python experiments/run_qa_experiment.py --backend gemini --model gemini-2.5-flash

  # Run only specific questions
  python experiments/run_qa_experiment.py --backend ollama --model gemma3:4b --questions Q1,Q3,Q7

  # Run only one mode (useful for testing)
  python experiments/run_qa_experiment.py --backend ollama --model gemma3:4b --mode graph

Output:
  results/metrics/<model>_<timestamp>.json   machine-readable
  results/metrics/<model>_<timestamp>.txt    human-readable report

Grading:
  Questions are graded manually (the script leaves a placeholder score).
  After the run, edit the output JSON and set each answer's "score" to:
    2  = correct     (all required facts, no false additions)
    1  = partial     (some facts correct, at least one missing or wrong)
    0  = incorrect   (mostly wrong or contradicts the graph)
  Re-run with --score <json_file> to compute final aggregate metrics.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).parent.parent

# Load .env into os.environ at import time, without needing python-dotenv as a dep.
# .env is gitignored — see .env.example for the variables this script expects.
_ENV_PATH = REPO_ROOT / ".env"
if _ENV_PATH.exists():
    for _line in _ENV_PATH.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _k, _v = _line.split("=", 1)
        os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

DEFAULT_BENCHMARK = "repo_synthetic_small"
METRICS_DIR = REPO_ROOT / "results" / "metrics"
METRICS_DIR.mkdir(parents=True, exist_ok=True)


def _paths_for(benchmark: str) -> tuple[Path, Path, Path]:
    """Resolve (benchmark_dir, compact_graph_json, qa_json) for a benchmark name."""
    return (
        REPO_ROOT / "data" / "benchmarks" / benchmark,
        REPO_ROOT / "results" / "graphs" / f"{benchmark}.compact.json",
        REPO_ROOT / "data" / "ground_truth" / f"{benchmark}.qa.json",
    )


# Module-level paths default to the small benchmark; overridden in main() if --benchmark is passed.
BENCHMARK_DIR, GRAPH_COMPACT, QA_PATH = _paths_for(DEFAULT_BENCHMARK)


# ---------------------------------------------------------------------------
# Context builders
# ---------------------------------------------------------------------------

def build_raw_context() -> str:
    """Concatenate all source files with filename headers."""
    extensions = {".py", ".ipynb", ".sql"}
    parts: list[str] = [
        f"# DEGraph Benchmark - {BENCHMARK_DIR.name}",
        "# The following are all source files in this PySpark repository.",
        "",
    ]
    for p in sorted(BENCHMARK_DIR.rglob("*")):
        if p.suffix not in extensions or not p.is_file():
            continue
        rel = p.relative_to(BENCHMARK_DIR).as_posix()
        parts.append(f"### FILE: {rel}")
        parts.append(p.read_text(encoding="utf-8", errors="ignore"))
        parts.append("")
    return "\n".join(parts)


def build_graph_context(graph_format: str = "json") -> str:
    """Load the compact graph as LLM context.

    Parameters
    ----------
    graph_format
        ``"json"`` (default) loads the compact JSON artifact produced by
        ``token_count.py``. ``"dsl"`` loads the compact DSL artifact (line-
        based text format with symbol interning and predicate deduplication —
        ~55% smaller than the JSON form on real-world pipelines).
    """
    if graph_format == "dsl":
        dsl_path = REPO_ROOT / "results" / "graphs" / f"{BENCHMARK_DIR.name}.compact.dsl"
        if not dsl_path.exists():
            raise FileNotFoundError(
                f"{dsl_path} not found. Run experiments/token_count.py first."
            )
        body = dsl_path.read_text(encoding="utf-8")
        lines: list[str] = [
            f"# DEGraph Benchmark - {BENCHMARK_DIR.name}",
            "# The following is the DEGraph lineage graph in compact DSL form.",
            "# The legend at the top of the DSL teaches you how to read each record.",
            "",
            body,
        ]
        return "\n".join(lines)

    # Default: JSON compact graph
    if not GRAPH_COMPACT.exists():
        raise FileNotFoundError(
            f"{GRAPH_COMPACT} not found. Run experiments/token_count.py first."
        )
    graph = json.loads(GRAPH_COMPACT.read_text(encoding="utf-8"))
    lines = [
        f"# DEGraph Benchmark - {BENCHMARK_DIR.name}",
        "# The following is the DEGraph lineage graph for this PySpark repository.",
        "# Use it to answer questions about data lineage, column provenance, and impact.",
        "",
        json.dumps(graph, indent=2),
    ]
    return "\n".join(lines)


SYSTEM_PROMPT = """You are an expert data engineer. You will be given context about a PySpark
repository (either as source code or as a DEGraph lineage graph) and asked impact-analysis
questions. Answer precisely and concisely based solely on the provided context. If the context
does not contain enough information to answer fully, say so explicitly and explain what is missing
rather than guessing."""


def make_prompt(context: str, question: str) -> str:
    return f"{context}\n\n---\n\nQuestion: {question}"


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------

def call_ollama(model: str, system: str, user: str, base_url: str) -> tuple[str, float]:
    """Call any OpenAI-compatible /v1/chat/completions endpoint.

    Works with Ollama (http://localhost:11434) and LM Studio (http://127.0.0.1:1234).
    Uses standard OpenAI params (temperature/max_tokens at top level) so both servers
    honour them — Ollama's nested 'options' dict is not used.
    """
    import urllib.request
    import urllib.error

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "temperature": 0.1,
        "max_tokens": 1024,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/v1/chat/completions",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.time()
    try:
        # 600s ceiling: Qwen3.5 9B on a 10K-token context can take 3+ minutes
        # per hard cross-file question. The 180s default proved too tight on E4.
        with urllib.request.urlopen(req, timeout=600) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            elapsed = time.time() - t0
            content = result["choices"][0]["message"]["content"]
            return content, elapsed
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Could not reach local LLM server at {base_url}. "
            f"Is LM Studio / Ollama running with the server enabled? Error: {e}"
        )


def call_openrouter(model: str, system: str, user: str, max_retries: int = 6) -> tuple[str, float]:
    """Call OpenRouter via its OpenAI-compatible /chat/completions endpoint.

    Requires OPENROUTER_API_KEY in environment / .env.
    Model names use the provider/model-slug format, e.g.:
      google/gemini-2.0-flash      ($0.10/1M in)
      anthropic/claude-haiku-3-5   ($0.80/1M in)
      meta-llama/llama-3.3-70b-instruct ($0.12/1M in)

    Automatically retries on 429 rate-limit errors with exponential backoff.
    """
    import urllib.request
    import urllib.error

    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("Set OPENROUTER_API_KEY in .env or environment.")

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.1,
        "max_tokens": 1024,
    }
    data = json.dumps(payload).encode("utf-8")

    last_err: Optional[str] = None
    for attempt in range(max_retries):
        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
                "HTTP-Referer": "https://github.com/Rojaneverest/DEGraph",
                "X-Title": "DEGraph Research Experiment",
            },
            method="POST",
        )
        t0 = time.time()
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                elapsed = time.time() - t0
                content = result["choices"][0]["message"]["content"]
                return content, elapsed
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            err_str = f"OpenRouter HTTP {e.code}: {body[:400]}"
            last_err = err_str
            # Retry on 429 rate-limit with exponential backoff
            if e.code == 429:
                wait = _parse_retry_seconds(body)
                if not wait:
                    wait = min(60, 15 * (2 ** attempt))  # exp backoff fallback
                print(f"         [rate-limited; sleeping {wait:.0f}s then retrying ({attempt+1}/{max_retries})]")
                time.sleep(wait + 2)  # +2s for clock skew safety
                continue
            # Non-rate-limit HTTP error — don't retry
            raise RuntimeError(err_str)
        except Exception as e:
            last_err = str(e)
            raise

    raise RuntimeError(f"OpenRouter gave up after {max_retries} retries. Last error: {last_err}")


def call_anthropic(model: str, system: str, user: str) -> tuple[str, float]:
    """Call Anthropic API via the anthropic SDK."""
    try:
        import anthropic
    except ImportError:
        raise ImportError("pip install anthropic")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("Set ANTHROPIC_API_KEY environment variable.")
    client = anthropic.Anthropic(api_key=api_key)
    t0 = time.time()
    msg = client.messages.create(
        model=model,
        max_tokens=1024,
        temperature=0.1,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    elapsed = time.time() - t0
    return msg.content[0].text, elapsed


def call_gemini(model: str, system: str, user: str, max_retries: int = 6) -> tuple[str, float]:
    """Call Google AI Studio (Gemini API) via the google-genai SDK.

    Free tier as of 2025 is tightly rate-limited:
      - gemini-2.5-flash : ~5-10 RPM, ~250 RPD
      - gemini-2.5-pro   : ~5 RPM, ~50 RPD
    See https://ai.google.dev/pricing for current limits.

    Automatically retries on 429 RESOURCE_EXHAUSTED, parsing the server's
    "retry in Xs" hint when present. Wall-clock cost on free tier ~14s/req minimum.

    Requires:
        pip install google-genai
        export GOOGLE_API_KEY=<your-key>   # get one at https://aistudio.google.com/apikey
    """
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        raise ImportError(
            "Install the SDK first: pip install google-genai"
        )
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Set GOOGLE_API_KEY environment variable. "
            "Get a free key at https://aistudio.google.com/apikey"
        )
    client = genai.Client(api_key=api_key)

    # Gemini 2.5 models use an internal "thinking budget" that consumes
    # max_output_tokens before producing visible output. On long-context
    # questions the visible answer can come out truncated or empty because
    # all tokens were spent thinking. Two mitigations:
    #   (a) bump max_output_tokens to 4096 (gives room for both thinking + answer)
    #   (b) for non-thinking models (1.5, etc.) thinking_config is ignored harmlessly
    config_kwargs = {
        "system_instruction": system,
        "temperature": 0.1,
        "max_output_tokens": 4096,
    }
    # Disable thinking for 2.5 models so all output tokens go to the answer
    # (safe to attempt — older models will just ignore unknown config fields)
    try:
        config_kwargs["thinking_config"] = types.ThinkingConfig(thinking_budget=0)
    except (AttributeError, TypeError):
        pass  # SDK too old or model doesn't support thinking config
    config = types.GenerateContentConfig(**config_kwargs)

    t0 = time.time()
    last_err = None
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model=model,
                contents=user,
                config=config,
            )
            elapsed = time.time() - t0
            # Defensive extraction — Gemini may return empty .text on safety block
            text = getattr(response, "text", None)
            if not text:
                if response.candidates:
                    cand = response.candidates[0]
                    parts = getattr(cand.content, "parts", []) if cand.content else []
                    text = "".join(getattr(p, "text", "") for p in parts)
                    if not text:
                        text = f"[GEMINI EMPTY RESPONSE; finish_reason={cand.finish_reason}]"
                else:
                    text = "[GEMINI EMPTY RESPONSE; no candidates returned]"
            return text, elapsed
        except Exception as e:
            err_str = str(e)
            last_err = e
            # Retry on 429 / RESOURCE_EXHAUSTED with parsed delay if available
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                wait = _parse_retry_seconds(err_str)
                if wait is None:
                    wait = min(60, 15 * (2 ** attempt))  # exp backoff fallback
                print(f"         [rate-limited; sleeping {wait:.0f}s then retrying ({attempt+1}/{max_retries})]")
                time.sleep(wait + 1)  # +1s for clock skew safety
                continue
            # Non-rate-limit error — don't retry
            raise
    raise RuntimeError(f"Gemini gave up after {max_retries} retries. Last error: {last_err}")


def _parse_retry_seconds(err_str: str) -> Optional[float]:
    """Best-effort parse of 'Please retry in X.XXs' or 'retryDelay: Xs' from a 429 body."""
    import re
    # Google sometimes returns "Please retry in 12.5s" or "retryDelay: 12s"
    for pat in (r"retry in (\d+(?:\.\d+)?)\s*s", r"retryDelay[\"']?\s*:\s*[\"']?(\d+(?:\.\d+)?)s"):
        m = re.search(pat, err_str)
        if m:
            return float(m.group(1))
    return None


# ---------------------------------------------------------------------------
# Experiment runner
# ---------------------------------------------------------------------------

def _is_complete_answer(r: dict) -> bool:
    """A result is 'complete enough to skip on resume' if it has a non-error answer."""
    ans = r.get("model_answer", "")
    if not ans:
        return False
    if ans.startswith("[ERROR"):
        return False
    return True


def run_experiment(
    backend: str,
    model: str,
    mode: str,
    question_ids: Optional[list[str]],
    ollama_url: str,
    context_limit: Optional[int],
    resume_results: Optional[list[dict]],
    save_callback=None,
    graph_format: str = "json",
) -> tuple[list[dict], int]:
    """Run the Q&A experiment for the given backend/model/mode.

    Parameters
    ----------
    context_limit
        If set, any question whose total prompt would exceed this many tokens
        gets recorded as SKIPPED rather than sent to the model. This is how
        we surface "RAW context doesn't fit at all" as a first-class result
        on context-constrained models (e.g. Qwen3 at 35k).
    resume_results
        If non-None, a list of previously-saved results for this mode. Questions
        with non-error answers in this list are skipped and re-used.
    save_callback
        Called as save_callback(results) after every question completes (or is
        skipped). Use this to write the JSON incrementally so a mid-run crash
        loses at most one in-flight answer.
    """
    qa = json.loads(QA_PATH.read_text(encoding="utf-8"))
    questions = qa["questions"]
    if question_ids:
        questions = [q for q in questions if q["id"] in question_ids]

    if mode == "raw":
        context = build_raw_context()
        context_label = "raw_source"
    else:
        context = build_graph_context(graph_format=graph_format)
        context_label = "compact_graph_dsl" if graph_format == "dsl" else "compact_graph"

    # Approximate token count
    try:
        import tiktoken
        enc = tiktoken.get_encoding("cl100k_base")
        ctx_tokens = len(enc.encode(context))
        def _toks(s: str) -> int:
            return len(enc.encode(s))
    except ImportError:
        ctx_tokens = len(context) // 4
        def _toks(s: str) -> int:
            return len(s) // 4

    sys_tokens = _toks(SYSTEM_PROMPT)

    print(f"\n{'='*65}")
    print(f"  Backend:  {backend} / {model}")
    print(f"  Mode:     {context_label}  (~{ctx_tokens:,} tokens)")
    print(f"  Questions: {len(questions)}")
    if context_limit:
        print(f"  Context limit: {context_limit:,} tokens  (over-budget -> SKIPPED)")
    if resume_results:
        already = sum(1 for r in resume_results if _is_complete_answer(r))
        print(f"  Resume:   {already} answers already complete in source JSON")
    print(f"{'='*65}\n")

    # Index resume results by qid for fast lookup
    resume_by_qid = {r["id"]: r for r in (resume_results or []) if _is_complete_answer(r)}

    results: list[dict] = []
    for i, q in enumerate(questions, 1):
        qid = q["id"]
        difficulty = q.get("difficulty", "?")

        # Resume: re-use existing answer if present and non-error
        if qid in resume_by_qid:
            print(f"[{i}/{len(questions)}] {qid} ({difficulty}) -- reused from existing JSON")
            results.append(resume_by_qid[qid])
            if save_callback:
                save_callback(results)
            continue

        prompt = make_prompt(context, q["question"])
        prompt_tokens = sys_tokens + _toks(prompt)

        print(f"[{i}/{len(questions)}] {qid} ({difficulty}) -- {q['question'][:60]}...")

        # Context-budget guard: skip if prompt would exceed model context
        if context_limit and prompt_tokens > context_limit:
            answer = (
                f"[SKIPPED: {context_label} prompt is ~{prompt_tokens:,} tokens, "
                f"exceeds context limit {context_limit:,}]"
            )
            elapsed = 0.0
            print(f"         OVER BUDGET ({prompt_tokens:,} > {context_limit:,}) -- skipped")
        else:
            try:
                if backend == "ollama":
                    answer, elapsed = call_ollama(model, SYSTEM_PROMPT, prompt, ollama_url)
                elif backend == "anthropic":
                    answer, elapsed = call_anthropic(model, SYSTEM_PROMPT, prompt)
                elif backend == "gemini":
                    answer, elapsed = call_gemini(model, SYSTEM_PROMPT, prompt)
                elif backend == "openrouter":
                    answer, elapsed = call_openrouter(model, SYSTEM_PROMPT, prompt)
                else:
                    raise ValueError(f"Unknown backend: {backend}")
                print(f"         answered in {elapsed:.1f}s")
            except Exception as e:
                answer = f"[ERROR: {e}]"
                elapsed = 0.0
                print(f"         ERROR: {e}")

        results.append({
            "id": qid,
            "category": q.get("category", ""),
            "difficulty": difficulty,
            "question": q["question"],
            "context_mode": context_label,
            "context_tokens": ctx_tokens,
            "prompt_tokens": prompt_tokens,
            "reference_answer": q["reference_answer"],
            "model_answer": answer,
            "elapsed_s": round(elapsed, 2),
            "score": None,   # fill in manually: 2=correct, 1=partial, 0=incorrect
            "notes": "",
        })

        # Incremental save so a mid-run crash loses at most this answer
        if save_callback:
            save_callback(results)

    return results, ctx_tokens


# ---------------------------------------------------------------------------
# Scoring (offline pass after manual grading)
# ---------------------------------------------------------------------------

def compute_scores(results: list[dict]) -> dict:
    """Compute aggregate metrics from manually-graded results."""
    scored = [r for r in results if r.get("score") is not None]
    if not scored:
        return {"message": "No scores yet — grade results manually then re-run with --score"}

    total = len(scored) * 2  # max score per question
    earned = sum(r["score"] for r in scored)
    correct   = sum(1 for r in scored if r["score"] == 2)
    partial   = sum(1 for r in scored if r["score"] == 1)
    incorrect = sum(1 for r in scored if r["score"] == 0)

    by_difficulty: dict = {}
    for r in scored:
        d = r.get("difficulty", "?")
        by_difficulty.setdefault(d, {"correct": 0, "partial": 0, "incorrect": 0, "total": 0})
        by_difficulty[d]["total"] += 1
        if r["score"] == 2:   by_difficulty[d]["correct"] += 1
        elif r["score"] == 1: by_difficulty[d]["partial"] += 1
        else:                 by_difficulty[d]["incorrect"] += 1

    return {
        "n_graded": len(scored),
        "score": f"{earned}/{total}",
        "accuracy_pct": round(earned / total * 100, 1),
        "correct": correct,
        "partial": partial,
        "incorrect": incorrect,
        "by_difficulty": by_difficulty,
    }


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------

def write_report(path: Path, meta: dict, results: list[dict], scores: dict) -> None:
    lines = [
        "DEGraph Q&A Experiment Report",
        "=" * 65,
        f"Backend:   {meta['backend']} / {meta['model']}",
        f"Mode:      {meta['context_mode']}",
        f"Tokens:    ~{meta['context_tokens']:,}",
        f"Timestamp: {meta['timestamp']}",
        "",
        "AGGREGATE SCORES",
        "-" * 40,
    ]
    if "n_graded" in scores:
        lines += [
            f"  Score:    {scores['score']}  ({scores['accuracy_pct']}%)",
            f"  Correct:  {scores['correct']}",
            f"  Partial:  {scores['partial']}",
            f"  Wrong:    {scores['incorrect']}",
            "",
            "  By difficulty:",
        ]
        for diff, s in scores.get("by_difficulty", {}).items():
            lines.append(f"    {diff:8s} C={s['correct']} P={s['partial']} X={s['incorrect']} / {s['total']}")
    else:
        lines.append("  Not graded yet. Edit the JSON file and set 'score' for each answer.")

    lines += ["", "INDIVIDUAL ANSWERS", "=" * 65]
    for r in results:
        lines += [
            "",
            f"[{r['id']}] {r['category']} / {r['difficulty']}",
            f"Q: {r['question']}",
            f"Score: {r.get('score', 'ungraded')}",
            "",
            "REFERENCE:",
            r["reference_answer"] or "",
            "",
            f"MODEL ({meta['model']}):",
            r["model_answer"] or "",
            "",
            f"Notes: {r.get('notes', '')}",
            "-" * 65,
        ]

    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="DEGraph Q&A experiment runner")
    parser.add_argument("--backend", choices=["ollama", "anthropic", "gemini", "openrouter"], default="ollama")
    parser.add_argument("--model", default="gemma3:4b",
                        help="Model name (e.g. gemma3:4b, gemma2:2b, claude-haiku-3-5)")
    parser.add_argument("--mode", choices=["raw", "graph", "both"], default="both",
                        help="Context mode to evaluate")
    parser.add_argument("--questions", default=None,
                        help="Comma-separated question IDs to run (e.g. Q1,Q3,Q7)")
    parser.add_argument("--ollama-url", default="http://localhost:11434",
                        help="OpenAI-compatible server URL (Ollama: http://localhost:11434, "
                             "LM Studio: http://127.0.0.1:1234)")
    parser.add_argument("--benchmark", default=DEFAULT_BENCHMARK,
                        help=f"Benchmark directory name under data/benchmarks/ "
                             f"(default: {DEFAULT_BENCHMARK}). Resolves "
                             f"data/benchmarks/<name>/, results/graphs/<name>.compact.json, "
                             f"and data/ground_truth/<name>.qa.json.")
    parser.add_argument("--qa-file", default=None, metavar="PATH",
                        help="Override the Q&A JSON path. Used for held-out / sealed-test "
                             "evaluation where the Qs live in <benchmark>.qa.heldout.json "
                             "(validation split) or <benchmark>.qa.test.json (sealed test "
                             "split, see dev/evaluation_protocol.md). Path is resolved "
                             "relative to the repo root if not absolute. If the filename "
                             "contains 'heldout' / '.test.', the matching suffix "
                             "('_heldout' / '_test') is appended to the output stem so "
                             "results never overwrite another split.")
    parser.add_argument("--context-limit", type=int, default=None,
                        help="Max prompt tokens; over-budget questions are recorded as "
                             "SKIPPED rather than sent. E.g. 35000 for Qwen3 9B at 35k.")
    parser.add_argument("--graph-format", choices=["json", "dsl"], default="json",
                        help="Compact-graph serialization to use for GRAPH mode. "
                             "'json' (default) = compact JSON (legacy). 'dsl' = compact "
                             "DSL text format (~55%% smaller, line-based with symbol "
                             "interning). The benchmark must have the matching "
                             ".compact.json / .compact.dsl artifact in results/graphs/.")
    parser.add_argument("--resume", default=None, metavar="JSON_FILE",
                        help="Resume from a previous run's JSON: re-use answered questions, "
                             "rerun only missing/errored ones. Output is written to the same path.")
    parser.add_argument("--score", default=None, metavar="JSON_FILE",
                        help="Re-compute scores from a previously-saved results JSON")
    args = parser.parse_args()

    # Rebind module-level paths to the requested benchmark (default: small)
    global BENCHMARK_DIR, GRAPH_COMPACT, QA_PATH
    BENCHMARK_DIR, GRAPH_COMPACT, QA_PATH = _paths_for(args.benchmark)

    # --qa-file: override the Q&A path (e.g. held-out split). Resolve relative to repo root.
    split_suffix = ""
    if args.qa_file:
        qa_override = Path(args.qa_file)
        if not qa_override.is_absolute():
            qa_override = REPO_ROOT / qa_override
        if not qa_override.exists():
            raise SystemExit(f"--qa-file target not found: {qa_override}")
        QA_PATH = qa_override
        if "heldout" in qa_override.name:
            split_suffix = "_heldout"
        elif ".test." in qa_override.name or qa_override.name.endswith(".test.json"):
            split_suffix = "_test"
            # Sealed-test discipline (dev/evaluation_protocol.md R5): a test split
            # must pass an integrity check before it may be evaluated. Fail loud
            # rather than silently produce an un-auditable headline number.
            _seal = REPO_ROOT / "experiments" / "seal_test_split.py"
            if _seal.exists():
                import subprocess
                _v = subprocess.run(
                    [sys.executable, str(_seal), "--verify", "--file", str(QA_PATH)],
                    capture_output=True, text=True)
                if _v.returncode != 0:
                    raise SystemExit(
                        "Sealed-test integrity check FAILED — refusing to evaluate.\n"
                        f"{_v.stdout}\n{_v.stderr}\n"
                        "See dev/evaluation_protocol.md (R2/R5). Seal the split first:\n"
                        "  python experiments/seal_test_split.py --seal")

    # --score mode: just recompute from existing file
    if args.score:
        data = json.loads(Path(args.score).read_text(encoding="utf-8"))
        for run in data.get("runs", [data]):
            s = compute_scores(run["results"])
            mode = run["meta"]["context_mode"]
            print(f"\n{mode}:  {s}")
        return

    question_ids = [q.strip() for q in args.questions.split(",")] if args.questions else None
    modes = ["raw", "graph"] if args.mode == "both" else [args.mode]

    # Resume: load existing JSON, derive out_stem from its path, pre-populate runs
    if args.resume:
        resume_path = Path(args.resume)
        if not resume_path.exists():
            raise SystemExit(f"--resume target not found: {resume_path}")
        existing = json.loads(resume_path.read_text(encoding="utf-8"))
        json_path = resume_path
        out_stem = resume_path.with_suffix("")
        timestamp = existing["runs"][0]["meta"]["timestamp"] if existing.get("runs") else \
            datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        prev_runs_by_mode = {r["meta"]["context_mode"]: r for r in existing.get("runs", [])}
        print(f"\n[RESUME] {resume_path.name} -- {len(prev_runs_by_mode)} mode(s) found")
    else:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_model = args.model.replace(":", "-").replace("/", "-")
        out_stem = METRICS_DIR / f"{safe_model}_{timestamp}{split_suffix}"
        json_path = Path(f"{out_stem}.json")
        prev_runs_by_mode = {}

    all_runs: list[dict] = []

    def _persist():
        """Atomic-ish write of the combined JSON. Called after every question."""
        tmp = json_path.with_suffix(json_path.suffix + ".tmp")
        tmp.write_text(json.dumps({"runs": all_runs}, indent=2), encoding="utf-8")
        tmp.replace(json_path)

    for mode in modes:
        if mode == "raw":
            mode_label = "raw_source"
        else:
            mode_label = "compact_graph_dsl" if args.graph_format == "dsl" else "compact_graph"
        resume_results = None
        if mode_label in prev_runs_by_mode:
            resume_results = prev_runs_by_mode[mode_label].get("results", [])

        # Reserve this run's slot in all_runs so incremental save sees it
        meta_placeholder = {
            "backend": args.backend,
            "model": args.model,
            "context_mode": mode_label,
            "context_tokens": 0,
            "timestamp": timestamp,
            "context_limit": args.context_limit,
        }
        run_slot = {"meta": meta_placeholder, "scores": {}, "results": []}
        all_runs.append(run_slot)

        def _save_partial(current_results):
            run_slot["results"] = current_results
            _persist()

        results, ctx_tokens = run_experiment(
            backend=args.backend,
            model=args.model,
            mode=mode,
            question_ids=question_ids,
            ollama_url=args.ollama_url,
            context_limit=args.context_limit,
            resume_results=resume_results,
            save_callback=_save_partial,
            graph_format=args.graph_format,
        )
        run_slot["meta"]["context_tokens"] = ctx_tokens
        run_slot["scores"] = compute_scores(results)
        run_slot["results"] = results
        _persist()

        # Write per-mode text report
        report_path = Path(f"{out_stem}_{mode}.txt")
        write_report(report_path, run_slot["meta"], results, run_slot["scores"])
        print(f"\nReport: {report_path}")

    print(f"\nResults JSON: {json_path}")
    print("\nNext step: open the JSON, set 'score' for each answer (2/1/0),")
    print("then re-run:  python experiments/run_qa_experiment.py --score", json_path)


if __name__ == "__main__":
    main()
