"""LLM-as-judge reliability proxy for the change-impact ground truth.

Every GT label in this project is single-annotator (the author). A second *human*
annotator + Cohen's kappa is the gold standard; absent that, this script provides
an automated reliability proxy, transparently disclosed as such (NOT human
inter-rater agreement).

Design (non-circular). For each of the 21 impact scenarios (imported from
`impact_eval.py`), we show an independent LLM judge the pipeline SOURCE and the
column a developer is about to change, then a SHUFFLED candidate list mixing the
GT-affected columns (positives) with sampled non-affected columns from the same
benchmark (negatives) — *unlabeled*. The judge, reasoning from source alone,
selects which candidates are affected. Each candidate is one rated item; we
compare the judge's yes/no to the author's GT yes/no and report:

  * Cohen's kappa (chance-corrected agreement) over all items, per judge;
  * raw agreement; and the judge-vs-author precision/recall/F1.

Two judges (different model families) are run for robustness. Per-item judgments
are saved so the kappa is reproducible from the cached outputs without re-calling.

Run:  python experiments/llm_judge_impact.py
Requires OPENROUTER_API_KEY in .env. dbdemos rows need the external repo present;
they are skipped (with a note) if it is absent — the synthetic rows stand alone.
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "experiments"))
from degraph.compact import _build_column_provenance  # noqa: E402
from impact_eval import SCENARIOS, _short  # noqa: E402
from run_qa_experiment import call_openrouter  # noqa: E402

JUDGES = ["openai/gpt-4o", "google/gemini-2.5-flash"]
MAX_NEG = 8          # negatives sampled per scenario
RNG_SEED = 20260530  # reproducible negative sampling

DBDEMOS_ROOT = Path(
    r"C:\Users\thapa\Desktop\Research\_external_repos\dbdemos-notebooks"
    r"\demo-retail\lakehouse-retail-c360\01-Data-ingestion\01.2-SDP-python\transformations"
)


def _benchmark_source(bench: str) -> str | None:
    """Concatenate the pipeline source the judge reasons over."""
    if bench == "dbdemos_retail_sdp":
        files = [DBDEMOS_ROOT / "01-bronze.py", DBDEMOS_ROOT / "02-silver.py",
                 DBDEMOS_ROOT / "03-gold.py"]
        if not all(f.exists() for f in files):
            return None
    else:
        base = REPO / "data" / "benchmarks" / bench
        files = sorted(base.rglob("*.py"))
    parts = []
    for f in files:
        try:
            parts.append(f"# ===== {f.name} =====\n{f.read_text(encoding='utf-8')}")
        except Exception:
            pass
    return "\n\n".join(parts) if parts else None


def _all_columns(bench: str) -> list[str]:
    g = json.loads((REPO / "results" / "graphs" / f"{bench}.graph.json").read_text(encoding="utf-8"))
    prov = _build_column_provenance(g)
    cols = set()
    for t, cs in prov.items():
        for c in cs:
            cols.add(_short(f"{t}.{c}"))
    return sorted(cols)


def _candidates(bench: str, table: str, col: str, gt: set[str], rng: random.Random) -> list[str]:
    """GT positives + sampled negatives (other real columns, not affected)."""
    seed_self = f"{table}.{col}"
    pool = [c for c in _all_columns(bench) if c not in gt and c != seed_self]
    rng.shuffle(pool)
    negatives = pool[:MAX_NEG]
    cands = sorted(gt) + negatives
    rng.shuffle(cands)
    return cands


_SYS = (
    "You are a meticulous data-engineering reviewer performing static change-impact "
    "analysis on PySpark pipeline source. You reason only from the code shown. A column "
    "is 'affected' by a change to another column if its value is computed from that "
    "column, directly or transitively (through derivations, aggregates, joins, or "
    "carried passthroughs). Pure unrelated columns are not affected. Answer only with "
    "the requested JSON."
)


def _judge_call(model: str, source: str, table: str, col: str, cands: list[str]) -> set[str]:
    user = (
        f"PIPELINE SOURCE:\n```python\n{source}\n```\n\n"
        f"A developer is about to change the column `{table}.{col}`.\n\n"
        f"From the candidate list below, return ONLY those downstream columns whose value "
        f"would be affected by that change. Each candidate is `table.column`.\n\n"
        f"CANDIDATES:\n{json.dumps(cands, indent=0)}\n\n"
        f'Respond with a JSON object exactly like {{"affected": ["table.col", ...]}} and '
        f"nothing else. Include a candidate only if you are confident it is affected."
    )
    txt, _ = call_openrouter(model, _SYS, user)
    txt = txt.strip()
    # strip code fences if present
    if txt.startswith("```"):
        txt = txt.split("```")[1] if "```" in txt[3:] else txt
        txt = txt.lstrip("json").strip().strip("`").strip()
    try:
        obj = json.loads(txt[txt.index("{"):txt.rindex("}") + 1])
        sel = set(obj.get("affected", []))
    except Exception:
        sel = set()
    return {c for c in sel if c in cands}  # ignore anything off-list


def _kappa(a_yes_j_yes: int, a_yes_j_no: int, a_no_j_yes: int, a_no_j_no: int) -> float:
    n = a_yes_j_yes + a_yes_j_no + a_no_j_yes + a_no_j_no
    if n == 0:
        return 0.0
    po = (a_yes_j_yes + a_no_j_no) / n
    p_a_yes = (a_yes_j_yes + a_yes_j_no) / n
    p_j_yes = (a_yes_j_yes + a_no_j_yes) / n
    pe = p_a_yes * p_j_yes + (1 - p_a_yes) * (1 - p_j_yes)
    return (po - pe) / (1 - pe) if (1 - pe) else 1.0


def main() -> int:
    records = []  # cached per-item judgments
    for model in JUDGES:
        rng = random.Random(RNG_SEED)
        # confusion: author(rows) vs judge(cols), classes yes/no
        ayjy = ayjn = anjy = anjn = 0
        n_scen = 0
        print(f"\n=== JUDGE: {model} ===")
        for bench, table, col, gt in SCENARIOS:
            cands = _candidates(bench, table, col, gt, rng)
            source = _benchmark_source(bench)
            if source is None:
                print(f"  [skip] {bench}: source unavailable (external repo absent)")
                continue
            sel = _judge_call(model, source, table, col, cands)
            for c in cands:
                a = c in gt          # author label
                j = c in sel         # judge label
                if a and j:
                    ayjy += 1
                elif a and not j:
                    ayjn += 1
                elif (not a) and j:
                    anjy += 1
                else:
                    anjn += 1
                records.append(dict(judge=model, bench=bench, change=f"{table}.{col}",
                                    candidate=c, author=int(a), judge_label=int(j)))
            n_scen += 1
            agree = sum(1 for c in cands if (c in gt) == (c in sel))
            print(f"  {bench.split('_')[-1]:9s} {table}.{col:18s} "
                  f"judge picked {len(sel)}/{len(gt)} GT  ({agree}/{len(cands)} items agree)")
        k = _kappa(ayjy, ayjn, anjy, anjn)
        n = ayjy + ayjn + anjy + anjn
        po = (ayjy + anjn) / n if n else 0.0
        prec = ayjy / (ayjy + anjy) if (ayjy + anjy) else 1.0
        rec = ayjy / (ayjy + ayjn) if (ayjy + ayjn) else 1.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
        print(f"  -> {n_scen} scenarios, {n} items | Cohen's kappa = {k:.2f} | "
              f"raw agreement {po*100:.0f}% | judge-vs-author P{prec*100:.0f}/R{rec*100:.0f}/F1{f1*100:.0f}")
        print(f"     confusion (author/judge): yy={ayjy} yn={ayjn} ny={anjy} nn={anjn}")

    out = REPO / "results" / "metrics" / "llm_judge_impact.json"
    out.write_text(json.dumps(records, indent=2), encoding="utf-8")
    print(f"\nsaved per-item judgments: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
