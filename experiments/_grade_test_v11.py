"""Apply manual grades to the v1.1 sealed-TEST-set runs (Fix #6 / by=<file>).

Sealed test split (dev/evaluation_protocol.md), evaluated ONCE against tool tag
v1.1-frozen-for-eval across 4 model families x 3 benchmarks x RAW+DSL x 20 Qs.

This is the paper's HEADLINE generalization number — the first measurement on a
split no fix was ever developed against.

Grades are 0/1/2 per the rubric in each *.qa.test.json metadata. Scores were
assigned by reading each model_answer against the authored reference_answer.

Run:  python experiments/_grade_test_v11.py        (applies scores + prints table)
"""
import glob
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
MET = REPO / "results" / "metrics"

MODEL_KEY = {
    "google/gemini-2.0-flash-001": "GEM",
    "openai/gpt-4o-mini": "GPT",
    "anthropic/claude-3.5-haiku": "CLA",
    "meta-llama/llama-3.3-70b-instruct": "LLA",
}


def bench_of(run):
    txt = " ".join(a["question"] for a in run["results"])
    if "coverage_plans" in txt or "claim_outcome" in txt or "other_procedures" in txt:
        return "clinical"
    if "promotions_raw" in txt or "revenue_forecast" in txt or "attribution_funnel" in txt:
        return "medium"
    return "small"


# GRADES[(model_key, benchmark, mode)] = [score for TQ1..TQ20]
# mode is "raw" or "dsl".
GRADES = {}

# ---------------------------------------------------------------- small
GRADES[("GEM", "small", "raw")] = [1,2,2,2,1,1,2,0,1,2, 2,2,2,2,2,2,1,1,2,2]
GRADES[("GEM", "small", "dsl")] = [1,1,2,2,1,2,2,2,1,2, 2,2,0,1,2,2,2,0,2,1]
GRADES[("GPT", "small", "raw")] = [1,2,2,2,1,1,2,0,1,2, 2,2,2,2,2,2,1,1,2,1]
GRADES[("GPT", "small", "dsl")] = [0,1,2,2,1,1,2,0,1,2, 2,2,0,2,2,2,1,0,2,1]
GRADES[("CLA", "small", "raw")] = [1,2,2,2,1,1,2,0,1,2, 2,2,2,2,2,2,2,2,2,2]
GRADES[("CLA", "small", "dsl")] = [1,1,2,2,2,2,2,2,1,2, 2,2,1,2,2,2,2,2,2,1]
GRADES[("LLA", "small", "raw")] = [0,2,2,2,1,1,2,0,2,2, 2,2,2,2,2,2,1,2,2,1]
GRADES[("LLA", "small", "dsl")] = [1,1,2,2,1,1,2,1,2,2, 2,2,2,2,2,2,1,1,2,1]

# ---------------------------------------------------------------- clinical
# DSL gaps surfaced by test set: TQ9 suffix_rename not represented;
# TQ11 etl literal/current_timestamp projection cols dropped; TQ16 partitionBy
# not emitted on W edge. Those 3 Qs drive most of the clinical RAW>DSL gap.
GRADES[("GEM", "clinical", "raw")] = [1,2,2,2,2,2,2,2,2,2, 2,2,2,2,2,1,2,0,2,1]
GRADES[("GEM", "clinical", "dsl")] = [1,2,1,1,1,2,2,2,0,2, 0,2,1,1,2,0,2,1,2,1]
GRADES[("GPT", "clinical", "raw")] = [1,2,1,1,1,2,2,2,2,2, 2,2,2,2,2,1,2,0,2,1]
GRADES[("GPT", "clinical", "dsl")] = [1,2,1,1,1,2,2,2,0,2, 0,2,0,1,2,0,2,1,2,1]
GRADES[("CLA", "clinical", "raw")] = [1,2,2,1,1,2,2,2,2,2, 2,2,2,2,2,2,2,1,2,1]
GRADES[("CLA", "clinical", "dsl")] = [1,2,2,1,1,2,2,2,0,2, 0,2,2,2,2,0,2,2,2,1]
GRADES[("LLA", "clinical", "raw")] = [1,2,1,0,2,2,2,2,2,2, 2,2,2,2,2,1,2,1,2,1]
GRADES[("LLA", "clinical", "dsl")] = [1,2,2,1,1,2,2,2,1,2, 0,2,1,1,2,0,2,2,2,1]

# ---------------------------------------------------------------- medium
# DSL gaps surfaced: TQ5 cross-join (F.lit(True)) serialized as bare 'inner';
# TQ6 CTE-internal alias (order_date AS snapshot_date) not surfaced; TQ18
# stream-vs-batch write marker inconsistent; TQ3 turnover_rate formula not
# surfaced. DSL wins: TQ2/TQ13 (rb= readers + pc= projected-cols make
# orphan-table + dead-column detection precise).
GRADES[("GEM", "medium", "raw")] = [2,1,2,2,2,2,2,2,2,2, 2,1,1,2,2,1,2,2,2,2]
GRADES[("GEM", "medium", "dsl")] = [2,2,1,2,1,1,2,2,2,2, 0,1,2,2,1,1,2,1,1,1]
GRADES[("GPT", "medium", "raw")] = [2,0,2,2,2,2,2,2,2,2, 2,0,1,2,1,1,2,2,1,2]
GRADES[("GPT", "medium", "dsl")] = [2,1,1,1,0,0,2,1,2,2, 1,2,0,2,1,1,1,0,1,1]
GRADES[("CLA", "medium", "raw")] = [2,1,2,2,2,2,2,2,2,2, 2,1,2,2,1,1,2,2,1,2]
GRADES[("CLA", "medium", "dsl")] = [2,2,1,2,1,1,1,2,2,2, 1,1,2,2,1,1,2,0,2,2]
GRADES[("LLA", "medium", "raw")] = [2,1,2,2,2,2,2,2,2,2, 2,1,2,2,1,1,2,2,1,2]
GRADES[("LLA", "medium", "dsl")] = [2,2,1,2,1,1,1,2,2,2, 1,1,1,1,1,1,2,1,1,2]


def main():
    files = sorted(glob.glob(str(MET / "*_test.json")))
    applied = 0
    for f in files:
        d = json.loads(Path(f).read_text(encoding="utf-8"))
        model = d["runs"][0]["meta"]["model"]
        mk = MODEL_KEY.get(model)
        if not mk:
            continue
        changed = False
        for run in d["runs"]:
            b = bench_of(run)
            mode = "dsl" if "dsl" in run["meta"]["context_mode"] else "raw"
            key = (mk, b, mode)
            if key not in GRADES:
                continue
            scores = GRADES[key]
            for a in run["results"]:
                idx = int(a["id"].replace("TQ", "")) - 1
                if 0 <= idx < len(scores):
                    a["score"] = scores[idx]
                    applied += 1
                    changed = True
        if changed:
            Path(f).write_text(json.dumps(d, indent=2), encoding="utf-8")

    print(f"Applied {applied} grades.\n")
    # Aggregate
    benches = ["small", "clinical", "medium"]
    models = ["GEM", "GPT", "CLA", "LLA"]
    name = {"GEM": "Gemini 2.0 Flash", "GPT": "GPT-4o-mini",
            "CLA": "Claude 3.5 Haiku", "LLA": "Llama 3.3 70B"}
    def pct(key):
        s = GRADES.get(key)
        return (sum(s), len(s) * 2) if s else (0, 0)

    print("v1.1 SEALED TEST SET — RAW vs DSL (max 40/cell, 20 Qs x 2)\n")
    print(f"{'model':18s} | {'benchmark':9s} | RAW        | DSL        | gap")
    print("-" * 66)
    pooled = {"raw": [0, 0], "dsl": [0, 0]}
    perfam = {m: {"raw": [0, 0], "dsl": [0, 0]} for m in models}
    for m in models:
        for b in benches:
            re_, rt = pct((m, b, "raw"))
            de, dt = pct((m, b, "dsl"))
            if rt == 0 and dt == 0:
                continue
            rp = re_ / rt * 100 if rt else 0
            dp = de / dt * 100 if dt else 0
            print(f"{name[m]:18s} | {b:9s} | {re_:2d}/{rt:2d}={rp:5.1f}% | {de:2d}/{dt:2d}={dp:5.1f}% | {dp-rp:+5.1f}")
            pooled["raw"][0] += re_; pooled["raw"][1] += rt
            pooled["dsl"][0] += de; pooled["dsl"][1] += dt
            perfam[m]["raw"][0] += re_; perfam[m]["raw"][1] += rt
            perfam[m]["dsl"][0] += de; perfam[m]["dsl"][1] += dt
    print("-" * 66)
    for m in models:
        r = perfam[m]["raw"]; dd = perfam[m]["dsl"]
        if r[1] == 0:
            continue
        rp = r[0]/r[1]*100; dp = dd[0]/dd[1]*100
        print(f"{name[m]:18s} | {'ALL':9s} | {r[0]:3d}/{r[1]:3d}={rp:4.1f}% | {dd[0]:3d}/{dd[1]:3d}={dp:4.1f}% | {dp-rp:+5.1f}")
    print("-" * 66)
    rp = pooled["raw"][0]/pooled["raw"][1]*100 if pooled["raw"][1] else 0
    dp = pooled["dsl"][0]/pooled["dsl"][1]*100 if pooled["dsl"][1] else 0
    print(f"{'POOLED (4 fam)':18s} | {'ALL':9s} | {pooled['raw'][0]:3d}/{pooled['raw'][1]:3d}={rp:4.1f}% | {pooled['dsl'][0]:3d}/{pooled['dsl'][1]:3d}={dp:4.1f}% | {dp-rp:+5.1f}")


if __name__ == "__main__":
    main()
