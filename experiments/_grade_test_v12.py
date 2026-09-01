"""v1.2 sealed-TEST-set result (Fix #7 partitionBy + Fix #8 select-literals/@PROV).

Evaluated against tag v1.2-frozen-for-eval. Only the CHANGED cells were re-run:
  - clinical DSL : Fix #7 pc=service_from_date + Fix #8 etl cols + @PROV section
  - medium DSL   : Fix #7 pc=funnel_date/snapshot_ts only (no medium test Q probes
                   partitioning → content-identical for every medium question →
                   medium DSL grades reused from v1.1, byte-equivalent inputs).
RAW (all benchmarks) is tool-independent → reused from v1.1. small DSL byte-
identical to v1.1 → reused.

This script records the new clinical-DSL grades and prints the v1.1 -> v1.2
comparison. Manual grades (0/1/2) assigned by reading each model_answer in the
v1.2 clinical-DSL runs against the sealed reference_answers.

Headline movement: clinical DSL 110->121 /160 (+6.9pp); driven by TQ11 0->8
(etl columns now surfaced) and TQ16 0->4 (partitionBy now on the Write edge),
plus TQ5 GEM 1->2; minus a stochastic GPT TQ8 2->0 (hallucinated TOB pattern).
"""

# Per-(model) totals out of 40 from v1.1 (experiments/_grade_test_v11.py),
# pooled structure: [small, clinical, medium] for RAW and DSL.
V11 = {
    # model: {"raw":[small,clin,med], "dsl":[small,clin,med]}  (each /40)
    "GEM": {"raw": [32, 35, 36], "dsl": [30, 26, 29]},
    "GPT": {"raw": [31, 32, 32], "dsl": [26, 25, 22]},
    "CLA": {"raw": [34, 35, 35], "dsl": [35, 30, 30]},
    "LLA": {"raw": [32, 33, 35], "dsl": [32, 29, 28]},
}

# v1.2 clinical-DSL per-question grades (TQ1..TQ20), newly graded.
V12_CLIN_DSL = {
    "GEM": [1,2,2,1,2,2,2,2,0,2, 2,2,1,1,2,1,2,1,2,1],  # =31
    "GPT": [1,2,1,1,1,2,2,0,0,2, 2,2,0,1,2,1,2,1,2,1],  # =26
    "CLA": [1,2,2,1,1,2,2,2,0,2, 2,2,2,2,2,1,2,2,2,1],  # =33
    "LLA": [1,2,2,1,1,2,2,2,1,2, 2,2,1,1,2,1,2,1,2,1],  # =31
}

MODELS = ["GEM", "GPT", "CLA", "LLA"]
NAME = {"GEM": "Gemini 2.0 Flash", "GPT": "GPT-4o-mini",
        "CLA": "Claude 3.5 Haiku", "LLA": "Llama 3.3 70B"}


def main():
    # Build v1.2 totals: reuse v1.1 except clinical-DSL (index 1 of dsl list).
    v12 = {}
    for m in MODELS:
        raw = list(V11[m]["raw"])               # unchanged
        dsl = list(V11[m]["dsl"])
        dsl[1] = sum(V12_CLIN_DSL[m])           # clinical DSL replaced
        v12[m] = {"raw": raw, "dsl": dsl}

    def pooled(tbl, mode):
        e = sum(sum(tbl[m][mode]) for m in MODELS)
        return e, len(MODELS) * 3 * 40

    print("v1.2 SEALED TEST SET (vs v1.1) — pooled 4 families x 3 benchmarks x 20 Qs\n")
    print(f"{'family':18s} | RAW (unch.) | DSL v1.1 | DSL v1.2 |  d")
    print("-" * 62)
    for m in MODELS:
        r = sum(v12[m]["raw"]); rt = 120
        d11 = sum(V11[m]["dsl"]); d12 = sum(v12[m]["dsl"])
        print(f"{NAME[m]:18s} | {r:3d}/120={r/rt*100:4.1f}% | {d11/120*100:5.1f}% | "
              f"{d12/120*100:5.1f}% | {(d12-d11)/120*100:+4.1f}")
    print("-" * 62)
    re_, rt = pooled(v12, "raw")
    d11e = sum(sum(V11[m]["dsl"]) for m in MODELS)
    d12e = sum(sum(v12[m]["dsl"]) for m in MODELS)
    tot = len(MODELS) * 3 * 40
    print(f"{'POOLED':18s} | {re_:3d}/480={re_/tot*100:4.1f}% | {d11e/tot*100:5.1f}% | "
          f"{d12e/tot*100:5.1f}% | {(d12e-d11e)/tot*100:+4.1f}")
    print()
    print(f"Pooled RAW 83.8% (unchanged) | DSL {d11e/tot*100:.1f}% -> {d12e/tot*100:.1f}% "
          f"| gap {(d12e-re_)/tot*100:+.1f}pp (was -12.5pp)")
    print()
    print("Per-benchmark DSL (pooled 4 families, /160):")
    for i, b in enumerate(["small", "clinical", "medium"]):
        a = sum(V11[m]["dsl"][i] for m in MODELS)
        c = sum(v12[m]["dsl"][i] for m in MODELS)
        tag = " (reused)" if b != "clinical" else " <- changed"
        print(f"  {b:9s} v1.1 {a/160*100:5.1f}%  ->  v1.2 {c/160*100:5.1f}%  ({(c-a)/160*100:+.1f}pp){tag}")


if __name__ == "__main__":
    main()
