"""Apply post-fix held-out grades to the 3 T1.4 result JSONs.

T1.4 phase script: scores for the post-fix held-out evaluation using
the 5-fix DSL (commit 9f8ad27). Compared against T1.3 pre-fix baseline.

Run after grading:
    python experiments/run_qa_experiment.py --score <json_path>

Post-fix DSL summary (scored 2026-05-28):
  clinical  DSL: 12/20 (60%)   — was  7/20 (35%)  +5  [+25pp]
  medium    DSL: 13/20 (65%)   — was 10/20 (50%)  +3  [+15pp]
  small     DSL: 15/20 (75%)   — was 13/20 (65%)  +2  [+10pp]
  POOLED    DSL: 40/60 (66.7%) — was 30/60 (50%)  +10 [+16.7pp]

Key improvements per fix:
  Fix #1 (struct_fields): clinical HQ1 0→1, HQ7 0→1
  Fix #2 (CTE parser):    clinical HQ6 0→2
  Fix #3/4 (ex=/pr=):     clinical HQ9 1→2, medium HQ10 1→2
  Fix #5 (window src):    small HQ8 error→2, medium HQ5 error→2
  Fix #2 (CTE):           medium HQ5 error→2

Regression:
  small HQ7: 1→0 (model confuses customer_profile aggregate with customer_ltv spark.sql)

Target was >65% pooled DSL — ACHIEVED (66.7%).
"""
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

GRADES = {}

# =============================================================
# silver_clinical_claims  (post-fix DSL)
# =============================================================
# RAW: same quality as pre-fix (18/20 = 90%)
GRADES[("clinical", "raw_source", "HQ1")] = (2, "All 8 fields + sources + filter+sort pipeline correctly identified")
GRADES[("clinical", "raw_source", "HQ2")] = (1, "6 columns + O00-O9A range correct; misses BH precedence + structural detail")
GRADES[("clinical", "raw_source", "HQ3")] = (2, "Both conditions named, all 5 revenue codes listed with normalization")
GRADES[("clinical", "raw_source", "HQ4")] = (2, "All 3 CTEs + outer SELECT identified; two-key LEFT JOIN ON clauses correct")
GRADES[("clinical", "raw_source", "HQ5")] = (2, "coalesce + concat_ws expression correct")
GRADES[("clinical", "raw_source", "HQ6")] = (2, "Window spec correct; 3 of 4 differences identified")
GRADES[("clinical", "raw_source", "HQ7")] = (2, "6 entries + struct-of-struct shape + derived provider_claim_id correct")
GRADES[("clinical", "raw_source", "HQ8")] = (2, "All 7 LEFT joins with correct keys")
GRADES[("clinical", "raw_source", "HQ9")] = (1, "PCP-override + coalesce chain; misses 2nd and 3rd reasons + signal count")
GRADES[("clinical", "raw_source", "HQ10")] = (2, "All 3 isNotNull filters + dq_passed_df name correct")

# DSL: post-fix
GRADES[("clinical", "compact_graph_dsl", "HQ1")] = (1, "sf= fix: lists 6/8 struct fields + null-removal logic; misses 2 null-literal fields (nf=reference_id,diagnosis_group)")
GRADES[("clinical", "compact_graph_dsl", "HQ2")] = (1, "X15 predicate: 6 columns + O00-O9A correct; BH precedence not addressed")
GRADES[("clinical", "compact_graph_dsl", "HQ3")] = (2, "X24: exact predicate with BOTH conditions + all 5 revenue codes correct")
GRADES[("clinical", "compact_graph_dsl", "HQ4")] = (0, "Refuses: says DSL does not show spark.sql CTE chain structure (provider_specialty_df multi-CTE join)")
GRADES[("clinical", "compact_graph_dsl", "HQ5")] = (1, "ex= partially shows coalesce inputs; wrong coalesce structure (flat 3-arg vs nested concat_ws)")
GRADES[("clinical", "compact_graph_dsl", "HQ6")] = (2, "Fix #2: provider_rank window now captured (sc=claim_id,seq_num|ws=part(claim_id)/ord(seq_num desc)); full diff vs diag_rank correct")
GRADES[("clinical", "compact_graph_dsl", "HQ7")] = (1, "Fix #1: identifiers struct shape correct (key+value+4 inner fields); misses 6-entry count + provider_claim_id as derived entry")
GRADES[("clinical", "compact_graph_dsl", "HQ8")] = (2, "7 joins + LEFT type + D-node keys correct")
GRADES[("clinical", "compact_graph_dsl", "HQ9")] = (2, "Fix #3: ex= coalesce chain for provider_specialty_description + pr= rule chain for provider_specialty_code correct")
GRADES[("clinical", "compact_graph_dsl", "HQ10")] = (0, "Fix #4 partial: only identifies 1 of 3 DQ filters from pr= entries (F.col('claim_id').isNotNull())")

# =============================================================
# repo_synthetic_medium  (post-fix DSL)
# =============================================================
# RAW: same quality as pre-fix (15/20 = 75%)
GRADES[("medium", "raw_source", "HQ1")] = (1, "Names both files but misses window in marketing_touchpoints + wrongly treats attribution_funnel_daily as only transitively affected")
GRADES[("medium", "raw_source", "HQ2")] = (2, "Producer + 2 consumers correct")
GRADES[("medium", "raw_source", "HQ3")] = (1, "Affected tables correct; misses silent row-loss framing for .filter(return_qty > 0)")
GRADES[("medium", "raw_source", "HQ4")] = (1, "Main chains correct; misses customer_id join keys and return_qty filter gate")
GRADES[("medium", "raw_source", "HQ5")] = (2, "Both joins + tables + customer_id/session_id keys correct")
GRADES[("medium", "raw_source", "HQ6")] = (2, "All 4 group keys + 5 outputs + multi-source last_event_ts identified")
GRADES[("medium", "raw_source", "HQ7")] = (1, "Semantics correct; misses aggregation impact detail + customer_id-not-on-shipments extraction error")
GRADES[("medium", "raw_source", "HQ8")] = (2, "All 4 kwargs + run() + registered/heuristic mechanism + fallback correct")
GRADES[("medium", "raw_source", "HQ9")] = (1, "Structural removals + 3 affected golds correct; misses new orphan-table warning")
GRADES[("medium", "raw_source", "HQ10")] = (2, "All 3 distinctions covered with specific examples")

# DSL: post-fix
GRADES[("medium", "compact_graph_dsl", "HQ1")] = (1, "Both files named; join + countDistinct(session_id) uses correct; misses window partitionBy('session_id'); attribution_funnel_daily wrongly transitive")
GRADES[("medium", "compact_graph_dsl", "HQ2")] = (2, "Producer + 2 consumers correct")
GRADES[("medium", "compact_graph_dsl", "HQ3")] = (1, "Affected tables (T16, T17) identified; filter impact mentioned; silent row-loss not explicitly framed")
GRADES[("medium", "compact_graph_dsl", "HQ4")] = (1, "Two contributing tables identified; skips some join-key detail")
GRADES[("medium", "compact_graph_dsl", "HQ5")] = (2, "Fix (was error): both joins identified with correct tables (customers_raw inner customer_id, events_raw left session_id)")
GRADES[("medium", "compact_graph_dsl", "HQ6")] = (2, "All 4 group keys + 5 outputs correct")
GRADES[("medium", "compact_graph_dsl", "HQ7")] = (1, "Semantics shift correct; some downstream impact detail missing")
GRADES[("medium", "compact_graph_dsl", "HQ8")] = (0, "Refuses class definition; recognition mechanism partially correct (sink= annotation); wrong about unregistered behavior")
GRADES[("medium", "compact_graph_dsl", "HQ9")] = (1, "Consumer impact + structural removals correct; wrongly says no new warnings")
GRADES[("medium", "compact_graph_dsl", "HQ10")] = (2, "Fix #3/4: |pass marker on O edge identified; @WARN consequence + impact-analysis implication correct")

# =============================================================
# repo_synthetic_small  (post-fix DSL)
# =============================================================
# RAW: same quality as pre-fix (15/20 = 75%)
GRADES[("small", "raw_source", "HQ1")] = (1, "Names both tables + uses but adds wrong fact about join key")
GRADES[("small", "raw_source", "HQ2")] = (2, "Producer + no consumers correct")
GRADES[("small", "raw_source", "HQ3")] = (2, "Null-propagation in customer_profile + customer_ltv correctly identified")
GRADES[("small", "raw_source", "HQ4")] = (1, "orders_raw.total_amount correct but adds wrong contributors")
GRADES[("small", "raw_source", "HQ5")] = (1, "Graph change + 10 values correct but misses bronze-becomes-70% semantic gotcha")
GRADES[("small", "raw_source", "HQ6")] = (2, "customers_raw + products_raw read status correct")
GRADES[("small", "raw_source", "HQ7")] = (2, "Both aggregations + keys + NTILE no-partition correctly identified")
GRADES[("small", "raw_source", "HQ8")] = (2, "Global vs per-country quartile semantics correctly explained")
GRADES[("small", "raw_source", "HQ9")] = (2, "All 3 kwargs + f-string resolution chain identified")
GRADES[("small", "raw_source", "HQ10")] = (1, "Opaque-call disappearance addressed but doesn't explicitly address the other 2 warnings remaining")

# DSL: post-fix
GRADES[("small", "compact_graph_dsl", "HQ1")] = (2, "Both files + all relevant uses (T4 prov + group key + T5 prov + country_avg group key) identified correctly via D-node refs")
GRADES[("small", "compact_graph_dsl", "HQ2")] = (2, "Producer + no consumers correct")
GRADES[("small", "compact_graph_dsl", "HQ3")] = (2, "Null-propagation through T4 and T5, no row-loss correctly stated")
GRADES[("small", "compact_graph_dsl", "HQ4")] = (1, "orders_raw.total_amount correct; skips revenue_quartile intermediate step in chain")
GRADES[("small", "compact_graph_dsl", "HQ5")] = (1, "Correctly identifies NTILE(4)→(10) semantic change + ltv_tier cascade impact via X0/X1/X2 rl=; notes ex= missing NTILE literal")
GRADES[("small", "compact_graph_dsl", "HQ6")] = (2, "customers_raw read by silver/customer_profile.py; products_raw not read by any file — correct")
GRADES[("small", "compact_graph_dsl", "HQ7")] = (0, "REGRESSION: says no spark.sql blocks; confuses A|D26|D27 (customer_profile) with customer_ltv aggregations")
GRADES[("small", "compact_graph_dsl", "HQ8")] = (2, "Fix #5: ws=ord(lifetime_revenue desc) with no PARTITION BY correctly shown; global vs per-country semantics explained correctly")
GRADES[("small", "compact_graph_dsl", "HQ9")] = (1, "Gets merge_keys from mk=product_id,order_date + notes resolved T7 FQN; misses mode='merge' (W-edge second field)")
GRADES[("small", "compact_graph_dsl", "HQ10")] = (2, "All 3 warnings correctly addressed (opaque-call-fallback disappears; orphan-table + dynamic-agg remain); column-propagation consequence correct")


FILES = {
    "clinical": REPO / "results/metrics/google-gemini-2.0-flash-001_20260528_224934_heldout.json",
    "medium":   REPO / "results/metrics/google-gemini-2.0-flash-001_20260528_224936_heldout.json",
    "small":    REPO / "results/metrics/google-gemini-2.0-flash-001_20260528_224937_heldout.json",
}


def main():
    print("Applying post-fix held-out grades (T1.4)...")
    for bench, path in FILES.items():
        data = json.loads(path.read_text(encoding="utf-8"))
        for run in data["runs"]:
            mode = run["meta"]["context_mode"]
            for ans in run["results"]:
                key = (bench, mode, ans["id"])
                if key in GRADES:
                    score, note = GRADES[key]
                    ans["score"] = score
                    ans["notes"] = note
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        print(f"  Graded {bench}: {path.name}")

    print()
    print("POST-FIX HELD-OUT RESULTS (T1.4)")
    print("=" * 55)
    totals = {"raw": [0, 0], "dsl": [0, 0]}
    for bench, path in FILES.items():
        data = json.loads(path.read_text(encoding="utf-8"))
        for run in data["runs"]:
            mode = run["meta"]["context_mode"]
            scored = [r for r in run["results"] if r.get("score") is not None]
            earned = sum(r["score"] for r in scored)
            total  = len(scored) * 2
            pct    = earned / total * 100 if total else 0
            key    = "dsl" if "dsl" in mode else "raw"
            totals[key][0] += earned
            totals[key][1] += total
            label = "DSL" if key == "dsl" else "RAW"
            print(f"  {bench:10s} {label}: {earned}/{total} = {pct:.0f}%")
    print()
    for key, (e, t) in totals.items():
        label = key.upper()
        pre = 30 if key == "dsl" else 48
        pre_pct = pre / 60 * 100
        pct = e / t * 100
        delta = pct - pre_pct
        print(f"  POOLED {label}: {e}/{t} = {pct:.1f}%  (pre-fix: {pre_pct:.0f}%  d={delta:+.1f}pp)")
    print()
    print("Fix-by-fix gains (DSL mode):")
    print("  Fix #1 (struct_fields): clinical HQ1 0->1, HQ7 0->1")
    print("  Fix #2 (CTE parser):    clinical HQ6 0->2")
    print("  Fix #3/4 (ex=/pr=):     clinical HQ9 1->2, medium HQ10 1->2")
    print("  Fix #5 (window src):    small HQ8 error->2")
    print("  Fix (error resolved):   medium HQ5 error->2")
    print("  Regression:             small HQ7 1->0 (spark.sql confusion)")
    print()
    print("Target: >65% pooled DSL. Result: see POOLED DSL above.")


if __name__ == "__main__":
    main()
