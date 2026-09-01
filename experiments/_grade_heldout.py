"""Apply held-out grades + notes to the 3 T1.3 result JSONs.

T1.3 phase script: scores set inline by the (author-equivalent) annotator
against the rubric in each held-out qa.heldout.json file. After running this
script, each result JSON has populated `score` and `notes` fields; the
runner's --score mode then computes aggregate accuracy.
"""
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

GRADES = {}

# silver_clinical_claims RAW
GRADES[("clinical_claims", "raw_source", "HQ1")] = (2, "All 8 fields + sources + filter+sort pipeline correctly identified")
GRADES[("clinical_claims", "raw_source", "HQ2")] = (1, "6 columns + O00-O9A range correct; misses BH precedence + structural detail")
GRADES[("clinical_claims", "raw_source", "HQ3")] = (2, "Both conditions named, all 5 revenue codes listed with normalization")
GRADES[("clinical_claims", "raw_source", "HQ4")] = (2, "All 3 CTEs + outer SELECT identified; two-key LEFT JOIN ON clauses correct")
GRADES[("clinical_claims", "raw_source", "HQ5")] = (2, "coalesce + concat_ws expression correct")
GRADES[("clinical_claims", "raw_source", "HQ6")] = (2, "Window spec correct; 3 of 4 differences identified")
GRADES[("clinical_claims", "raw_source", "HQ7")] = (2, "6 entries + struct-of-struct shape + derived provider_claim_id correct")
GRADES[("clinical_claims", "raw_source", "HQ8")] = (2, "All 7 LEFT joins with correct keys")
GRADES[("clinical_claims", "raw_source", "HQ9")] = (1, "PCP-override as 1 of 3 reasons; misses coalesce vs when-chain + signal count")
GRADES[("clinical_claims", "raw_source", "HQ10")] = (2, "All 3 isNotNull filters + dq_passed_df name correct")

# silver_clinical_claims DSL
GRADES[("clinical_claims", "compact_graph_dsl", "HQ1")] = (0, "Refuses; DSL does not surface struct_fields for other_diagnoses")
GRADES[("clinical_claims", "compact_graph_dsl", "HQ2")] = (1, "Same level as RAW: columns + range correct, misses BH precedence")
GRADES[("clinical_claims", "compact_graph_dsl", "HQ3")] = (2, "X17/X24 predicates referenced; both conditions + 5 revenue codes correct")
GRADES[("clinical_claims", "compact_graph_dsl", "HQ4")] = (0, "Refuses to reconstruct CTE structure from D-node refs")
GRADES[("clinical_claims", "compact_graph_dsl", "HQ5")] = (1, "Identifies 3 source cols via sc= edge but punts on coalesce+concat_ws structure")
GRADES[("clinical_claims", "compact_graph_dsl", "HQ6")] = (0, "Says provider_specialty_df SQL window not present in DEGraph; only diag_rank captured")
GRADES[("clinical_claims", "compact_graph_dsl", "HQ7")] = (0, "Confidently wrong: says no derived entries exist when provider_claim_id is derived via concat_ws")
GRADES[("clinical_claims", "compact_graph_dsl", "HQ8")] = (2, "All 7 joins + keys + all-left correct, using D-node refs")
GRADES[("clinical_claims", "compact_graph_dsl", "HQ9")] = (1, "Identifies coalesce inputs + rl= chain for code; same single-reason simplification as RAW")
GRADES[("clinical_claims", "compact_graph_dsl", "HQ10")] = (0, "Names only 1 filter of 3; D45/D46 D-nodes instead of dq_passed_df")

# repo_synthetic_medium RAW
GRADES[("medium", "raw_source", "HQ1")] = (1, "Names both files but misses the 3 specific uses in marketing_touchpoints and 2 CTE uses in attribution_funnel_daily")
GRADES[("medium", "raw_source", "HQ2")] = (2, "Producer + 2 consumers correct; revenue_forecast_features disambiguated as transitive")
GRADES[("medium", "raw_source", "HQ3")] = (1, "Gets silent-filter behavior but adds wrong facts: refunds_enriched doesnt use return_qty; revenue_forecast_features doesnt inherit returns")
GRADES[("medium", "raw_source", "HQ4")] = (1, "Two main chains right but misses customer_id join keys and return_qty filter gate")
GRADES[("medium", "raw_source", "HQ5")] = (2, "Both joins + suffix purpose correctly explained")
GRADES[("medium", "raw_source", "HQ6")] = (2, "All 4 group keys + 5 outputs + multi-source last_event_ts identified")
GRADES[("medium", "raw_source", "HQ7")] = (1, "Wrong about aggregation impact; misses customer_id-not-on-shipments_seq extraction error")
GRADES[("medium", "raw_source", "HQ8")] = (2, "All 4 kwargs + run + registered/heuristic mechanism + fallback correct")
GRADES[("medium", "raw_source", "HQ9")] = (1, "Structural removals + 3 affected golds correct; misses new orphan-table warning")
GRADES[("medium", "raw_source", "HQ10")] = (2, "All 3 distinctions covered with specific examples")

# repo_synthetic_medium DSL
GRADES[("medium", "compact_graph_dsl", "HQ1")] = (1, "Both files named but misses the window; wrongly says attribution_funnel_daily is only indirectly affected")
GRADES[("medium", "compact_graph_dsl", "HQ2")] = (2, "Producer + 2 consumers correct")
GRADES[("medium", "compact_graph_dsl", "HQ3")] = (1, "Names T16/T17 affected but completely misses the silent .filter(return_qty > 0) row-loss")
GRADES[("medium", "compact_graph_dsl", "HQ4")] = (1, "Two correct chains but adds many wrong contributors")
GRADES[("medium", "compact_graph_dsl", "HQ5")] = (0, "OpenRouter connection error; no answer")
GRADES[("medium", "compact_graph_dsl", "HQ6")] = (2, "All 4 group keys + 5 outputs + multi-source identified")
GRADES[("medium", "compact_graph_dsl", "HQ7")] = (1, "Wrong conclusion: says aggregation would break; misses that .groupBy(order_id) is unaffected")
GRADES[("medium", "compact_graph_dsl", "HQ8")] = (0, "Refuses class structure; secondary recognition mechanism is vague")
GRADES[("medium", "compact_graph_dsl", "HQ9")] = (1, "Edge/table removals + 4 affected golds correct but misses new warnings")
GRADES[("medium", "compact_graph_dsl", "HQ10")] = (1, "Right marker + impact-analysis but WRONG about registration location (says inside DEGraph tool code, actually .degraph/helpers.json)")

# repo_synthetic_small RAW
GRADES[("small", "raw_source", "HQ1")] = (1, "Names both tables + 3 customer_ltv places but WRONG fact: claims three-way join uses country_code (actually uses customer_id)")
GRADES[("small", "raw_source", "HQ2")] = (2, "Producer = customer_ltv.py; no consumers correct")
GRADES[("small", "raw_source", "HQ3")] = (2, "Null-propagation in customer_profile + customer_ltv correctly identified")
GRADES[("small", "raw_source", "HQ4")] = (1, "orders_raw.total_amount correct but adds wrong contributors customers_raw.* and events_raw.*")
GRADES[("small", "raw_source", "HQ5")] = (1, "Graph change + 10 values correct but misses the bronze-becomes-70% semantic gotcha")
GRADES[("small", "raw_source", "HQ6")] = (2, "customers_raw + products_raw read status correct")
GRADES[("small", "raw_source", "HQ7")] = (1, "Both aggregations identified but WRONG: claims NTILE partitioned by customer_id (no PARTITION BY)")
GRADES[("small", "raw_source", "HQ8")] = (2, "Global vs per-country quartile semantics correctly explained")
GRADES[("small", "raw_source", "HQ9")] = (2, "All 3 kwargs + setup/config/database resolution chain traced thoroughly")
GRADES[("small", "raw_source", "HQ10")] = (1, "Addresses opaque-call disappearance but doesnt explicitly address whether the other 2 warnings change")

# repo_synthetic_small DSL
GRADES[("small", "compact_graph_dsl", "HQ1")] = (2, "T4 + T5 references with specific edges identified; 4 distinct uses substantively covered")
GRADES[("small", "compact_graph_dsl", "HQ2")] = (2, "Producer + no consumers correct")
GRADES[("small", "compact_graph_dsl", "HQ3")] = (2, "Null-propagation through both tables, no row loss correctly stated")
GRADES[("small", "compact_graph_dsl", "HQ4")] = (1, "orders_raw.total_amount correct but adds country_code (DSL appears to misrepresent NTILE as depending on country_code)")
GRADES[("small", "compact_graph_dsl", "HQ5")] = (1, "Graph change correct; same gotcha-miss as RAW HQ5")
GRADES[("small", "compact_graph_dsl", "HQ6")] = (2, "Both customers_raw + products_raw read status correct")
GRADES[("small", "compact_graph_dsl", "HQ7")] = (1, "Identifies country_avg + NTILE but adds upstream customer_profile aggregate as out-of-scope third")
GRADES[("small", "compact_graph_dsl", "HQ8")] = (0, "Read timed out; no answer")
GRADES[("small", "compact_graph_dsl", "HQ9")] = (0, "Names only mk= and sink= (latter not a constructor kwarg); misses target_table and mode; refuses f-string resolution")
GRADES[("small", "compact_graph_dsl", "HQ10")] = (2, "Explicitly addresses all 3 warnings: opaque-call disappears, dynamic-agg remains, orphan-table remains")


FILES = {
    "clinical_claims": REPO / "results/metrics/google-gemini-2.0-flash-001_20260528_101952_heldout.json",
    "medium":          REPO / "results/metrics/google-gemini-2.0-flash-001_20260528_102114_heldout.json",
    "small":           REPO / "results/metrics/google-gemini-2.0-flash-001_20260528_102116_heldout.json",
}


def main():
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
        print(f"Graded {bench}: {path.name}")
    print("\nDone. Run --score on each path to compute aggregates.")


if __name__ == "__main__":
    main()
