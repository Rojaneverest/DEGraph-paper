"""_grade_helper.py — apply manual scores to a results JSON.

Usage:
    python experiments/_grade_helper.py <results.json> <experiment_id>

Looks up the scores+notes for <experiment_id> from the dict at the top of this
file and writes them into the results JSON in place. This is a one-off
scratch file used to grade individual experiment runs in this session.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

# E2 — gemma-4-e2b re-run on patched compact graph (2026-05-17)
E2 = {
    "raw_source": {
        "scores": {"Q1": 1, "Q2": 2, "Q3": 1, "Q4": 0, "Q5": 1,
                   "Q6": 2, "Q7": 1, "Q8": 1, "Q9": 2, "Q10": 0},
        "notes": {
            "Q1": "Lists silver tables correctly; wrongly includes product_performance as affected gold; also includes bronze (out of scope)",
            "Q2": "Exact match",
            "Q3": "Answer truncated mid-CTE block; identifies customer_ltv but does not complete the explanation",
            "Q4": "Empty answer",
            "Q5": "Correctly identifies opaque_transform edge removal; wrongly claims column set will change (misses passthrough semantics)",
            "Q6": "Exact match",
            "Q7": "Full dynamic-aggregation answer with sum/avg/max per metric; answer truncated mid-final-paragraph",
            "Q8": "Identifies last_event_ts risk and window null-handling; does not explicitly trace to gold.customer_ltv.last_event_ts",
            "Q9": "Table + mode + merge_keys all correct",
            "Q10": "Answer truncated after first warning header",
        },
    },
    "compact_graph": {
        "scores": {"Q1": 1, "Q2": 2, "Q3": 2, "Q4": 1, "Q5": 1,
                   "Q6": 0, "Q7": 2, "Q8": 1, "Q9": 2, "Q10": 2},
        "notes": {
            "Q1": "Names silver tables correctly; wrongly includes product_performance; does not name gold.customer_ltv specifically",
            "Q2": "Exact match",
            "Q3": "Concise and exactly correct — names gold.customer_ltv.last_event_ts",
            "Q4": "Identifies orders_raw as source bronze table but answer truncated before listing columns",
            "Q5": "Identifies OpaqueTransform removal correctly; truncated answer does not reach the passthrough conclusion",
            "Q6": "REGRESSION vs E1: model incorrectly claims no file reads events_raw, despite a clear ReadsEdge in the graph. Sampling variance at temp 0.1 on 2B model.",
            "Q7": "Reads <unresolved> notation from graph correctly; cites grouping keys, count, dynamic warning",
            "Q8": "Lists 7 _last_event suffixed columns (overly broad); does not isolate last_event_ts as the specific risk column",
            "Q9": "GAINED vs E1: Table + mode + merge_keys ALL CORRECT — the merge_keys fix in the compact graph worked as predicted",
            "Q10": "All 3 warnings correctly identified with file locations and meanings",
        },
    },
}

# E3a — gemini-2.5-flash, RAW mode only (GRAPH lost when daily quota
# hit and the process was killed before JSON serialization). 3 questions
# truncated by max_output_tokens + thinking budget consuming reasoning
# tokens. This is a partial salvage; fixed in the runner via
# thinking_config(thinking_budget=0) and max_output_tokens=4096.
E3a_FLASH_RAW_PARTIAL = {
    "raw_source": {
        "scores": {"Q1": 0, "Q2": 2, "Q3": 2, "Q4": 2, "Q5": 2,
                   "Q6": 2, "Q7": 2, "Q8": 0, "Q9": 2, "Q10": 1},
        "notes": {
            "Q1": "Empty answer — only intro 'the following silver and gold tables need updating:' before truncation by thinking-budget",
            "Q2": "Exact match: silver/orders_cleaned.py",
            "Q3": "Exact match: last_event_ts in customer_ltv — concise and correct",
            "Q4": "Concise correct answer: bronze.orders_raw.total_amount as the single contributing column",
            "Q5": "Excellent — correctly identifies OpaqueTransform removal AND correctly concludes column set will NOT change because trim only mutates values, not schema. Best answer in any experiment for Q5.",
            "Q6": "Exact match",
            "Q7": "Excellent — names grouping keys, count(order_id), dynamic sum/avg/max from metric_columns, distinguishes what graph can vs cannot tell",
            "Q8": "Truncated at 'silver.ecom' — essentially empty content",
            "Q9": "Table + mode + merge_keys all correct",
            "Q10": "Identifies one warning (parse_event_payload) with correct meaning; truncated before reaching other two warnings",
        },
    },
}

# E3 — gemini-2.5-flash-lite, both modes, full run (2026-05-17)
# Re-run after E3a (flash) hit daily quota. Uses thinking_config(thinking_budget=0)
# and max_output_tokens=4096 so answers don't truncate.
E3 = {
    "raw_source": {
        "scores": {"Q1": 2, "Q2": 2, "Q3": 0, "Q4": 2, "Q5": 2,
                   "Q6": 0, "Q7": 2, "Q8": 1, "Q9": 2, "Q10": 2},
        "notes": {
            "Q1": "Names all three correct tables (orders_cleaned, customer_profile, customer_ltv) with no false additions",
            "Q2": "Exact match",
            "Q3": "WRONG — says days_since_last_order would become undefined, but that column derives from last_order_ts not last_event_ts. Model confidently hallucinated the dependency.",
            "Q4": "Correct: only orders_raw.total_amount",
            "Q5": "Excellent — identifies edge removal AND correctly concludes no column-set change because trim is passthrough. Best Q5 answer in any experiment.",
            "Q6": "WRONG — says bronze/03_ingest_events.py consumes events_raw and produces events_raw. Completely inverted producer/consumer relationship. Reference says silver/customer_profile.py is the consumer.",
            "Q7": "Comprehensive — names dynamic sum/avg/max per metric_column, count for order_id, distinguishes graph-known vs graph-unknown",
            "Q8": "Identifies last_event_ts and last_order_ts as at risk; last_order_ts is wrong (not affected per reference); partial credit for correct chain",
            "Q9": "Table + mode + merge_keys all correct",
            "Q10": "All 3 warnings correctly identified (parse_event_payload, dynamic-aggregation, orphan-table)",
        },
    },
    "compact_graph": {
        "scores": {"Q1": 1, "Q2": 2, "Q3": 0, "Q4": 1, "Q5": 2,
                   "Q6": 2, "Q7": 2, "Q8": 1, "Q9": 2, "Q10": 2},
        "notes": {
            "Q1": "Names customer_profile and customer_ltv correctly but MISSES orders_cleaned (reference cites it as silver table affected via column-set propagation)",
            "Q2": "Exact match",
            "Q3": "WRONG — claims last_event_ts derives ltv_vs_country_avg, which is false. ltv_vs_country_avg derives from lifetime_revenue + country avg. Hallucinated dependency.",
            "Q4": "Names total_amount (correct) but adds quantity and unit_price (wrong — not in lineage to lifetime_revenue per graph)",
            "Q5": "Excellent — explicitly references opaque_kind=passthrough field; correctly concludes column set unchanged",
            "Q6": "Exact match — RECOVERED vs E2 GRAPH which hallucinated 'no consumer'",
            "Q7": "Reads <unresolved> notation correctly; concise and accurate",
            "Q8": "Identifies rn → events_latest → events_suffixed → customer_profile chain (correct); then mentions customer_ltv with both last_order_ts and last_event_ts (last_order_ts is wrong)",
            "Q9": "Table + mode + merge_keys all correct",
            "Q10": "All 3 warnings correctly identified",
        },
    },
}

EXPERIMENTS = {"E2": E2, "E3a": E3a_FLASH_RAW_PARTIAL, "E3": E3}


def apply(results_path: Path, exp_id: str) -> None:
    grades = EXPERIMENTS[exp_id]
    data = json.loads(results_path.read_text(encoding="utf-8"))
    for run in data["runs"]:
        mode = run["meta"]["context_mode"]
        if mode not in grades:
            continue
        gmap = grades[mode]
        for r in run["results"]:
            qid = r["id"]
            if qid in gmap["scores"]:
                r["score"] = gmap["scores"][qid]
                r["notes"] = gmap["notes"][qid]
    results_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"{exp_id}: scores applied to {results_path.name}")


if __name__ == "__main__":
    apply(Path(sys.argv[1]), sys.argv[2])
