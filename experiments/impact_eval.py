"""Evaluate DEGraph's static change-impact analysis: precision / recall / F1.

For a set of realistic change scenarios (rename/drop a column), the ground-truth
set of affected downstream columns is known *from the source* (hand-derived, the
same authority as the sealed Q&A reference answers). We compare DEGraph's computed
impact set (graph reachability, no execution, no LLM) against that ground truth
and report precision/recall/F1 per scenario, per benchmark, and pooled.

This is the post-BM25-pivot spine evidence: a capability neither keyword retrieval
(no structure) nor runtime lineage tools (need execution) provide.

Scenario set (21): the three synthetic medallion benchmarks PLUS the real
third-party dbdemos retail Spark-Declarative-Pipelines chain (loaded from the
committed `dbdemos_retail_sdp.graph.json` fixture so the real-code scenarios
reproduce without the external repo). Scenarios deliberately span edge types
(derive / filter-col / aggregate / join-key / window) and hop depths, and include
columns whose flow crosses a `spark.sql` CTE or a DLT-body subquery aggregate —
the known recall-gap family — so the recall number is honest, not inflated.

GT convention (consistent with the original 8 scenarios):
  * include every downstream column whose value depends on the changed column and
    whose provenance the extractor's lineage model represents (derives, aggregates,
    join/group keys, CTE projections, carried passthroughs the extractor emits);
  * exclude opaque columns (dynamic `*aggregations`, the predict_churn UDF) — they
    are genuinely unknowable from source;
  * a column is never downstream of itself (the seed is excluded from `found`).

Run:  python experiments/impact_eval.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
from degraph.impact import build_forward_index, column_impact  # noqa: E402

_PREFIXES = ("main.dbdemos_ecom.", "meridian.silver.", "meridian.bronze.", "meridian.")


def _short(fqn: str) -> str:
    for p in _PREFIXES:
        if fqn.startswith(p):
            fqn = fqn[len(p):]
            break
    # dbdemos DLT function bodies surface the silver table as `spark_<name>`
    # (the `spark.read...`-rooted dataframe); normalize to the table name so the
    # node label matches the developer-facing table (`spark_churn_users` ->
    # `churn_users`).
    if fqn.startswith("spark_"):
        fqn = fqn[len("spark_"):]
    return fqn


# (benchmark, change-target table, column) -> ground-truth affected {table.col}.
# Ground truth hand-derived from source. The first 8 are the original set; the
# remaining 13 (S-*/M-*/D-*) extend coverage to more edge types, both medallion
# tiers as the change-point, and real dbdemos code.
SCENARIOS = [
    # ----- original 8 -------------------------------------------------------
    ("repo_synthetic_small", "orders_raw", "total_amount", {
        "orders_cleaned.total_amount", "orders_cleaned.is_high_value",
        "customer_profile.lifetime_revenue",
        "customer_ltv.lifetime_revenue",
        "customer_ltv.ltv_vs_country_avg", "customer_ltv.revenue_quartile",
        "customer_ltv.ltv_tier",
        # NOTE: customer_ltv.avg_country_ltv removed — it is an intermediate CTE
        # column (country_avg), NOT an output column of customer_ltv
        # (`SELECT * FROM ranked`; ranked never selects it). GT phantom fix.
    }),
    ("repo_synthetic_small", "orders_raw", "order_ts", {
        "orders_cleaned.order_ts", "orders_cleaned.order_date",
        "orders_cleaned.order_year", "orders_cleaned.order_month",
        "customer_profile.last_order_ts",
        "customer_ltv.last_order_ts", "customer_ltv.days_since_last_order",
        "product_performance.order_date",
    }),
    ("repo_synthetic_small", "customers_raw", "country_code", {
        "customer_profile.country_code", "customer_ltv.country_code",
        "customer_ltv.ltv_vs_country_avg",
        # customer_ltv.avg_country_ltv removed (same phantom as above).
        # ltv_vs_country_avg kept: country_code is the GROUP BY key of country_avg,
        # so it affects avg_country_ltv's value -> ltv_vs_country_avg.
    }),
    ("repo_synthetic_small", "customers_raw", "email", {
        "customer_profile.email", "customer_ltv.email",
    }),
    ("silver_clinical_claims", "claim_header", "claim_status_hdr", {
        "clinical_claims.processing_status", "clinical_claims.claim_outcome",
    }),
    ("silver_clinical_claims", "claim_header", "product_code_hdr", {
        "clinical_claims.plan_id", "clinical_claims.payer_id",
        "clinical_claims.coverage_plans", "clinical_claims.product_code",
        "clinical_claims.extension_source_values",
    }),
    ("repo_synthetic_medium", "orders_raw", "quantity", {
        "orders_cleaned.quantity", "inventory_turnover_daily.sold_qty",
        "inventory_turnover_daily.turnover_rate",
        "inventory_turnover_daily.days_of_stock",
        "revenue_forecast_features.avg_turnover",
    }),
    ("repo_synthetic_medium", "marketing_attribution_raw", "utm_campaign", {
        "marketing_touchpoints.utm_campaign",
        "attribution_funnel_daily.utm_campaign",
    }),

    # ----- new: repo_synthetic_small (6) ------------------------------------
    # S-a: filter-referenced column; status feeds the cleanup derive (and the
    # row filter, which produces no new column).
    ("repo_synthetic_small", "orders_raw", "status", {
        "orders_cleaned.status",
    }),
    # S-b: one column feeding two count() aggregates in two sink tables, plus
    # the carried passthrough and the gold carry of total_orders.
    ("repo_synthetic_small", "orders_raw", "order_id", {
        "orders_cleaned.order_id",
        "customer_profile.total_orders", "customer_ltv.total_orders",
        "product_performance.order_count",
    }),
    # S-c: window order-key + suffix-rename + max() aggregate, multi-hop.
    # (recall-gap family: the suffix-rename/window chain is not column-linked.)
    ("repo_synthetic_small", "events_raw", "event_ts", {
        "customer_profile.last_event_ts", "customer_ltv.last_event_ts",
    }),
    # S-d: join key + window partition key + group key.
    ("repo_synthetic_small", "customers_raw", "customer_id", {
        "customer_profile.customer_id", "customer_ltv.customer_id",
    }),
    # S-f: change-point at the SILVER tier (not bronze); group key into gold.
    ("repo_synthetic_small", "orders_cleaned", "order_date", {
        "product_performance.order_date",
    }),
    # S-g: change-point at the GOLD tier; intra-table window->CASE derive.
    # (recall-gap family: SQL-CTE output -> Python withColumn boundary.)
    ("repo_synthetic_small", "customer_ltv", "revenue_quartile", {
        "customer_ltv.ltv_tier",
    }),

    # ----- new: repo_synthetic_medium (3) -----------------------------------
    # M-a: numeric feeding two SQL-CTE-internal derives + a downstream agg.
    # (recall-gap family: intra-CTE column flow.)
    ("repo_synthetic_medium", "inventory_daily", "on_hand_units", {
        "inventory_turnover_daily.on_hand_units",  # passthrough output (inv.on_hand_units)
        "inventory_turnover_daily.turnover_rate",
        "inventory_turnover_daily.days_of_stock",
        "revenue_forecast_features.avg_turnover",
    }),
    # M-b: wide multi-table blast radius (order_id replicates the small chain
    # AND feeds the inventory CTE count). sales_count is CTE-internal (missed).
    ("repo_synthetic_medium", "orders_raw", "order_id", {
        "orders_cleaned.order_id",
        "customer_profile.total_orders", "customer_ltv.total_orders",
        "product_performance.order_count",
        "revenue_forecast_features.total_orders",
        "inventory_turnover_daily.sales_count",
        # GT completeness fixes (order_id is the highest-fan-out scenario):
        "customer_return_rate.return_rate",       # = total_returns / total_orders
        "customer_return_rate.total_orders",       # passthrough of customer_profile.total_orders
        "attribution_funnel_daily.purchase_count", # COUNT(DISTINCT order_id)
    }),
    # M-c: gold->gold single hop; change-point at the gold tier.
    ("repo_synthetic_medium", "inventory_turnover_daily", "turnover_rate", {
        "revenue_forecast_features.avg_turnover",
    }),

    # ----- new: dbdemos retail (REAL Databricks DLT code) (4) ----------------
    # D-a: flagship real-code silver->gold derive (the diff_demo headline).
    ("dbdemos_retail_sdp", "churn_users", "creation_date", {
        "churn_features.days_since_creation",
    }),
    # D-b: real-code datediff derive.
    ("dbdemos_retail_sdp", "churn_users", "last_activity_date", {
        "churn_features.days_since_last_activity",
    }),
    # D-c: real-code sum() aggregate via a DLT-body subquery join.
    # (recall-gap family: aggregate in an intermediate df assignment.)
    ("dbdemos_retail_sdp", "churn_orders", "amount", {
        "churn_features.total_amount",
    }),
    # D-d: real-code count_distinct() aggregate via a DLT-body subquery.
    # (recall-gap family.)
    ("dbdemos_retail_sdp", "churn_app_events", "session_id", {
        "churn_features.session_count",
    }),
]


def _load(bench: str) -> dict:
    return json.loads(
        (REPO / "results" / "graphs" / f"{bench}.graph.json").read_text(encoding="utf-8")
    )


def main() -> int:
    fwd_cache: dict[str, tuple] = {}
    by_bench: dict[str, list[int]] = {}  # bench -> [tp, fp, fn]
    tot_tp = tot_fp = tot_fn = 0
    print(f"{'scenario':54s} {'P':>5s} {'R':>5s} {'F1':>5s}  found/GT")
    print("-" * 86)
    for bench, table, col, gt in SCENARIOS:
        if bench not in fwd_cache:
            g = _load(bench)
            fwd_cache[bench] = (g, build_forward_index(g))
        g, fwd = fwd_cache[bench]
        seed_self = f"{table}.{col}"
        found = {_short(x) for x in column_impact(g, table, col, _fwd=fwd)}
        found.discard(seed_self)  # a column is not downstream of itself
        tp = len(found & gt)
        fp = len(found - gt)
        fn = len(gt - found)
        prec = tp / (tp + fp) if (tp + fp) else 1.0
        rec = tp / (tp + fn) if (tp + fn) else 1.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
        tot_tp += tp
        tot_fp += fp
        tot_fn += fn
        b = by_bench.setdefault(bench, [0, 0, 0])
        b[0] += tp
        b[1] += fp
        b[2] += fn
        name = f"{bench.split('_')[-1] if bench != 'dbdemos_retail_sdp' else 'dbdemos'}: {table}.{col}"
        print(f"{name:54s} {prec*100:4.0f}% {rec*100:4.0f}% {f1*100:4.0f}%  {tp}+{fp}fp/{len(gt)}")
        miss = gt - found
        if miss:
            print(f"    missed (recall gap): {sorted(miss)}")
        extra = found - gt
        if extra:
            print(f"    FALSE POSITIVE: {sorted(extra)}")
    print("-" * 86)
    # per-benchmark
    for bench in sorted(by_bench):
        tp, fp, fn = by_bench[bench]
        P = tp / (tp + fp) if (tp + fp) else 1.0
        R = tp / (tp + fn) if (tp + fn) else 1.0
        F1 = 2 * P * R / (P + R) if (P + R) else 0.0
        print(f"  {bench:30s} P {P*100:3.0f}%  R {R*100:3.0f}%  F1 {F1*100:3.0f}%  "
              f"(tp={tp} fp={fp} fn={fn})")
    print("-" * 86)
    P = tot_tp / (tot_tp + tot_fp) if (tot_tp + tot_fp) else 1.0
    R = tot_tp / (tot_tp + tot_fn) if (tot_tp + tot_fn) else 1.0
    F1 = 2 * P * R / (P + R) if (P + R) else 0.0
    print(f"{'POOLED (micro, %d scenarios)' % len(SCENARIOS):54s} "
          f"{P*100:4.0f}% {R*100:4.0f}% {F1*100:4.0f}%  "
          f"tp={tot_tp} fp={tot_fp} fn={tot_fn}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
