"""Regression tests that pin the paper's reproducible claims.

Two groups:
  1. Synthetic-benchmark edge counts — the extractor's accuracy numbers (§5.1)
     are computed against these graphs; if the counts drift, the reported
     precision/recall no longer correspond to the committed benchmarks.
  2. Real-idiom support (§5.5 external-validity study) — the three Databricks
     idioms the extractor learned to handle (Fixes #9-#11), pinned via small
     self-contained snippets so the test needs no external repo clone.

Run:  python -m pytest tests/ -q
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
from degraph.extractor.assembler import extract_repo  # noqa: E402
from degraph.impact import column_impact  # noqa: E402


def _extract(path: Path) -> dict:
    return json.loads(extract_repo(path).model_dump_json())


def _kinds(graph: dict) -> Counter:
    return Counter(e["kind"] for e in graph["edges"])


# --------------------------------------------------------------------------- #
# 1. Synthetic-benchmark edge counts (the accuracy numbers rest on these)      #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("benchmark, expected_edges", [
    ("repo_synthetic_small", 57),
    # 121 since the select column-rename fix (col("id").alias("user_id") is a real
    # lineage edge); was 80 before — the +41 are genuine renames in the clinical
    # canonical projection. Impact precision stays 100% (no false positives).
    ("silver_clinical_claims", 121),
    # 174 since the spark.sql CTE/outer-SELECT column-lineage fix: computed
    # columns in an outer SELECT (e.g. `inv.on_hand/NULLIF(ds.sold,0) AS
    # turnover_rate`) now get a DerivesEdge carrying their source columns; was
    # 163 before — the +11 are genuine derive edges for previously-dropped SQL
    # computed columns. Lifts impact recall through spark.sql blocks; impact
    # precision stays 100% (no false positives).
    ("repo_synthetic_medium", 174),
])
def test_benchmark_edge_counts_stable(benchmark, expected_edges):
    path = REPO / "data" / "benchmarks" / benchmark
    if not path.exists():
        pytest.skip(f"benchmark {benchmark} not present")
    assert len(_extract(path)["edges"]) == expected_edges


# --------------------------------------------------------------------------- #
# 2. Real Databricks idiom support (Fixes #9-#11)                              #
# --------------------------------------------------------------------------- #

def _write(tmp_path: Path, name: str, src: str) -> Path:
    (tmp_path / name).write_text(src, encoding="utf-8")
    return tmp_path


def test_fluent_unassigned_chain(tmp_path):
    """Fix #9: a whole read->transform->write pipeline as one un-assigned
    fluent expression (no df = ...) must yield read + derives + write."""
    src = (
        "from pyspark.sql.functions import col, sha1\n"
        "(spark.readStream.table('bronze_users')\n"
        "    .withColumnRenamed('id', 'user_id')\n"
        "    .withColumn('email', sha1(col('email')))\n"
        "    .writeStream.table('silver_users'))\n"
    )
    g = _extract(_write(tmp_path, "fluent.py", src))
    k = _kinds(g)
    assert k["reads"] >= 1 and k["derives"] >= 1 and k["writes"] >= 1
    fqns = {t["fqn"] for t in g["tables"]}
    assert {"bronze_users", "silver_users"} <= fqns


def test_dlt_decorated_function(tmp_path):
    """Fix #10: a @dp.table-decorated function -> output table is the function
    name; the returned chain is the pipeline."""
    src = (
        "from pyspark import pipelines as dp\n"
        "from pyspark.sql.functions import col, sha1\n"
        "@dp.table(comment='clean users')\n"
        "def silver_users():\n"
        "    return (spark.readStream.table('bronze_users')\n"
        "            .withColumn('email', sha1(col('email'))))\n"
    )
    g = _extract(_write(tmp_path, "dlt.py", src))
    k = _kinds(g)
    assert k["reads"] >= 1 and k["derives"] >= 1 and k["writes"] >= 1
    # the write target is the function name
    write_targets = {e["target"] for e in g["edges"] if e["kind"] == "writes"}
    assert "table:silver_users" in write_targets


def test_interproc_ingestion_helper(tmp_path):
    """Fix #11: a parameterized single-return ingestion helper called N times
    resolves read source + write target per call site."""
    src = (
        "from pyspark.sql.functions import col\n"
        "def ingest(folder, fmt, table):\n"
        "    return (spark.readStream.format('cloudFiles')\n"
        "            .option('cloudFiles.format', fmt).load(folder)\n"
        "            .writeStream.table(table))\n"
        "ingest('/vol/orders', 'json', 'orders_bronze')\n"
        "ingest('/vol/users', 'json', 'users_bronze')\n"
    )
    g = _extract(_write(tmp_path, "interproc.py", src))
    write_targets = {e["target"] for e in g["edges"] if e["kind"] == "writes"}
    assert {"table:orders_bronze", "table:users_bronze"} <= write_targets


def test_imperative_assignment_still_works(tmp_path):
    """Baseline: the original imperative style (df = ...; df.write...) must
    still extract fully (guards against the unroll_chain rewrite regressing)."""
    src = (
        "from pyspark.sql.functions import col, upper\n"
        "df = spark.read.table('bronze_users')\n"
        "df = df.withColumn('country_up', upper(col('country')))\n"
        "df.write.saveAsTable('silver_users')\n"
    )
    g = _extract(_write(tmp_path, "imperative.py", src))
    k = _kinds(g)
    assert k["reads"] >= 1 and k["derives"] >= 1 and k["writes"] >= 1


# --------------------------------------------------------------------------- #
# 3. Column-level impact resolves through a known chain (Fix #12)              #
# --------------------------------------------------------------------------- #

DBDEMOS_SDP = Path(
    r"C:\Users\thapa\Desktop\Research\_external_repos\dbdemos-notebooks"
    r"\demo-retail\lakehouse-retail-c360\01-Data-ingestion"
    r"\01.2-SDP-python\transformations")


def test_real_code_precision_no_false_positives():
    """The real-code ground truth (dbdemos retail SDP) must keep 100% precision —
    zero false positives — the no-hallucination property impact analysis relies on.
    Skips when the (gitignored) dbdemos clone is absent."""
    if not DBDEMOS_SDP.exists():
        pytest.skip("dbdemos clone not present")
    gt = json.loads((REPO / "data" / "ground_truth"
                     / "dbdemos_retail_sdp.graph.json").read_text(encoding="utf-8"))
    sys.path.insert(0, str(REPO / "experiments"))
    from extractor_precision_dbdemos import _keys_by_kind  # noqa
    ex = _extract(DBDEMOS_SDP)
    gtb, exb = _keys_by_kind(gt), _keys_by_kind(ex)
    false_positives = {k: exb.get(k, set()) - gtb.get(k, set()) for k in exb}
    assert all(not v for v in false_positives.values()), false_positives


def test_column_impact_through_dlt_chain(tmp_path):
    """Fix #12: column-level impact must resolve across schema-less DLT tables
    (the bare-name-provenance fix). bronze.x -> silver derives -> gold derives."""
    src = (
        "from pyspark import pipelines as dp\n"
        "from pyspark.sql.functions import col, datediff, current_date\n"
        "@dp.table()\n"
        "def silver_users():\n"
        "    return spark.readStream.table('bronze_users').withColumn('signup', col('raw_signup'))\n"
        "@dp.table()\n"
        "def gold_users():\n"
        "    return spark.readStream.table('silver_users').withColumn('age_days', datediff(current_date(), col('signup')))\n"
    )
    g = _extract(_write(tmp_path, "chain.py", src))
    downstream = column_impact(g, "silver_users", "signup")
    assert any("gold_users.age_days" in c for c in downstream), downstream
