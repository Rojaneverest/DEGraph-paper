"""Custom sink classes for non-trivial write patterns.

Production Databricks repos frequently wrap writes in custom classes —
typically to encapsulate merge / upsert / SCD logic that the bare
`df.write.saveAsTable` API does not express directly. This module hosts
the `DeltaMergeSink` class used by `gold/product_performance.py`.

### Why this file matters for the DEGraph benchmark

A bare `DeltaMergeSink(target_table=..., merge_keys=[...]).run()` call
site is **invisible to a naive extractor**: the extractor sees a method
call on an instance of an imported class, with no `.write` and no
`saveAsTable` anywhere. Without explicit recognition, the write would be
silently dropped — which is the single worst failure mode for a lineage
tool (silent gap, no warning).

Per `dev/methodology.md` Decision 3.8, DEGraph handles this with a
two-tier mechanism:

1. **Registered (preferred):** `.degraph/sinks.json` maps `DeltaMergeSink`
   to its semantics — which kwarg holds the target table FQN
   (`target_table`), which holds the write mode (`mode`, default `merge`).
   The extractor emits a proper `Writes` edge with `sink_class="DeltaMergeSink"`
   and `mode="merge"`.

2. **Heuristic fallback:** if the class were *not* registered but matched
   the `*Sink` / `*Writer` name pattern AND had a kwarg in
   `{target_table, target, sink_table}`, the extractor would still emit
   a `Writes` edge (with `<unresolved>` mode) plus a `GraphWarning`.
   This benchmark exercises path 1 — the class IS registered.
"""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable


class DeltaMergeSink:
    """Upsert a source DataFrame into a Delta target via MERGE.

    Encapsulates the standard "SCD-Type-1" upsert pattern: rows matching
    on `merge_keys` are updated in place; non-matching source rows are
    inserted. Encapsulated here rather than inlined at the call site
    because three different gold notebooks use the same pattern with
    different keys.
    """

    def __init__(
        self,
        target_table: str,
        merge_keys: List[str],
        mode: str = "merge",
        update_condition: Optional[str] = None,
    ) -> None:
        self.target_table = target_table
        self.merge_keys = merge_keys
        self.mode = mode
        self.update_condition = update_condition

    def run(self, df: DataFrame) -> None:
        """Execute the merge."""
        spark = SparkSession.getActiveSession()
        if not spark.catalog.tableExists(self.target_table):
            df.write.format("delta").saveAsTable(self.target_table)
            return

        target = DeltaTable.forName(spark, self.target_table)
        condition = " AND ".join(f"t.{k} = s.{k}" for k in self.merge_keys)
        builder = target.alias("t").merge(df.alias("s"), condition)
        if self.update_condition:
            builder = builder.whenMatchedUpdateAll(condition=self.update_condition)
        else:
            builder = builder.whenMatchedUpdateAll()
        builder.whenNotMatchedInsertAll().execute()
