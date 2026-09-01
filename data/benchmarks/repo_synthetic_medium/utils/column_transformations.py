"""Column-transformation helpers reused across silver pipelines.

These are the kind of utility functions every production Databricks repo
accumulates — a few generic operations on DataFrame column sets, factored
out so individual silver notebooks can stay focused on business logic.

### Why this file matters for the DEGraph benchmark

The two functions below appear in `silver/orders_cleaned.py` and
`silver/customer_profile.py` as imported helpers. The DEGraph extractor
will encounter call sites of the form

    df = trim_string_columns(df)
    df = rename_columns_with_suffix(df, "_claim")

and must decide how to represent them. Per `dev/methodology.md` Decision 3.7:

- WITHOUT a known-helper registry entry, both calls produce an
  `OpaqueTransform` edge with `is_passthrough=false` — the variable's
  identity is preserved through the call but column-level semantics are lost.

- WITH a registry entry in `.degraph/helpers.json` declaring `trim_string_columns`
  as `"kind": "passthrough"` and `rename_columns_with_suffix` as
  `"kind": "suffix_rename"`, the extractor emits semantically-rich edges:
  an `OpaqueTransform` with `is_passthrough=true` for the trim (preserves
  the column set, just mutates values), and one explicit `Derives` edge
  per renamed column for the suffix rename.

This file therefore exercises the "lift ⚠️ → ✅" mechanism that the
registry provides, and validates that the registry-driven path produces
strictly more information than the default opaque path.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Strip leading/trailing whitespace from every string-typed column.

    Pure column-set passthrough: the output schema matches the input schema
    column-for-column; only the string values are mutated. Registered in
    `.degraph/helpers.json` as `"kind": "passthrough"`.
    """
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_cols:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
    return df


def rename_columns_with_suffix(df: DataFrame, suffix: str) -> DataFrame:
    """Append `suffix` to every column name in `df`.

    Used in silver-layer joins to disambiguate columns that share names
    across the two sides of a join (e.g. both `orders` and `customers`
    have a `customer_id` — only one survives, but other columns may need
    suffixing). Registered in `.degraph/helpers.json` as
    `"kind": "suffix_rename"` with `suffix_arg: 1` so the extractor knows
    to read the suffix from the second positional argument.
    """
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name + suffix)
    return df
