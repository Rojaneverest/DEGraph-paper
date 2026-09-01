"""Event-payload parsing helpers used by the events ingest pipeline.

The events_raw stream carries a JSON `payload` column whose schema varies
by `event_type`. This module factors the parsing logic out of the bronze
ingest notebook.

### Why this file matters for the DEGraph benchmark

`bronze/03_ingest_events.py` calls `parse_event_payload(df)` exactly once.
Unlike the helpers in `column_transformations.py`, this function is
DELIBERATELY NOT REGISTERED in `.degraph/helpers.json`. That makes it the
benchmark's representative of the "unregistered helper" case:

- The extractor encounters `df = parse_event_payload(df)`.
- No registry entry exists.
- Per `dev/methodology.md` Decision 3.7 default behavior, it emits an
  `OpaqueTransform` edge with `is_passthrough=false` — meaning the column
  set may have changed but the extractor cannot say how.
- A `GraphWarning` is appended noting "unregistered helper
  parse_event_payload at bronze/03_ingest_events.py:N".

The ground-truth graph for this benchmark must reflect that: one
OpaqueTransform edge plus the warning. The Q&A pair on "lineage gaps"
specifically probes whether the LLM can read GraphWarnings and report
"there is an unregistered helper here that changes the columns in an
unknown way."
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType


_PAYLOAD_SCHEMA = StructType([
    StructField("page_url", StringType(), nullable=True),
    StructField("referrer", StringType(), nullable=True),
    StructField("user_agent", StringType(), nullable=True),
    StructField("cart_total", StringType(), nullable=True),
])


def parse_event_payload(df: DataFrame) -> DataFrame:
    """Extract structured fields from the JSON `payload` column.

    Adds four new columns — `page_url`, `referrer`, `user_agent`,
    `cart_total` — parsed from the payload JSON, then drops the raw
    `payload` column. Column-set change is non-trivial: one column out,
    four columns in. This is exactly the kind of transformation the
    DEGraph extractor cannot infer without analyzing the body, and which
    must be reported as opaque-with-warning when not registered.
    """
    parsed = df.withColumn("_parsed", F.from_json(F.col("payload"), _PAYLOAD_SCHEMA))
    parsed = (
        parsed
        .withColumn("page_url", F.col("_parsed.page_url"))
        .withColumn("referrer", F.col("_parsed.referrer"))
        .withColumn("user_agent", F.col("_parsed.user_agent"))
        .withColumn("cart_total", F.col("_parsed.cart_total").cast("decimal(18,2)"))
        .drop("_parsed", "payload")
    )
    return parsed
