"""
silver_clinical_claims.py

Builds meridian.silver.clinical_claims from bronze claim, member, and
provider sources for Meridian Health's claims adjudication warehouse.

Bronze sources (10):
    claim_header       — one row per claim submission (header grain)
    claim_diagnosis    — per-claim ICD-10 diagnosis codes (long)
    claim_procedure    — per-claim HCPCS/CPT procedure codes (long)
    claim_cob_payment  — coordination-of-benefits submitted payments (long)
    claim_payable      — adjudicated payable amounts per line
    claim_interest     — interest adjustments on late payments
    claim_occurrence   — occurrence and occurrence-span codes (long)
    claim_provider     — claim-level provider assignments (billing, rendering)
    member_eligibility — subscriber / member demographic and eligibility data
    provider_registry  — NPI registry: specialty, taxonomy, PCP flag

Pipeline (one COMMAND block per stage):
    1.  SQL CTE — aggregate COB payments to claim+line grain
    2.  trim_string_columns — blank strings -> NULL
    3.  suffix_rename — column disambiguation before cascade joins
    4.  Pivot diagnoses wide (ICD-10, up to 5 + overflow array)
    5.  Pivot procedures wide (HCPCS, up to 4 + other_procedures array)
    6.  Cascade joins: diag + proc + cob + provider + payment + interest
    7.  Derive processing_status / claim_outcome (when-chain)
    8.  Build identifiers map<string, struct>
    9.  Build coverage_plans array<struct>
   10.  Build service_locations array<struct>
   11.  Provider specialty rollup — SQL CTE + Window.partitionBy().orderBy()
   12.  build_claim_criteria() — shared predicate dict (8 categories)
   13.  Classify claim_category (first-match) + claim_categories (array-acc)
   14.  extension_source_values + other_amounts name/value arrays
   15.  Final canonical column projection (~85 columns)
   16.  Data quality assertions
   17.  Write -> meridian.silver.clinical_claims (delta, overwrite)
"""

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (
    array, array_sort, coalesce, col, concat_ws, expr, lit,
    regexp_replace, row_number, struct, trim, when,
)
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# Catalog / schema coordinates.  Override MERIDIAN_DATABASE to point at a
# different Unity Catalog schema in non-prod environments.
DATABASE = "meridian"
BRONZE   = f"{DATABASE}.bronze"
SILVER   = f"{DATABASE}.silver"
_env_db  = os.environ.get("MERIDIAN_DATABASE", DATABASE)   # runtime override hook


# COMMAND ----------
# Load bronze DataFrames.

claim_hdr_df      = spark.table(f"{BRONZE}.claim_header")
claim_diag_df     = spark.table(f"{BRONZE}.claim_diagnosis")
claim_proc_df     = spark.table(f"{BRONZE}.claim_procedure")
claim_cob_df      = spark.table(f"{BRONZE}.claim_cob_payment")
claim_payable_df  = spark.table(f"{BRONZE}.claim_payable")
claim_interest_df = spark.table(f"{BRONZE}.claim_interest")
claim_occ_df      = spark.table(f"{BRONZE}.claim_occurrence")
claim_prov_df     = spark.table(f"{BRONZE}.claim_provider")
member_df         = spark.table(f"{BRONZE}.member_eligibility")
provider_df       = spark.table(f"{BRONZE}.provider_registry")


# COMMAND ----------
# Aggregate coordination-of-benefits payments and adjudicated payable amounts
# to claim+line grain using CTEs so that the GROUP BY and NULL-coalescing
# happen in one SQL pass before the main PySpark join cascade.
#
# Two CTEs:
#   cob_line_agg  — sums submitted COB amounts per (claim_id, claim_line_id)
#   payable_agg   — sums adjudicated paid amount + contractual adjustments
#
# The outer LEFT JOINs preserve every claim_header row even when no COB or
# payable record exists for that line.

aggregated_cob_df = spark.sql(f"""
    WITH cob_line_agg AS (
        SELECT
            claim_id,
            claim_line_id,
            SUM(allowed_amount)     AS cob_allowed_amount,
            SUM(billed_amount)      AS cob_billed_amount,
            SUM(patient_liability)  AS cob_patient_liability,
            SUM(copay_amount)       AS cob_copay_amount,
            SUM(deductible_amount)  AS cob_deductible_amount
        FROM {BRONZE}.claim_cob_payment
        GROUP BY claim_id, claim_line_id
    ),
    payable_agg AS (
        SELECT
            claim_id,
            claim_line_id,
            SUM(paid_amount)        AS total_paid_amount,
            SUM(contractual_adj)    AS total_contractual_adj
        FROM {BRONZE}.claim_payable
        GROUP BY claim_id, claim_line_id
    )
    SELECT
        h.claim_id,
        h.claim_line_id,
        COALESCE(c.cob_allowed_amount,    0) AS cob_allowed_amount,
        COALESCE(c.cob_billed_amount,     0) AS cob_billed_amount,
        COALESCE(c.cob_patient_liability, 0) AS cob_patient_liability,
        COALESCE(c.cob_copay_amount,      0) AS cob_copay_amount,
        COALESCE(c.cob_deductible_amount, 0) AS cob_deductible_amount,
        COALESCE(p.total_paid_amount,     0) AS total_paid_amount,
        COALESCE(p.total_contractual_adj, 0) AS total_contractual_adj
    FROM {BRONZE}.claim_header h
    LEFT JOIN cob_line_agg c
        ON  h.claim_id      = c.claim_id
        AND h.claim_line_id = c.claim_line_id
    LEFT JOIN payable_agg p
        ON  h.claim_id      = p.claim_id
        AND h.claim_line_id = p.claim_line_id
""")


# COMMAND ----------
# Trim all string columns so blank or whitespace-only values ("  ") become
# NULL — this keeps downstream IS NULL / COALESCE logic well-behaved across
# every join path.

def trim_string_columns(df):
    """Replace blank/whitespace-only strings with NULL in every string column."""
    from pyspark.sql.types import StringType
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in str_cols:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c))),
        )
    return df


claim_hdr_df      = trim_string_columns(claim_hdr_df)
claim_diag_df     = trim_string_columns(claim_diag_df)
claim_proc_df     = trim_string_columns(claim_proc_df)
claim_prov_df     = trim_string_columns(claim_prov_df)
member_df         = trim_string_columns(member_df)
aggregated_cob_df = trim_string_columns(aggregated_cob_df)


# COMMAND ----------
# Rename columns on each source DataFrame to carry a short table suffix so
# that after the cascade of joins every column's origin is unambiguous (e.g.
# claim_id_hdr vs claim_id_prov vs claim_id_cob).

def suffix_rename(df, suffix):
    """Append *suffix* to every column name (e.g. '_hdr')."""
    return df.toDF(*[f"{c}{suffix}" for c in df.columns])


claim_hdr_df      = suffix_rename(claim_hdr_df,      "_hdr")
claim_prov_df     = suffix_rename(claim_prov_df,     "_prov")
member_df         = suffix_rename(member_df,         "_mbr")
aggregated_cob_df = suffix_rename(aggregated_cob_df, "_cob")
claim_interest_df = suffix_rename(claim_interest_df, "_int")


# COMMAND ----------
# Pivot ICD-10 diagnosis codes wide.
#
# Strategy:
#   1. Left-join claim_hdr to claim_diag on claim_id (long format, one row per code).
#   2. Rank codes within each claim by (diag_priority ASC, icd10_code ASC) using
#      a Window function so the most clinically significant code lands in diag_1.
#   3. Pivot the ranked long rows into wide columns diagnosis_1_* … diagnosis_5_*.
#   4. Collect all codes into other_diagnoses: array<struct> for downstream use.

diag_rank_window = Window.partitionBy("claim_id_hdr").orderBy(
    F.col("diag_priority").asc_nulls_last(),
    F.col("icd10_code").asc_nulls_last(),
)

ranked_diag_df = (
    claim_hdr_df
    .join(claim_diag_df,
          claim_hdr_df["claim_id_hdr"] == claim_diag_df["claim_id"],
          "left")
    .withColumn("diag_rank", row_number().over(diag_rank_window))
)

MAX_DIAG = 5

diag_pivot_exprs = []
for i in range(1, MAX_DIAG + 1):
    diag_pivot_exprs += [
        expr(f"MAX(CASE WHEN diag_rank = {i} THEN icd10_code          END) AS diagnosis_{i}_code"),
        expr(f"MAX(CASE WHEN diag_rank = {i} THEN icd10_description   END) AS diagnosis_{i}_description"),
        expr(f"MAX(CASE WHEN diag_rank = {i} THEN diag_type           END) AS diagnosis_{i}_type"),
        expr(f"MAX(CASE WHEN diag_rank = {i} THEN poa_indicator       END) AS diagnosis_{i}_poa"),
        expr(f"MAX(CASE WHEN diag_rank = {i} THEN diag_priority       END) AS diagnosis_{i}_priority"),
    ]

pivoted_diag_df = (
    ranked_diag_df
    .groupBy("claim_id_hdr")
    .agg(*diag_pivot_exprs)
    .withColumnRenamed("claim_id_hdr", "claim_id_diag")
)

# Build other_diagnoses: array<struct> of all ranked diagnosis records, filtering nulls.
diag_structs = []
for i in range(1, MAX_DIAG + 1):
    diag_structs.append(
        when(
            col(f"diagnosis_{i}_code").isNotNull(),
            struct(
                lit(None).cast("string").alias("reference_id"),
                col(f"diagnosis_{i}_priority").alias("sequence_number"),
                col(f"diagnosis_{i}_code").alias("codeset"),
                col(f"diagnosis_{i}_description").alias("codeset_display"),
                col(f"diagnosis_{i}_type").alias("diagnosis_type"),
                col(f"diagnosis_{i}_poa").alias("present_on_admission_code"),
                lit("ICD-10-CM").alias("codeset_system"),
                lit(None).cast("string").alias("diagnosis_group"),
            ),
        )
    )

pivoted_diag_df = (
    pivoted_diag_df
    .withColumn("other_diagnoses_raw", array(*diag_structs))
    .withColumn(
        "other_diagnoses",
        array_sort(expr("filter(other_diagnoses_raw, x -> x is not null)")),
    )
    .drop("other_diagnoses_raw")
)


# COMMAND ----------
# Pivot HCPCS / CPT procedure codes wide.
#
# Same strategy as diagnoses: rank by (proc_priority, hcpcs_code), pivot up to
# MAX_PROC wide columns, and pack a typed other_procedures array<struct>.

proc_rank_window = Window.partitionBy("claim_id_hdr").orderBy(
    F.col("proc_priority").asc_nulls_last(),
    F.col("hcpcs_code").asc_nulls_last(),
)

ranked_proc_df = (
    claim_hdr_df
    .join(claim_proc_df,
          claim_hdr_df["claim_id_hdr"] == claim_proc_df["claim_id"],
          "left")
    .withColumn("proc_rank", row_number().over(proc_rank_window))
)

MAX_PROC = 4

proc_pivot_exprs = []
for i in range(1, MAX_PROC + 1):
    proc_pivot_exprs += [
        expr(f"MAX(CASE WHEN proc_rank = {i} THEN hcpcs_code       END) AS procedure_{i}_code"),
        expr(f"MAX(CASE WHEN proc_rank = {i} THEN hcpcs_description END) AS procedure_{i}_description"),
        expr(f"MAX(CASE WHEN proc_rank = {i} THEN modifier_code    END) AS procedure_{i}_modifier"),
        expr(f"MAX(CASE WHEN proc_rank = {i} THEN proc_date        END) AS procedure_{i}_date"),
        expr(f"MAX(CASE WHEN proc_rank = {i} THEN proc_priority    END) AS procedure_{i}_priority"),
        expr(f"""MAX(CASE WHEN proc_rank = {i} THEN
                    CASE WHEN is_principal_proc = 'Y' THEN 'Principal' ELSE 'Secondary' END
               END) AS procedure_{i}_type"""),
    ]

pivoted_proc_df = (
    ranked_proc_df
    .groupBy("claim_id_hdr")
    .agg(*proc_pivot_exprs)
    .withColumnRenamed("claim_id_hdr", "claim_id_proc")
)

proc_structs = []
for i in range(1, MAX_PROC + 1):
    proc_structs.append(
        when(
            col(f"procedure_{i}_code").isNotNull(),
            struct(
                lit(None).cast("string").alias("reference_id"),
                col(f"procedure_{i}_code").alias("codeset"),
                col(f"procedure_{i}_description").alias("codeset_display"),
                col(f"procedure_{i}_modifier").alias("modifier_code"),
                col(f"procedure_{i}_date").alias("procedure_date"),
                col(f"procedure_{i}_priority").alias("sequence_number"),
                col(f"procedure_{i}_type").alias("procedure_type"),
                lit("HCPCS").alias("codeset_system"),
            ),
        )
    )

pivoted_proc_df = (
    pivoted_proc_df
    .withColumn("other_procedures_raw", array(*proc_structs))
    .withColumn(
        "other_procedures",
        expr("filter(other_procedures_raw, x -> x is not null)"),
    )
    .drop("other_procedures_raw")
)


# COMMAND ----------
# Aggregate occurrence codes to claim grain.
#
# claim_occurrence is in long format (one row per occurrence code per claim).
# Collect into typed structs keyed by occurrence code category, then join
# back to the main chain.

occ_agg_df = (
    claim_occ_df
    .withColumn(
        "occ_struct",
        when(
            F.col("occ_code").isNotNull(),
            struct(
                lit(None).cast("string").alias("reference_id"),
                F.col("occ_code").alias("occurrence_code"),
                F.col("occ_date").alias("occurrence_date"),
                F.col("occ_span_from_date").alias("span_from_date"),
                F.col("occ_span_to_date").alias("span_to_date"),
            ),
        ),
    )
    .groupBy("claim_id")
    .agg(
        F.array_distinct(F.collect_list("occ_struct")).alias("occurrence_codes"),
    )
    .withColumnRenamed("claim_id", "claim_id_occ")
)


# COMMAND ----------
# Aggregate interest adjustments to claim grain.

interest_agg_df = (
    claim_interest_df
    .groupBy("claim_id_int")
    .agg(
        F.sum("interest_amount_int").alias("total_interest_amount"),
        F.count("interest_amount_int").alias("interest_line_count"),
    )
    .withColumnRenamed("claim_id_int", "claim_id_int_agg")
)


# COMMAND ----------
# Cascade joins: attach pivoted diagnoses, pivoted procedures, COB amounts,
# provider assignments, occurrence codes, and interest to the claim header.
# All joins are LEFT so every claim_header row survives regardless of whether
# supplemental detail exists.

final_df = (
    claim_hdr_df
    .join(pivoted_diag_df,
          claim_hdr_df["claim_id_hdr"] == pivoted_diag_df["claim_id_diag"],
          "left")
    .join(pivoted_proc_df,
          claim_hdr_df["claim_id_hdr"] == pivoted_proc_df["claim_id_proc"],
          "left")
    .join(aggregated_cob_df,
          claim_hdr_df["claim_id_hdr"] == aggregated_cob_df["claim_id_cob"],
          "left")
    .join(claim_prov_df,
          claim_hdr_df["claim_id_hdr"] == claim_prov_df["claim_id_prov"],
          "left")
    .join(member_df,
          claim_hdr_df["member_id_hdr"] == member_df["member_id_mbr"],
          "left")
    .join(occ_agg_df,
          claim_hdr_df["claim_id_hdr"] == occ_agg_df["claim_id_occ"],
          "left")
    .join(interest_agg_df,
          claim_hdr_df["claim_id_hdr"] == interest_agg_df["claim_id_int_agg"],
          "left")
)


# COMMAND ----------
# Map claim_status to processing_status and claim_outcome.
#
# claim_status values follow the X12 277 claim-status category codes used by
# the adjudication engine.  processing_status maps to the FHIR ClaimResponse
# status value set; claim_outcome maps to FHIR ClaimResponse.outcome.
#
# Both columns are first-match when-chains driven by the same source column
# so a change to claim_status_hdr vocabulary affects both outputs.

final_df = final_df.withColumn(
    "processing_status",
    when(col("claim_status_hdr") == "Adjudicated", lit("active"))
    .when(col("claim_status_hdr") == "Denied",      lit("active"))
    .when(col("claim_status_hdr") == "Pended",      lit("draft"))
    .when(col("claim_status_hdr") == "Suspended",   lit("draft"))
    .when(col("claim_status_hdr") == "Voided",      lit("cancelled"))
    .when(col("claim_status_hdr") == "Adjusted",    lit("active"))
    .otherwise(lit("active")),
)

final_df = final_df.withColumn(
    "claim_outcome",
    when(col("claim_status_hdr") == "Adjudicated", lit("complete"))
    .when(col("claim_status_hdr") == "Denied",      lit("complete"))
    .when(col("claim_status_hdr") == "Pended",      lit("queued"))
    .when(col("claim_status_hdr") == "Suspended",   lit("error"))
    .when(col("claim_status_hdr") == "Voided",      lit("complete"))
    .when(col("claim_status_hdr") == "Adjusted",    lit("partial"))
    .otherwise(lit(None)),
)


# COMMAND ----------
# Derive member_uid: a stable surrogate key for the member built from the
# subscriber ID and the plan's member sequence number.  Used as a foreign key
# in the identifiers array and in the coverage_plans policy_id derivation.

final_df = final_df.withColumn(
    "member_uid",
    coalesce(
        col("member_uid_mbr"),
        concat_ws("_", col("subscriber_id_hdr"), col("member_seq_hdr")),
    ),
)


# COMMAND ----------
# Build identifiers: array<struct> of every claim-level ID in a uniform
# key/value envelope for FHIR Claim.identifier population.
#
# Each entry carries: reference_id (cross-resource pointer), identifier_type
# (code), identifier (value), and identifier_source (the originating system).

final_df = final_df.withColumn(
    "identifiers",
    array(
        struct(
            lit("claim_id").alias("key"),
            struct(
                lit(None).cast("string").alias("reference_id"),
                lit("claim_id").alias("identifier_type"),
                col("claim_id_hdr").alias("identifier"),
                lit("meridian").alias("identifier_source"),
            ).alias("value"),
        ),
        struct(
            lit("subscriber_id").alias("key"),
            struct(
                lit(None).cast("string").alias("reference_id"),
                lit("subscriber_id").alias("identifier_type"),
                col("subscriber_id_hdr").alias("identifier"),
                lit("meridian").alias("identifier_source"),
            ).alias("value"),
        ),
        struct(
            lit("member_id").alias("key"),
            struct(
                lit(None).cast("string").alias("reference_id"),
                lit("member_id").alias("identifier_type"),
                col("member_id_hdr").alias("identifier"),
                lit("meridian").alias("identifier_source"),
            ).alias("value"),
        ),
        struct(
            lit("payer_claim_control_number").alias("key"),
            struct(
                lit(None).cast("string").alias("reference_id"),
                lit("payer_claim_control_number").alias("identifier_type"),
                col("icn_hdr").alias("identifier"),
                lit("meridian").alias("identifier_source"),
            ).alias("value"),
        ),
        struct(
            lit("provider_claim_id").alias("key"),
            struct(
                lit(None).cast("string").alias("reference_id"),
                lit("provider_claim_id").alias("identifier_type"),
                concat_ws("_", col("claim_id_hdr"), col("billing_npi_prov")).alias("identifier"),
                lit("meridian").alias("identifier_source"),
            ).alias("value"),
        ),
        struct(
            lit("group_number").alias("key"),
            struct(
                lit(None).cast("string").alias("reference_id"),
                lit("group_number").alias("identifier_type"),
                col("group_number_hdr").alias("identifier"),
                lit("meridian").alias("identifier_source"),
            ).alias("value"),
        ),
    ),
)


# COMMAND ----------
# Derive plan_id and payer_id from the product_code via a when-chain.
# Four Meridian Health plan lines map to internal plan codes and CMS-assigned
# NPI-equivalent payer identifiers.  Both intermediate columns are referenced
# inside the coverage_plans struct below, so a product_code change propagates
# to three output columns: plan_id, payer_id, and coverage_plans[0].policy_id.

final_df = final_df.withColumn(
    "plan_id",
    when(col("product_code_hdr") == "MERIDIAN_PPO",  lit("MRD-PPO-001"))
    .when(col("product_code_hdr") == "MERIDIAN_HMO",  lit("MRD-HMO-001"))
    .when(col("product_code_hdr") == "MERIDIAN_EPO",  lit("MRD-EPO-001"))
    .when(col("product_code_hdr") == "MERIDIAN_DSNP", lit("MRD-DSNP-001"))
    .otherwise(lit("UNKNOWN")),
)

final_df = final_df.withColumn(
    "payer_id",
    when(col("product_code_hdr") == "MERIDIAN_PPO",  lit("1234567890"))
    .when(col("product_code_hdr") == "MERIDIAN_HMO",  lit("1234567891"))
    .when(col("product_code_hdr") == "MERIDIAN_EPO",  lit("1234567892"))
    .when(col("product_code_hdr") == "MERIDIAN_DSNP", lit("1234567893"))
    .otherwise(lit("UNKNOWN")),
)


# COMMAND ----------
# Build coverage_plans: array<struct> representing the FHIR Coverage resource
# for the member's active insurance plan on this claim.
#
# policy_id is the business key — a concat of the member_uid and the resolved
# plan_id.  The plan_id resolution repeats the product_code when-chain inline
# (rather than referencing the intermediate column) so that struct_fields
# captures the full derivation: concat_ws(member_uid, when(product_code...)).
#
# The remaining 28 fields are FHIR Coverage placeholder fields populated by a
# separate enrichment job; they carry lit(None) here so the struct schema is
# locked and downstream consumers never see missing-field errors.

final_df = final_df.withColumn(
    "coverage_plans",
    array(struct(
        lit(None).cast("string").alias("reference_id"),
        concat_ws(
            "_",
            coalesce(col("member_uid"), lit("UNKNOWN")),
            coalesce(
                when(col("product_code_hdr") == "MERIDIAN_PPO",  lit("MRD-PPO-001"))
                .when(col("product_code_hdr") == "MERIDIAN_HMO",  lit("MRD-HMO-001"))
                .when(col("product_code_hdr") == "MERIDIAN_EPO",  lit("MRD-EPO-001"))
                .when(col("product_code_hdr") == "MERIDIAN_DSNP", lit("MRD-DSNP-001"))
                .otherwise(lit("UNKNOWN")),
                lit("UNKNOWN"),
            ),
        ).alias("policy_id"),
        lit(None).cast("string").alias("policy_name"),
        lit(None).cast("string").alias("policy_holder_id"),
        lit(None).cast("string").alias("policy_type_code"),
        lit(None).cast("string").alias("policy_type_display"),
        lit(None).cast("string").alias("policy_type_system"),
        lit(None).cast("string").alias("policy_order"),
        lit(None).cast("timestamp").alias("coverage_start_date"),
        lit(None).cast("timestamp").alias("coverage_end_date"),
        lit(None).cast("string").alias("group_id"),
        lit(None).cast("string").alias("group_name"),
        lit(None).cast("string").alias("sub_group_id"),
        lit(None).cast("string").alias("sub_group_name"),
        lit(None).cast("string").alias("plan_name"),
        coalesce(col("product_code_hdr"), lit("UNKNOWN")).alias("product_code"),
        lit("active").cast("string").alias("status"),
        lit(None).cast("string").alias("subrogation_flag"),
        lit(None).cast("string").alias("cost_sharing_type_system"),
        lit(None).cast("string").alias("cost_sharing_type_code"),
        lit(None).cast("string").alias("cost_sharing_type_display"),
        lit(None).cast("string").alias("cost_sharing_qty_system"),
        lit(None).cast("decimal(38,10)").alias("cost_sharing_qty_value"),
        lit(None).cast("string").alias("cost_sharing_qty_unit"),
        lit(None).cast("string").alias("cost_sharing_qty_code"),
        lit(None).cast("string").alias("cost_sharing_qty_comparator"),
        lit(None).cast("decimal(38,10)").alias("cost_sharing_amount"),
        lit(None).cast("string").alias("cost_sharing_exc_type_system"),
        lit(None).cast("string").alias("cost_sharing_exc_type_code"),
        lit(None).cast("string").alias("cost_sharing_exc_type_display"),
        lit(None).cast("timestamp").alias("cost_sharing_exc_start_date"),
        lit(None).cast("timestamp").alias("cost_sharing_exc_end_date"),
        coalesce(col("payer_id"), lit("UNKNOWN")).cast("string").alias("payer_id"),
    )),
)


# COMMAND ----------
# Build service_locations: array<struct> describing the facility where
# services were rendered.
#
# The first four fields are sourced from claim_provider (rendering facility
# NPI, name, type code) and claim_header (place-of-service code).  The
# remaining 27 fields are FHIR Location resource placeholders — address,
# geo-coordinates, contact info, operational status, managing organisation —
# all set to lit(None) here and populated by the facility-enrichment pipeline.

final_df = final_df.withColumn(
    "service_locations",
    array(struct(
        lit(None).cast("string").alias("reference_id"),
        col("rendering_facility_npi_prov").cast("string").alias("facility_npi"),
        col("rendering_facility_name_prov").alias("facility_name"),
        col("facility_type_code_prov").alias("facility_type_code"),
        col("place_of_service_code_hdr").alias("place_of_service_code"),
        lit(None).cast("string").alias("facility_taxonomy_code"),
        lit(None).cast("string").alias("address_line1"),
        lit(None).cast("string").alias("address_line2"),
        lit(None).cast("string").alias("city"),
        lit(None).cast("string").alias("state"),
        lit(None).cast("string").alias("postal_code"),
        lit(None).cast("string").alias("county_fips"),
        lit(None).cast("string").alias("country_code"),
        lit(None).cast("double").alias("latitude"),
        lit(None).cast("double").alias("longitude"),
        lit(None).cast("string").alias("phone"),
        lit(None).cast("string").alias("fax"),
        lit(None).cast("string").alias("email"),
        lit(None).cast("string").alias("contact_name"),
        lit(None).cast("string").alias("operational_status_code"),
        lit(None).cast("string").alias("operational_status_display"),
        lit(None).cast("string").alias("physical_type_code"),
        lit(None).cast("string").alias("physical_type_display"),
        lit(None).cast("string").alias("physical_type_system"),
        lit(None).cast("string").alias("managing_org_npi"),
        lit(None).cast("string").alias("managing_org_name"),
        lit(None).cast("string").alias("managing_org_id"),
        lit(None).cast("string").alias("part_of_location_id"),
        lit(None).cast("string").alias("availability_exceptions"),
        lit(None).cast("string").alias("endpoint_url"),
        lit(None).cast("string").alias("identifier_system"),
        lit(None).cast("string").alias("identifier_value"),
        lit(None).cast("string").alias("identifier_type_code"),
    )),
)


# COMMAND ----------
# Provider specialty rollup.
#
# Each claim may have multiple provider records (billing, rendering, referring).
# We rank the rendering providers within each claim by seq_num (descending, so
# the most-recently-added record wins on ties) and join the top-ranked NPI
# against the provider_registry to recover specialty, taxonomy, and PCP flag.
#
# Two-step approach:
#   Step 1 — SQL CTE:  rank claim_provider rows + LEFT JOIN provider_registry
#            on (rendering_npi, taxonomy_code).  The ON clause is intentionally
#            two-column so that a provider with multiple taxonomy codes maps to
#            exactly the right specialty row in the registry.
#   Step 2 — PySpark:  apply a signal-precedence when-chain to resolve the
#            final provider_specialty_code from three signal sources in order.
#
# Signal precedence (highest to lowest):
#   1. is_pcp_eligible from provider_registry  →  "PCP"
#   2. specialty_code from provider_registry NPI lookup
#   3. rendering_specialty from claim_provider billing data (fallback)

provider_specialty_df = spark.sql(f"""
    WITH ranked_providers AS (
        SELECT
            claim_id,
            rendering_npi,
            taxonomy_code,
            rendering_specialty,
            seq_num,
            ROW_NUMBER() OVER (
                PARTITION BY claim_id
                ORDER BY seq_num DESC NULLS LAST
            ) AS provider_rank
        FROM {BRONZE}.claim_provider
        WHERE provider_role = 'Rendering'
    ),
    primary_provider AS (
        SELECT
            rp.claim_id,
            rp.rendering_npi,
            rp.rendering_specialty,
            rp.provider_rank,
            pr.specialty_code        AS specialty_code_reg,
            pr.specialty_description AS specialty_desc_reg,
            pr.is_pcp_eligible       AS is_pcp_eligible_reg,
            pr.taxonomy_group        AS taxonomy_group_reg
        FROM ranked_providers rp
        LEFT JOIN {BRONZE}.provider_registry pr
            ON  rp.rendering_npi  = pr.npi
            AND rp.taxonomy_code  = pr.taxonomy_code
        WHERE rp.provider_rank = 1
    ),
    any_pcp_signal AS (
        SELECT
            rp.claim_id,
            MAX(CASE WHEN pr.is_pcp_eligible = true THEN 1 ELSE 0 END) AS any_pcp_flag,
            COUNT(DISTINCT rp.rendering_npi)                            AS distinct_rendering_npis
        FROM ranked_providers rp
        LEFT JOIN {BRONZE}.provider_registry pr
            ON  rp.rendering_npi  = pr.npi
            AND rp.taxonomy_code  = pr.taxonomy_code
        GROUP BY rp.claim_id
    )
    SELECT
        pp.claim_id,
        pp.rendering_npi,
        pp.rendering_specialty,
        pp.specialty_code_reg,
        pp.specialty_desc_reg,
        pp.is_pcp_eligible_reg,
        pp.taxonomy_group_reg,
        aps.any_pcp_flag,
        aps.distinct_rendering_npis
    FROM primary_provider pp
    LEFT JOIN any_pcp_signal aps
        ON pp.claim_id = aps.claim_id
""")

final_df = final_df.join(
    provider_specialty_df,
    final_df["claim_id_hdr"] == provider_specialty_df["claim_id"],
    "left",
)


# COMMAND ----------
# Resolve provider_specialty_code from signal hierarchy.
#
# Signal 1 — PCP registry flag:  if the primary rendering NPI is marked
#   is_pcp_eligible in the provider_registry, the specialty is "PCP"
#   regardless of what the claim billing data says.
# Signal 2 — Registry specialty code:  the NPI+taxonomy-matched specialty_code
#   from provider_registry, which reflects the provider's actual enrolled
#   specialty on file with Meridian Health credentialing.
# Signal 3 — Claim billing specialty:  the specialty code submitted on the
#   claim by the billing provider.  Used only when the registry has no match.
#
# any_pcp_flag broadens Signal 1: if ANY rendering provider on the claim is
# PCP-eligible (not only the top-ranked one), the claim counts as PCP-touched.

final_df = final_df.withColumn(
    "provider_specialty_code",
    when(
        F.coalesce(F.col("is_pcp_eligible_reg").cast("boolean"), F.lit(False)) == True,
        F.lit("PCP"),
    )
    .when(
        F.coalesce(F.col("any_pcp_flag"), F.lit(0)) == F.lit(1),
        F.lit("PCP"),
    )
    .when(
        F.col("specialty_code_reg").isNotNull(),
        F.col("specialty_code_reg"),
    )
    .when(
        F.col("rendering_specialty").isNotNull(),
        F.col("rendering_specialty"),
    )
    .otherwise(F.lit(None)),
)

final_df = final_df.withColumn(
    "provider_specialty_description",
    coalesce(
        col("specialty_desc_reg"),
        col("rendering_specialty"),
        lit(None).cast("string"),
    ),
)

final_df = final_df.withColumn(
    "is_pcp_claim",
    coalesce(
        F.col("is_pcp_eligible_reg").cast("boolean"),
        (F.col("any_pcp_flag") == F.lit(1)),
        F.lit(False),
    ),
)


# COMMAND ----------
# Claim category conditions.
#
# build_claim_criteria() returns a dict mapping category label to a Spark
# Column expression (predicate).  Both claim_category and claim_categories
# are driven by the SAME dict — the single source of truth for category
# membership logic.  A change to any predicate automatically propagates to
# both output columns.
#
# Categories follow standard healthcare utilisation management groupings:
#   Behavioral Health      — ICD-10 F01–F99.9999 (psychiatric / substance use)
#   Maternity              — ICD-10 O00–O9A      (obstetric)
#   Skilled Nursing        — TOB 21x             (post-acute SNF)
#   Inpatient Rehab        — TOB 11x + rehab revenue codes
#   Hospice                — TOB 81x + hospice revenue codes
#   Emergency / Obs        — admit_status != 1 AND (rev 0762/0450 OR TOB 13x)
#   Durable Medical Equip  — place of service 12 OR claim_type = DME
#   Home Health            — TOB 32x or 33x

def build_claim_criteria(df):
    """Return a label→Column dict of claim category membership predicates."""

    # Behavioral Health: primary or any secondary diagnosis in ICD-10 F chapter
    cond_bh = (
        F.col("primary_diag_code_hdr").isNotNull()
        & (F.trim(F.col("primary_diag_code_hdr")) != "")
        & (F.upper(F.trim(F.col("primary_diag_code_hdr"))) >= "F01")
        & (F.upper(F.trim(F.col("primary_diag_code_hdr"))) <= "F99.9999")
    )

    # Maternity: any diagnosis in ICD-10 O chapter across all diagnosis columns
    cond_maternity = F.expr("""
        exists(
            filter(
                flatten(array(
                    primary_diag_code_hdr,
                    diagnosis_1_code, diagnosis_2_code,
                    diagnosis_3_code, diagnosis_4_code, diagnosis_5_code
                )),
                x -> x is not null AND trim(x) != ''
            ),
            d -> upper(trim(d)) >= 'O00' AND upper(trim(d)) <= 'O9A'
        )
    """)

    # Skilled Nursing Facility: Type of Bill 21x
    cond_snf = F.trim(F.col("type_of_bill_hdr")).like("21%")

    # Inpatient Rehabilitation: TOB 11x + rehabilitation revenue codes
    cond_rehab = (
        F.upper(F.trim(F.col("type_of_bill_hdr"))).like("11%")
        & F.expr("""
            exists(
                revenue_code_list,
                x -> x is not null
                    AND trim(x) != ''
                    AND lpad(regexp_replace(trim(x), '[^0-9]', ''), 4, '0')
                        IN ('0118', '0128', '0129', '0138', '0158')
            )
        """)
    )

    # Hospice: TOB 81x + hospice revenue codes
    cond_hospice = (
        F.upper(F.trim(F.col("type_of_bill_hdr"))).like("81%")
        & F.expr("""
            exists(
                revenue_code_list,
                x -> x is not null
                    AND trim(x) != ''
                    AND lpad(regexp_replace(trim(x), '[^0-9]', ''), 4, '0')
                        IN ('0651', '0652', '0655', '0656')
            )
        """)
    )

    # Emergency / Observation: non-elective admit AND ER/OBS revenue or TOB
    cond_er_obs = (
        (F.coalesce(F.col("admit_status_hdr"), F.lit("")) != F.lit("1"))
        & (
            F.expr("""
                exists(
                    revenue_code_list,
                    x -> x is not null AND trim(x) != ''
                        AND lpad(regexp_replace(trim(x), '[^0-9]', ''), 4, '0')
                            IN ('0762', '0450')
                )
            """)
            | F.upper(F.trim(F.col("type_of_bill_hdr"))).like("13%")
        )
    )

    # Durable Medical Equipment: POS 12 (Home) or claim type flag
    cond_dme = (
        (F.col("place_of_service_code_hdr") == F.lit("12"))
        | (F.upper(F.trim(F.col("claim_type_hdr"))) == F.lit("DME"))
    )

    # Home Health: TOB 32x (home health) or 33x (home health — non-DME)
    cond_home_health = (
        F.upper(F.trim(F.col("type_of_bill_hdr"))).like("32%")
        | F.upper(F.trim(F.col("type_of_bill_hdr"))).like("33%")
    )

    return {
        "Behavioral Health":         cond_bh,
        "Maternity":                  cond_maternity,
        "Skilled Nursing Facility":   cond_snf,
        "Inpatient Rehabilitation":   cond_rehab,
        "Hospice":                    cond_hospice,
        "Emergency / Observation":    cond_er_obs,
        "Durable Medical Equipment":  cond_dme,
        "Home Health":                cond_home_health,
    }


conds = build_claim_criteria(final_df)


# COMMAND ----------
# Classify claim_category (FIRST-MATCH) and claim_categories (ARRAY-ACCUMULATOR).
#
# claim_category — a single string: the highest-priority category whose
#   predicate is satisfied.  Categories are tested in the order listed in
#   build_claim_criteria().  A Behavioral Health claim that also satisfies
#   the Maternity predicate is classified as "Behavioral Health".
#
# claim_categories — an array<string>: every category whose predicate fires,
#   in sorted order.  A claim can appear in multiple categories.  Driven by
#   the same conds dict so the two columns are always consistent.

claim_category_expr = (
    when(conds["Behavioral Health"],        F.lit("Behavioral Health"))
    .when(conds["Maternity"],               F.lit("Maternity"))
    .when(conds["Skilled Nursing Facility"],F.lit("Skilled Nursing Facility"))
    .when(conds["Inpatient Rehabilitation"],F.lit("Inpatient Rehabilitation"))
    .when(conds["Hospice"],                 F.lit("Hospice"))
    .when(conds["Emergency / Observation"], F.lit("Emergency / Observation"))
    .when(conds["Durable Medical Equipment"],F.lit("Durable Medical Equipment"))
    .when(conds["Home Health"],             F.lit("Home Health"))
    .otherwise(F.lit(None))
)

final_df = final_df.withColumn("claim_category", claim_category_expr)

# Array accumulator: one when().otherwise(None) per category; filter nulls.
multi_category_col = array(*[
    when(conds[k], lit(k)).otherwise(lit(None))
    for k in conds
])
final_df = (
    final_df
    .withColumn("claim_categories_raw", multi_category_col)
    .withColumn(
        "claim_categories",
        array_sort(expr("filter(claim_categories_raw, x -> x is not null)")),
    )
    .drop("claim_categories_raw")
)


# COMMAND ----------
# Build extension_source_values: array<struct> of name/value pairs that
# record which source column fed each key business attribute.
# Used by downstream FHIR ExplanationOfBenefit extension population jobs
# to audit field provenance without re-deriving it from raw claims.

final_df = final_df.withColumn(
    "extension_source_values",
    array(
        struct(lit("claim_id").alias("name"),
               col("claim_id_hdr").cast("string").alias("value")),
        struct(lit("claim_line_id").alias("name"),
               col("claim_line_id_hdr").cast("string").alias("value")),
        struct(lit("member_id").alias("name"),
               col("member_id_hdr").cast("string").alias("value")),
        struct(lit("subscriber_id").alias("name"),
               col("subscriber_id_hdr").cast("string").alias("value")),
        struct(lit("billing_npi").alias("name"),
               col("billing_npi_prov").cast("string").alias("value")),
        struct(lit("rendering_npi").alias("name"),
               col("rendering_npi").cast("string").alias("value")),
        struct(lit("claim_type").alias("name"),
               col("claim_type_hdr").cast("string").alias("value")),
        struct(lit("type_of_bill").alias("name"),
               col("type_of_bill_hdr").cast("string").alias("value")),
        struct(lit("place_of_service").alias("name"),
               col("place_of_service_code_hdr").cast("string").alias("value")),
        struct(lit("product_code").alias("name"),
               col("product_code_hdr").cast("string").alias("value")),
        struct(lit("plan_id").alias("name"),
               col("plan_id").cast("string").alias("value")),
        struct(lit("payer_id").alias("name"),
               col("payer_id").cast("string").alias("value")),
        struct(lit("primary_diag_code").alias("name"),
               col("primary_diag_code_hdr").cast("string").alias("value")),
        struct(lit("admit_status").alias("name"),
               col("admit_status_hdr").cast("string").alias("value")),
        struct(lit("provider_specialty_code").alias("name"),
               col("provider_specialty_code").cast("string").alias("value")),
    ),
)


# COMMAND ----------
# Build other_amounts: array<struct> of name/value pairs carrying every
# monetary attribute on the claim in a uniform envelope.
# Downstream analytics pivot this array to compare allowed vs paid vs billed
# without schema changes when new amount types are added.

final_df = final_df.withColumn(
    "other_amounts",
    array(
        struct(lit("billed_amount").alias("name"),
               col("billed_amount_hdr").cast("decimal(18,2)").alias("value")),
        struct(lit("allowed_amount").alias("name"),
               col("allowed_amount_hdr").cast("decimal(18,2)").alias("value")),
        struct(lit("paid_amount").alias("name"),
               col("total_paid_amount_cob").cast("decimal(18,2)").alias("value")),
        struct(lit("contractual_adjustment").alias("name"),
               col("total_contractual_adj_cob").cast("decimal(18,2)").alias("value")),
        struct(lit("cob_allowed_amount").alias("name"),
               col("cob_allowed_amount_cob").cast("decimal(18,2)").alias("value")),
        struct(lit("cob_billed_amount").alias("name"),
               col("cob_billed_amount_cob").cast("decimal(18,2)").alias("value")),
        struct(lit("patient_liability").alias("name"),
               col("cob_patient_liability_cob").cast("decimal(18,2)").alias("value")),
        struct(lit("copay_amount").alias("name"),
               col("cob_copay_amount_cob").cast("decimal(18,2)").alias("value")),
        struct(lit("deductible_amount").alias("name"),
               col("cob_deductible_amount_cob").cast("decimal(18,2)").alias("value")),
        struct(lit("interest_amount").alias("name"),
               col("total_interest_amount").cast("decimal(18,2)").alias("value")),
        struct(lit("coinsurance_amount").alias("name"),
               col("coinsurance_amount_hdr").cast("decimal(18,2)").alias("value")),
        struct(lit("member_responsibility").alias("name"),
               col("member_responsibility_hdr").cast("decimal(18,2)").alias("value")),
    ),
)


# COMMAND ----------
# Final canonical column projection.
#
# Select the ~85 columns that form the silver.clinical_claims contract.
# Column order follows the canonical Meridian Health Claims Data Model v3.
# Any column not listed here is an intermediate and does not survive to silver.

silver_df = final_df.select(
    # --- Claim header keys ---
    col("claim_id_hdr").alias("claim_id"),
    col("claim_line_id_hdr").alias("claim_line_id"),
    col("icn_hdr").alias("internal_control_number"),
    col("claim_type_hdr").alias("claim_type"),
    col("claim_status_hdr").alias("claim_status"),
    col("processing_status"),
    col("claim_outcome"),

    # --- Member / subscriber ---
    col("member_id_hdr").alias("member_id"),
    col("subscriber_id_hdr").alias("subscriber_id"),
    col("member_uid"),
    col("member_dob_mbr").alias("member_date_of_birth"),
    col("member_gender_mbr").alias("member_gender"),
    col("member_state_mbr").alias("member_state"),

    # --- Provider ---
    col("billing_npi_prov").alias("billing_npi"),
    col("rendering_npi").alias("rendering_npi"),
    col("referring_npi_prov").alias("referring_npi"),
    col("provider_specialty_code"),
    col("provider_specialty_description"),
    col("is_pcp_claim"),
    col("taxonomy_group_reg").alias("provider_taxonomy_group"),

    # --- Service dates ---
    col("service_from_date_hdr").alias("service_from_date"),
    col("service_to_date_hdr").alias("service_to_date"),
    col("admit_date_hdr").alias("admit_date"),
    col("discharge_date_hdr").alias("discharge_date"),
    col("statement_from_date_hdr").alias("statement_from_date"),
    col("statement_to_date_hdr").alias("statement_to_date"),

    # --- Facility / service location ---
    col("type_of_bill_hdr").alias("type_of_bill_code"),
    col("place_of_service_code_hdr").alias("place_of_service_code"),
    col("admit_status_hdr").alias("admit_status_code"),
    col("drg_code_hdr").alias("drg_code"),
    col("drg_description_hdr").alias("drg_description"),
    col("discharge_status_hdr").alias("discharge_status_code"),

    # --- Financial amounts (scalar) ---
    col("billed_amount_hdr").alias("billed_amount"),
    col("allowed_amount_hdr").alias("allowed_amount"),
    col("total_paid_amount_cob").alias("paid_amount"),
    col("total_contractual_adj_cob").alias("contractual_adjustment"),
    col("cob_allowed_amount_cob").alias("cob_allowed_amount"),
    col("cob_billed_amount_cob").alias("cob_billed_amount"),
    col("cob_patient_liability_cob").alias("patient_liability"),
    col("cob_copay_amount_cob").alias("copay_amount"),
    col("cob_deductible_amount_cob").alias("deductible_amount"),
    col("total_interest_amount").alias("interest_amount"),
    col("coinsurance_amount_hdr").alias("coinsurance_amount"),
    col("member_responsibility_hdr").alias("member_responsibility"),

    # --- Product / plan ---
    col("product_code_hdr").alias("product_code"),
    col("plan_id"),
    col("payer_id"),
    col("group_number_hdr").alias("group_number"),
    col("network_status_hdr").alias("network_status"),

    # --- Classification ---
    col("claim_category"),
    col("claim_categories"),

    # --- Diagnosis (wide + overflow array) ---
    col("primary_diag_code_hdr").alias("primary_diagnosis_code"),
    col("diagnosis_1_code"),
    col("diagnosis_2_code"),
    col("diagnosis_3_code"),
    col("diagnosis_4_code"),
    col("diagnosis_5_code"),
    col("other_diagnoses"),

    # --- Procedure (wide + overflow array) ---
    col("procedure_1_code"),
    col("procedure_2_code"),
    col("procedure_3_code"),
    col("procedure_4_code"),
    col("other_procedures"),

    # --- Occurrence codes ---
    col("occurrence_codes"),

    # --- Structured arrays ---
    col("identifiers"),
    col("coverage_plans"),
    col("service_locations"),

    # --- Audit / provenance ---
    col("extension_source_values"),
    col("other_amounts"),
    F.current_timestamp().alias("etl_loaded_at"),
    F.lit("silver_clinical_claims").alias("etl_source_job"),
    F.lit("meridian").alias("source_system"),
)


# COMMAND ----------
# Data quality checks.
#
# Rows failing mandatory-field checks are filtered out and counted so the
# pipeline can emit a DQ metric without halting on bad data.  The DQ filter
# is intentionally permissive (only hard nulls on primary keys fail); soft
# violations (e.g. missing provider NPI) are recorded in extension_source_values.

dq_passed_df = (
    silver_df
    .filter(F.col("claim_id").isNotNull())
    .filter(F.col("member_id").isNotNull())
    .filter(F.col("service_from_date").isNotNull())
)


# COMMAND ----------
# Write to silver.
#
# Format: Delta Lake, mode overwrite (full refresh).
# The table is partitioned by service_from_date (date-truncated to month) to
# allow incremental downstream reads using partition pruning.

(
    dq_passed_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("service_from_date")
    .saveAsTable(f"{SILVER}.clinical_claims")
)
