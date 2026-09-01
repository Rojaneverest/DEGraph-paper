# DEGraph DSL v1 — data-lineage graph in compact line format.
# Read each line as ONE record. Sections marked by @HEADERS.
#
# Symbols:
#   T<n>  table        (defined in @TABLES; reference elsewhere)
#   D<n>  dataframe    (file-local intermediate; reference in edges)
#   X<n>  predicate    (defined in @PREDICATES; referenced as X<n> in rule_logic)
#
# Sections (in order):
#   @TABLES    one per line: T<n>|<fqn>|<col1,col2,...>|by=<writer_file>|rb=<reader_file1,...>
#              by= the file that PRODUCES (writes/creates) this table — its WRITER/PRODUCER
#              rb= the file(s) that CONSUME this table as input — its READERS/CONSUMERS
#              IMPORTANT: by= ≠ consumer; rb= ≠ producer. by= is the source/writer; rb= is the sink/reader.
#   @PROV      column provenance:  <Tn>.<col>=<role>:<src1>,<src2>[;op=<f>][;win=<spec>]
#              roles: P=passthrough  D=derived  A=aggregate  G=group_key
#              sources are either "<Tn>.<col>" or bare column names (file-local)
#   @PREDS     X<n>=<predicate text>
#   @RULES    named when-chain Python vars.  Two shapes:
#              FIRST-MATCH (has else): <var>=X1=>v;X2=>v;else=>v
#                — returns the FIRST matching value; if nothing fires returns else-value (often NULL).
#              ARRAY-ACC (no else):   <var>=X1=>v;X2=>v;X3=>v
#                — ALL matching predicates fire; results collected into an array;
#                  if nothing fires the array is EMPTY [] (NOT null).
#              To distinguish: look for `else=>` at the end of the rule chain.
#              [→col] suffix on a rule means that rule's result is stored directly in col
#              (confirmed by a cv= annotation on the D-edge in @EDGES).
#   @PDICTS   shared predicate dicts:  <var>|<label>=<X<n> or text>
#   @PUSE     predicate cross-reference:  X<n>:<rule_or_edge>,<rule_or_edge>,...
#              lists every named rule + derives edge that references X<n>.
#              If two entries share X<n>, they implement IDENTICAL predicate logic —
#              when X<n> fires, ALL listed columns/rules respond to the same condition.
#              CRITICAL: this means first-match columns (else in @RULES) and
#              array-accumulator columns (no else in @RULES) that share an X<n>
#              can NEVER produce conflicting NULL-vs-non-empty states:
#              if no X<n> fires → first-match = NULL AND array-acc = [] (both empty).
#   @EDGES    one edge per line, code-prefixed:
#     R|<Tn>|<Dn>[|pc=c1,c2][|stream]                  reads table->df
#     W|<Dn>|<Tn>|<mode>[|<fmt>][|mk=k1,k2][|pc=p1,p2][|sink=<class>][|kw=k=v,...] writes df->table
#          sink= custom sink class (e.g. MergeSink) used instead of df.write.saveAsTable
#          kw=   extra constructor kwargs for the sink (e.g. enable_leap_column=True)
#     D|<Dn>|<Dn>|<out>[|sc=c1,c2][|ex=<expr>][|cv=<var>][|rl=X1=>v;...;else=>v][|ws=part(...)/ord(...)][|sf=...][|nf=f1,f2]
#                                                       derives one new column
#                  ex= source expression text (truncated to 80 chars) for non-when-chain derives.
#                      Gives the LLM the actual PySpark/SQL expression rather than just the col list.
#                  cv= named Python Column variable from @RULES used as the expression.
#                      Trace: @RULES entry <var> → this output column.
#                  sf = struct field list (array(struct(...)) outputs).
#                       format: <field>:<kind>=<value> separated by ';'
#                       kinds: L=literal C=column E=expr
#                       For E with nested when-chain, value is:
#                         <expr-text>~~rl=<X1=>v;X2=>v;else=>v>
#                       where ~~rl= delimits the inner conditional rules.
#                  nf = lit-null placeholder field names (collapsed)
#     F|<Dn>|<Dn>[|rc=c1,c2][|pr=<pred>]               filters df->df (row restrict)
#          pr= predicate expression text (truncated to 80 chars); gives the actual filter condition.
#     P|<Dn>|<Dn>[|rm=c1,c2][|kp=c1,c2|*]              projects df->df (col restrict)
#     J|<Dn>|<Dn>|<Dn>|<type>[|jk=l1=r1,l2=r2][|rpc=...][|lpc=...]
#                                                       join (left,right -> target)
#     A|<Dn>|<Dn>[|by=<file>]|gk=k1,k2|out=col=op(in);col=op(in)[|dyn=<note>]   aggregates
#          by= the file this aggregation is performed IN (disambiguates same-shaped GROUP BYs across files)
#          gk= aggregate GROUP key — NOT the same as a window part() partition key
#          dyn= agg column list cannot be enumerated statically (runtime variable); note explains why
#     O|<Dn>|<Dn>|<operator>[|pass]                    opaque helper transform
#          pass = column-set passthrough (output has SAME columns as input;
#                 values may change but NO columns are added or removed;
#                 removing a |pass transform NEVER changes the column set)
#   @WARN     <file>|<category>|<message>
#
# Reverse-lineage rule: to find "which input columns supply VALUES to T<n>.col",
# read @PROV first. If T<n>.col is absent from @PROV, fall back to @EDGES.
# Join keys (jk=) and group keys (gk=) describe row-matching, NOT value sources.
#
# Parsing note: '|' is the FIELD separator AND a legitimate Python operator
# (bitwise/logical OR) inside predicate text. Predicate text always appears
# AFTER a fixed prefix (X<n>=, var=, label=, rl=, ws=, sf=) and runs to end-of-
# line. Split each record on '|' from the LEFT for the first few structural
# fields, then treat the remainder as a single body. Pipes inside that body
# are part of the expression, not new fields.

@TABLES
T0|meridian.bronze.claim_header||by=|rb=silver_clinical_claims.py
T1|meridian.bronze.claim_diagnosis||by=|rb=silver_clinical_claims.py
T2|meridian.bronze.claim_procedure||by=|rb=silver_clinical_claims.py
T3|meridian.bronze.claim_cob_payment||by=|rb=silver_clinical_claims.py
T4|meridian.bronze.claim_payable||by=|rb=silver_clinical_claims.py
T5|meridian.bronze.claim_interest||by=|rb=silver_clinical_claims.py
T6|meridian.bronze.claim_occurrence||by=|rb=silver_clinical_claims.py
T7|meridian.bronze.claim_provider||by=|rb=silver_clinical_claims.py
T8|meridian.bronze.member_eligibility||by=|rb=silver_clinical_claims.py
T9|meridian.bronze.provider_registry||by=|rb=silver_clinical_claims.py
T10|meridian.silver.clinical_claims|claim_id_diag,other_diagnoses,claim_id_proc,other_procedures,claim_id_occ,occurrence_codes,claim_id_int_agg,total_interest_amount,interest_line_count,processing_status,claim_outcome,member_uid,identifiers,plan_id,payer_id,coverage_plans,service_locations,pp.claim_id,pp.rendering_npi,pp.rendering_specialty,pp.specialty_code_reg,pp.specialty_desc_reg,pp.is_pcp_eligible_reg,pp.taxonomy_group_reg,aps.any_pcp_flag,aps.distinct_rendering_npis,provider_specialty_code,provider_specialty_description,is_pcp_claim,claim_category,claim_categories,extension_source_values,other_amounts|by=silver_clinical_claims.py|rb=

@PROV
T10.other_diagnoses=P:T0.other_diagnoses
T10.other_procedures=P:T0.other_procedures
T10.occurrence_codes=P:T0.occurrence_codes
T10.total_interest_amount=P:T0.total_interest_amount
T10.interest_line_count=P:T0.interest_line_count
T10.processing_status=D:claim_status_hdr
T10.claim_outcome=D:claim_status_hdr
T10.member_uid=D:member_uid_mbr,subscriber_id_hdr,member_seq_hdr
T10.identifiers=D:claim_id_hdr,subscriber_id_hdr,member_id_hdr,icn_hdr,group_number_hdr,billing_npi_prov
T10.plan_id=D:product_code_hdr
T10.payer_id=D:product_code_hdr
T10.coverage_plans=D:product_code_hdr,member_uid,payer_id
T10.service_locations=D:rendering_facility_name_prov,facility_type_code_prov,place_of_service_code_hdr,rendering_facility_npi_prov
T10.provider_specialty_code=D:rendering_specialty,specialty_code_reg,any_pcp_flag,is_pcp_eligible_reg
T10.provider_specialty_description=D:specialty_desc_reg,rendering_specialty
T10.is_pcp_claim=D:any_pcp_flag,is_pcp_eligible_reg
T10.claim_category=D:Home Health,Durable Medical Equipment,Emergency / Observation,Hospice,Inpatient Rehabilitation,Skilled Nursing Facility,Maternity,Behavioral Health
T10.claim_categories=D:
T10.extension_source_values=D:claim_id_hdr,claim_line_id_hdr,member_id_hdr,subscriber_id_hdr,billing_npi_prov,rendering_npi,claim_type_hdr,type_of_bill_hdr,place_of_service_code_hdr,product_code_hdr,plan_id,payer_id,primary_diag_code_hdr,admit_status_hdr,provider_specialty_code
T10.other_amounts=D:billed_amount_hdr,allowed_amount_hdr,total_paid_amount_cob,total_contractual_adj_cob,cob_allowed_amount_cob,cob_billed_amount_cob,cob_patient_liability_cob,cob_copay_amount_cob,cob_deductible_amount_cob,total_interest_amount,coinsurance_amount_hdr,member_responsibility_hdr

@PREDS
X0=col('claim_status_hdr') == 'Adjudicated'
X1=col('claim_status_hdr') == 'Denied'
X2=col('claim_status_hdr') == 'Pended'
X3=col('claim_status_hdr') == 'Suspended'
X4=col('claim_status_hdr') == 'Voided'
X5=col('claim_status_hdr') == 'Adjusted'
X6=col('product_code_hdr') == 'MERIDIAN_PPO'
X7=col('product_code_hdr') == 'MERIDIAN_HMO'
X8=col('product_code_hdr') == 'MERIDIAN_EPO'
X9=col('product_code_hdr') == 'MERIDIAN_DSNP'
X10=F.coalesce(F.col('is_pcp_eligible_reg').cast('boolean'), F.lit(False)) == True
X11=F.coalesce(F.col('any_pcp_flag'), F.lit(0)) == F.lit(1)
X12=F.col('specialty_code_reg').isNotNull()
X13=F.col('rendering_specialty').isNotNull()
X14=F.col('primary_diag_code_hdr').isNotNull() & (F.trim(F.col('primary_diag_code_hdr')) != '') & (F.upper(F.trim(F.col('primary_diag_code_hdr'))) >= 'F01') & (F.upper(F.trim(F.col('primary_diag_code_hdr'))) <= 'F99.9999')
X15=F.expr("\n        exists(\n            filter(\n                flatten(array(\n                    primary_diag_code_hdr,\n                    diagnosis_1_code, diagnosis_2_code,\n                    diagnosis_3_code, diagnosis_4_code, diagnosis_5_code\n                )),\n                x -> x is not null AND trim(x) != ''\n            ),\n            d -> upper(trim(d)) >= 'O00' AND upper(trim(d)) <= 'O9A'\n        )\n    ")
X16=F.trim(F.col('type_of_bill_hdr')).like('21%')
X17=F.upper(F.trim(F.col('type_of_bill_hdr'))).like('11%') & F.expr("\n            exists(\n                revenue_code_list,\n                x -> x is not null\n                    AND trim(x) != ''\n                    AND lpad(regexp_replace(trim(x), '[^0-9]', ''), 4, '0')\n                        IN ('0118', '0128', '0129', '0138', '0158')\n            )\n        ")
X18=F.upper(F.trim(F.col('type_of_bill_hdr'))).like('81%') & F.expr("\n            exists(\n                revenue_code_list,\n                x -> x is not null\n                    AND trim(x) != ''\n                    AND lpad(regexp_replace(trim(x), '[^0-9]', ''), 4, '0')\n                        IN ('0651', '0652', '0655', '0656')\n            )\n        ")
X19=(F.coalesce(F.col('admit_status_hdr'), F.lit('')) != F.lit('1')) & (F.expr("\n                exists(\n                    revenue_code_list,\n                    x -> x is not null AND trim(x) != ''\n                        AND lpad(regexp_replace(trim(x), '[^0-9]', ''), 4, '0')\n                            IN ('0762', '0450')\n                )\n            ") | F.upper(F.trim(F.col('type_of_bill_hdr'))).like('13%'))
X20=(F.col('place_of_service_code_hdr') == F.lit('12')) | (F.upper(F.trim(F.col('claim_type_hdr'))) == F.lit('DME'))
X21=F.upper(F.trim(F.col('type_of_bill_hdr'))).like('32%') | F.upper(F.trim(F.col('type_of_bill_hdr'))).like('33%')
X22=F.col('primary_diag_code_hdr').isNotNull() & (F.trim(F.col('primary_diag_code_hdr')) != '') & (F.upper(F.trim(F.col('primary_diag_code_hdr'))) >= 'F01') & (F.upper(F.trim(F.col('primary_diag_code_hdr'
X23=F.expr("\n        exists(\n            filter(\n                flatten(array(\n                    primary_diag_code_hdr,\n                    diagnosis_1_code, diagnosis_2_code,\n
X24=F.upper(F.trim(F.col('type_of_bill_hdr'))).like('11%') & F.expr("\n            exists(\n                revenue_code_list,\n                x -> x is not null\n                    AND trim(x) != ''\n
X25=F.upper(F.trim(F.col('type_of_bill_hdr'))).like('81%') & F.expr("\n            exists(\n                revenue_code_list,\n                x -> x is not null\n                    AND trim(x) != ''\n
X26=(F.coalesce(F.col('admit_status_hdr'), F.lit('')) != F.lit('1')) & (F.expr("\n                exists(\n                    revenue_code_list,\n                    x -> x is not null AND trim(x) != ''\

@RULES
claim_category_expr=X14=>F.lit('Behavioral Health');X15=>F.lit('Maternity');X16=>F.lit('Skilled Nursing Facility');X17=>F.lit('Inpatient Rehabilitation');X18=>F.lit('Hospice');X19=>F.lit('Emergency / Observation');X20=>F.lit('Durable Medical Equipment');X21=>F.lit('Home Health');else=>F.lit(None)  [→claim_category]

@PDICTS
conds|Behavioral Health=X22
conds|Maternity=X23
conds|Skilled Nursing Facility=X16
conds|Inpatient Rehabilitation=X24
conds|Hospice=X25
conds|Emergency / Observation=X26
conds|Durable Medical Equipment=X20
conds|Home Health=X21

@PUSE
X0:processing_status,claim_outcome
X1:processing_status,claim_outcome
X2:processing_status,claim_outcome
X3:processing_status,claim_outcome
X4:processing_status,claim_outcome
X5:processing_status,claim_outcome
X6:plan_id,payer_id,coverage_plans.policy_id
X7:plan_id,payer_id,coverage_plans.policy_id
X8:plan_id,payer_id,coverage_plans.policy_id
X9:plan_id,payer_id,coverage_plans.policy_id
X10:provider_specialty_code
X11:provider_specialty_code
X12:provider_specialty_code
X13:provider_specialty_code
X14:claim_category,claim_category_expr
X15:claim_category,claim_category_expr
X16:claim_category,claim_category_expr
X17:claim_category,claim_category_expr
X18:claim_category,claim_category_expr
X19:claim_category,claim_category_expr
X20:claim_category,claim_category_expr
X21:claim_category,claim_category_expr

@EDGES
R|T0|D0
R|T1|D1
R|T2|D2
R|T3|D3
R|T4|D4
R|T5|D5
R|T6|D6
R|T7|D7
R|T8|D8
R|T9|D9
R|T3|D10|pc=claim_id,claim_line_id
D|D10|D10|cob_allowed_amount
D|D10|D10|cob_billed_amount
D|D10|D10|cob_patient_liability
D|D10|D10|cob_copay_amount
D|D10|D10|cob_deductible_amount
R|T4|D11|pc=claim_id,claim_line_id
D|D11|D11|total_paid_amount
D|D11|D11|total_contractual_adj
R|T0|D12
J|T0|D10|D13|left|jk=claim_id=claim_id,claim_line_id=claim_line_id
J|D13|D11|D13|left|jk=claim_id=claim_id,claim_line_id=claim_line_id
J|D0|D1|D14|left|jk=claim_id_hdr=claim_id
D|D14|D15|diag_rank|sc=claim_id_hdr,diag_priority,icd10_code|ex=row_number().over(diag_rank_window)|ws=part(claim_id_hdr)/ord(diag_priority asc_nulls_last,icd10_code asc_nulls_last)
A|D15|D16|by=silver_clinical_claims.py|gk=claim_id_hdr|out=<unresolved>=<unresolved>(<unresolved>)|dyn=agg list star-unpacked from a runtime-built variable; column list cannot be enumerated statically; controlled by env-var(s): $MERIDIAN_DATABASE
D|D16|D17|claim_id_diag|sc=claim_id_hdr|ex=rename(claim_id_hdr → claim_id_diag)
D|D17|D18|other_diagnoses_raw|sf=sequence_number:E=col(f'diagnosis_{i}_priority');codeset:E=col(f'diagnosis_{i}_code');codeset_display:E=col(f'diagnosis_{i}_description');diagnosis_type:E=col(f'diagnosis_{i}_type');present_on_admission_code:E=col(f'diagnosis_{i}_poa');codeset_system:L='ICD-10-CM'|nf=reference_id,diagnosis_group
D|D17|D18|other_diagnoses|ex=array_sort(expr('filter(other_diagnoses_raw, x -> x is not null)'))
P|D18|D17|rm=other_diagnoses_raw
J|D0|D2|D19|left|jk=claim_id_hdr=claim_id
D|D19|D20|proc_rank|sc=claim_id_hdr,proc_priority,hcpcs_code|ex=row_number().over(proc_rank_window)|ws=part(claim_id_hdr)/ord(proc_priority asc_nulls_last,hcpcs_code asc_nulls_last)
A|D20|D21|by=silver_clinical_claims.py|gk=claim_id_hdr|out=<unresolved>=<unresolved>(<unresolved>)|dyn=agg list star-unpacked from a runtime-built variable; column list cannot be enumerated statically; controlled by env-var(s): $MERIDIAN_DATABASE
D|D21|D22|claim_id_proc|sc=claim_id_hdr|ex=rename(claim_id_hdr → claim_id_proc)
D|D22|D23|other_procedures_raw|sf=codeset:E=col(f'procedure_{i}_code');codeset_display:E=col(f'procedure_{i}_description');modifier_code:E=col(f'procedure_{i}_modifier');procedure_date:E=col(f'procedure_{i}_date');sequence_number:E=col(f'procedure_{i}_priority');procedure_type:E=col(f'procedure_{i}_type');codeset_system:L='HCPCS'|nf=reference_id
D|D22|D23|other_procedures|ex=expr('filter(other_procedures_raw, x -> x is not null)')
P|D23|D22|rm=other_procedures_raw
D|D6|D24|occ_struct|sc=occ_code,occ_date,occ_span_from_date,occ_span_to_date|ex=when(F.col('occ_code').isNotNull(), struct(lit(None).cast('string').alias('re...
A|D24|D25|by=silver_clinical_claims.py|gk=claim_id|out=occurrence_codes=array_distinct(<unresolved>)
D|D25|D26|claim_id_occ|sc=claim_id|ex=rename(claim_id → claim_id_occ)
A|D5|D27|by=silver_clinical_claims.py|gk=claim_id_int|out=total_interest_amount=sum(interest_amount_int);interest_line_count=count(interest_amount_int)
D|D27|D28|claim_id_int_agg|sc=claim_id_int|ex=rename(claim_id_int → claim_id_int_agg)
J|D0|D17|D29|left|jk=claim_id_hdr=claim_id_diag
J|D29|D22|D30|left|jk=claim_id_hdr=claim_id_proc
J|D30|D12|D31|left|jk=claim_id_hdr=claim_id_cob
J|D31|D7|D32|left|jk=claim_id_hdr=claim_id_prov
J|D32|D8|D33|left|jk=member_id_hdr=member_id_mbr
J|D33|D26|D34|left|jk=claim_id_hdr=claim_id_occ
J|D34|D28|D35|left|jk=claim_id_hdr=claim_id_int_agg
D|D35|D35|processing_status|sc=claim_status_hdr|rl=X0=>lit('active');X1=>lit('active');X2=>lit('draft');X3=>lit('draft');X4=>lit('cancelled');X5=>lit('active');else=>lit('active')
D|D35|D35|claim_outcome|sc=claim_status_hdr|rl=X0=>lit('complete');X1=>lit('complete');X2=>lit('queued');X3=>lit('error');X4=>lit('complete');X5=>lit('partial');else=>lit(None)
D|D35|D35|member_uid|sc=member_uid_mbr,subscriber_id_hdr,member_seq_hdr|ex=coalesce(col('member_uid_mbr'), concat_ws('_', col('subscriber_id_hdr'), col(...
D|D35|D35|identifiers|sc=claim_id_hdr,subscriber_id_hdr,member_id_hdr,icn_hdr,group_number_hdr,billing_npi_prov|sf=key:L='claim_id';value:E=struct(lit(None).cast('string').alias('reference_id'), lit('claim_id').alias('identifier_type'), col('claim_id_hdr').alias('identifier'), lit('meridian').alias('identifier_source'))
D|D35|D35|plan_id|sc=product_code_hdr|rl=X6=>lit('MRD-PPO-001');X7=>lit('MRD-HMO-001');X8=>lit('MRD-EPO-001');X9=>lit('MRD-DSNP-001');else=>lit('UNKNOWN')
D|D35|D35|payer_id|sc=product_code_hdr|rl=X6=>lit('1234567890');X7=>lit('1234567891');X8=>lit('1234567892');X9=>lit('1234567893');else=>lit('UNKNOWN')
D|D35|D35|coverage_plans|sc=product_code_hdr,member_uid,payer_id|sf=policy_id:E=concat_ws('_', coalesce(col('member_uid'), lit('UNKNOWN')), coalesce(when(col('product_code_hdr') == 'MERIDIAN_PPO', lit('MRD-PPO-001')).when(col('product_code_hdr') == 'MERIDIAN_HMO', lit('MRD-HMO-001')).when(col('product_code_hdr') == 'MERIDIAN_...~~rl=X6=>lit('MRD-PPO-001');X7=>lit('MRD-HMO-001');X8=>lit('MRD-EPO-001');X9=>lit('MRD-DSNP-001');else=>lit('UNKNOWN');product_code:E=coalesce(col('product_code_hdr'), lit('UNKNOWN'));status:L='active';payer_id:E=coalesce(col('payer_id'), lit('UNKNOWN'))|nf=reference_id,policy_name,policy_holder_id,policy_type_code,policy_type_display,policy_type_system,policy_order,coverage_start_date,coverage_end_date,group_id,group_name,sub_group_id,sub_group_name,plan_name,subrogation_flag,cost_sharing_type_system,cost_sharing_type_code,cost_sharing_type_display,cost_sharing_qty_system,cost_sharing_qty_value,cost_sharing_qty_unit,cost_sharing_qty_code,cost_sharing_qty_comparator,cost_sharing_amount,cost_sharing_exc_type_system,cost_sharing_exc_type_code,cost_sharing_exc_type_display,cost_sharing_exc_start_date,cost_sharing_exc_end_date
D|D35|D35|service_locations|sc=rendering_facility_name_prov,facility_type_code_prov,place_of_service_code_hdr,rendering_facility_npi_prov|sf=facility_npi:C=rendering_facility_npi_prov;facility_name:C=rendering_facility_name_prov;facility_type_code:C=facility_type_code_prov;place_of_service_code:C=place_of_service_code_hdr|nf=reference_id,facility_taxonomy_code,address_line1,address_line2,city,state,postal_code,county_fips,country_code,latitude,longitude,phone,fax,email,contact_name,operational_status_code,operational_status_display,physical_type_code,physical_type_display,physical_type_system,managing_org_npi,managing_org_name,managing_org_id,part_of_location_id,availability_exceptions,endpoint_url,identifier_system,identifier_value,identifier_type_code
R|T7|D36|pc=claim_id,rendering_npi,taxonomy_code,rendering_specialty,seq_num
D|D36|D36|provider_rank|sc=claim_id,seq_num|ws=part(claim_id)/ord(seq_num desc)
J|D36|D38|D37|left|jk=rendering_npi=npi,taxonomy_code=taxonomy_code
J|D36|D38|D39|left|jk=rendering_npi=npi,taxonomy_code=taxonomy_code
D|D39|D40|any_pcp_flag
D|D39|D40|distinct_rendering_npis
A|D39|D40|by=silver_clinical_claims.py|gk=claim_id|out=any_pcp_flag=max(is_pcp_eligible);distinct_rendering_npis=count(rendering_npi)
J|D42|D40|D41|left|jk=claim_id=claim_id
P|D42|D43|kp=pp.claim_id,pp.rendering_npi,pp.rendering_specialty,pp.specialty_code_reg,pp.specialty_desc_reg,pp.is_pcp_eligible_reg,pp.taxonomy_group_reg,aps.any_pcp_flag,aps.distinct_rendering_npis
J|D35|D43|D35|left|jk=claim_id_hdr=claim_id
D|D35|D35|provider_specialty_code|sc=rendering_specialty,specialty_code_reg,any_pcp_flag,is_pcp_eligible_reg|rl=X10=>F.lit('PCP');X11=>F.lit('PCP');X12=>F.col('specialty_code_reg');X13=>F.col('rendering_specialty');else=>F.lit(None)
D|D35|D35|provider_specialty_description|sc=specialty_desc_reg,rendering_specialty|ex=coalesce(col('specialty_desc_reg'), col('rendering_specialty'), lit(None).cas...
D|D35|D35|is_pcp_claim|sc=any_pcp_flag,is_pcp_eligible_reg|ex=coalesce(F.col('is_pcp_eligible_reg').cast('boolean'), F.col('any_pcp_flag') ...
D|D35|D35|claim_category|sc=Home Health,Durable Medical Equipment,Emergency / Observation,Hospice,Inpatient Rehabilitation,Skilled Nursing Facility,Maternity,Behavioral Health|cv=claim_category_expr|rl=X14=>F.lit('Behavioral Health');X15=>F.lit('Maternity');X16=>F.lit('Skilled Nursing Facility');X17=>F.lit('Inpatient Rehabilitation');X18=>F.lit('Hospice');X19=>F.lit('Emergency / Observation');X20=>F.lit('Durable Medical Equipment');X21=>F.lit('Home Health');else=>F.lit(None)
D|D35|D44|claim_categories_raw|cv=multi_category_col
D|D35|D44|claim_categories|ex=array_sort(expr('filter(claim_categories_raw, x -> x is not null)'))
P|D44|D35|rm=claim_categories_raw
D|D35|D35|extension_source_values|sc=claim_id_hdr,claim_line_id_hdr,member_id_hdr,subscriber_id_hdr,billing_npi_prov,rendering_npi,claim_type_hdr,type_of_bill_hdr,place_of_service_code_hdr,product_code_hdr,plan_id,payer_id,primary_diag_code_hdr,admit_status_hdr,provider_specialty_code|sf=name:L='claim_id';value:C=claim_id_hdr
D|D35|D35|other_amounts|sc=billed_amount_hdr,allowed_amount_hdr,total_paid_amount_cob,total_contractual_adj_cob,cob_allowed_amount_cob,cob_billed_amount_cob,cob_patient_liability_cob,cob_copay_amount_cob,cob_deductible_amount_cob,total_interest_amount,coinsurance_amount_hdr,member_responsibility_hdr|sf=name:L='billed_amount';value:C=billed_amount_hdr
D|D35|D45|etl_loaded_at|ex=F.current_timestamp()
D|D35|D45|etl_source_job|ex=F.lit('silver_clinical_claims')
D|D35|D45|source_system|ex=F.lit('meridian')
F|D45|D46|rc=claim_id|pr=F.col('claim_id').isNotNull()
W|D46|T10|overwrite|delta|pc=service_from_date

@WARN
silver_clinical_claims.py|dynamic-aggregation|agg list contains star-unpacked runtime variable in silver_clinical_claims.py:200
silver_clinical_claims.py|dynamic-aggregation|agg list contains star-unpacked runtime variable in silver_clinical_claims.py:270
