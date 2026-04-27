# ================================================================
# NOTEBOOK 03: GOLD ANALYTICS
# ================================================================
# 
# ┌─────────────────────────────────────────────────────────────┐
# │  MODULE 2 — DATA ENGINEERING (Part B: Gold Layer)            │
# │  Fabric Capability: Spark SQL, Delta Lake, Window Functions  │
# └─────────────────────────────────────────────────────────────┘
#
# ── INSTRUCTIONS ──────────────────────────────────────────────
#   1. Create a notebook in Fabric named "03 - Gold Analytics"
#   2. Attach your HealthcareLakehouse
#   3. Create one cell per section below (each "CELL" block)
#   4. Run cells sequentially
#
# ================================================================


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 1 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# # 🥇 Gold Layer — Business-Ready Healthcare KPIs
#
# ## What is the Gold Layer?
#
# The Gold layer contains **pre-computed, business-ready analytics 
# tables** that Power BI dashboards and Data Agents consume directly. 
# These are the "answers" to the organization's most important 
# questions, pre-calculated for performance.
#
# ## What we'll compute
#
# | Gold Table | Healthcare Metric | Why It Matters |
# |---|---|---|
# | gold_readmissions | 30-day readmission rate | CMS penalizes up to 3% of Medicare payments |
# | gold_ed_utilization | ED frequent flyers | 5% of patients drive 30% of ED costs |
# | gold_alos | Avg length of stay by diagnosis | Each extra day costs $2,000-3,000 |
# | gold_encounter_summary | Volume with demographics | Demand forecasting & capacity planning |
# | gold_financial | Revenue, denials, collection rates | $262B lost to denied claims annually |
# | gold_population_health | Chronic disease prevalence | Proactive care reduces acute events |


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 2 — CODE: Imports                                        ║
# ╚════════════════════════════════════════════════════════════════╝

from pyspark.sql.functions import *
# Window is needed for advanced analytics like running totals,
# row_number(), rank(), lag(), lead() — partition-based operations
# that compare rows within a group (e.g., "previous encounter for
# same patient"). We use it in the readmission self-join approach.
from pyspark.sql.window import Window


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 3 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## 30-Day Hospital Readmissions
#
# ### What is a readmission?
# A **readmission** occurs when a patient is discharged from an 
# inpatient stay and then returns to the hospital within 30 days 
# for another inpatient admission.
#
# ### Why does this matter?
# - **CMS Hospital Readmissions Reduction Program (HRRP):** Hospitals 
#   with "excess" readmissions for heart failure, heart attack, 
#   pneumonia, COPD, hip/knee replacement, and CABG surgery face 
#   Medicare payment reductions of up to **3%** of total DRG payments.
# - **Patient impact:** Readmissions indicate the patient wasn't 
#   fully recovered or didn't have adequate follow-up care.
# - **Financial impact:** The average inpatient stay costs $13,000. 
#   A preventable readmission is $13K in avoidable cost.
# - **National benchmark:** ~15% readmission rate nationally.
#
# ### Technical approach: Self-Join
# The algorithm works by joining the encounters table to itself:
# 1. Take all inpatient discharges as "index admissions"
# 2. For each index admission, look for another inpatient admission 
#    by the **same patient** within 30 days of discharge
# 3. Exclude deaths and AMA (Against Medical Advice) discharges 
#    from the index set (per CMS methodology)
# 4. Mark each index admission as readmitted = TRUE/FALSE
# 5. Deduplicate so each index admission appears only once


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 4 — CODE: 30-Day Readmission Calculation                ║
# ╚════════════════════════════════════════════════════════════════╝

# ── STEP 1: Filter to Inpatient encounters only ───────────────
# Per CMS definition, readmissions only apply to inpatient stays,
# not ED visits or outpatient encounters
encounters = spark.table("silver_encounters") \
    .filter(col("encounter_type") == "Inpatient")

# ── STEP 2: Self-Join Setup ────────────────────────────────
# WHY a self-join? We need to compare each encounter against ALL
# other encounters for the same patient. A self-join creates two
# "copies" of the same table that we can compare row-by-row.
#
# alias("idx") = the "index admission" (the initial discharge)
# alias("readmit") = the potential readmission (a later admission)
# Without aliases, Spark can't tell which table's columns you mean
index_admissions = encounters.alias("idx")
readmissions = encounters.alias("readmit")

# ── STEP 3: Join with 5 conditions ────────────────────────
readmission_pairs = index_admissions.join(
    readmissions,
    # Condition 1: Same patient (must be the SAME person returning)
    (col("idx.patient_id") == col("readmit.patient_id")) &
    # Condition 2: Readmission happened AFTER the index discharge
    (col("readmit.encounter_date") > col("idx.discharge_date")) &
    # Condition 3: Within 30 days (the CMS readmission window)
    #   datediff() returns the number of days between two dates
    (datediff(col("readmit.encounter_date"), col("idx.discharge_date")) <= 30) &
    # Condition 4: Not the same encounter (prevent self-matching)
    (col("idx.encounter_id") != col("readmit.encounter_id")) &
    # Condition 5: CMS exclusions — remove patients who died or left
    #   against medical advice (these are not "preventable" readmissions)
    (col("idx.discharge_disposition") != "Expired") &
    (col("idx.discharge_disposition") != "Against Medical Advice"),
    # LEFT join: keep ALL index admissions, even those WITHOUT a readmission
    # (those will have NULL values in the readmit.* columns)
    "left"
)

# ── STEP 4: Select and compute readmission flag ──────────────
gold_readmissions = readmission_pairs.select(
    # Index admission details (the original discharge)
    col("idx.encounter_id").alias("index_encounter_id"),
    col("idx.patient_id"),
    col("idx.encounter_date").alias("index_admission_date"),
    col("idx.discharge_date").alias("index_discharge_date"),
    col("idx.primary_diagnosis_code").alias("index_diagnosis_code"),
    col("idx.primary_diagnosis_description").alias("index_diagnosis"),
    col("idx.facility_name").alias("index_facility"),
    col("idx.attending_provider").alias("index_provider"),
    col("idx.length_of_stay_days").alias("index_los"),
    col("idx.discharge_disposition").alias("index_disposition"),
    # Readmission details (NULL if patient was NOT readmitted)
    col("readmit.encounter_id").alias("readmit_encounter_id"),
    col("readmit.encounter_date").alias("readmit_date"),
    # Boolean flag: TRUE if a matching readmission was found
    # isNotNull() checks if the LEFT join found a match
    when(col("readmit.encounter_id").isNotNull(), True).otherwise(False).alias("was_readmitted"),
    # Days until readmission (NULL if not readmitted)
    when(col("readmit.encounter_id").isNotNull(),
         datediff(col("readmit.encounter_date"), col("idx.discharge_date"))
    ).alias("days_to_readmission")
# STEP 5: Deduplicate — if a patient was readmitted multiple times
# within 30 days, we only count the FIRST readmission for each index
).dropDuplicates(["index_encounter_id"])

gold_readmissions.write.mode("overwrite").format("delta").saveAsTable("gold_readmissions")

# ── STEP 6: Print readmission rate for validation ────────────
# National benchmark: ~15%. Above 15% triggers CMS penalties.
total = gold_readmissions.count()
readmitted = gold_readmissions.filter(col("was_readmitted") == True).count()
rate = (readmitted / total * 100) if total > 0 else 0
print(f"✓ gold_readmissions: {total} index admissions, {readmitted} readmitted ({rate:.1f}%)")

# Show readmission rates by diagnosis — which conditions are 
# driving the most returns? Heart failure typically has the highest
# readmission rate (23%+), which is why CMS specifically targets it.
print("\nReadmission Rate by Diagnosis:")
gold_readmissions.groupBy("index_diagnosis") \
    .agg(
        count("*").alias("total_admissions"),
        sum(when(col("was_readmitted"), 1).otherwise(0)).alias("readmissions"),
        round(sum(when(col("was_readmitted"), 1).otherwise(0)) / count("*") * 100, 1).alias("readmission_rate_pct")
    ) \
    .filter(col("total_admissions") >= 5) \
    .orderBy("readmission_rate_pct", ascending=False) \
    .show(15, truncate=False)


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 5 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## ED Utilization & Frequent Flyers
#
# ### Business context
# **ED frequent flyers** — patients with 4+ ED visits per year —  
# represent a small fraction of the population but consume a 
# disproportionate share of resources:
# - The top 5% of ED utilizers account for ~30% of all ED visits
# - Many frequent flyers have unmanaged chronic conditions that 
#   could be treated more effectively in outpatient settings
# - Each ED visit costs $2,000-5,000 vs. $150-300 for primary care
#
# Identifying frequent flyers enables **care management outreach** — 
# connecting these patients with primary care, social workers, and 
# community health resources to address root causes.
#
# ### Technical approach
# 1. Filter to ED encounters only
# 2. Group by patient + year to count visits
# 3. Flag anyone with ≥4 visits as a frequent flyer
# 4. Join with patient demographics to profile the population


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 6 — CODE: ED Utilization & Frequent Flyers               ║
# ╚════════════════════════════════════════════════════════════════╝

encounters = spark.table("silver_encounters")
patients = spark.table("silver_patients")

# ── STEP 1: Count ED visits per patient per year ───────────────
# We group by year because the 4-visit threshold is annual.
# We also track:
#   - facilities_visited: >1 facility suggests "ED shopping"
#     (going to different EDs to avoid follow-up coordination)
#   - total_ed_charges: Financial impact of each patient's ED use
ed_visits = encounters \
    .filter(col("encounter_type") == "ED") \
    .groupBy("patient_id", "encounter_year") \
    .agg(
        count("*").alias("ed_visit_count"),
        countDistinct("facility_name").alias("facilities_visited"),
        sum("total_charges").alias("total_ed_charges")
    )

# ── STEP 2: Flag frequent flyers and enrich with demographics ──
# Threshold: 4+ visits/year is the standard clinical definition
# JOIN with patients to get demographics for population profiling:
#   "Who are our frequent flyers? Are they elderly? Uninsured?
#    High-risk? This drives intervention design."
ed_frequent_flyers = ed_visits \
    .withColumn("is_frequent_flyer", when(col("ed_visit_count") >= 4, True).otherwise(False)) \
    .join(patients.select("patient_id", "first_name", "last_name", "age", "insurance_type", "risk_score"),
          "patient_id", "left")

ed_frequent_flyers.write.mode("overwrite").format("delta").saveAsTable("gold_ed_utilization")

frequent_flyers = ed_frequent_flyers.filter(col("is_frequent_flyer") == True).count()
print(f"✓ gold_ed_utilization: {ed_frequent_flyers.count()} records, {frequent_flyers} frequent flyers")

# Validate: Most patients should have 1-2 ED visits; a small tail
# should have 4+. If 50%+ are frequent flyers, check your data.
print("\nED Visit Distribution:")
ed_frequent_flyers.groupBy("ed_visit_count").count().orderBy("ed_visit_count").show()


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 7 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Average Length of Stay (ALOS) by Diagnosis
#
# ### Business context
# ALOS is a core **operational efficiency metric**:
# - The national average for inpatient stays is ~4.5 days
# - Each additional inpatient day costs the hospital $2,000-3,000 
#   in direct costs (nursing, meds, supplies, food)
# - ALOS outliers may indicate complications, care delays, or 
#   discharge planning inefficiencies
# - Diagnosis-specific ALOS helps set expectations (e.g., a hip 
#   replacement should be 2-3 days; >5 days signals complications)
#
# ### Technical approach
# Group inpatient encounters by diagnosis + facility, compute 
# statistical measures (mean, std dev, min, max) and average charges.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 8 — CODE: ALOS by Diagnosis                              ║
# ╚════════════════════════════════════════════════════════════════╝

encounters = spark.table("silver_encounters")

# Filter: Inpatient only, AND LOS > 0 (exclude observation/same-day stays
# that were coded as inpatient in error — LOS of 0 skews the average down)
gold_alos = encounters \
    .filter((col("encounter_type") == "Inpatient") & (col("length_of_stay_days") > 0)) \
    .groupBy(
        "primary_diagnosis_code",
        "primary_diagnosis_description",
        "facility_name"   # Group by facility to compare performance BETWEEN hospitals
    ) \
    .agg(
        count("*").alias("admission_count"),         # Volume: how many cases?
        round(avg("length_of_stay_days"), 1).alias("avg_los"),  # Mean LOS
        round(stddev("length_of_stay_days"), 1).alias("stddev_los"),  # Variability
        min("length_of_stay_days").alias("min_los"),   # Best case
        max("length_of_stay_days").alias("max_los"),   # Worst case (outlier detection)
        round(avg("total_charges"), 2).alias("avg_charges")  # Cost per case
    )

gold_alos.write.mode("overwrite").format("delta").saveAsTable("gold_alos")
print(f"✓ gold_alos: {gold_alos.count()} diagnosis-facility combinations")

# Aggregate across facilities to see system-wide ALOS by diagnosis
# filter(admission_count >= 3) removes rare diagnoses with unreliable stats
print("\nTop 10 Diagnoses by Average LOS:")
gold_alos.groupBy("primary_diagnosis_description") \
    .agg(
        sum("admission_count").alias("total_admissions"),
        round(avg("avg_los"), 1).alias("overall_avg_los")
    ) \
    .filter(col("total_admissions") >= 3) \
    .orderBy("overall_avg_los", ascending=False) \
    .show(10, truncate=False)


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 9 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Encounter Summary (Volume & Demographics)
#
# ### Business context
# This table is the **central fact table** for Power BI reporting. 
# It combines encounter details with patient demographics, enabling 
# slicing and dicing by:
# - **Time:** month, quarter, year, day of week
# - **Location:** facility, department
# - **Patient:** age group, gender, insurance, risk level, race
# - **Clinical:** diagnosis, encounter type, LOS category
#
# This supports questions like: "How many Medicare ED visits did 
# Metro General have in Q3?" or "What's the average LOS for 
# high-risk inpatient patients aged 75+?"


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 10 — CODE: Encounter Summary                             ║
# ╚════════════════════════════════════════════════════════════════╝

encounters = spark.table("silver_encounters")
patients = spark.table("silver_patients")

# ── Denormalization: JOIN encounters + patients into a wide table ──
# WHY? Power BI works best with wide, flat tables ("star schema").
# Instead of making Power BI join 2 tables at query time, we
# pre-join them here. This gives Power BI a single table with
# ALL the columns it needs for slicing:
#   - Encounter dimensions (type, facility, department, month)
#   - Patient dimensions (age_group, gender, insurance, risk)
#
# We select SPECIFIC columns from patients (not *) to:
#   1. Avoid duplicate column names after join
#   2. Control exactly what Power BI analysts can see
#   3. Keep the table focused (no addresses, SSNs, etc.)

gold_encounter_summary = encounters \
    .join(patients.select("patient_id", "age", "age_group", "gender",
                          "insurance_type", "risk_category", "race"),
          "patient_id", "left") \
    .select(
        # Encounter facts
        "encounter_id", "patient_id", "encounter_date", "discharge_date",
        "encounter_type", "facility_name", "department",
        "primary_diagnosis_code", "primary_diagnosis_description",
        "attending_provider", "discharge_disposition",
        "length_of_stay_days", "total_charges",
        # Time dimensions (pre-computed in Silver for performance)
        "encounter_month", "encounter_year", "encounter_quarter",
        "day_of_week", "is_weekend", "los_category",
        # Patient dimensions (from the join)
        "age", "age_group", "gender", "insurance_type", "risk_category", "race"
    )

gold_encounter_summary.write.mode("overwrite").format("delta").saveAsTable("gold_encounter_summary")
print(f"✓ gold_encounter_summary: {gold_encounter_summary.count()} rows")

# Monthly volume trend — is demand growing, stable, or seasonal?
# This is the first thing any healthcare COO asks for:
#   "Show me volume trends by encounter type by month"
print("\nMonthly Encounter Volumes:")
gold_encounter_summary.groupBy("encounter_month") \
    .agg(
        count("*").alias("total_encounters"),
        sum(when(col("encounter_type") == "ED", 1).otherwise(0)).alias("ed_visits"),
        sum(when(col("encounter_type") == "Inpatient", 1).otherwise(0)).alias("inpatient")
    ) \
    .orderBy("encounter_month") \
    .show(30)


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 11 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Financial Analysis — Revenue, Denials, Collections
#
# ### Business context
# Healthcare revenue cycle management is complex:
# - A claim is **submitted** to the payer (insurance company)
# - The payer can **pay** (full or partial), **deny**, or leave 
#   the claim **pending**
# - Denied claims must be appealed — each appeal costs $25-50 in 
#   administrative labor
# - The industry loses ~$262 billion to denied claims annually
#
# Key metrics:
# - **Collection rate** = paid/billed — How many cents on the 
#   dollar are you actually collecting? Target: >85%
# - **Denial rate** = denied claims / total claims — Target: <10%
# - **Days to payment** — How long until you get paid? Cash flow matters.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 12 — CODE: Financial Analysis                            ║
# ╚════════════════════════════════════════════════════════════════╝

claims = spark.table("silver_claims")
encounters = spark.table("silver_encounters")

# ── Denormalize: Enrich claims with encounter context ───────────
# Claims alone have financial data but lack clinical context.
# By joining with encounters, Power BI can answer questions like:
#   "What's the denial rate for ED visits at Metro General?"
#   "Which diagnosis has the lowest collection rate?"
# We select specific encounter columns to add dimensional context
# without duplicating the full encounter data.
gold_financial = claims \
    .join(
        encounters.select("encounter_id", "encounter_type", "facility_name",
                         "department", "primary_diagnosis_description",
                         "encounter_month", "encounter_year"),
        "encounter_id", "left"
    )

gold_financial.write.mode("overwrite").format("delta").saveAsTable("gold_financial")

print("✓ gold_financial created")

# Denial analysis by payer — the most actionable financial report
# This shows:
#   - denial_rate_pct: % of claims denied (target: <10%)
#   - collection_rate_pct: cents on the dollar collected (target: >85%)
# High denial rates by specific payers help the revenue cycle team
# prioritize which payer contracts to renegotiate or which claim
# types need better documentation before submission.
print("\nClaims Denial Analysis by Payer:")
gold_financial.groupBy("payer") \
    .agg(
        count("*").alias("total_claims"),
        sum(when(col("claim_status") == "Denied", 1).otherwise(0)).alias("denied_claims"),
        round(sum(when(col("claim_status") == "Denied", 1).otherwise(0)) / count("*") * 100, 1).alias("denial_rate_pct"),
        round(sum("claim_amount"), 2).alias("total_billed"),
        round(sum("paid_amount"), 2).alias("total_collected"),
        # collection_rate = total_collected / total_billed
        # This is the "net collection rate" — the ultimate measure of
        # revenue cycle health. Below 85% signals serious problems.
        round(sum("paid_amount") / sum("claim_amount") * 100, 1).alias("collection_rate_pct")
    ) \
    .orderBy("denial_rate_pct", ascending=False) \
    .show(truncate=False)


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 13 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Population Health — Chronic Disease Prevalence
#
# ### Business context
# **Population health management** identifies groups of patients 
# with common risk factors or conditions to deliver targeted 
# interventions *before* they need acute care.
#
# - Patients with **3+ chronic conditions** (multimorbidity) 
#   account for 70% of healthcare spending in the US
# - Identifying these patients enables proactive outreach: 
#   medication management, care coordination, home health visits
# - Common "clusters" to watch: Diabetes + Hypertension + CKD 
#   (the "cardiometabolic triad") — these patients are at very 
#   high risk for heart failure, stroke, and dialysis
#
# ### Technical approach
# 1. Count chronic conditions per patient from silver_conditions
# 2. Create boolean flags for key conditions (diabetes, CHF, etc.)
# 3. Classify into multimorbidity tiers (None, Moderate, High)


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 14 — CODE: Population Health                             ║
# ╚════════════════════════════════════════════════════════════════╝

conditions = spark.table("silver_conditions")
patients = spark.table("silver_patients")

# ── STEP 1: Count chronic conditions per patient ───────────────
# Filter to "Chronic" conditions only (exclude acute illnesses like flu)
# collect_set() aggregates all unique condition categories into an array:
#   e.g., ["Diabetes", "Hypertension", "CKD"] for a single patient
# This array enables the boolean flags below
patient_condition_count = conditions \
    .filter(col("condition_type") == "Chronic") \
    .groupBy("patient_id") \
    .agg(
        count("*").alias("chronic_condition_count"),
        collect_set("condition_category").alias("condition_list")
    )

# ── STEP 2: Join with patients and compute derived columns ─────
# LEFT join ensures patients with ZERO chronic conditions are included
# (they appear with NULL condition_count, which we coalesce to 0)
gold_population_health = patients \
    .join(patient_condition_count, "patient_id", "left") \
    \
    # coalesce() handles NULL → 0 for patients with no chronic conditions
    .withColumn("chronic_condition_count",
        coalesce(col("chronic_condition_count"), lit(0))) \
    \
    # Boolean flags for key conditions using array_contains()
    # WHY individual flags? Power BI can use these as filters:
    #   "Show me all patients with diabetes" → filter has_diabetes = TRUE
    # array_contains() checks if a value exists in the collected array
    .withColumn("has_diabetes",
        array_contains(col("condition_list"), "Diabetes")) \
    .withColumn("has_heart_failure",
        array_contains(col("condition_list"), "Heart Failure")) \
    .withColumn("has_copd",
        array_contains(col("condition_list"), "COPD")) \
    .withColumn("has_hypertension",
        array_contains(col("condition_list"), "Hypertension")) \
    .withColumn("has_ckd",
        array_contains(col("condition_list"), "Chronic Kidney Disease")) \
    \
    # Multimorbidity tiers — a key population health stratification
    #   None: Healthy, minimal intervention
    #   Moderate (1-2): Standard chronic disease management
    #   High (3+): Complex care coordination needed, care manager assigned
    # Patients with 3+ conditions account for ~70% of healthcare spending
    .withColumn("multimorbidity",
        when(col("chronic_condition_count") >= 3, "High (3+)")
        .when(col("chronic_condition_count") >= 1, "Moderate (1-2)")
        .otherwise("None")) \
    \
    # Drop the raw array — we've extracted what we need into boolean flags
    .drop("condition_list")

gold_population_health.write.mode("overwrite").format("delta").saveAsTable("gold_population_health")

# ── STEP 3: Validate prevalence rates ─────────────────────────
# Compare to national benchmarks:
#   Hypertension: ~47% of US adults
#   Diabetes: ~15% of US adults
#   Heart Failure: ~6M Americans (~2%)
#   COPD: ~16M Americans (~6%)
#   CKD: ~37M Americans (~15%)
print(f"✓ gold_population_health: {gold_population_health.count()} rows")
print("\nChronic Condition Prevalence:")
total_patients = gold_population_health.count()
for cond in ["has_diabetes", "has_heart_failure", "has_copd", "has_hypertension", "has_ckd"]:
    cnt = gold_population_health.filter(col(cond) == True).count()
    print(f"  {cond.replace('has_', '').replace('_', ' ').title()}: {cnt} ({cnt/total_patients*100:.1f}%)")

print(f"\nMultimorbidity Distribution:")
gold_population_health.groupBy("multimorbidity").count().orderBy("multimorbidity").show()

print("\n✅ All Gold layer tables created!")
