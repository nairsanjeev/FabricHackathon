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
# ## 1. What this notebook does (business meaning)
#
# The Gold layer is the **final tier** of the Medallion Architecture 
# (Bronze → Silver → Gold). It contains **pre-computed, business-ready 
# analytics tables** that Power BI dashboards and AI Data Agents 
# consume directly.
#
# While Silver tables are "clean data," Gold tables are "answers" — 
# each one pre-computes a specific healthcare KPI so Power BI doesn't 
# have to recalculate complex joins and aggregations at query time.
#
# ### Why pre-compute these metrics?
# - **Performance:** A 30-day readmission calculation requires a 
#   self-join across millions of encounters. Pre-computing it once 
#   means Power BI renders dashboards in <1 second.
# - **Consistency:** Every report, dashboard, and AI agent uses the 
#   SAME definition of "readmission" or "frequent flyer" because 
#   the logic lives in one place (here).
# - **Governance:** Business rule changes (e.g., CMS changes the 
#   readmission window from 30 to 60 days) only need to be updated 
#   in this one notebook.
#
# ## 2. What we'll compute
#
# | Gold Table | Healthcare Metric | Why It Matters |
# |---|---|---|
# | gold_readmissions | 30-day readmission rate | CMS penalizes up to 3% of Medicare payments |
# | gold_ed_utilization | ED frequent flyers | 5% of patients drive 30% of ED costs |
# | gold_alos | Avg length of stay by diagnosis | Each extra day costs $2,000-3,000 |
# | gold_encounter_summary | Volume with demographics | Demand forecasting & capacity planning |
# | gold_financial | Revenue, denials, collection rates | $262B lost to denied claims annually |
# | gold_population_health | Chronic disease prevalence | Proactive care reduces acute events |
#
# ## 3. Common patterns used in this notebook
#
# - **Self-join:** Comparing a table against itself to find 
#   related rows (e.g., readmissions = same patient, later date)
# - **Aggregation with `agg()`:** Computing multiple statistics 
#   (count, sum, avg, stddev, min, max) in a single pass
# - **Denormalization:** Pre-joining encounters + patients into 
#   one wide table for Power BI star-schema consumption
# - **Boolean flags:** `when(condition, True).otherwise(False)` 
#   for easy filtering (e.g., `was_readmitted`, `is_frequent_flyer`)
# - **Validation prints:** Each Gold table includes benchmark 
#   comparisons to national healthcare statistics


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
# ### 1. What this measure represents (business meaning)
#
# A **30-day readmission** occurs when a patient is discharged from 
# an inpatient hospital stay and then returns to the hospital for 
# another inpatient admission within 30 calendar days of discharge.
#
# This is one of the most important quality metrics in US healthcare:
# - **CMS Hospital Readmissions Reduction Program (HRRP):** Hospitals 
#   with "excess" readmissions for heart failure, heart attack, 
#   pneumonia, COPD, hip/knee replacement, and CABG surgery face 
#   Medicare payment reductions of up to **3%** of total DRG payments.
# - **Patient impact:** A readmission means the patient wasn't fully 
#   recovered, didn't understand discharge instructions, or lacked 
#   adequate follow-up care. It signals a care transition failure.
# - **Financial impact:** The average inpatient stay costs $13,000. 
#   Every preventable readmission = $13K in avoidable cost.
# - **National benchmark:** ~15% readmission rate nationally. Above 
#   15% triggers CMS scrutiny and potential payment penalties.
#
# ### 2. Step-by-step walkthrough of the logic
#
# #### Step 1: Filter to Inpatient encounters only
#     encounters = spark.table("silver_encounters")
#         .filter(col("encounter_type") == "Inpatient")
# - Per CMS definition, readmissions ONLY apply to inpatient stays
# - ED visits, outpatient appointments, and ambulatory encounters 
#   are excluded — even if the patient returned to the ED within 
#   30 days, that's not a "readmission" by CMS rules
#
# #### Step 2: Create two aliases for the self-join
#     index_admissions = encounters.alias("idx")
#     readmissions = encounters.alias("readmit")
# - **Why a self-join?** We need to compare EACH encounter against 
#   ALL other encounters for the SAME patient. This requires two 
#   "copies" of the same table — one representing the "index" 
#   (original) admission, one representing the potential readmission.
# - `alias("idx")` and `alias("readmit")` give each copy a name so 
#   Spark can distinguish `idx.encounter_date` from 
#   `readmit.encounter_date`
# - Without aliases, Spark would throw an
#   `AnalysisException: ambiguous reference` error
#
# #### Step 3: Join with 5 conditions
#     readmission_pairs = index_admissions.join(
#         readmissions,
#         (col("idx.patient_id") == col("readmit.patient_id")) &
#         (col("readmit.encounter_date") > col("idx.discharge_date")) &
#         (datediff(col("readmit.encounter_date"), col("idx.discharge_date")) <= 30) &
#         (col("idx.encounter_id") != col("readmit.encounter_id")) &
#         (col("idx.discharge_disposition") != "Expired") &
#         (col("idx.discharge_disposition") != "Against Medical Advice"),
#         "left")
# - ✅ **Condition 1:** Same patient (`patient_id` match). The readmission 
#   must be by the SAME person returning.
# - ✅ **Condition 2:** Readmission happened AFTER the index discharge. 
#   Without this, Spark would match encounters that happened BEFORE 
#   the index admission.
# - ✅ **Condition 3:** Within 30 days. `datediff()` returns the number 
#   of calendar days between two dates. This is the CMS-defined window.
# - ✅ **Condition 4:** Not the same encounter. Without this, every 
#   encounter matches itself (patient_id = patient_id, date within 0 
#   days of itself).
# - ✅ **Conditions 5-6:** CMS exclusions. Patients who died during the 
#   index stay (`Expired`) or left against medical advice (`AMA`) are 
#   excluded because their readmissions are not considered preventable.
# - `"left"` join ensures ALL index admissions appear in the output, 
#   even those WITHOUT a readmission (they get NULL in readmit columns).
#
# #### Step 4: Build the output table
#     gold_readmissions = readmission_pairs.select(
#         col("idx.encounter_id").alias("index_encounter_id"),
#         ...
#         when(col("readmit.encounter_id").isNotNull(), True)
#             .otherwise(False).alias("was_readmitted"),
#         ...)
# - Each row = one index admission with its readmission status
# - **Output columns explained:**
#   - `index_encounter_id` — The original admission being evaluated
#   - `index_admission_date` / `index_discharge_date` — When the index 
#     stay started and ended
#   - `index_diagnosis` — Primary diagnosis of the original stay
#   - `index_facility` / `index_provider` — Where and who treated them
#   - `index_los` — Length of the original stay (short stays may 
#     indicate premature discharge)
#   - `index_disposition` — How the patient left (Home, SNF, etc.)
#   - `readmit_encounter_id` — The readmission encounter (NULL if none)
#   - `readmit_date` — When the patient returned (NULL if not readmitted)
#   - `was_readmitted` — Boolean flag: `isNotNull()` on the readmit 
#     encounter ID. If the LEFT join found a match, it's TRUE.
#   - `days_to_readmission` — How quickly they returned (NULL if not)
#
# #### Step 5: Deduplicate
#     .dropDuplicates(["index_encounter_id"])
# - If a patient was readmitted MULTIPLE times within 30 days, the 
#   self-join creates multiple rows for the same index admission. 
#   We keep only the FIRST match (earliest readmission) per index.
#
# #### Step 6: Validate readmission rate
#     rate = (readmitted / total * 100)
# - Compare to the national benchmark (~15%). Rates above 15% 
#   indicate potential care transition gaps. Rates below 10% may 
#   indicate insufficient case volume for statistical significance.
# - Readmission rates by diagnosis reveal which conditions are 
#   driving returns. Heart failure typically leads (23%+), which is 
#   why CMS specifically targets it under HRRP.
#
# ### 3. Summary
#
# The readmission logic uses a self-join on the inpatient encounters 
# table to pair each index admission with any subsequent inpatient 
# admission by the same patient within 30 days of discharge. CMS 
# exclusions (death, AMA) are applied in the join conditions. The 
# result is a Boolean `was_readmitted` flag and `days_to_readmission` 
# for each index admission, deduplicated to count each index event 
# only once. This enables readmission rate reporting by diagnosis, 
# facility, provider, and time period.


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
# ### 1. What this measure represents (business meaning)
#
# **ED frequent flyers** are patients with 4+ ED visits per year. 
# They represent a small fraction of the population but consume a 
# disproportionate share of emergency resources:
# - The top 5% of ED utilizers account for ~30% of all ED visits
# - Each ED visit costs $2,000–$5,000 vs. $150–$300 for primary care
# - Many frequent flyers have unmanaged chronic conditions (diabetes, 
#   COPD, CHF) that could be treated in outpatient settings at 1/10th 
#   the cost
# - Identifying them enables **care management outreach** — connecting 
#   patients with primary care, social workers, transportation, and 
#   community health resources
#
# ### 2. Step-by-step walkthrough of the logic
#
# #### Step 1: Filter to ED encounters and count visits per patient
#     ed_visits = encounters
#         .filter(col("encounter_type") == "ED")
#         .groupBy("patient_id", "encounter_year")
#         .agg(
#             count("*").alias("ed_visit_count"),
#             countDistinct("facility_name").alias("facilities_visited"),
#             sum("total_charges").alias("total_ed_charges"))
# - `filter(encounter_type == "ED")` isolates emergency department visits
# - `groupBy("patient_id", "encounter_year")` groups by patient AND year 
#   because the 4-visit threshold is annual (4 visits in 2 years = 2/year 
#   = NOT a frequent flyer)
# - Three aggregations:
#   - ✅ `count("*")` → total ED visit count per year
#   - ✅ `countDistinct("facility_name")` → number of different hospitals 
#     visited. If >1, this suggests "ED shopping" (going to different EDs 
#     to avoid follow-up coordination or to seek specific treatments)
#   - ✅ `sum("total_charges")` → total financial impact of each patient's 
#     ED utilization
#
# #### Step 2: Flag frequent flyers
#     .withColumn("is_frequent_flyer",
#         when(col("ed_visit_count") >= 4, True).otherwise(False))
# - The 4+ threshold is the standard clinical definition used by 
#   most health systems and population health platforms
# - Boolean flag enables easy Power BI filtering and aggregation
#
# #### Step 3: Join with patient demographics
#     .join(patients.select("patient_id", "first_name", "last_name",
#                           "age", "insurance_type", "risk_score"),
#           "patient_id", "left")
# - LEFT join with Silver Patients to answer: "Who ARE our frequent 
#   flyers? Are they elderly? Uninsured? High-risk?"
# - Demographics drive intervention design:
#   - Elderly frequent flyers → home health visits, fall prevention
#   - Uninsured frequent flyers → Medicaid enrollment assistance
#   - High-risk frequent flyers → care manager assignment
#
# ### 3. Summary
#
# The ED utilization analysis counts per-patient ED visits by year, 
# flags patients with 4+ annual visits as frequent flyers, tracks 
# multi-facility usage ("ED shopping"), and joins with demographics 
# to profile the frequent flyer population for targeted intervention.


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
# ### 1. What this measure represents (business meaning)
#
# ALOS is a core **operational efficiency metric** that measures how 
# many days, on average, patients stay in the hospital for a given 
# diagnosis. It directly impacts cost, capacity, and quality:
# - **Cost:** Each additional inpatient day costs $2,000–$3,000 in 
#   direct costs (nursing, medications, supplies, dietary)
# - **Capacity:** Longer stays = fewer available beds = longer wait 
#   times in the ED ("boarding") and cancelled elective surgeries
# - **Quality signal:** ALOS outliers may indicate complications, 
#   missed diagnoses, care coordination delays, or discharge 
#   planning inefficiencies
# - **National benchmark:** ~4.5 days for all inpatient stays
# - **Diagnosis-specific expectations:**
#   - Hip replacement: 2–3 days (>5 days = complications)
#   - Pneumonia: 3–4 days
#   - COPD exacerbation: 4–6 days
#   - Heart failure: 4–7 days
#
# ### 2. Step-by-step walkthrough of the logic
#
# #### Step 1: Filter to valid inpatient stays
#     encounters.filter(
#         (col("encounter_type") == "Inpatient") &
#         (col("length_of_stay_days") > 0))
# - `encounter_type == "Inpatient"` — ALOS only applies to admitted 
#   patients, not ED or outpatient visits
# - `length_of_stay_days > 0` — Excludes observation/same-day stays 
#   that were coded as inpatient in error. LOS of 0 would skew the 
#   average downward and misrepresent true inpatient length.
#
# #### Step 2: Group by diagnosis AND facility
#     .groupBy(
#         "primary_diagnosis_code",
#         "primary_diagnosis_description",
#         "facility_name")
# - Grouping by BOTH diagnosis and facility enables performance 
#   comparison BETWEEN hospitals: "Does Metro General discharge 
#   CHF patients faster than Regional Medical Center?"
# - This is essential for internal benchmarking and best-practice 
#   sharing across a health system
#
# #### Step 3: Compute statistical measures
#     .agg(
#         count("*").alias("admission_count"),
#         round(avg("length_of_stay_days"), 1).alias("avg_los"),
#         round(stddev("length_of_stay_days"), 1).alias("stddev_los"),
#         min("length_of_stay_days").alias("min_los"),
#         max("length_of_stay_days").alias("max_los"),
#         round(avg("total_charges"), 2).alias("avg_charges"))
# - ✅ `count("*")` → Volume: how many cases? Small volumes (<3) are 
#   statistically unreliable
# - ✅ `avg("length_of_stay_days")` → Mean LOS: the headline metric
# - ✅ `stddev("length_of_stay_days")` → Variability: high stddev means 
#   inconsistent care (some patients discharged quickly, others not)
# - ✅ `min` / `max` → Range for outlier detection. A max of 30 days 
#   when avg is 4 signals a case that needs review
# - ✅ `avg("total_charges")` → Cost-per-case for financial analysis
#
# ### 3. Summary
#
# The ALOS analysis groups valid inpatient encounters (LOS > 0) by 
# diagnosis and facility, then computes mean, standard deviation, 
# min, max, count, and average charges. This enables cross-facility 
# benchmarking, outlier detection, and cost-per-case analysis by 
# diagnosis.


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
# ### 1. What this table represents (business meaning)
#
# This is the **central fact table** for Power BI reporting — the 
# single most important Gold table. It combines encounter details 
# with patient demographics into one wide, denormalized table that 
# supports every slice-and-dice question a healthcare executive 
# might ask:
# - "How many Medicare ED visits did Metro General have in Q3?"
# - "What's the average LOS for high-risk inpatient patients aged 75+?"
# - "Which facility has the highest weekend admission rate?"
# - "Show ED volume by month, filtered to Medicaid female patients"
#
# ### 2. Step-by-step walkthrough of the logic
#
# #### Step 1: Load Silver tables
#     encounters = spark.table("silver_encounters")
#     patients = spark.table("silver_patients")
# - We need TWO tables: encounters (the facts) and patients (the 
#   dimensions). Power BI works best with wide, flat tables.
#
# #### Step 2: Join encounters with patient demographics
#     encounters.join(
#         patients.select("patient_id", "age", "age_group", "gender",
#                         "insurance_type", "risk_category", "race"),
#         "patient_id", "left")
# - **Denormalization:** Instead of making Power BI join 2 tables at 
#   query time (which is slow and error-prone), we pre-join them here.
# - `.select()` on patients picks ONLY the demographic columns we 
#   want. This prevents:
#   - ✅ Duplicate column names (both tables have `patient_id`)
#   - ✅ Including sensitive data not needed for analytics
#   - ✅ Bloating the table with unnecessary columns
# - `"left"` join ensures encounters without a matching patient 
#   record are still included (data quality issue, not silently lost)
#
# #### Step 3: Select the final column set
#     .select(
#         # Encounter facts
#         "encounter_id", "encounter_date", "encounter_type", ...
#         # Time dimensions (pre-computed in Silver)
#         "encounter_month", "encounter_year", "encounter_quarter", ...
#         # Patient dimensions (from the join)
#         "age", "age_group", "gender", "insurance_type", ...)
# - Three categories of columns in the output:
#   - ✅ **Encounter facts:** encounter_id, dates, type, facility, 
#     department, diagnosis, provider, disposition, LOS, charges
#   - ✅ **Time dimensions:** month, year, quarter, day_of_week, 
#     is_weekend, los_category (all pre-computed in Notebook 02)
#   - ✅ **Patient dimensions:** age, age_group, gender, insurance, 
#     risk_category, race (from the patient join)
# - This creates a **star schema** fact table optimized for Power BI
#
# ### 3. Summary
#
# The Encounter Summary denormalizes Silver encounters and patients 
# into one wide fact table containing encounter details, time 
# dimensions, and patient demographics. This eliminates the need for 
# Power BI to perform runtime joins, enabling fast interactive 
# filtering by any combination of time, location, clinical, and 
# demographic dimensions.


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
# ### 1. What this table represents (business meaning)
#
# Healthcare revenue cycle management tracks the journey of a claim 
# from service delivery to payment:
# 1. Patient receives care (encounter)
# 2. Hospital submits a **claim** to the payer (insurance company)
# 3. Payer adjudicates: **pay** (full or partial), **deny**, or **pend**
# 4. Denied claims must be **appealed** — each appeal costs $25–$50 
#    in administrative labor
# 5. The industry loses ~$262 billion to denied claims annually
#
# Key financial metrics this table supports:
# - **Collection rate** = paid / billed — cents on the dollar 
#   actually collected. Target: >85%
# - **Denial rate** = denied claims / total claims — Target: <10%
# - **Days to payment** — cash flow velocity. 45+ days is concerning.
#
# ### 2. Step-by-step walkthrough of the logic
#
# #### Step 1: Load and join claims with encounter context
#     gold_financial = claims.join(
#         encounters.select("encounter_id", "encounter_type",
#                          "facility_name", "department",
#                          "primary_diagnosis_description",
#                          "encounter_month", "encounter_year"),
#         "encounter_id", "left")
# - Claims alone have financial data but no clinical context. Without 
#   the join, you can't answer "What's the denial rate for ED visits 
#   at Metro General?" or "Which diagnosis has the lowest collection 
#   rate?"
# - We select SPECIFIC encounter columns to add dimensional context:
#   - ✅ `encounter_type` → Filter by ED, Inpatient, Outpatient
#   - ✅ `facility_name` → Compare financial performance across hospitals
#   - ✅ `department` → Drill down within a facility
#   - ✅ `primary_diagnosis_description` → Which diagnoses earn the most/least?
#   - ✅ `encounter_month` / `encounter_year` → Time-series revenue trending
#
# #### Step 2: Validate with denial analysis by payer
#     gold_financial.groupBy("payer").agg(
#         count("*").alias("total_claims"),
#         sum(when(claim_status == "Denied", 1).otherwise(0)).alias("denied_claims"),
#         round(denied / total * 100, 1).alias("denial_rate_pct"),
#         round(sum("paid_amount") / sum("claim_amount") * 100, 1)
#             .alias("collection_rate_pct"))
# - This is the most actionable financial report: denial and 
#   collection rates broken down by insurance payer
# - High denial rates for a specific payer indicate:
#   - ✅ Contract terms may need renegotiation
#   - ✅ Claim documentation may not meet that payer's requirements
#   - ✅ Prior authorization rules may be changing
# - The revenue cycle team uses this to prioritize where to focus 
#   denial prevention and appeal efforts
#
# ### 3. Summary
#
# The Financial Analysis table denormalizes claims with encounter 
# context (type, facility, diagnosis, time period) to enable 
# revenue cycle reporting. The validation query computes denial 
# rates and collection rates by payer, revealing which insurance 
# companies are the most difficult to collect from.


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
# ### 1. What this table represents (business meaning)
#
# **Population health management** identifies groups of patients with 
# common risk factors or chronic conditions to deliver targeted 
# interventions BEFORE they need acute care (ED visits, hospital 
# admissions). This is the shift from **reactive** ("treat the sick") 
# to **proactive** ("keep the healthy from getting sick") healthcare.
#
# Key facts driving this analysis:
# - Patients with **3+ chronic conditions** (multimorbidity) account 
#   for **70% of all healthcare spending** in the US
# - Common high-cost clusters to watch:
#   - **Cardiometabolic triad:** Diabetes + Hypertension + CKD — these 
#     patients are at very high risk for heart failure, stroke, and 
#     dialysis
#   - **Cardiopulmonary:** CHF + COPD — each condition worsens the other
#   - **Mental health comorbidity:** Depression + any chronic disease — 
#     doubles healthcare costs and halves medication adherence
# - Identifying these patients enables proactive outreach: medication 
#   management, care coordination, home health visits, telehealth 
#   check-ins
#
# ### 2. Step-by-step walkthrough of the logic
#
# #### Step 1: Count chronic conditions per patient
#     patient_condition_count = conditions
#         .filter(col("condition_type") == "Chronic")
#         .groupBy("patient_id")
#         .agg(
#             count("*").alias("chronic_condition_count"),
#             collect_set("condition_category").alias("condition_list"))
# - `filter("Chronic")` excludes acute conditions (flu, fractures, 
#   infections) that resolve — we want ongoing conditions only
# - `groupBy("patient_id")` computes one row per patient
# - Two aggregations:
#   - ✅ `count("*")` → how many chronic conditions this patient has
#   - ✅ `collect_set("condition_category")` → collects all UNIQUE 
#     condition categories into an array: `["Diabetes", "Hypertension", 
#     "CKD"]`. This array enables the boolean flags in Step 3.
#
# #### Step 2: Join with patients
#     patients.join(patient_condition_count, "patient_id", "left")
# - LEFT join ensures patients with ZERO chronic conditions are still 
#   included (they appear with NULL values, which we handle next)
#
# #### Step 3: Handle NULLs and create condition flags
#     .withColumn("chronic_condition_count",
#         coalesce(col("chronic_condition_count"), lit(0)))
#     .withColumn("has_diabetes",
#         array_contains(col("condition_list"), "Diabetes"))
# - `coalesce(value, 0)` replaces NULL with 0 for patients with no 
#   chronic conditions (they had no rows in the condition table)
# - `array_contains(array, "Diabetes")` checks if "Diabetes" exists 
#   in the patient's collected condition list. Returns TRUE/FALSE.
# - We create boolean flags for 5 key conditions:
#   - ✅ `has_diabetes` — Type 2 Diabetes (E11)
#   - ✅ `has_heart_failure` — Congestive Heart Failure (I50)
#   - ✅ `has_copd` — Chronic Obstructive Pulmonary Disease (J44)
#   - ✅ `has_hypertension` — Essential Hypertension (I10)
#   - ✅ `has_ckd` — Chronic Kidney Disease (N18)
# - **Why boolean flags?** Power BI can use these directly as slicer 
#   filters: "Show me all patients with diabetes" = filter 
#   `has_diabetes = TRUE`
#
# #### Step 4: Classify multimorbidity tiers
#     .withColumn("multimorbidity",
#         when(col("chronic_condition_count") >= 3, "High (3+)")
#         .when(col("chronic_condition_count") >= 1, "Moderate (1-2)")
#         .otherwise("None"))
# - Three tiers with clinical implications:
#   - ✅ **None:** Healthy, low-cost, preventive care only
#   - ✅ **Moderate (1-2):** Standard chronic disease management 
#     (quarterly visits, medication management)
#   - ✅ **High (3+):** Complex care coordination needed. These patients 
#     should have an assigned care manager, monthly check-ins, and 
#     medication reconciliation. They represent ~5% of patients but 
#     ~50% of total cost.
#
# #### Step 5: Validate prevalence rates
#     for cond in ["has_diabetes", "has_heart_failure", ...]:
#         cnt = gold_population_health.filter(col(cond) == True).count()
# - Compare computed prevalence to national benchmarks:
#   - Hypertension: ~47% of US adults
#   - Diabetes: ~15% of US adults
#   - COPD: ~6% of US adults
#   - Heart Failure: ~2% of US adults
#   - CKD: ~15% of US adults
# - If prevalence is wildly different, investigate the data generator
#
# ### 3. Summary
#
# The Population Health table combines patient demographics with 
# per-patient chronic condition counts. It creates boolean flags 
# for 5 key conditions using `array_contains()` on the 
# `collect_set()` array, and classifies patients into multimorbidity 
# tiers (None, Moderate, High). This enables population health 
# dashboards to identify high-risk patient cohorts for targeted 
# care management interventions.


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
