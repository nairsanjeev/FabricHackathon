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

encounters = spark.table("silver_encounters") \
    .filter(col("encounter_type") == "Inpatient")

# Self-join: match each discharge to any subsequent admission 
# within 30 days for the same patient
index_admissions = encounters.alias("idx")
readmissions = encounters.alias("readmit")

readmission_pairs = index_admissions.join(
    readmissions,
    (col("idx.patient_id") == col("readmit.patient_id")) &
    (col("readmit.encounter_date") > col("idx.discharge_date")) &
    (datediff(col("readmit.encounter_date"), col("idx.discharge_date")) <= 30) &
    (col("idx.encounter_id") != col("readmit.encounter_id")) &
    # CMS exclusions: remove deaths and AMA discharges from index
    (col("idx.discharge_disposition") != "Expired") &
    (col("idx.discharge_disposition") != "Against Medical Advice"),
    "left"
)

gold_readmissions = readmission_pairs.select(
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
    col("readmit.encounter_id").alias("readmit_encounter_id"),
    col("readmit.encounter_date").alias("readmit_date"),
    when(col("readmit.encounter_id").isNotNull(), True).otherwise(False).alias("was_readmitted"),
    when(col("readmit.encounter_id").isNotNull(),
         datediff(col("readmit.encounter_date"), col("idx.discharge_date"))
    ).alias("days_to_readmission")
).dropDuplicates(["index_encounter_id"])

gold_readmissions.write.mode("overwrite").format("delta").saveAsTable("gold_readmissions")

total = gold_readmissions.count()
readmitted = gold_readmissions.filter(col("was_readmitted") == True).count()
rate = (readmitted / total * 100) if total > 0 else 0
print(f"✓ gold_readmissions: {total} index admissions, {readmitted} readmitted ({rate:.1f}%)")

# Show readmission rates by diagnosis — which conditions are 
# driving the most returns?
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

# Count ED visits per patient per year
ed_visits = encounters \
    .filter(col("encounter_type") == "ED") \
    .groupBy("patient_id", "encounter_year") \
    .agg(
        count("*").alias("ed_visit_count"),
        countDistinct("facility_name").alias("facilities_visited"),
        sum("total_charges").alias("total_ed_charges")
    )

# Flag frequent flyers (clinical threshold: 4+ visits/year)
ed_frequent_flyers = ed_visits \
    .withColumn("is_frequent_flyer", when(col("ed_visit_count") >= 4, True).otherwise(False)) \
    .join(patients.select("patient_id", "first_name", "last_name", "age", "insurance_type", "risk_score"),
          "patient_id", "left")

ed_frequent_flyers.write.mode("overwrite").format("delta").saveAsTable("gold_ed_utilization")

frequent_flyers = ed_frequent_flyers.filter(col("is_frequent_flyer") == True).count()
print(f"✓ gold_ed_utilization: {ed_frequent_flyers.count()} records, {frequent_flyers} frequent flyers")

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

gold_alos = encounters \
    .filter((col("encounter_type") == "Inpatient") & (col("length_of_stay_days") > 0)) \
    .groupBy(
        "primary_diagnosis_code",
        "primary_diagnosis_description",
        "facility_name"
    ) \
    .agg(
        count("*").alias("admission_count"),
        round(avg("length_of_stay_days"), 1).alias("avg_los"),
        round(stddev("length_of_stay_days"), 1).alias("stddev_los"),
        min("length_of_stay_days").alias("min_los"),
        max("length_of_stay_days").alias("max_los"),
        round(avg("total_charges"), 2).alias("avg_charges")
    )

gold_alos.write.mode("overwrite").format("delta").saveAsTable("gold_alos")
print(f"✓ gold_alos: {gold_alos.count()} diagnosis-facility combinations")

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

gold_encounter_summary = encounters \
    .join(patients.select("patient_id", "age", "age_group", "gender",
                          "insurance_type", "risk_category", "race"),
          "patient_id", "left") \
    .select(
        "encounter_id", "patient_id", "encounter_date", "discharge_date",
        "encounter_type", "facility_name", "department",
        "primary_diagnosis_code", "primary_diagnosis_description",
        "attending_provider", "discharge_disposition",
        "length_of_stay_days", "total_charges",
        "encounter_month", "encounter_year", "encounter_quarter",
        "day_of_week", "is_weekend", "los_category",
        "age", "age_group", "gender", "insurance_type", "risk_category", "race"
    )

gold_encounter_summary.write.mode("overwrite").format("delta").saveAsTable("gold_encounter_summary")
print(f"✓ gold_encounter_summary: {gold_encounter_summary.count()} rows")

# Monthly volume trend — is demand growing, stable, or seasonal?
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

gold_financial = claims \
    .join(
        encounters.select("encounter_id", "encounter_type", "facility_name",
                         "department", "primary_diagnosis_description",
                         "encounter_month", "encounter_year"),
        "encounter_id", "left"
    )

gold_financial.write.mode("overwrite").format("delta").saveAsTable("gold_financial")

print("✓ gold_financial created")
print("\nClaims Denial Analysis by Payer:")
gold_financial.groupBy("payer") \
    .agg(
        count("*").alias("total_claims"),
        sum(when(col("claim_status") == "Denied", 1).otherwise(0)).alias("denied_claims"),
        round(sum(when(col("claim_status") == "Denied", 1).otherwise(0)) / count("*") * 100, 1).alias("denial_rate_pct"),
        round(sum("claim_amount"), 2).alias("total_billed"),
        round(sum("paid_amount"), 2).alias("total_collected"),
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

# Count chronic conditions per patient and collect condition names
patient_condition_count = conditions \
    .filter(col("condition_type") == "Chronic") \
    .groupBy("patient_id") \
    .agg(
        count("*").alias("chronic_condition_count"),
        collect_set("condition_category").alias("condition_list")
    )

# Join with patient demographics and create condition flags
gold_population_health = patients \
    .join(patient_condition_count, "patient_id", "left") \
    .withColumn("chronic_condition_count",
        coalesce(col("chronic_condition_count"), lit(0))) \
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
    .withColumn("multimorbidity",
        when(col("chronic_condition_count") >= 3, "High (3+)")
        .when(col("chronic_condition_count") >= 1, "Moderate (1-2)")
        .otherwise("None")) \
    .drop("condition_list")

gold_population_health.write.mode("overwrite").format("delta").saveAsTable("gold_population_health")

print(f"✓ gold_population_health: {gold_population_health.count()} rows")
print("\nChronic Condition Prevalence:")
total_patients = gold_population_health.count()
for cond in ["has_diabetes", "has_heart_failure", "has_copd", "has_hypertension", "has_ckd"]:
    cnt = gold_population_health.filter(col(cond) == True).count()
    print(f"  {cond.replace('has_', '').replace('_', ' ').title()}: {cnt} ({cnt/total_patients*100:.1f}%)")

print(f"\nMultimorbidity Distribution:")
gold_population_health.groupBy("multimorbidity").count().orderBy("multimorbidity").show()

print("\n✅ All Gold layer tables created!")
