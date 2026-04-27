# ================================================================
# NOTEBOOK 02: SILVER TRANSFORMATIONS
# ================================================================
# 
# ┌─────────────────────────────────────────────────────────────┐
# │  MODULE 2 — DATA ENGINEERING (Part A: Silver Layer)          │
# │  Fabric Capability: Spark Notebooks, Delta Lake              │
# └─────────────────────────────────────────────────────────────┘
#
# ── INSTRUCTIONS ──────────────────────────────────────────────
#   1. Create a notebook in Fabric named "02 - Silver Transformations"
#   2. Attach your HealthcareLakehouse
#   3. Create one cell per section below (each "CELL" block)
#   4. Run cells sequentially
#
# ================================================================


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 1 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# # 🔄 Silver Layer — Cleansed & Standardized Healthcare Data
#
# ## What is the Silver Layer?
#
# The Silver layer takes raw Bronze data and applies:
# - **Type casting** — Dates become proper dates, numbers become 
#   proper numbers (Bronze data from CSVs often has everything as 
#   strings)
# - **Computed columns** — Derived fields like age groups, risk 
#   categories, and clinical flags that don't exist in the source
# - **Standardization** — Consistent naming, categorization, and 
#   null handling
#
# ## Why is this important in healthcare?
#
# Healthcare data is notoriously messy. EHR systems export dates in 
# different formats, numeric fields arrive as strings, and critical 
# clinical categories (like "is this patient high-risk?") must be 
# computed from raw data. The Silver layer creates a **single source 
# of truth** that all downstream analytics can rely on.
#
# ## What we'll create
#
# | Silver Table | Key Transformations |
# |---|---|
# | silver_patients | Age groups (18-29, 30-44, ..., 75+), risk categories (Low/Medium/High) |
# | silver_encounters | Date parsing, LOS categories, month/quarter extraction, weekend flag |
# | silver_conditions | ICD-10 code → clinical category mapping (Diabetes, CHF, COPD, etc.) |
# | silver_claims | Payment ratio calculation, denial flag |
# | silver_medications | Date parsing for start/end dates |
# | silver_vitals | SIRS criteria flag for sepsis early detection |
# | silver_clinical_notes | Date parsing for note dates |


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 2 — CODE: Setup & Imports                                ║
# ╚════════════════════════════════════════════════════════════════╝

from pyspark.sql.functions import *
from pyspark.sql.types import *


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 3 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Silver Patients
#
# ### Business context
# Patient demographics drive everything in healthcare analytics:
# - **Age groups** are used for population health segmentation, 
#   staffing models, and CMS quality reporting brackets
# - **Risk categories** (derived from risk scores assigned by the 
#   primary care provider) determine care management outreach 
#   priorities — high-risk patients cost 5-10x more than low-risk
# - **Insurance type** determines reimbursement rates and reporting 
#   obligations
#
# ### Technical approach
# - Cast `age` and `risk_score` to proper numeric types
# - Compute `age_group` buckets aligned with CMS reporting brackets
# - Compute `risk_category` using clinical risk score thresholds:
#   - Low: < 1.5 (healthy, minimal chronic disease)
#   - Medium: 1.5–3.0 (some chronic conditions, manageable)
#   - High: ≥ 3.0 (multiple comorbidities, frequent utilization)


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 4 — CODE: Silver Patients                                ║
# ╚════════════════════════════════════════════════════════════════╝

patients = spark.table("bronze_patients")

silver_patients = patients \
    .withColumn("date_of_birth", to_date(col("date_of_birth"))) \
    .withColumn("age", col("age").cast(IntegerType())) \
    .withColumn("risk_score", col("risk_score").cast(DoubleType())) \
    .withColumn("age_group", 
        when(col("age") < 30, "18-29")
        .when(col("age") < 45, "30-44")
        .when(col("age") < 60, "45-59")
        .when(col("age") < 75, "60-74")
        .otherwise("75+")) \
    .withColumn("risk_category",
        when(col("risk_score") < 1.5, "Low")
        .when(col("risk_score") < 3.0, "Medium")
        .otherwise("High")) \
    .select(
        "patient_id", "first_name", "last_name", "date_of_birth", "age",
        "age_group", "gender", "race", "zip_code", "city", "state",
        "insurance_type", "primary_care_provider", "risk_score", "risk_category"
    )

silver_patients.write.mode("overwrite").format("delta").saveAsTable("silver_patients")
print(f"✓ silver_patients: {silver_patients.count()} rows")
silver_patients.groupBy("age_group", "risk_category").count().orderBy("age_group").show()


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 5 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Silver Encounters
#
# ### Business context
# Encounters are the **transaction records** of healthcare — every 
# time a patient interacts with the health system, an encounter is 
# created. Key enrichments:
#
# - **encounter_month / quarter** — Enables time-series trending for 
#   volume forecasting, seasonal pattern analysis (flu season, etc.)
# - **day_of_week / is_weekend** — Weekend admissions often have 
#   worse outcomes due to reduced staffing ("weekend effect")
# - **los_category** — Length of stay buckets help identify outliers. 
#   A "Same Day" inpatient encounter is suspicious (possible coding 
#   error), while "Extended (>10 days)" cases need care management 
#   review
#
# ### Technical approach
# - Parse date strings into proper Date types for date arithmetic
# - Extract temporal dimensions (month, year, quarter, day of week)
# - Categorize length of stay into clinically meaningful buckets


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 6 — CODE: Silver Encounters                              ║
# ╚════════════════════════════════════════════════════════════════╝

encounters = spark.table("bronze_encounters")

silver_encounters = encounters \
    .withColumn("encounter_date", to_date(col("encounter_date"))) \
    .withColumn("discharge_date", to_date(col("discharge_date"))) \
    .withColumn("length_of_stay_days", col("length_of_stay_days").cast(IntegerType())) \
    .withColumn("total_charges", col("total_charges").cast(DoubleType())) \
    .withColumn("encounter_month", date_format(col("encounter_date"), "yyyy-MM")) \
    .withColumn("encounter_year", year(col("encounter_date"))) \
    .withColumn("encounter_quarter", quarter(col("encounter_date"))) \
    .withColumn("day_of_week", dayofweek(col("encounter_date"))) \
    .withColumn("is_weekend", 
        when(dayofweek(col("encounter_date")).isin(1, 7), True).otherwise(False)) \
    .withColumn("los_category",
        when(col("length_of_stay_days") == 0, "Same Day")
        .when(col("length_of_stay_days") <= 2, "Short (1-2 days)")
        .when(col("length_of_stay_days") <= 5, "Medium (3-5 days)")
        .when(col("length_of_stay_days") <= 10, "Long (6-10 days)")
        .otherwise("Extended (>10 days)"))

silver_encounters.write.mode("overwrite").format("delta").saveAsTable("silver_encounters")
print(f"✓ silver_encounters: {silver_encounters.count()} rows")
silver_encounters.groupBy("encounter_type").count().orderBy("count", ascending=False).show()


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 7 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Silver Conditions
#
# ### Business context
# Diagnoses are coded using **ICD-10-CM** (International Classification 
# of Diseases, 10th Revision, Clinical Modification). These codes are:
# - Required for claims submission and reimbursement
# - Used by CMS for risk adjustment (Medicare Advantage payments)
# - The basis for quality measure reporting (e.g., diabetes care, 
#   heart failure management)
#
# We map ICD-10 codes to **clinical categories** because:
# - Raw codes like "E11.9" are meaningless to business users
# - Categories enable population health segmentation 
# - They align with disease management program definitions
#
# ### Code-to-category mapping
# | ICD-10 Prefix | Category | Why it matters |
# |---|---|---|
# | E11 | Diabetes | Affects 37.3M Americans; drives complications |
# | I50 | Heart Failure | #1 cause of readmissions (CMS penalty) |
# | J44 | COPD | 3rd leading cause of death in US |
# | I10 | Hypertension | Affects nearly half of US adults |
# | N18 | Chronic Kidney Disease | Often comorbid with diabetes/HTN |


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 8 — CODE: Silver Conditions                              ║
# ╚════════════════════════════════════════════════════════════════╝

conditions = spark.table("bronze_conditions")

silver_conditions = conditions \
    .withColumn("date_diagnosed", to_date(col("date_diagnosed"))) \
    .withColumn("condition_category",
        when(col("condition_code").startswith("E11"), "Diabetes")
        .when(col("condition_code").startswith("I50"), "Heart Failure")
        .when(col("condition_code").startswith("J44"), "COPD")
        .when(col("condition_code") == "I10", "Hypertension")
        .when(col("condition_code").startswith("E78"), "Hyperlipidemia")
        .when(col("condition_code").startswith("N18"), "Chronic Kidney Disease")
        .when(col("condition_code").startswith("F32"), "Depression")
        .when(col("condition_code").startswith("J45"), "Asthma")
        .when(col("condition_code").startswith("I25"), "Coronary Artery Disease")
        .when(col("condition_code").startswith("E66"), "Obesity")
        .otherwise("Other"))

silver_conditions.write.mode("overwrite").format("delta").saveAsTable("silver_conditions")
print(f"✓ silver_conditions: {silver_conditions.count()} rows")
silver_conditions.groupBy("condition_category").count().orderBy("count", ascending=False).show()


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 9 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Silver Claims
#
# ### Business context
# Claims data is the **financial backbone** of healthcare analytics.
# Each claim represents a bill submitted to an insurance payer.
#
# Key computed fields:
# - **payment_ratio** = paid_amount / claim_amount — Measures how 
#   many cents on the dollar you actually collect. Below 0.80 is 
#   concerning. Medicare typically pays 0.83 on average.
# - **is_denied** flag — Denied claims generate zero revenue but 
#   still cost $25-50 each in admin re-work. The national average 
#   denial rate is ~12%, but Medicare Advantage plans can be 17%+.
#
# ### Why this matters financially
# A 200-bed hospital with an average denial rate of 15% and $200M 
# in annual charges loses ~$30M to denials. Even a 2% reduction in 
# denial rate can recover $4M+ in revenue.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 10 — CODE: Silver Claims, Medications, Vitals, Notes     ║
# ╚════════════════════════════════════════════════════════════════╝

# --- Silver Claims ---
claims = spark.table("bronze_claims")

silver_claims = claims \
    .withColumn("claim_date", to_date(col("claim_date"))) \
    .withColumn("claim_amount", col("claim_amount").cast(DoubleType())) \
    .withColumn("paid_amount", col("paid_amount").cast(DoubleType())) \
    .withColumn("denied_amount", col("denied_amount").cast(DoubleType())) \
    .withColumn("patient_responsibility", col("patient_responsibility").cast(DoubleType())) \
    .withColumn("days_to_payment", col("days_to_payment").cast(IntegerType())) \
    .withColumn("payment_ratio", 
        when(col("claim_amount") > 0, round(col("paid_amount") / col("claim_amount"), 4))
        .otherwise(0)) \
    .withColumn("is_denied", when(col("claim_status") == "Denied", True).otherwise(False))

silver_claims.write.mode("overwrite").format("delta").saveAsTable("silver_claims")
print(f"✓ silver_claims: {silver_claims.count()} rows")

# --- Silver Medications ---
# Simple date parsing. In production, you'd also normalize drug names 
# to RxNorm codes and check for drug-drug interactions.
medications = spark.table("bronze_medications")
silver_medications = medications \
    .withColumn("start_date", to_date(col("start_date"))) \
    .withColumn("end_date", to_date(col("end_date")))

silver_medications.write.mode("overwrite").format("delta").saveAsTable("silver_medications")
print(f"✓ silver_medications: {silver_medications.count()} rows")

# --- Silver Vitals ---
# CRITICAL: We add a SIRS (Systemic Inflammatory Response Syndrome) 
# flag. SIRS is an early indicator of sepsis — a life-threatening 
# condition where the body's response to infection damages its own 
# organs. Sepsis kills 270,000 Americans/year.
#
# SIRS criteria (need ≥2 of 3 in our simplified model):
#   1. Temperature > 100.4°F (38°C) or < 96.8°F (36°C)
#   2. Heart rate > 90 bpm
#   3. Respiratory rate > 20 breaths/min
# (The 4th real criterion — WBC count — is a lab value we don't have)
vitals = spark.table("bronze_vitals")

silver_vitals = vitals \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("heart_rate", col("heart_rate").cast(IntegerType())) \
    .withColumn("systolic_bp", col("systolic_bp").cast(IntegerType())) \
    .withColumn("diastolic_bp", col("diastolic_bp").cast(IntegerType())) \
    .withColumn("temperature_f", col("temperature_f").cast(DoubleType())) \
    .withColumn("respiratory_rate", col("respiratory_rate").cast(IntegerType())) \
    .withColumn("spo2_percent", col("spo2_percent").cast(IntegerType())) \
    .withColumn("pain_level", col("pain_level").cast(IntegerType())) \
    .withColumn("is_sirs_positive",
        (  (col("temperature_f") > 100.4) | (col("temperature_f") < 96.8) ) &
        (col("heart_rate") > 90) &
        (col("respiratory_rate") > 20)
    )

silver_vitals.write.mode("overwrite").format("delta").saveAsTable("silver_vitals")
print(f"✓ silver_vitals: {silver_vitals.count()} rows")

# --- Silver Clinical Notes ---
clinical_notes = spark.table("bronze_clinical_notes")
silver_clinical_notes = clinical_notes \
    .withColumn("note_date", to_date(col("note_date")))

silver_clinical_notes.write.mode("overwrite").format("delta").saveAsTable("silver_clinical_notes")
print(f"✓ silver_clinical_notes: {silver_clinical_notes.count()} rows")

print("\n✅ All Silver tables created!")
