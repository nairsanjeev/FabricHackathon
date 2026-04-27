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
# ## 1. What this notebook does (business meaning)
#
# The Silver layer is the **second tier** of the Medallion 
# Architecture (Bronze → Silver → Gold). It takes raw Bronze data 
# and applies data engineering transformations to produce a clean, 
# trustworthy, analytics-ready dataset.
#
# ### Three categories of transformation applied:
# - **Type casting** — Convert strings to proper dates, integers, and 
#   doubles. Bronze data from CSVs stores everything as strings, which 
#   prevents date arithmetic, numeric comparisons, and correct sorting.
# - **Computed columns** — Derived fields that don't exist in the source 
#   but are essential for analytics: age groups, risk categories, 
#   temporal dimensions, clinical flags. These save every downstream 
#   consumer from re-computing the same logic.
# - **Standardization** — Consistent categorization, null handling, and 
#   naming conventions. Raw data may encode the same concept differently 
#   across source systems.
#
# ### Why this matters in healthcare
# Healthcare data is notoriously messy. EHR systems export dates in 
# different formats, numeric fields arrive as strings, and critical 
# clinical categories (like "is this patient high-risk?") must be 
# computed from raw data. The Silver layer creates a **single source 
# of truth** that all downstream analytics (Gold tables, reports, 
# AI models) can rely on without re-cleaning the data.
#
# ## 2. What we'll create
#
# | Silver Table | Source Bronze | Key Transformations |
# |---|---|---|
# | silver_patients | bronze_patients | Age groups (18-29…75+), risk categories (Low/Medium/High) |
# | silver_encounters | bronze_encounters | Date parsing, LOS categories, month/quarter/year, weekend flag |
# | silver_conditions | bronze_conditions | ICD-10 code → clinical category mapping (Diabetes, CHF, COPD, etc.) |
# | silver_claims | bronze_claims | Payment ratio (paid/charged), denial flag |
# | silver_medications | bronze_medications | Date parsing for start/end dates |
# | silver_vitals | bronze_vitals | Numeric casting, SIRS sepsis early warning flag |
# | silver_clinical_notes | bronze_clinical_notes | Date parsing for note timestamps |
#
# ## 3. Common PySpark patterns used throughout
#
# Every Silver table follows the same 4-step pattern:
#
#     df = spark.table("bronze_xxx")            # Step A: Read from Bronze
#     silver = df.withColumn(...)                # Step B: Transform columns
#                .withColumn(...)                # (chain multiple transforms)
#     silver.write.mode("overwrite")             # Step C: Write as Delta
#           .format("delta").saveAsTable(...)     
#     silver.groupBy(...).count().show()          # Step D: Validate output
#
# - `.withColumn()` adds or replaces a column. PySpark DataFrames are 
#   immutable, so each call returns a NEW DataFrame.
# - `.mode("overwrite")` makes notebooks **idempotent** (safe to re-run).
# - `.saveAsTable()` registers the table in the Lakehouse catalog.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 2 — CODE: Setup & Imports                                ║
# ╚════════════════════════════════════════════════════════════════╝

# Import ALL PySpark SQL functions and types we'll need
# We use wildcard imports here for convenience in an interactive notebook.
# In production code, you'd import specific functions to avoid namespace conflicts.
#
# Key functions we'll use:
#   to_date()     — Converts string columns to proper Date type
#   to_timestamp()— Converts string columns to proper Timestamp type  
#   col()         — References a DataFrame column by name
#   when()        — SQL CASE WHEN equivalent for conditional logic
#   round()       — Round numeric values to specified decimal places
#   date_format() — Extract formatted date strings (e.g., "yyyy-MM")
#   year() / quarter() / dayofweek() — Extract date components
#
# Key types:
#   IntegerType() — 32-bit integer (for counts, IDs, etc.)
#   DoubleType()  — 64-bit floating point (for money, scores, etc.)
from pyspark.sql.functions import *
from pyspark.sql.types import *


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 3 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Silver Patients
#
# ### 1. What this table represents (business meaning)
# 
# Patient demographics are the **foundation** of healthcare analytics. 
# Every quality metric, financial report, and population health 
# dashboard starts with "which patients are we talking about?" 
# This Silver table enriches raw demographics with computed 
# categories that segment patients for downstream analytics.
#
# Key enrichments and why they matter:
# - **Age groups** — CMS (Centers for Medicare & Medicaid Services) 
#   requires reporting by age bracket. Population health programs 
#   target interventions by age segment (e.g., fall prevention for 
#   75+, diabetes screening for 45-59).
# - **Risk categories** — A patient's risk score (assigned by their 
#   PCP based on comorbidities, utilization, and social factors) 
#   determines care management intensity. High-risk patients (top 
#   5%) drive 50% of total healthcare spending.
#
# ### 2. Step-by-step walkthrough of the logic
#
# #### Step 1: Read the Bronze source table
#     patients = spark.table("bronze_patients")
# - `spark.table()` reads a registered Delta table from the Lakehouse
# - Returns a Spark DataFrame — a distributed, lazy collection of rows
# - At this point, no data has been read from disk (Spark is lazy)
#
# #### Step 2: Parse dates
#     .withColumn("date_of_birth", to_date(col("date_of_birth")))
# - `to_date()` converts a string like `"1985-03-15"` to a proper 
#   Date type, enabling date arithmetic (e.g., `DATEDIFF`)
# - Without this, `"2024-01-01" > "2023-12-31"` works as a string 
#   comparison, but `"2024-01-01" - "2023-12-31"` fails
#
# #### Step 3: Cast numeric types
#     .withColumn("age", col("age").cast(IntegerType()))
#     .withColumn("risk_score", col("risk_score").cast(DoubleType()))
# - Bronze CSVs store everything as strings. Without casting:
#   - `"25" > "3"` evaluates to `False` (string comparison!)
#   - `"25" + "3"` produces `"253"` (string concatenation!)
# - `IntegerType()` for whole numbers, `DoubleType()` for decimals
#
# #### Step 4: Compute age groups
#     .withColumn("age_group",
#         when(col("age") < 30, "18-29")
#         .when(col("age") < 45, "30-44")
#         .when(col("age") < 60, "45-59")
#         .when(col("age") < 75, "60-74")
#         .otherwise("75+"))
# - `when().when().otherwise()` is PySpark's equivalent of SQL 
#   `CASE WHEN ... WHEN ... ELSE ... END`
# - Conditions are evaluated top-to-bottom; first match wins
# - These brackets align with standard CMS reporting age groups:
#   - ✅ 18-29: Young adults (low utilization, high ED use)
#   - ✅ 30-44: Working age (employer insurance dominant)
#   - ✅ 45-59: Pre-Medicare (chronic disease onset)
#   - ✅ 60-74: Medicare transition (highest cost growth)
#   - ✅ 75+: Seniors (complex care, polypharmacy risks)
#
# #### Step 5: Compute risk categories
#     .withColumn("risk_category",
#         when(col("risk_score") < 1.5, "Low")
#         .when(col("risk_score") < 3.0, "Medium")
#         .otherwise("High"))
# - Risk score thresholds are based on clinical conventions:
#   - ✅ Low (< 1.5): Minimal chronic disease, preventive care only
#   - ✅ Medium (1.5–3.0): Some chronic conditions, regular follow-up
#   - ✅ High (≥ 3.0): Multiple comorbidities, active care management
# - High-risk patients cost 5-10x more than Low-risk patients
#
# #### Step 6: Select and write
#     .select("patient_id", "first_name", ...)
#     silver_patients.write.mode("overwrite").format("delta").saveAsTable("silver_patients")
# - `.select()` explicitly lists output columns — prevents accidental 
#   inclusion of raw/temp columns and documents the exact schema
# - `mode("overwrite")` replaces the table if it exists (idempotent)
# - Validation: `groupBy("age_group", "risk_category").count()` cross-tab 
#   should show more "High" risk in older age groups
#
# ### 3. Summary
# 
# The Silver Patients table enriches raw demographics with computed 
# age_group buckets (aligned with CMS age brackets) and risk_category 
# labels (Low/Medium/High based on clinical risk scores). These 
# enrichments enable patient segmentation, population health 
# stratification, and care management prioritization in all 
# downstream Gold analytics.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 4 — CODE: Silver Patients                                ║
# ╚════════════════════════════════════════════════════════════════╝

# Read the raw Bronze table — this is our starting point
# spark.table() reads a Delta table from the Lakehouse by name
patients = spark.table("bronze_patients")

# Build the Silver layer using a chain of .withColumn() transformations
# PySpark DataFrames are IMMUTABLE — each .withColumn() returns a NEW DataFrame
# Chaining them with backslash (\) creates a clean, readable pipeline

silver_patients = patients \
    # 1. PARSE DATES — Convert date strings ("1985-03-15") to Date type
    #    This enables date arithmetic like DATEDIFF, age calculations, etc.
    .withColumn("date_of_birth", to_date(col("date_of_birth"))) \
    \
    # 2. CAST NUMERIC TYPES — Bronze may store these as strings from CSV
    #    IntegerType for age (whole years), DoubleType for risk_score (decimal)
    .withColumn("age", col("age").cast(IntegerType())) \
    .withColumn("risk_score", col("risk_score").cast(DoubleType())) \
    \
    # 3. COMPUTE AGE GROUP — Bucket ages into CMS reporting brackets
    #    These brackets are standard in healthcare analytics:
    #      18-29: Young adults (low utilization, high ED use)
    #      30-44: Working age (employer insurance dominant)
    #      45-59: Pre-Medicare (chronic disease onset)
    #      60-74: Medicare transition (highest cost growth)
    #      75+:   Seniors (complex care, polypharmacy)
    .withColumn("age_group", 
        when(col("age") < 30, "18-29")
        .when(col("age") < 45, "30-44")
        .when(col("age") < 60, "45-59")
        .when(col("age") < 75, "60-74")
        .otherwise("75+")) \
    \
    # 4. COMPUTE RISK CATEGORY — Based on clinical risk score thresholds
    #    Risk scores are assigned by primary care providers based on:
    #      - Number of chronic conditions
    #      - Medication complexity
    #      - Social determinants of health
    #    Low (<1.5):  Minimal intervention needed
    #    Medium (1.5-3.0): Regular follow-up, preventive care
    #    High (≥3.0): Active care management, frequent monitoring
    .withColumn("risk_category",
        when(col("risk_score") < 1.5, "Low")
        .when(col("risk_score") < 3.0, "Medium")
        .otherwise("High")) \
    \
    # 5. SELECT — Explicitly list columns to control output schema
    #    This prevents accidental inclusion of raw/temp columns
    #    and documents the exact Silver schema
    .select(
        "patient_id", "first_name", "last_name", "date_of_birth", "age",
        "age_group", "gender", "race", "zip_code", "city", "state",
        "insurance_type", "primary_care_provider", "risk_score", "risk_category"
    )

# Write to Delta table with "overwrite" for idempotency (safe to re-run)
# Delta format adds ACID transactions, schema enforcement, and time travel
silver_patients.write.mode("overwrite").format("delta").saveAsTable("silver_patients")
print(f"✓ silver_patients: {silver_patients.count()} rows")

# Validation: Cross-tab age_group x risk_category to verify distribution
# Expect: more "High" risk in older age groups (75+, 60-74)
silver_patients.groupBy("age_group", "risk_category").count().orderBy("age_group").show()


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 5 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Silver Encounters
#
# ### 1. What this table represents (business meaning)
# 
# Encounters are the **transaction records** of healthcare. Every 
# patient interaction — an ED visit, a hospital admission, an 
# outpatient appointment — generates an encounter record with dates, 
# type, charges, and length of stay.
#
# This Silver table enriches encounters with temporal dimensions and 
# length-of-stay categories that power time-series analytics, 
# seasonal forecasting, and operational efficiency metrics.
#
# ### 2. Step-by-step walkthrough of the logic
#
# #### Step 1: Read Bronze source
#     encounters = spark.table("bronze_encounters")
# - Loads the raw Delta table registered during Bronze ingestion
#
# #### Step 2: Parse date columns
#     .withColumn("encounter_date", to_date(col("encounter_date")))
#     .withColumn("discharge_date", to_date(col("discharge_date")))
# - Converts strings like `"2024-03-15"` to proper Spark DateType
# - Required for all date arithmetic and temporal extraction below
#
# #### Step 3: Cast numeric types
#     .withColumn("length_of_stay_days", col(...).cast(IntegerType()))
#     .withColumn("total_charges", col(...).cast(DoubleType()))
# - LOS must be numeric for bucket comparisons (`<= 2`, `<= 5`)
# - Charges must be Double for financial aggregations (SUM, AVG)
#
# #### Step 4: Extract temporal dimensions
#     .withColumn("encounter_month", date_format(col("encounter_date"), "yyyy-MM"))
#     .withColumn("encounter_year", year(col("encounter_date")))
#     .withColumn("encounter_quarter", quarter(col("encounter_date")))
# - `date_format(date, "yyyy-MM")` → `"2024-03"` (monthly trending)
# - `year(date)` → `2024` (year-over-year comparisons)
# - `quarter(date)` → `1` through `4` (financial period alignment)
# - **Why these matter:** Volume forecasting requires trending by 
#   month (flu season = Dec-Feb spike in ED visits). Financial 
#   reporting aligns to quarterly periods.
#
# #### Step 5: Day of week and weekend flag
#     .withColumn("day_of_week", dayofweek(col("encounter_date")))
#     .withColumn("is_weekend", when(dayofweek(...).isin(1, 7), True).otherwise(False))
# - `dayofweek()` returns 1=Sunday through 7=Saturday
# - `isin(1, 7)` checks for Sunday or Saturday
# - **The "weekend effect":** Research shows patients admitted on 
#   weekends have 10-15% higher mortality due to reduced specialist 
#   staffing and limited diagnostic services. Tracking this flag 
#   enables the hospital to measure and address the gap.
#
# #### Step 6: Length of stay categories
#     .withColumn("los_category",
#         when(col("length_of_stay_days") == 0, "Same Day")
#         .when(col("length_of_stay_days") <= 2, "Short (1-2 days)")
#         .when(col("length_of_stay_days") <= 5, "Medium (3-5 days)")
#         .when(col("length_of_stay_days") <= 10, "Long (6-10 days)")
#         .otherwise("Extended (>10 days)"))
# - These buckets are clinically meaningful:
#   - ✅ Same Day: Observation stays, day surgeries
#   - ✅ Short (1-2): Uncomplicated admits (pneumonia, chest pain r/o)
#   - ✅ Medium (3-5): Typical medical admits (CHF exacerbation, COPD)
#   - ✅ Long (6-10): Complicated cases (surgery + complications)
#   - ✅ Extended (>10): ICU stays, complex surgical, social admits
# - **Outlier detection:** If 30%+ are Extended, investigate coding 
#   issues or care management gaps
#
# ### 3. Summary
# 
# The Silver Encounters table parses raw date strings into proper 
# date types, extracts temporal dimensions (month, quarter, year, 
# day of week), adds a weekend admission flag for studying the 
# "weekend effect," and buckets length of stay into five clinically 
# meaningful categories. These enrichments enable time-series 
# trending, seasonal forecasting, and LOS outlier detection in 
# Gold analytics.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 6 — CODE: Silver Encounters                              ║
# ╚════════════════════════════════════════════════════════════════╝

encounters = spark.table("bronze_encounters")

silver_encounters = encounters \
    # 1. PARSE DATES — Convert string dates to proper Date type
    #    This enables date arithmetic (e.g., encounter_date - admit_date)
    .withColumn("encounter_date", to_date(col("encounter_date"))) \
    .withColumn("discharge_date", to_date(col("discharge_date"))) \
    \
    # 2. CAST NUMERIC TYPES from strings
    .withColumn("length_of_stay_days", col("length_of_stay_days").cast(IntegerType())) \
    .withColumn("total_charges", col("total_charges").cast(DoubleType())) \
    \
    # 3. EXTRACT TEMPORAL DIMENSIONS — These enable time-series analytics
    #    encounter_month ("2024-01"): Monthly volume trending, seasonality
    #    encounter_year: Year-over-year comparisons
    #    encounter_quarter: Q1-Q4 for financial reporting periods
    .withColumn("encounter_month", date_format(col("encounter_date"), "yyyy-MM")) \
    .withColumn("encounter_year", year(col("encounter_date"))) \
    .withColumn("encounter_quarter", quarter(col("encounter_date"))) \
    \
    # 4. DAY OF WEEK — dayofweek() returns 1=Sunday through 7=Saturday
    #    The "weekend effect" is a well-studied phenomenon: patients
    #    admitted on weekends have 10-15% higher mortality rates due to
    #    reduced specialist staffing and limited diagnostic availability
    .withColumn("day_of_week", dayofweek(col("encounter_date"))) \
    .withColumn("is_weekend", 
        when(dayofweek(col("encounter_date")).isin(1, 7), True).otherwise(False)) \
    \
    # 5. LENGTH OF STAY CATEGORIES — Clinically meaningful buckets
    #    Same Day: Observation stays, day surgeries (should not be inpatient)
    #    Short (1-2 days): Uncomplicated admits (pneumonia, chest pain r/o)
    #    Medium (3-5 days): Typical medical admits (CHF exacerbation, COPD)
    #    Long (6-10 days): Complicated cases (surgery + complications)
    #    Extended (>10 days): ICU stays, complex surgical, social admits
    #    Outlier detection: if 30%+ are Extended, investigate coding issues
    .withColumn("los_category",
        when(col("length_of_stay_days") == 0, "Same Day")
        .when(col("length_of_stay_days") <= 2, "Short (1-2 days)")
        .when(col("length_of_stay_days") <= 5, "Medium (3-5 days)")
        .when(col("length_of_stay_days") <= 10, "Long (6-10 days)")
        .otherwise("Extended (>10 days)"))

silver_encounters.write.mode("overwrite").format("delta").saveAsTable("silver_encounters")
print(f"✓ silver_encounters: {silver_encounters.count()} rows")

# Validate: Encounter type distribution should show Outpatient > Inpatient > ED
silver_encounters.groupBy("encounter_type").count().orderBy("count", ascending=False).show()


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 7 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Silver Conditions
#
# ### 1. What this table represents (business meaning)
# 
# Diagnoses are coded using **ICD-10-CM** (International Classification 
# of Diseases, 10th Revision, Clinical Modification) — the universal 
# language of clinical documentation. Every claim, quality metric, and 
# risk adjustment calculation depends on ICD-10 codes.
#
# However, raw codes like `"E11.9"` are meaningless to business users, 
# executives, and care managers. This Silver table maps ICD-10 codes 
# to **human-readable clinical categories** ("Diabetes", "Heart 
# Failure") that enable population health segmentation and disease 
# management program tracking.
#
# ### 2. Step-by-step walkthrough of the logic
#
# #### Step 1: Read Bronze source
#     conditions = spark.table("bronze_conditions")
# - Loads raw diagnosis records with ICD-10 codes like `"E11.9"`, 
#   `"I50.9"`, `"J44.1"`
#
# #### Step 2: Parse diagnosis date
#     .withColumn("date_diagnosed", to_date(col("date_diagnosed")))
# - Converts date string to Spark DateType for temporal analysis 
#   (e.g., "when was this condition first diagnosed?")
#
# #### Step 3: Map ICD-10 codes to clinical categories
#     .withColumn("condition_category",
#         when(col("condition_code").startswith("E11"), "Diabetes")
#         .when(col("condition_code").startswith("I50"), "Heart Failure")
#         ...)
# - **Why `startswith()` instead of exact match:**
#   ICD-10 codes are hierarchical. `"E11"` is the parent code for 
#   Type 2 Diabetes, with subtypes adding decimal digits:
#   - ✅ E11.0 = Diabetes with hyperosmolarity
#   - ✅ E11.2 = Diabetes with kidney complications
#   - ✅ E11.9 = Diabetes, unspecified
#   Using `startswith("E11")` catches ALL diabetes subtypes in one rule.
#
# - **Exception — Hypertension uses exact match:**
#     `.when(col("condition_code") == "I10", "Hypertension")`
#   Because I10 is the ONLY code for Essential Hypertension — it has 
#   no decimal subtypes, and `startswith("I10")` would accidentally 
#   match I10x codes from other categories.
#
# - **Full mapping table:**
#
# | ICD-10 Prefix | Category | National Prevalence | Clinical Significance |
# |---|---|---|---|
# | E11 | Diabetes | 37.3M Americans | Drives kidney failure, blindness, amputations |
# | I50 | Heart Failure | 6.7M Americans | #1 cause of hospital readmissions (CMS penalty) |
# | J44 | COPD | 16M Americans | 3rd leading cause of death in the US |
# | I10 | Hypertension | 119M Americans | "Silent killer" — damages arteries over decades |
# | E78 | Hyperlipidemia | 94M Americans | Primary driver of atherosclerosis and MI |
# | N18 | Chronic Kidney Disease | 37M Americans | Often comorbid with diabetes/HTN |
# | F32 | Depression | 21M Americans | Doubles healthcare costs when comorbid |
# | J45 | Asthma | 25M Americans | Leading chronic disease in children |
# | I25 | Coronary Artery Disease | 20M Americans | #1 cause of death globally |
# | E66 | Obesity | 100M+ Americans | Underpins diabetes, HTN, CAD, and more |
# | — | Other | — | All conditions not mapped above |
#
# ### 3. Summary
# 
# The Silver Conditions table parses diagnosis dates and maps raw 
# ICD-10-CM codes to 10 human-readable clinical categories using 
# prefix-based matching (with an exact-match exception for 
# Hypertension). This enables population health segmentation, 
# disease management tracking, and condition-based analytics in 
# the Gold layer without requiring clinical coding expertise.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 8 — CODE: Silver Conditions                              ║
# ╚════════════════════════════════════════════════════════════════╝

conditions = spark.table("bronze_conditions")

# ICD-10-CM Code → Clinical Category Mapping
# ─────────────────────────────────────────────
# WHY we use startswith() instead of exact match:
#   ICD-10 codes have a hierarchical structure. "E11" is the parent code
#   for Type 2 Diabetes, but specific subtypes add decimal digits:
#     E11.0 = Diabetes with hyperosmolarity
#     E11.2 = Diabetes with kidney complications
#     E11.9 = Diabetes without complications
#   Using startswith("E11") captures ALL diabetes subtypes with one rule.
#   The exception is I10 (Hypertension) which has no subtypes — it's an
#   exact match because I10 is the only code for Essential Hypertension.
#
# WHY these 10 categories?
#   These represent the conditions that:
#   1. Drive the most cost (heart failure, diabetes, CKD)
#   2. Are targeted by CMS quality programs
#   3. Are most actionable for population health management
#   All others map to "Other" — in production you'd have 50+ mappings

silver_conditions = conditions \
    .withColumn("date_diagnosed", to_date(col("date_diagnosed"))) \
    .withColumn("condition_category",
        when(col("condition_code").startswith("E11"), "Diabetes")          # Type 2 Diabetes
        .when(col("condition_code").startswith("I50"), "Heart Failure")    # Congestive Heart Failure
        .when(col("condition_code").startswith("J44"), "COPD")            # Chronic Obstructive Pulmonary
        .when(col("condition_code") == "I10", "Hypertension")             # Essential HTN (exact match)
        .when(col("condition_code").startswith("E78"), "Hyperlipidemia")  # High cholesterol
        .when(col("condition_code").startswith("N18"), "Chronic Kidney Disease")  # CKD stages
        .when(col("condition_code").startswith("F32"), "Depression")       # Major depressive disorder
        .when(col("condition_code").startswith("J45"), "Asthma")          # Asthma all types
        .when(col("condition_code").startswith("I25"), "Coronary Artery Disease")  # CAD
        .when(col("condition_code").startswith("E66"), "Obesity")          # Obesity/overweight
        .otherwise("Other"))                                               # Everything else

silver_conditions.write.mode("overwrite").format("delta").saveAsTable("silver_conditions")
print(f"✓ silver_conditions: {silver_conditions.count()} rows")

# Validate: Hypertension and Diabetes should be among the top conditions
# (they affect ~50% and ~15% of US adults respectively)
silver_conditions.groupBy("condition_category").count().orderBy("count", ascending=False).show()


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 9 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Silver Claims, Medications, Vitals & Clinical Notes
#
# ### 1. What these tables represent (business meaning)
# 
# This cell creates **four** Silver tables from their Bronze sources. 
# The most complex is Claims; the others require simpler transforms.
#
# #### Silver Claims — Financial performance metrics
# Claims data is the **financial backbone** of healthcare analytics. 
# Each claim = one bill submitted to an insurance payer. We add two 
# computed fields that drive revenue cycle management:
# - **payment_ratio** = paid_amount / claim_amount
# - **is_denied** = boolean flag for denied claims
#
# #### Silver Medications — Clean date fields
# Medication records with parsed start/end dates. In production, 
# you'd also normalize drug names to RxNorm and check interactions.
#
# #### Silver Vitals — SIRS sepsis early warning flag
# Vital signs with a computed **SIRS flag** — an early indicator of 
# sepsis, which kills 270,000 Americans per year.
#
# #### Silver Clinical Notes — Date-parsed notes
# Free-text physician notes with parsed timestamps. These notes 
# will be processed by Azure OpenAI in Notebook 04.
#
# ### 2. Step-by-step walkthrough of the Claims logic
#
# #### Step 1: Parse dates and cast monetary amounts
#     .withColumn("claim_date", to_date(col("claim_date")))
#     .withColumn("claim_amount", col("claim_amount").cast(DoubleType()))
#     .withColumn("paid_amount", col("paid_amount").cast(DoubleType()))
#     .withColumn("denied_amount", col("denied_amount").cast(DoubleType()))
# - All monetary fields must be `DoubleType()` for arithmetic (SUM, AVG, 
#   division). In production billing, you'd use `DecimalType(10,2)` to 
#   avoid floating-point rounding errors on currency.
# - `days_to_payment` cast to `IntegerType()` for comparisons and averages
#
# #### Step 2: Calculate payment ratio
#     .withColumn("payment_ratio",
#         when(col("claim_amount") > 0,
#              round(col("paid_amount") / col("claim_amount"), 4))
#         .otherwise(0))
# - Payment ratio = cents collected per dollar charged
# - `when(claim_amount > 0, ...)` guards against division by zero
# - `round(..., 4)` gives 4 decimal places (e.g., 0.8325)
# - **Benchmark values:**
#   - ✅ 1.0 = Paid in full (rare — usually only self-pay)
#   - ✅ 0.83 = Average Medicare reimbursement rate
#   - ✅ 0.0 = Denied claim (zero payment received)
#   - ⚠️ Below 0.80 = Red flag for contract review
#
# #### Step 3: Denial flag
#     .withColumn("is_denied",
#         when(col("claim_status") == "Denied", True).otherwise(False))
# - Boolean flag for easy filtering: `df.filter(col("is_denied"))`
# - National average denial rate: ~12%. Medicare Advantage: 17%+.
# - Each denied claim costs $25-50 in administrative re-work
# - **Financial impact:** A hospital with 15% denial rate on $200M 
#   in charges loses ~$30M/year to denials
#
# ### 3. Step-by-step walkthrough of the Vitals SIRS logic
#
# #### SIRS flag computation
#     .withColumn("is_sirs_positive",
#         ((col("temperature_f") > 100.4) | (col("temperature_f") < 96.8)) &
#         (col("heart_rate") > 90) &
#         (col("respiratory_rate") > 20))
# - **SIRS** = Systemic Inflammatory Response Syndrome, an early 
#   indicator of sepsis (a life-threatening organ-dysfunction response 
#   to infection)
# - Criteria (need all 3 in our simplified model):
#   - ✅ Abnormal temperature: > 100.4°F OR < 96.8°F
#   - ✅ Elevated heart rate: > 90 beats per minute
#   - ✅ Elevated respiratory rate: > 20 breaths per minute
# - PySpark boolean operators: `&` (AND), `|` (OR) — each condition 
#   must be wrapped in parentheses for correct operator precedence
# - The 4th real SIRS criterion (WBC count) requires lab data we 
#   don't have in this dataset
#
# ### 4. Summary
# 
# This cell creates four Silver tables: Claims (with payment_ratio 
# and is_denied for revenue cycle analysis), Medications (date 
# parsing), Vitals (numeric casting + SIRS sepsis flag using 
# temperature, heart rate, and respiratory rate thresholds), and 
# Clinical Notes (date parsing for AI processing in Notebook 04).


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 10 — CODE: Silver Claims, Medications, Vitals, Notes     ║
# ╚════════════════════════════════════════════════════════════════╝

# ═══════════════════════════════════════════════════════════════
# SILVER CLAIMS — Financial data with computed payment metrics
# ═══════════════════════════════════════════════════════════════
claims = spark.table("bronze_claims")

silver_claims = claims \
    # Parse date and cast monetary amounts to DoubleType for arithmetic
    # WHY DoubleType for money? In a notebook/analytics context, DoubleType
    # is fine. In production billing systems, you'd use DecimalType(10,2)
    # to avoid floating-point rounding errors on currency.
    .withColumn("claim_date", to_date(col("claim_date"))) \
    .withColumn("claim_amount", col("claim_amount").cast(DoubleType())) \
    .withColumn("paid_amount", col("paid_amount").cast(DoubleType())) \
    .withColumn("denied_amount", col("denied_amount").cast(DoubleType())) \
    .withColumn("patient_responsibility", col("patient_responsibility").cast(DoubleType())) \
    .withColumn("days_to_payment", col("days_to_payment").cast(IntegerType())) \
    \
    # PAYMENT RATIO = paid_amount / claim_amount
    # This is the single most important financial metric:
    #   1.0 = Paid in full (rare — usually only self-pay or fully insured)
    #   0.83 = Average Medicare reimbursement rate
    #   0.0 = Denied claim (zero payment)
    # Guard against division by zero with when(claim_amount > 0)
    .withColumn("payment_ratio", 
        when(col("claim_amount") > 0, round(col("paid_amount") / col("claim_amount"), 4))
        .otherwise(0)) \
    \
    # DENIAL FLAG — Boolean for easy filtering and aggregation
    # Denied claims are the #1 target for revenue cycle improvement
    .withColumn("is_denied", when(col("claim_status") == "Denied", True).otherwise(False))

silver_claims.write.mode("overwrite").format("delta").saveAsTable("silver_claims")
print(f"✓ silver_claims: {silver_claims.count()} rows")

# ═══════════════════════════════════════════════════════════════
# SILVER MEDICATIONS — Date parsing for medication records
# ═══════════════════════════════════════════════════════════════
# Simple date parsing. In production, you'd also:
#   - Normalize drug names to RxNorm codes (standard terminology)
#   - Check for drug-drug interactions
#   - Flag high-risk medications (opioids, anticoagulants)
#   - Calculate medication adherence rates (proportion of days covered)
medications = spark.table("bronze_medications")
silver_medications = medications \
    .withColumn("start_date", to_date(col("start_date"))) \
    .withColumn("end_date", to_date(col("end_date")))

silver_medications.write.mode("overwrite").format("delta").saveAsTable("silver_medications")
print(f"✓ silver_medications: {silver_medications.count()} rows")

# ═══════════════════════════════════════════════════════════════
# SILVER VITALS — With SIRS sepsis early warning flag
# ═══════════════════════════════════════════════════════════════
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
    # Parse timestamp and cast all vital sign values to proper numeric types
    # to_timestamp() is used instead of to_date() because vitals need
    # time-of-day precision (a patient's 2am vitals differ from 2pm)
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("heart_rate", col("heart_rate").cast(IntegerType())) \
    .withColumn("systolic_bp", col("systolic_bp").cast(IntegerType())) \
    .withColumn("diastolic_bp", col("diastolic_bp").cast(IntegerType())) \
    .withColumn("temperature_f", col("temperature_f").cast(DoubleType())) \
    .withColumn("respiratory_rate", col("respiratory_rate").cast(IntegerType())) \
    .withColumn("spo2_percent", col("spo2_percent").cast(IntegerType())) \
    .withColumn("pain_level", col("pain_level").cast(IntegerType())) \
    \
    # SIRS flag: True when abnormal temp AND high HR AND high RR
    # Using PySpark boolean operators: & (AND), | (OR)
    # Each condition must be wrapped in parentheses for correct precedence
    .withColumn("is_sirs_positive",
        (  (col("temperature_f") > 100.4) | (col("temperature_f") < 96.8) ) &
        (col("heart_rate") > 90) &
        (col("respiratory_rate") > 20)
    )

silver_vitals.write.mode("overwrite").format("delta").saveAsTable("silver_vitals")
print(f"✓ silver_vitals: {silver_vitals.count()} rows")

# ═══════════════════════════════════════════════════════════════
# SILVER CLINICAL NOTES — Date parsing for note timestamps
# ═══════════════════════════════════════════════════════════════
# Clinical notes contain unstructured text (free-form physician notes).
# In Notebook 04, we'll use Azure OpenAI to extract structured insights
# from these notes (summarization, entity extraction, ICD-10 coding).
clinical_notes = spark.table("bronze_clinical_notes")
silver_clinical_notes = clinical_notes \
    .withColumn("note_date", to_date(col("note_date")))

silver_clinical_notes.write.mode("overwrite").format("delta").saveAsTable("silver_clinical_notes")
print(f"✓ silver_clinical_notes: {silver_clinical_notes.count()} rows")

print("\n✅ All Silver tables created!")
