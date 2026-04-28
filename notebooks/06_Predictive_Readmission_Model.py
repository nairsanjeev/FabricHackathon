# ================================================================
# NOTEBOOK 06: PREDICTING HOSPITAL READMISSIONS WITH GEN AI
# ================================================================
# 
# ┌─────────────────────────────────────────────────────────────┐
# │  MODULE 9 — PREDICTIVE READMISSION MODEL (Optional)         │
# │  Fabric Capability: Spark ML, Azure OpenAI, Feature Eng.    │
# └─────────────────────────────────────────────────────────────┘
#
# ── INSTRUCTIONS ──────────────────────────────────────────────
#   1. Create a notebook in Fabric named "06 - Predictive Readmission Model"
#   2. Attach your HealthcareLakehouse
#   3. Create one cell per section below (each "CELL" block)
#   4. Run cells sequentially
#
# ⚠️ PREREQUISITES:
#   - All Silver and Gold tables from Notebooks 01-03 populated
#   - Azure OpenAI resource with a deployed model (for feature engineering)
#   - Endpoint URL and API key from your Azure OpenAI deployment
#
# ── APPROACH ──────────────────────────────────────────────────
#   Inspired by "Predictive Analytics for Hospital Readmission
#   Using GenAI Feature Engineering" (Kaggle healthcare hackathon
#   writeup, CC BY 4.0). We adapt the approach to our Fabric
#   Lakehouse data:
#     1. Use Azure OpenAI as a feature engineering assistant
#     2. Build ML features from clinical, financial & temporal data
#     3. Train a gradient-boosted classifier (XGBoost)
#     4. Evaluate with AUC-ROC, feature importance, and clinical review
#
# ================================================================


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 1 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# # 🔮 Predicting Hospital Readmissions with Gen AI Feature Engineering
#
# ## 1. What this notebook does (business meaning)
#
# Hospital readmissions within 30 days cost the US healthcare
# system over **$26 billion annually**. The CMS Hospital
# Readmissions Reduction Program (HRRP) penalizes hospitals
# with excess readmission rates — up to **3% of total Medicare
# DRG payments**.
#
# Rather than manually crafting features for a prediction model,
# we use **Azure OpenAI as a feature engineering assistant**:
# 1. Ask the LLM to analyze our data schema and suggest predictive
#    features from clinical literature
# 2. Implement the suggested features using PySpark
# 3. Train a gradient-boosted classifier (XGBoost) to predict
#    which patients are at high risk of 30-day readmission
# 4. Evaluate with AUC-ROC and feature importance (SHAP-like)
#
# ### Why Gen AI for Feature Engineering?
#
# Traditional feature engineering requires deep domain expertise
# and is the most time-consuming step in any ML pipeline:
#
# | Approach | Time | Features | AUC-ROC (typical) |
# |----------|------|----------|--------------------|
# | Manual (domain expert) | 2-3 weeks | 15-25 | 0.70-0.75 |
# | Gen AI-assisted | 2-4 days | 30-50 | 0.78-0.85 |
# | AutoML (no domain) | Hours | Many but noisy | 0.65-0.72 |
#
# Gen AI combines the **speed** of automation with the **clinical
# knowledge** embedded in the LLM's training data — suggesting
# features that a data scientist without clinical background
# would miss (e.g., polypharmacy interaction scores, comorbidity
# cluster embeddings, admission-to-discharge lab trajectories).
#
# ### What we'll build
#
# ```
# ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
# │ Step 1: GenAI     │    │ Step 2: Feature   │    │ Step 3: Train    │
# │ Feature Discovery │───→│ Implementation    │───→│ & Evaluate       │
# │ (Azure OpenAI)    │    │ (PySpark)         │    │ (XGBoost)        │
# └──────────────────┘    └──────────────────┘    └──────────────────┘
#       Ask LLM to               Build 25+              Train model,
#      suggest features        features from            compute AUC-ROC,
#      from our schema        Silver/Gold tables        rank features
# ```
#
# ## 2. Important: Clinical Context
#
# - This is a **demonstration** using synthetic data. Real clinical
#   prediction models require IRB approval, clinical validation,
#   and regulatory review before deployment.
# - All predictions must be reviewed by qualified clinicians —
#   this is a **decision-support** tool, not an autonomous system.
# - In production, you'd also need: calibration curves, fairness
#   audits (across race/gender/age), and prospective validation.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 2 — CODE: Imports & Configuration                       ║
# ╚════════════════════════════════════════════════════════════════╝

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json

# ── Azure OpenAI Configuration ────────────────────────────────
# ⚠️ REPLACE with your Azure OpenAI resource details
# (Same values used in Notebook 04)
AZURE_OPENAI_ENDPOINT = "https://<your-resource-name>.openai.azure.com/"
AZURE_OPENAI_KEY = "<your-api-key>"
AZURE_OPENAI_DEPLOYMENT = "<your-deployment-name>"
AZURE_OPENAI_API_VERSION = "2024-06-01"

print("✅ Configuration ready")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 3 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Step 1: Gen AI Feature Discovery
#
# ### 1. What this step does (business meaning)
#
# Instead of spending weeks brainstorming features manually, we
# send our data schema to Azure OpenAI and ask it to suggest
# predictive features for readmission. The LLM draws on its
# training data (which includes medical literature, clinical
# informatics textbooks, and ML research papers) to suggest
# features that would take a data scientist weeks to discover.
#
# ### 2. Step-by-step walkthrough
#
# #### Step 1: Build a schema description
#     schema_description = "silver_patients columns: patient_id, age, ..."
# - We describe our actual table schemas so the LLM knows what
#   raw data is available for feature computation
#
# #### Step 2: System prompt as a clinical ML expert
#     system_prompt = """You are a senior clinical data scientist
#     specializing in hospital readmission prediction..."""
# - The persona primes the LLM to think about clinically validated
#   predictive factors from published literature (LACE index,
#   HOSPITAL score, etc.)
# - We request JSON output with: feature name, computation logic,
#   clinical rationale, and expected predictive importance
#
# #### Step 3: Parse and display suggestions
# - The LLM returns structured feature suggestions that we'll
#   implement in Step 2
# - Each suggestion includes the clinical reasoning — critical
#   for model interpretability and clinician trust
#
# ### 3. Summary
#
# Azure OpenAI analyzes our data schema and returns feature
# engineering suggestions grounded in clinical literature. This
# replaces weeks of manual domain research with a single API call.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 4 — CODE: Gen AI Feature Discovery                      ║
# ╚════════════════════════════════════════════════════════════════╝

%pip install openai -q
# ⚠️ Expected: pip warnings about nni dependency — safe to ignore.
# If the kernel restarts, re-run Cell 2 (Configuration) first.

# Re-set config after potential kernel restart
AZURE_OPENAI_ENDPOINT = "https://<your-resource-name>.openai.azure.com/"
AZURE_OPENAI_KEY = "<your-api-key>"
AZURE_OPENAI_DEPLOYMENT = "<your-deployment-name>"
AZURE_OPENAI_API_VERSION = "2024-06-01"


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 5 — CODE: Ask Azure OpenAI for Feature Suggestions      ║
# ╚════════════════════════════════════════════════════════════════╝

import json
from openai import AzureOpenAI

client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_KEY,
    api_version=AZURE_OPENAI_API_VERSION
)

# ── Describe our data schema to the LLM ────────────────────────
# The more detail we provide, the better the feature suggestions.
# We include column names, data types, and sample values so the
# LLM understands what it has to work with.
schema_description = """
We have a healthcare Lakehouse with these tables:

1. silver_patients: patient_id, first_name, last_name, date_of_birth, age (int),
   age_group (18-29/30-44/45-59/60-74/75+), gender (M/F), race, zip_code, city,
   state, insurance_type (Medicare/Medicaid/Commercial/Self-Pay),
   primary_care_provider, risk_score (float 0-5), risk_category (Low/Medium/High)

2. silver_encounters: encounter_id, patient_id, encounter_date (date),
   discharge_date (date), encounter_type (Inpatient/ED/Outpatient/Ambulatory),
   facility_name, department, primary_diagnosis_code (ICD-10),
   primary_diagnosis_description, attending_provider, discharge_disposition
   (Home/SNF/Home Health/Expired/Against Medical Advice),
   length_of_stay_days (int), total_charges (float), encounter_month,
   encounter_year, encounter_quarter, day_of_week (1-7), is_weekend (bool),
   los_category (Same Day/Short/Medium/Long/Extended)

3. silver_conditions: patient_id, condition_code (ICD-10), condition_description,
   condition_type (Chronic/Acute), date_diagnosed (date),
   condition_category (Diabetes/Heart Failure/COPD/Hypertension/CKD/
   Hyperlipidemia/Depression/Asthma/CAD/Obesity/Other)

4. silver_claims: encounter_id, patient_id, claim_date, claim_amount (float),
   paid_amount (float), denied_amount (float), patient_responsibility (float),
   payer, claim_status (Paid/Denied/Pending), days_to_payment (int),
   payment_ratio (float 0-1), is_denied (bool)

5. silver_medications: patient_id, medication_name, dosage, frequency,
   prescribing_provider, start_date, end_date

6. silver_vitals: patient_id, encounter_id, timestamp, heart_rate (int),
   systolic_bp (int), diastolic_bp (int), temperature_f (float),
   respiratory_rate (int), spo2_percent (int), pain_level (int 0-10),
   is_sirs_positive (bool)

7. gold_readmissions: index_encounter_id, patient_id, index_admission_date,
   index_discharge_date, index_diagnosis_code, index_diagnosis,
   index_facility, index_provider, index_los, index_disposition,
   readmit_encounter_id, readmit_date, was_readmitted (bool),
   days_to_readmission (int)
"""

system_prompt = """You are a senior clinical data scientist specializing in
hospital readmission prediction. You have deep knowledge of validated readmission
risk models (LACE index, HOSPITAL score, PARR-30, Yale/CMS model) and published
predictive features from medical literature.

Given the database schema below, suggest 25 predictive features for a 30-day
hospital readmission model. For each feature, provide:

Return a JSON array with this structure:
[
  {
    "feature_name": "descriptive_snake_case_name",
    "category": "one of: Demographics, Comorbidity, Utilization, Clinical, Financial, Temporal",
    "computation": "Brief PySpark-style pseudocode showing how to compute from our tables",
    "clinical_rationale": "Why this feature predicts readmission (cite evidence if possible)",
    "expected_importance": "high/medium/low"
  }
]

Focus on features that:
1. Are computable from the tables described (don't hallucinate columns)
2. Have clinical evidence supporting their predictive value
3. Span multiple feature categories (not all demographics)
4. Include interaction features and temporal patterns

Return valid JSON only, no other text."""

print("🤖 Asking Azure OpenAI for feature engineering suggestions...")
print("   (This may take 15-30 seconds)\n")

response = client.chat.completions.create(
    model=AZURE_OPENAI_DEPLOYMENT,
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Here is our data schema:\n\n{schema_description}\n\nSuggest 25 features for 30-day readmission prediction."}
    ],
    max_tokens=4000,
    temperature=0.3
)

result_text = response.choices[0].message.content.strip()

# Strip markdown code fences if present
if result_text.startswith("```"):
    result_text = result_text.split("\n", 1)[-1].rsplit("```", 1)[0]

ai_features = json.loads(result_text)

# ── Display the AI-suggested features ────────────────────────
print(f"✅ Azure OpenAI suggested {len(ai_features)} features:\n")
print(f"{'#':<3} {'Feature':<40} {'Category':<15} {'Importance':<10}")
print("─" * 70)
for i, feat in enumerate(ai_features, 1):
    print(f"{i:<3} {feat['feature_name']:<40} {feat['category']:<15} {feat['expected_importance']:<10}")

# Show detailed rationale for top 5
print(f"\n{'='*70}")
print("📋 DETAILED CLINICAL RATIONALE (Top 5):")
print(f"{'='*70}")
for feat in ai_features[:5]:
    print(f"\n🔹 {feat['feature_name']}")
    print(f"   Category: {feat['category']}")
    print(f"   How to compute: {feat['computation']}")
    print(f"   Clinical rationale: {feat['clinical_rationale']}")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 6 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Step 2: Implement Features Using PySpark
#
# ### 1. What this step does (business meaning)
#
# Now we take the AI-suggested features and implement them in
# PySpark. We build a **training dataset** where:
# - Each row = one inpatient index admission
# - Target variable = `was_readmitted` (0 or 1)
# - Feature columns = 25+ predictive variables
#
# We organize features into 6 categories inspired by the
# Kaggle writeup:
#
# | Category | Example Features | Source Tables |
# |----------|-----------------|---------------|
# | Demographics | age, gender, insurance, risk_score | silver_patients |
# | Comorbidity | chronic_condition_count, has_diabetes, has_chf | silver_conditions |
# | Utilization | prior_admissions_12m, prior_ed_visits_12m | silver_encounters |
# | Clinical | index_los, sirs_positive_count, max_pain | silver_vitals, encounters |
# | Financial | payment_ratio, is_denied, total_charges | silver_claims |
# | Temporal | is_weekend_discharge, discharge_month | gold_readmissions |
#
# ### 2. Step-by-step walkthrough
#
# #### Step 1: Start with gold_readmissions as the base
# - Each row already has the target variable (`was_readmitted`)
# - Includes index admission details (diagnosis, LOS, facility)
#
# #### Step 2: Join patient demographics
# - Age, gender, insurance type, risk score — the "LACE" model
#   uses Length of stay, Acuity, Comorbidities, and ED visits
#
# #### Step 3: Compute comorbidity features
# - Count chronic conditions per patient (Charlson-like index)
# - Flag key conditions: diabetes, CHF, COPD, CKD, depression
# - Multimorbidity is the #1 predictor of readmission
#
# #### Step 4: Compute prior utilization features
# - Prior admissions and ED visits in last 12 months
# - Prior readmission history is the strongest predictor
# - The "revolving door" pattern is highly predictive
#
# #### Step 5: Add clinical and financial features
# - Vitals-based features (SIRS, abnormal vitals during stay)
# - Claims-based features (denial rate, payment ratio)
#
# #### Step 6: Add temporal features
# - Weekend/holiday discharge, quarter, month
# - The "weekend effect" is a validated risk factor
#
# ### 3. Summary
#
# We implement 25+ features across 6 categories by joining
# Silver/Gold tables with PySpark, creating a wide training
# DataFrame where each row is an index admission with all
# its predictive features and the readmission outcome label.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 7 — CODE: Build Training Dataset with Features           ║
# ╚════════════════════════════════════════════════════════════════╝

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ── Load base tables ────────────────────────────────────────────
readmissions = spark.table("gold_readmissions")
patients = spark.table("silver_patients")
encounters = spark.table("silver_encounters")
conditions = spark.table("silver_conditions")
claims = spark.table("silver_claims")
vitals = spark.table("silver_vitals")
medications = spark.table("silver_medications")

print(f"Base: {readmissions.count()} index admissions")
print(f"  Readmitted: {readmissions.filter(col('was_readmitted') == True).count()}")
print(f"  Not readmitted: {readmissions.filter(col('was_readmitted') == False).count()}")

# ═══════════════════════════════════════════════════════════════
# CATEGORY 1: DEMOGRAPHICS (from silver_patients)
# ═══════════════════════════════════════════════════════════════
# These features align with the "L" (Length of stay) and
# "A" (Acuity) components of the LACE readmission risk index.

# Join patient demographics
base_df = readmissions.join(
    patients.select(
        "patient_id", "age", "gender", "insurance_type",
        "risk_score", "risk_category", "race"
    ),
    "patient_id", "left"
)

# Encode categorical variables as numeric for ML
base_df = base_df \
    .withColumn("is_male", when(col("gender") == "M", 1).otherwise(0)) \
    .withColumn("is_medicare", when(col("insurance_type") == "Medicare", 1).otherwise(0)) \
    .withColumn("is_medicaid", when(col("insurance_type") == "Medicaid", 1).otherwise(0)) \
    .withColumn("is_self_pay", when(col("insurance_type") == "Self-Pay", 1).otherwise(0)) \
    .withColumn("is_high_risk", when(col("risk_category") == "High", 1).otherwise(0))

print("✓ Demographics features added")

# ═══════════════════════════════════════════════════════════════
# CATEGORY 2: COMORBIDITY (from silver_conditions)
# ═══════════════════════════════════════════════════════════════
# Comorbidity burden is the strongest predictor of readmission.
# The Charlson Comorbidity Index (CCI) uses a similar approach:
# count specific chronic conditions and score severity.

# Count chronic conditions per patient
condition_features = conditions \
    .filter(col("condition_type") == "Chronic") \
    .groupBy("patient_id") \
    .agg(
        count("*").alias("chronic_condition_count"),
        collect_set("condition_category").alias("cond_list")
    )

# Create boolean flags for key comorbidities
condition_features = condition_features \
    .withColumn("has_diabetes", array_contains(col("cond_list"), "Diabetes").cast("int")) \
    .withColumn("has_chf", array_contains(col("cond_list"), "Heart Failure").cast("int")) \
    .withColumn("has_copd", array_contains(col("cond_list"), "COPD").cast("int")) \
    .withColumn("has_hypertension", array_contains(col("cond_list"), "Hypertension").cast("int")) \
    .withColumn("has_ckd", array_contains(col("cond_list"), "Chronic Kidney Disease").cast("int")) \
    .withColumn("has_depression", array_contains(col("cond_list"), "Depression").cast("int")) \
    .withColumn("has_cad", array_contains(col("cond_list"), "Coronary Artery Disease").cast("int")) \
    .withColumn("has_obesity", array_contains(col("cond_list"), "Obesity").cast("int")) \
    .withColumn("comorbidity_cluster_count",
        # Count high-risk clusters: cardiometabolic triad, cardiopulmonary
        (array_contains(col("cond_list"), "Diabetes").cast("int") +
         array_contains(col("cond_list"), "Hypertension").cast("int") +
         array_contains(col("cond_list"), "Chronic Kidney Disease").cast("int") +
         array_contains(col("cond_list"), "Heart Failure").cast("int") +
         array_contains(col("cond_list"), "COPD").cast("int"))) \
    .drop("cond_list")

base_df = base_df.join(condition_features, "patient_id", "left")
# Fill nulls with 0 for patients without chronic conditions
for c in ["chronic_condition_count", "has_diabetes", "has_chf", "has_copd",
           "has_hypertension", "has_ckd", "has_depression", "has_cad",
           "has_obesity", "comorbidity_cluster_count"]:
    base_df = base_df.withColumn(c, coalesce(col(c), lit(0)))

print("✓ Comorbidity features added")

# ═══════════════════════════════════════════════════════════════
# CATEGORY 3: PRIOR UTILIZATION (from silver_encounters)
# ═══════════════════════════════════════════════════════════════
# Prior utilization is the strongest single predictor of future
# utilization. "The best predictor of a readmission is a prior
# readmission." (Donzé et al., JAMA Internal Medicine, 2016)

# For each index admission, count prior encounters in the last
# 12 months BEFORE the index admission date.
all_encounters = encounters.select(
    "patient_id", "encounter_id", "encounter_date", "encounter_type",
    "length_of_stay_days", "total_charges"
)

# Self-join: match each index admission to prior encounters
prior_util = readmissions.alias("idx").join(
    all_encounters.alias("prior"),
    (col("idx.patient_id") == col("prior.patient_id")) &
    (col("prior.encounter_date") < col("idx.index_admission_date")) &
    (datediff(col("idx.index_admission_date"), col("prior.encounter_date")) <= 365),
    "left"
)

# Aggregate prior utilization by index encounter
utilization_features = prior_util.groupBy("idx.index_encounter_id").agg(
    count("prior.encounter_id").alias("prior_encounters_12m"),
    sum(when(col("prior.encounter_type") == "Inpatient", 1).otherwise(0)).alias("prior_admissions_12m"),
    sum(when(col("prior.encounter_type") == "ED", 1).otherwise(0)).alias("prior_ed_visits_12m"),
    sum(when(col("prior.encounter_type") == "Outpatient", 1).otherwise(0)).alias("prior_outpatient_12m"),
    coalesce(sum("prior.total_charges"), lit(0)).alias("prior_total_charges_12m"),
    coalesce(avg("prior.length_of_stay_days"), lit(0)).alias("prior_avg_los")
)

base_df = base_df.join(utilization_features, 
    base_df["index_encounter_id"] == utilization_features["index_encounter_id"], "left") \
    .drop(utilization_features["index_encounter_id"])

# Fill nulls (patients with no prior encounters)
for c in ["prior_encounters_12m", "prior_admissions_12m", "prior_ed_visits_12m",
           "prior_outpatient_12m", "prior_total_charges_12m", "prior_avg_los"]:
    base_df = base_df.withColumn(c, coalesce(col(c), lit(0)))

print("✓ Prior utilization features added")

# ═══════════════════════════════════════════════════════════════
# CATEGORY 4: CLINICAL (from silver_vitals)
# ═══════════════════════════════════════════════════════════════
# Vital sign patterns during the index stay indicate clinical
# instability. SIRS-positive readings suggest underlying infection
# or inflammatory response — a strong readmission risk factor.

# Aggregate vitals per encounter (for the index admission)
vitals_features = vitals.groupBy("encounter_id").agg(
    count("*").alias("vitals_reading_count"),
    sum(col("is_sirs_positive").cast("int")).alias("sirs_positive_count"),
    avg("heart_rate").alias("avg_heart_rate"),
    avg("temperature_f").alias("avg_temperature"),
    avg("respiratory_rate").alias("avg_respiratory_rate"),
    avg("spo2_percent").alias("avg_spo2"),
    max("pain_level").alias("max_pain_level"),
    max("heart_rate").alias("max_heart_rate"),
    min("spo2_percent").alias("min_spo2")
)

base_df = base_df.join(vitals_features,
    base_df["index_encounter_id"] == vitals_features["encounter_id"], "left") \
    .drop(vitals_features["encounter_id"])

# Fill nulls
for c in ["vitals_reading_count", "sirs_positive_count", "avg_heart_rate",
           "avg_temperature", "avg_respiratory_rate", "avg_spo2",
           "max_pain_level", "max_heart_rate", "min_spo2"]:
    base_df = base_df.withColumn(c, coalesce(col(c), lit(0)))

print("✓ Clinical vitals features added")

# ═══════════════════════════════════════════════════════════════
# CATEGORY 5: FINANCIAL (from silver_claims)
# ═══════════════════════════════════════════════════════════════
# Financial patterns correlate with care complexity. Denied claims
# may indicate documentation gaps or coding issues that affect
# continuity of care.

claims_features = claims.groupBy("encounter_id").agg(
    sum("claim_amount").alias("claim_total_amount"),
    avg("payment_ratio").alias("avg_payment_ratio"),
    sum(col("is_denied").cast("int")).alias("denied_claims_count"),
    avg("days_to_payment").alias("avg_days_to_payment")
)

base_df = base_df.join(claims_features,
    base_df["index_encounter_id"] == claims_features["encounter_id"], "left") \
    .drop(claims_features["encounter_id"])

for c in ["claim_total_amount", "avg_payment_ratio", "denied_claims_count", "avg_days_to_payment"]:
    base_df = base_df.withColumn(c, coalesce(col(c), lit(0)))

print("✓ Financial features added")

# ═══════════════════════════════════════════════════════════════
# CATEGORY 6: TEMPORAL (from gold_readmissions)
# ═══════════════════════════════════════════════════════════════
# The "weekend effect" — patients discharged on weekends have
# higher readmission rates due to reduced post-discharge support.

base_df = base_df \
    .withColumn("discharge_day_of_week", dayofweek(col("index_discharge_date"))) \
    .withColumn("is_weekend_discharge",
        when(dayofweek(col("index_discharge_date")).isin(1, 7), 1).otherwise(0)) \
    .withColumn("discharge_month", month(col("index_discharge_date"))) \
    .withColumn("discharge_quarter", quarter(col("index_discharge_date"))) \
    .withColumn("is_snf_discharge",
        when(col("index_disposition") == "SNF", 1).otherwise(0)) \
    .withColumn("is_home_health_discharge",
        when(col("index_disposition") == "Home Health", 1).otherwise(0))

print("✓ Temporal features added")

# ═══════════════════════════════════════════════════════════════
# MEDICATION FEATURES (from silver_medications)
# ═══════════════════════════════════════════════════════════════
# Polypharmacy (5+ medications) is a strong readmission risk
# factor — it increases drug interaction risk and non-adherence.

med_features = medications.groupBy("patient_id").agg(
    countDistinct("medication_name").alias("unique_medication_count")
)
med_features = med_features.withColumn(
    "is_polypharmacy",
    when(col("unique_medication_count") >= 5, 1).otherwise(0)
)

base_df = base_df.join(med_features, "patient_id", "left")
base_df = base_df \
    .withColumn("unique_medication_count", coalesce(col("unique_medication_count"), lit(0))) \
    .withColumn("is_polypharmacy", coalesce(col("is_polypharmacy"), lit(0)))

print("✓ Medication features added")

# ═══════════════════════════════════════════════════════════════
# CREATE TARGET VARIABLE & FEATURE MATRIX
# ═══════════════════════════════════════════════════════════════
# Select only numeric feature columns + target variable

feature_columns = [
    # Demographics
    "age", "is_male", "risk_score", "is_medicare", "is_medicaid",
    "is_self_pay", "is_high_risk",
    # Comorbidity
    "chronic_condition_count", "has_diabetes", "has_chf", "has_copd",
    "has_hypertension", "has_ckd", "has_depression", "has_cad",
    "has_obesity", "comorbidity_cluster_count",
    # Prior Utilization
    "prior_encounters_12m", "prior_admissions_12m", "prior_ed_visits_12m",
    "prior_outpatient_12m", "prior_total_charges_12m", "prior_avg_los",
    # Clinical
    "index_los", "vitals_reading_count", "sirs_positive_count",
    "avg_heart_rate", "avg_temperature", "avg_respiratory_rate",
    "avg_spo2", "max_pain_level", "max_heart_rate", "min_spo2",
    # Financial
    "claim_total_amount", "avg_payment_ratio", "denied_claims_count",
    "avg_days_to_payment",
    # Temporal
    "is_weekend_discharge", "discharge_month", "discharge_quarter",
    "is_snf_discharge", "is_home_health_discharge",
    # Medications
    "unique_medication_count", "is_polypharmacy"
]

# Cast target to integer (0/1)
training_df = base_df.withColumn("label", col("was_readmitted").cast("int")) \
    .select(feature_columns + ["label", "index_encounter_id", "patient_id"])

# Cast all feature columns to double for ML compatibility
for c in feature_columns:
    training_df = training_df.withColumn(c, col(c).cast("double"))

# Fill any remaining nulls with 0
training_df = training_df.na.fill(0.0)

print(f"\n📊 Training dataset: {training_df.count()} rows × {len(feature_columns)} features")
readmit_count = training_df.filter(col("label") == 1).count()
no_readmit = training_df.filter(col("label") == 0).count()
print(f"   Label distribution: {readmit_count} readmitted ({readmit_count/(readmit_count+no_readmit)*100:.1f}%), "
      f"{no_readmit} not readmitted ({no_readmit/(readmit_count+no_readmit)*100:.1f}%)")

# Save the feature-engineered dataset to the Gold layer
training_df.write.mode("overwrite").format("delta").saveAsTable("gold_readmission_training")
print("✓ Saved to gold_readmission_training")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 8 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Step 3: Train a Predictive Model (XGBoost)
#
# ### 1. What this step does (business meaning)
#
# We train a **gradient-boosted decision tree** (XGBoost) to
# predict which patients are at high risk of 30-day readmission.
# XGBoost is the most widely used ML algorithm in healthcare
# predictive analytics because:
#
# - **Handles mixed data**: Numeric, categorical, sparse features
# - **Robust to missing values**: Auto-learns optimal split for NA
# - **Feature interactions**: Automatically discovers non-linear
#   relationships (e.g., "age > 75 AND has_chf AND polypharmacy")
# - **Interpretable**: Feature importance scores show which factors
#   drive predictions — critical for clinician trust
# - **State of the art**: In the Kaggle healthcare hackathon,
#   XGBoost + LightGBM ensembles achieved AUC-ROC of 0.81
#
# ### 2. Step-by-step walkthrough
#
# #### Step 1: Convert Spark DataFrame to Pandas
#     pandas_df = training_df.select(feature_columns + ["label"]).toPandas()
# - XGBoost runs on the driver node, not distributed on Spark
# - For our dataset (~200-300 rows), this is fast and appropriate
# - For production (100K+ rows), use SparkML or SynapseML
#
# #### Step 2: Train/test split (80/20)
#     X_train, X_test, y_train, y_test = train_test_split(...)
# - 80% for training, 20% for evaluation
# - `stratify=y` ensures both sets have the same readmission rate
# - `random_state=42` for reproducibility
#
# #### Step 3: Handle class imbalance
#     scale_pos_weight = len(y_train[y_train==0]) / len(y_train[y_train==1])
# - Readmissions are ~15% of cases (imbalanced dataset)
# - `scale_pos_weight` upweights the minority class (readmitted)
#   so the model doesn't just predict "not readmitted" for everyone
#
# #### Step 4: Train XGBoost
#     model = XGBClassifier(
#         n_estimators=200, max_depth=4, learning_rate=0.1,
#         scale_pos_weight=..., eval_metric='auc', ...)
# - `n_estimators=200`: 200 boosting rounds (trees)
# - `max_depth=4`: Shallow trees to prevent overfitting
# - `learning_rate=0.1`: Step size shrinkage for regularization
# - `eval_metric='auc'`: Optimize for AUC-ROC (the standard
#   healthcare model evaluation metric)
#
# #### Step 5: Evaluate
# - **AUC-ROC**: Area under the ROC curve. Measures overall
#   discriminative ability. Target: >0.75 (good), >0.80 (excellent)
# - **Feature importance**: Which features drive predictions?
#   Clinical teams need this for trust and actionability.
#
# ### 3. Summary
#
# XGBoost is trained on the feature-engineered dataset with class
# imbalance handling. The model is evaluated by AUC-ROC and
# feature importance to identify the top readmission risk factors.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 9 — CODE: Install ML Libraries & Train XGBoost          ║
# ╚════════════════════════════════════════════════════════════════╝

%pip install xgboost scikit-learn matplotlib -q
# ⚠️ Expected: pip warnings — safe to ignore


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 10 — CODE: Train & Evaluate the Model                   ║
# ╚════════════════════════════════════════════════════════════════╝

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score, classification_report, confusion_matrix,
    roc_curve, precision_recall_curve, average_precision_score
)
from xgboost import XGBClassifier
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for Fabric notebooks
import matplotlib.pyplot as plt

# ── Load feature-engineered data ────────────────────────────
feature_columns = [
    "age", "is_male", "risk_score", "is_medicare", "is_medicaid",
    "is_self_pay", "is_high_risk",
    "chronic_condition_count", "has_diabetes", "has_chf", "has_copd",
    "has_hypertension", "has_ckd", "has_depression", "has_cad",
    "has_obesity", "comorbidity_cluster_count",
    "prior_encounters_12m", "prior_admissions_12m", "prior_ed_visits_12m",
    "prior_outpatient_12m", "prior_total_charges_12m", "prior_avg_los",
    "index_los", "vitals_reading_count", "sirs_positive_count",
    "avg_heart_rate", "avg_temperature", "avg_respiratory_rate",
    "avg_spo2", "max_pain_level", "max_heart_rate", "min_spo2",
    "claim_total_amount", "avg_payment_ratio", "denied_claims_count",
    "avg_days_to_payment",
    "is_weekend_discharge", "discharge_month", "discharge_quarter",
    "is_snf_discharge", "is_home_health_discharge",
    "unique_medication_count", "is_polypharmacy"
]

# Load from Gold table
training_spark_df = spark.table("gold_readmission_training")
pandas_df = training_spark_df.select(feature_columns + ["label"]).toPandas()

X = pandas_df[feature_columns]
y = pandas_df["label"]

print(f"Dataset: {len(X)} samples, {len(feature_columns)} features")
print(f"Label distribution: {y.sum()} readmitted ({y.mean()*100:.1f}%), "
      f"{len(y)-y.sum()} not readmitted ({(1-y.mean())*100:.1f}%)")

# ── Train/Test Split ────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"\nTrain: {len(X_train)} samples | Test: {len(X_test)} samples")

# ── Handle class imbalance ──────────────────────────────────
# Readmissions are the minority class (~15%). Without this,
# the model would just predict "not readmitted" for everyone
# and still get ~85% accuracy (but 0% recall on readmissions).
neg_count = len(y_train[y_train == 0])
pos_count = len(y_train[y_train == 1])
scale_pos = neg_count / pos_count if pos_count > 0 else 1.0
print(f"Class imbalance ratio: {scale_pos:.2f} (using scale_pos_weight)")

# ── Train XGBoost ────────────────────────────────────────────
model = XGBClassifier(
    n_estimators=200,         # 200 boosting rounds
    max_depth=4,              # Shallow trees (prevent overfitting)
    learning_rate=0.1,        # Step size shrinkage
    scale_pos_weight=scale_pos,  # Handle class imbalance
    eval_metric='auc',        # Optimize for AUC-ROC
    random_state=42,
    use_label_encoder=False,
    verbosity=0
)

model.fit(X_train, y_train)
print("\n✅ Model trained!")

# ── Evaluate ─────────────────────────────────────────────────
y_prob = model.predict_proba(X_test)[:, 1]  # Probability of readmission
y_pred = model.predict(X_test)

auc_roc = roc_auc_score(y_test, y_prob)
avg_precision = average_precision_score(y_test, y_prob)

print(f"\n{'='*60}")
print(f"📊 MODEL EVALUATION RESULTS")
print(f"{'='*60}")
print(f"  AUC-ROC:            {auc_roc:.4f}")
print(f"  Average Precision:  {avg_precision:.4f}")
print(f"\n  Interpretation:")
if auc_roc >= 0.80:
    print(f"  ✅ Excellent discriminative ability (≥0.80)")
elif auc_roc >= 0.75:
    print(f"  ✅ Good discriminative ability (0.75-0.80)")
elif auc_roc >= 0.70:
    print(f"  ⚠️ Acceptable but could improve (0.70-0.75)")
else:
    print(f"  ⚠️ Needs improvement (<0.70) — try more features or data")

print(f"\n{'='*60}")
print(f"📋 CLASSIFICATION REPORT")
print(f"{'='*60}")
print(classification_report(y_test, y_pred, target_names=["Not Readmitted", "Readmitted"]))

# ── Confusion Matrix ─────────────────────────────────────────
cm = confusion_matrix(y_test, y_pred)
print(f"Confusion Matrix:")
print(f"  True Negatives:  {cm[0][0]:4d}  |  False Positives: {cm[0][1]:4d}")
print(f"  False Negatives: {cm[1][0]:4d}  |  True Positives:  {cm[1][1]:4d}")

# ── Feature Importance ──────────────────────────────────────
importance = model.feature_importances_
feat_importance = sorted(zip(feature_columns, importance), key=lambda x: x[1], reverse=True)

print(f"\n{'='*60}")
print(f"🔑 TOP 15 FEATURES BY IMPORTANCE (XGBoost gain)")
print(f"{'='*60}")
for i, (feat, imp) in enumerate(feat_importance[:15], 1):
    bar = "█" * int(imp / feat_importance[0][1] * 30)
    print(f"  {i:2d}. {feat:<35s} {imp:.4f}  {bar}")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 11 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Step 4: Visualize Model Performance
#
# ### 1. What this step does (business meaning)
#
# Visualization is critical for clinical AI adoption. Clinicians
# and administrators need to SEE how the model performs, not just
# read metrics. We generate three key visualizations:
#
# 1. **ROC Curve** — The gold standard for binary classifier
#    evaluation. Shows the tradeoff between sensitivity (catching
#    all readmissions) and specificity (not false-alarming on
#    patients who won't be readmitted).
#
# 2. **Precision-Recall Curve** — Better than ROC for imbalanced
#    datasets (which readmission prediction always is). Shows the
#    tradeoff between positive predictive value (precision) and
#    sensitivity (recall).
#
# 3. **Feature Importance Bar Chart** — Which features drive
#    predictions? This is the most important chart for clinical
#    stakeholders — it answers "WHY does the model think this
#    patient is high-risk?"
#
# ### 2. Summary
#
# Three visualizations (ROC, PR curve, feature importance) are
# generated for model interpretation and clinical stakeholder
# communication.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 12 — CODE: Visualize Results                             ║
# ╚════════════════════════════════════════════════════════════════╝

fig, axes = plt.subplots(1, 3, figsize=(20, 6))

# ── Plot 1: ROC Curve ─────────────────────────────────────────
fpr, tpr, _ = roc_curve(y_test, y_prob)
axes[0].plot(fpr, tpr, 'b-', linewidth=2, label=f'XGBoost (AUC = {auc_roc:.3f})')
axes[0].plot([0, 1], [0, 1], 'k--', linewidth=1, label='Random (AUC = 0.500)')
axes[0].set_xlabel('False Positive Rate (1 - Specificity)')
axes[0].set_ylabel('True Positive Rate (Sensitivity)')
axes[0].set_title('ROC Curve — Readmission Prediction')
axes[0].legend(loc='lower right')
axes[0].grid(True, alpha=0.3)

# ── Plot 2: Precision-Recall Curve ────────────────────────────
precision, recall, _ = precision_recall_curve(y_test, y_prob)
axes[1].plot(recall, precision, 'r-', linewidth=2,
             label=f'XGBoost (AP = {avg_precision:.3f})')
axes[1].set_xlabel('Recall (Sensitivity)')
axes[1].set_ylabel('Precision (PPV)')
axes[1].set_title('Precision-Recall Curve')
axes[1].legend(loc='upper right')
axes[1].grid(True, alpha=0.3)

# ── Plot 3: Top 15 Feature Importance ─────────────────────────
top15 = feat_importance[:15]
top15_names = [f[0].replace("_", " ").title() for f in reversed(top15)]
top15_values = [f[1] for f in reversed(top15)]
colors = ['#e74c3c' if v > 0.05 else '#3498db' for v in top15_values]
axes[2].barh(range(len(top15_names)), top15_values, color=colors)
axes[2].set_yticks(range(len(top15_names)))
axes[2].set_yticklabels(top15_names, fontsize=9)
axes[2].set_xlabel('Feature Importance (Gain)')
axes[2].set_title('Top 15 Predictive Features')
axes[2].grid(True, alpha=0.3, axis='x')

plt.tight_layout()
plt.savefig('/tmp/readmission_model_results.png', dpi=150, bbox_inches='tight')
plt.show()
print("📈 Plots generated — see above for ROC curve, PR curve, and feature importance")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 13 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Step 5: Generate Risk Scores for All Patients
#
# ### 1. What this step does (business meaning)
#
# The trained model scores every inpatient admission with a
# **readmission risk probability** (0.0 to 1.0). This enables:
#
# - **Care management triage**: Focus resources on the highest-risk
#   patients (e.g., top 20% by risk score)
# - **Automated alerts**: Trigger a "high readmission risk" alert
#   before discharge for patients scoring > 0.5
# - **Population health**: Identify systemic patterns (e.g., "CHF
#   patients discharged on weekends from Facility A have the
#   highest readmission risk")
# - **Power BI dashboards**: Visualize risk distribution by
#   facility, diagnosis, insurance type, etc.
#
# ### 2. Step-by-step walkthrough
#
# #### Score all patients
#     all_probs = model.predict_proba(X_all)[:, 1]
# - `predict_proba()` returns probability estimates (not binary predictions)
# - Column `[:, 1]` = probability of class 1 (readmission)
# - Range: 0.0 (very low risk) to 1.0 (very high risk)
#
# #### Classify into risk tiers
#     Low (< 0.2): Routine discharge, standard follow-up
#     Medium (0.2-0.5): Enhanced follow-up within 48 hours
#     High (≥ 0.5): Active intervention before discharge
#
# ### 3. Summary
#
# Every index admission is scored with a readmission probability.
# Patients are classified into risk tiers (Low/Medium/High) and
# saved to a Gold layer table for Power BI consumption.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 14 — CODE: Score All Patients & Save to Gold             ║
# ╚════════════════════════════════════════════════════════════════╝

# Score ALL patients (not just the test set)
X_all = pandas_df[feature_columns]
all_probs = model.predict_proba(X_all)[:, 1]
all_preds = model.predict(X_all)

# Load full training data from Spark to get encounter/patient IDs
full_df = training_spark_df.select(
    "index_encounter_id", "patient_id", "label", *feature_columns
).toPandas()

# Add predictions
full_df["readmission_risk_score"] = all_probs
full_df["predicted_readmission"] = all_preds
full_df["risk_tier"] = pd.cut(
    all_probs,
    bins=[0, 0.2, 0.5, 1.0],
    labels=["Low", "Medium", "High"],
    include_lowest=True
)

# Convert back to Spark and save
scored_df = spark.createDataFrame(full_df[["index_encounter_id", "patient_id",
    "readmission_risk_score", "predicted_readmission", "risk_tier", "label"]])

scored_df = scored_df \
    .withColumn("readmission_risk_score", col("readmission_risk_score").cast("double")) \
    .withColumn("predicted_readmission", col("predicted_readmission").cast("int")) \
    .withColumn("actual_readmission", col("label").cast("int")) \
    .drop("label")

scored_df.write.mode("overwrite").format("delta").saveAsTable("gold_readmission_risk_scores")

# ── Display risk distribution ────────────────────────────────
print(f"✅ Saved {scored_df.count()} risk scores to gold_readmission_risk_scores\n")

print("Risk Tier Distribution:")
scored_df.groupBy("risk_tier").agg(
    count("*").alias("patients"),
    round(avg("readmission_risk_score"), 3).alias("avg_risk_score"),
    sum("actual_readmission").alias("actual_readmissions")
).orderBy("risk_tier").show()

# Show high-risk patients for care management review
print("🚨 HIGH RISK PATIENTS (top 10 by readmission risk score):")
scored_df.filter(col("risk_tier") == "High") \
    .orderBy(col("readmission_risk_score").desc()) \
    .show(10)


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 15 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## Step 6: Ask Gen AI to Interpret the Results
#
# ### 1. What this step does (business meaning)
#
# We send the model results (AUC-ROC, top features, risk
# distribution) back to Azure OpenAI and ask it to generate:
# 1. A **clinical interpretation** of the top features
# 2. **Actionable recommendations** for care management
# 3. A comparison to **published readmission risk models**
#    (LACE, HOSPITAL score)
#
# This closes the loop: Gen AI suggested the features → we built
# and trained the model → Gen AI interprets the results for
# clinical stakeholders who don't speak "AUC-ROC".
#
# ### 2. Summary
#
# Azure OpenAI translates model metrics and feature importance
# into clinician-readable insights and care management
# recommendations.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 16 — CODE: Ask Gen AI to Interpret Results               ║
# ╚════════════════════════════════════════════════════════════════╝

# Build a summary of model results for the LLM
top_features_text = "\n".join([
    f"  {i+1}. {feat} (importance: {imp:.4f})"
    for i, (feat, imp) in enumerate(feat_importance[:15])
])

risk_dist = scored_df.groupBy("risk_tier").agg(
    count("*").alias("patients"),
    round(avg("readmission_risk_score"), 3).alias("avg_risk"),
    sum("actual_readmission").alias("actual_readmits")
).orderBy("risk_tier").toPandas()

risk_dist_text = risk_dist.to_string(index=False)

model_summary = f"""
We trained an XGBoost model to predict 30-day hospital readmissions.

Model Performance:
- AUC-ROC: {auc_roc:.4f}
- Average Precision: {avg_precision:.4f}
- Dataset: {len(pandas_df)} inpatient index admissions
- Readmission rate: {y.mean()*100:.1f}%
- Features used: {len(feature_columns)} across 6 categories
  (Demographics, Comorbidity, Utilization, Clinical, Financial, Temporal)

Top 15 Features by Importance:
{top_features_text}

Risk Tier Distribution:
{risk_dist_text}
"""

interpretation_prompt = """You are a clinical informatics expert presenting
ML model results to a hospital's Chief Medical Officer and VP of Quality.

Given the readmission prediction model results below, provide:

1. **Clinical Interpretation** (3-4 paragraphs): Explain what the model found
   in plain language. What drives readmissions at this hospital? How do the
   top features align with published literature (LACE index, HOSPITAL score)?

2. **Actionable Recommendations** (5-7 bullet points): Based on the top
   predictive features, what specific interventions should the hospital
   implement? Be specific — name programs, staffing changes, or workflows.

3. **Model Limitations** (3-4 bullet points): What are the caveats? What
   should we tell clinicians about trusting these predictions?

4. **Comparison to Published Models**: How does this AUC-ROC compare to
   LACE (0.68-0.72), HOSPITAL score (0.72), and other published models?

Write for a clinical audience, not data scientists."""

print("🤖 Asking Azure OpenAI to interpret model results...\n")

interpretation_response = client.chat.completions.create(
    model=AZURE_OPENAI_DEPLOYMENT,
    messages=[
        {"role": "system", "content": interpretation_prompt},
        {"role": "user", "content": model_summary}
    ],
    max_tokens=2000,
    temperature=0.4
)

interpretation = interpretation_response.choices[0].message.content.strip()

print("=" * 70)
print("📋 AI-GENERATED CLINICAL INTERPRETATION")
print("=" * 70)
print(interpretation)


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 17 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## ✅ What You Built
#
# In this notebook, you used **Gen AI-assisted feature engineering**
# to build a hospital readmission prediction model:
#
# | Step | What You Did | Gen AI Role |
# |------|-------------|-------------|
# | 1 | Feature Discovery | Azure OpenAI analyzed our schema and suggested 25+ features from clinical literature |
# | 2 | Feature Implementation | PySpark built features across 6 categories from Silver/Gold tables |
# | 3 | Model Training | XGBoost gradient-boosted classifier with class imbalance handling |
# | 4 | Visualization | ROC curve, PR curve, feature importance chart |
# | 5 | Risk Scoring | Every patient scored 0.0-1.0 and classified into Low/Medium/High risk tiers |
# | 6 | Interpretation | Azure OpenAI translated model results into clinical recommendations |
#
# ### Output Tables Created
# - **gold_readmission_training** — Feature-engineered training dataset (25+ features)
# - **gold_readmission_risk_scores** — Per-patient risk scores and risk tiers
#
# ### How This Connects to the Rest of the Lab
# - The risk scores can be added to **Power BI dashboards** (Module 3)
#   as a "Readmission Risk" page
# - The **Data Agent** (Module 7) can answer questions like "Which
#   high-risk patients are being discharged this week?"
# - **Real-time vitals** (Module 4) could feed the model for
#   dynamic re-scoring during the hospital stay
#
# ### Next Steps for Production
# - Prospective validation on new patient cohorts
# - Fairness audit across demographics (race, gender, age, insurance)
# - Calibration analysis (are predicted probabilities reliable?)
# - Integration with EHR discharge workflow (BPA — Best Practice Alert)
# - Regular model retraining as patient mix evolves
