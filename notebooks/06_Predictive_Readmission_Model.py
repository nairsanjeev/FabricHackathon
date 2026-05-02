# ================================================================
# NOTEBOOK 06: PREDICTING HOSPITAL READMISSIONS WITH AUTOML
# ================================================================
# 
# ┌─────────────────────────────────────────────────────────────┐
# │  MODULE 9 — PREDICTIVE READMISSION MODEL (Optional)         │
# │  Fabric Capability: AutoML (FLAML), MLflow, Azure OpenAI    │
# └─────────────────────────────────────────────────────────────┘
#
# ── INSTRUCTIONS ──────────────────────────────────────────────
#   1. Create a notebook in Fabric named "06 - Predictive Readmission Model"
#   2. Attach your HealthcareLakehouse
#   3. Create one cell per section below (each "CELL" block)
#   4. Run cells sequentially
#
# ⚠️ SESSION NOTE:
#   If your Spark session expires or is stopped, you will need
#   to re-run all cells from the top. Fabric does not preserve
#   variables, imports, or DataFrames across session restarts.
#
# ⚠️ PREREQUISITES:
#   - All Silver and Gold tables from Notebooks 01-03 populated
#   - Fabric capacity with built-in AI endpoint access
#
# ── APPROACH ──────────────────────────────────────────────────
#   We combine Gen AI feature engineering with AutoML model selection:
#     1. Use Fabric's built-in AI endpoint as a feature engineering assistant
#     2. Build ML features from clinical, financial & temporal data
#     3. Run FLAML AutoML to automatically find the best algorithm
#     4. Track experiments with MLflow for reproducibility
#     5. Score patients and generate clinical insights with Gen AI
#
# ================================================================


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 1 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# # 🔮 Predicting Hospital Readmissions with Gen AI + AutoML
#
# ## 1. What this notebook does (business meaning)
#
# Hospital readmissions within 30 days cost the US healthcare
# system over **$26 billion annually**. The CMS Hospital
# Readmissions Reduction Program (HRRP) penalizes hospitals
# with excess readmission rates — up to **3% of total Medicare
# DRG payments**.
#
# Our approach combines two accelerators:
# 1. **Gen AI** as a feature engineering assistant — analyzes our
#    data schema and suggests features from clinical literature
# 2. **AutoML (FLAML)** for automatic model selection — tries
#    multiple algorithms (LightGBM, XGBoost, CatBoost, Random
#    Forest, Extra Trees) and finds the best one automatically
#
# ### Why This Approach?
#
# | Approach | Time | Features | AUC-ROC (typical) |
# |----------|------|----------|--------------------|
# | Manual (domain expert) | 2-3 weeks | 15-25 | 0.70-0.75 |
# | Gen AI features + single model | 2-4 days | 30-50 | 0.78-0.82 |
# | **Gen AI features + AutoML** | **Hours** | **30-50** | **0.80-0.86** |
#
# Gen AI combines the **speed** of automation with the **clinical
# knowledge** embedded in the LLM's training data. AutoML then
# removes the guesswork of algorithm selection and hyperparameter
# tuning — trying dozens of configurations to find the best model.
#
# ### What we'll build
#
# ```
# ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
# │ Step 1: GenAI     │    │ Step 2: Feature   │    │ Step 3: AutoML   │    │ Step 4: Score    │
# │ Feature Discovery │───→│ Implementation    │───→│ Model Selection  │───→│ & Interpret      │
# │ (Fabric AI)       │    │ (PySpark)         │    │ (FLAML + MLflow) │    │ (Risk Tiers)     │
# └──────────────────┘    └──────────────────┘    └──────────────────┘    └──────────────────┘
#       Ask LLM to               Build 42              Try 5 algorithms,      Score patients,
#      suggest features        features from            track with MLflow,    generate clinical
#      from our schema        Silver/Gold tables        pick the best         recommendations
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
# ║  CELL 2 — CODE: Install Libraries                             ║
# ╚════════════════════════════════════════════════════════════════╝

%pip install -U openai flaml[automl] scikit-learn matplotlib -q
# ⚠️ Expected: pip warnings about dependencies — safe to ignore.
# The kernel will restart after install. Wait for restart to complete.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 3 — CODE: Initialize Fabric AI Client                   ║
# ╚════════════════════════════════════════════════════════════════╝

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from synapse.ml.fabric.credentials import get_openai_httpx_sync_client
import openai
import json

# ── Fabric AI Endpoint ────────────────────────────────────────
# No API keys or endpoints needed — Fabric handles authentication
client = openai.AzureOpenAI(
    http_client=get_openai_httpx_sync_client(),
    api_version="2025-04-01-preview",
)

MODEL_NAME = "gpt-4.1"

# Quick test
response = client.chat.completions.create(
    model=MODEL_NAME,
    messages=[{"role": "user", "content": "Say 'Connection successful' if you can read this."}],
    max_tokens=10
)

print(f"✅ {response.choices[0].message.content}")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 4 — CODE: Gen AI Feature Discovery                      ║
# ╚════════════════════════════════════════════════════════════════╝

import json

# ── Describe our data schema to the LLM ────────────────────────
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

print("🤖 Asking Fabric AI endpoint for feature engineering suggestions...")
print("   (This may take 15-30 seconds)\n")

response = client.chat.completions.create(
    model=MODEL_NAME,
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
print(f"✅ AI endpoint suggested {len(ai_features)} features:\n")
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
# ║  CELL 5 — CODE: Build Training Dataset with Features           ║
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

base_df = readmissions.join(
    patients.select(
        "patient_id", "age", "gender", "insurance_type",
        "risk_score", "risk_category", "race"
    ),
    "patient_id", "left"
)

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

condition_features = conditions \
    .filter(col("condition_type") == "Chronic") \
    .groupBy("patient_id") \
    .agg(
        count("*").alias("chronic_condition_count"),
        collect_set("condition_category").alias("cond_list")
    )

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
        (array_contains(col("cond_list"), "Diabetes").cast("int") +
         array_contains(col("cond_list"), "Hypertension").cast("int") +
         array_contains(col("cond_list"), "Chronic Kidney Disease").cast("int") +
         array_contains(col("cond_list"), "Heart Failure").cast("int") +
         array_contains(col("cond_list"), "COPD").cast("int"))) \
    .drop("cond_list")

base_df = base_df.join(condition_features, "patient_id", "left")
for c in ["chronic_condition_count", "has_diabetes", "has_chf", "has_copd",
           "has_hypertension", "has_ckd", "has_depression", "has_cad",
           "has_obesity", "comorbidity_cluster_count"]:
    base_df = base_df.withColumn(c, coalesce(col(c), lit(0)))

print("✓ Comorbidity features added")

# ═══════════════════════════════════════════════════════════════
# CATEGORY 3: PRIOR UTILIZATION (from silver_encounters)
# ═══════════════════════════════════════════════════════════════

all_encounters = encounters.select(
    "patient_id", "encounter_id", "encounter_date", "encounter_type",
    "length_of_stay_days", "total_charges"
)

prior_util = readmissions.alias("idx").join(
    all_encounters.alias("prior"),
    (col("idx.patient_id") == col("prior.patient_id")) &
    (col("prior.encounter_date") < col("idx.index_admission_date")) &
    (datediff(col("idx.index_admission_date"), col("prior.encounter_date")) <= 365),
    "left"
)

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

for c in ["prior_encounters_12m", "prior_admissions_12m", "prior_ed_visits_12m",
           "prior_outpatient_12m", "prior_total_charges_12m", "prior_avg_los"]:
    base_df = base_df.withColumn(c, coalesce(col(c), lit(0)))

print("✓ Prior utilization features added")

# ═══════════════════════════════════════════════════════════════
# CATEGORY 4: CLINICAL (from silver_vitals)
# ═══════════════════════════════════════════════════════════════

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

for c in ["vitals_reading_count", "sirs_positive_count", "avg_heart_rate",
           "avg_temperature", "avg_respiratory_rate", "avg_spo2",
           "max_pain_level", "max_heart_rate", "min_spo2"]:
    base_df = base_df.withColumn(c, coalesce(col(c), lit(0)))

print("✓ Clinical vitals features added")

# ═══════════════════════════════════════════════════════════════
# CATEGORY 5: FINANCIAL (from silver_claims)
# ═══════════════════════════════════════════════════════════════

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
# ║  CELL 6 — CODE: AutoML Training with MLflow                   ║
# ╚════════════════════════════════════════════════════════════════╝

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score, classification_report, confusion_matrix,
    roc_curve, precision_recall_curve, average_precision_score
)
from flaml import AutoML
import mlflow
import matplotlib.pyplot as plt
%matplotlib inline

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

# ── Run AutoML with MLflow Tracking ─────────────────────────
automl = AutoML()

automl_settings = {
    "time_budget": 120,          # 2 minutes
    "metric": "roc_auc",         # Optimize for AUC-ROC
    "task": "classification",    # Binary classification
    "estimator_list": [          # Algorithms to search
        "lgbm",                  # LightGBM
        "xgboost",               # XGBoost
        "catboost",              # CatBoost
        "rf",                    # Random Forest
        "extra_tree",            # Extra Trees
    ],
    "log_file_name": "automl_readmission.log",
    "seed": 42,
    "verbose": 1,
}

print(f"\n{'='*60}")
print(f"🔬 STARTING AUTOML (FLAML)")
print(f"{'='*60}")
print(f"  Time budget: {automl_settings['time_budget']} seconds")
print(f"  Metric: {automl_settings['metric']}")
print(f"  Algorithms: {', '.join(automl_settings['estimator_list'])}")
print(f"{'='*60}\n")

# Enable MLflow autologging
mlflow.autolog(exclusive=False)

with mlflow.start_run(run_name="AutoML_Readmission_Prediction"):
    automl.fit(X_train=X_train, y_train=y_train, **automl_settings)

# ── Display AutoML Results ────────────────────────────────────
print(f"\n{'='*60}")
print(f"🏆 AUTOML RESULTS")
print(f"{'='*60}")
print(f"  Best algorithm:      {automl.best_estimator}")
print(f"  Best AUC-ROC (CV):   {1 - automl.best_loss:.4f}")
print(f"  Training time:       {automl.best_config_train_time:.1f} seconds")
print(f"  Total trials:        {len(automl.config_history)}")
print(f"\n  Best hyperparameters:")
for param, value in automl.best_config.items():
    print(f"    {param}: {value}")

# ── Evaluate on Hold-out Test Set ─────────────────────────────
y_prob = automl.predict_proba(X_test)[:, 1]
y_pred = automl.predict(X_test)

auc_roc = roc_auc_score(y_test, y_prob)
avg_precision = average_precision_score(y_test, y_prob)

print(f"\n{'='*60}")
print(f"📊 TEST SET EVALUATION (hold-out)")
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
best_model = automl.model.estimator
if hasattr(best_model, 'feature_importances_'):
    importance = best_model.feature_importances_
    feat_importance = sorted(zip(feature_columns, importance), key=lambda x: x[1], reverse=True)

    print(f"\n{'='*60}")
    print(f"🔑 TOP 15 FEATURES BY IMPORTANCE ({automl.best_estimator})")
    print(f"{'='*60}")
    for i, (feat, imp) in enumerate(feat_importance[:15], 1):
        bar = "█" * int(imp / feat_importance[0][1] * 30)
        print(f"  {i:2d}. {feat:<35s} {imp:.4f}  {bar}")
else:
    print("\n  (Feature importance not available for this model type)")
    feat_importance = list(zip(feature_columns, [0.0] * len(feature_columns)))


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 7 — CODE: Visualize Results                              ║
# ╚════════════════════════════════════════════════════════════════╝

fig, axes = plt.subplots(1, 3, figsize=(20, 6))

# ── Plot 1: ROC Curve ─────────────────────────────────────────
fpr, tpr, _ = roc_curve(y_test, y_prob)
axes[0].plot(fpr, tpr, 'b-', linewidth=2, label=f'{automl.best_estimator} (AUC = {auc_roc:.3f})')
axes[0].plot([0, 1], [0, 1], 'k--', linewidth=1, label='Random (AUC = 0.500)')
axes[0].set_xlabel('False Positive Rate (1 - Specificity)')
axes[0].set_ylabel('True Positive Rate (Sensitivity)')
axes[0].set_title('ROC Curve — Readmission Prediction (AutoML Best)')
axes[0].legend(loc='lower right')
axes[0].grid(True, alpha=0.3)

# ── Plot 2: Precision-Recall Curve ────────────────────────────
precision, recall, _ = precision_recall_curve(y_test, y_prob)
axes[1].plot(recall, precision, 'r-', linewidth=2,
             label=f'{automl.best_estimator} (AP = {avg_precision:.3f})')
axes[1].set_xlabel('Recall (Sensitivity)')
axes[1].set_ylabel('Precision (PPV)')
axes[1].set_title('Precision-Recall Curve')
axes[1].legend(loc='upper right')
axes[1].grid(True, alpha=0.3)

# ── Plot 3: Top 15 Feature Importance ─────────────────────────
if feat_importance[0][1] > 0:
    top15 = feat_importance[:15]
    top15_names = [f[0].replace("_", " ").title() for f in reversed(top15)]
    top15_values = [f[1] for f in reversed(top15)]
    colors = ['#e74c3c' if v > 0.05 else '#3498db' for v in top15_values]
    axes[2].barh(range(len(top15_names)), top15_values, color=colors)
    axes[2].set_yticks(range(len(top15_names)))
    axes[2].set_yticklabels(top15_names, fontsize=9)
    axes[2].set_xlabel('Feature Importance (Gain)')
    axes[2].set_title(f'Top 15 Features ({automl.best_estimator})')
    axes[2].grid(True, alpha=0.3, axis='x')
else:
    axes[2].text(0.5, 0.5, "Feature importance\nnot available",
                 ha='center', va='center', fontsize=14)
    axes[2].set_title('Feature Importance')

plt.tight_layout()
display(fig)
print("📈 Plots generated — see above for ROC curve, PR curve, and feature importance")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 8 — CODE: AutoML Trial Summary                          ║
# ╚════════════════════════════════════════════════════════════════╝

print(f"{'='*70}")
print(f"🔬 AUTOML TRIAL HISTORY ({len(automl.config_history)} configurations tried)")
print(f"{'='*70}")
print(f"\n{'Trial':<7} {'Algorithm':<15} {'AUC-ROC (CV)':<15} {'Train Time':<12}")
print("─" * 50)

for trial_id, config in automl.config_history.items():
    estimator = config.get("Current Learner", "unknown")
    print(f"{trial_id:<7} {estimator:<15}")

# Compare AutoML result to baseline
print(f"\n{'='*70}")
print(f"📊 COMPARISON: AutoML vs Published Readmission Models")
print(f"{'='*70}")
print(f"  {'Model':<35} {'AUC-ROC':<12} {'Notes'}")
print(f"  {'─'*65}")
print(f"  {'LACE Index':<35} {'0.68-0.72':<12} {'Validated in >50 studies'}")
print(f"  {'HOSPITAL Score':<35} {'0.72':<12} {'7-point scale'}")
print(f"  {'Yale/CMS Model':<35} {'0.73-0.76':<12} {'Used for HRRP penalties'}")
print(f"  {'─'*65}")
print(f"  {'Our AutoML Model':<35} {f'{auc_roc:.4f}':<12} {'AutoML-optimized, 42 features'}")
print(f"  {'─'*65}")

if auc_roc > 0.76:
    print(f"\n  ✅ Our model OUTPERFORMS published readmission risk models!")
elif auc_roc > 0.72:
    print(f"\n  ✅ Our model is on par with the best published models.")
else:
    print(f"\n  ⚠️ Consider increasing time_budget or adding more features.")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 9 — CODE: Score All Patients & Save to Gold             ║
# ╚════════════════════════════════════════════════════════════════╝

from pyspark.sql.functions import col, count, avg, sum, round

# Score ALL patients using the AutoML best model
X_all = pandas_df[feature_columns]
all_probs = automl.predict_proba(X_all)[:, 1]
all_preds = automl.predict(X_all)

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
# ║  CELL 10 — CODE: Ask Gen AI to Interpret Results               ║
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
We trained a readmission prediction model using AutoML (FLAML).

AutoML Trial Summary:
- Best algorithm: {automl.best_estimator}
- Total algorithms tried: LightGBM, XGBoost, CatBoost, Random Forest, Extra Trees
- Total configurations evaluated: {len(automl.config_history)}
- Best hyperparameters: {automl.best_config}

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

5. **AutoML Advantage**: Briefly explain the value of using AutoML vs.
   manually tuning a single model. Why should the CMO trust that we found
   the best model?

Write for a clinical audience, not data scientists."""

print("🤖 Asking Fabric AI endpoint to interpret model results...\n")

interpretation_response = client.chat.completions.create(
    model=MODEL_NAME,
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
# ║  CELL 11 — MARKDOWN                                           ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## ✅ What You Built
#
# In this notebook, you used **Gen AI + AutoML** to build a
# hospital readmission prediction model:
#
# | Step | What You Did | Technology |
# |------|-------------|------------|
# | 1 | Feature Discovery | Fabric AI endpoint analyzed schema, suggested 25+ features |
# | 2 | Feature Implementation | PySpark built 42 features across 6 categories |
# | 3 | AutoML Model Selection | FLAML tried 5 algorithms with hyperparameter tuning |
# | 4 | Experiment Tracking | MLflow logged all trials for reproducibility |
# | 5 | Visualization | ROC curve, PR curve, feature importance chart |
# | 6 | Risk Scoring | Every patient scored 0.0-1.0 and classified Low/Medium/High |
# | 7 | Interpretation | Fabric AI endpoint translated results into clinical recommendations |
#
# ### Output Tables Created
# - **gold_readmission_training** — Feature-engineered training dataset (42 features)
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
