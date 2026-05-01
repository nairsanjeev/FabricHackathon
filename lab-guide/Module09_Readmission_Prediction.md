# Module 9: Predicting Hospital Readmissions with Gen AI (Optional)

| Duration | 45–60 minutes |
|----------|---------------|
| Objective | Use Fabric's built-in AI endpoint as a feature engineering assistant to build a machine learning model that predicts 30-day hospital readmissions |
| Tools | Fabric Notebook, Fabric AI Services (built-in Azure OpenAI), PySpark, XGBoost |

> **⚠️ This module is OPTIONAL.** It builds on the data created in Modules 1–2 and the Gen AI skills from Module 5. You will use Gen AI not just for text analysis, but as an **ML feature engineering assistant**.

---

## Why Predict Hospital Readmissions?

Hospital readmissions within 30 days are one of the most important quality metrics in healthcare:

| Metric | Value |
|--------|-------|
| Annual cost of readmissions (US) | **$26 billion** |
| CMS penalty for excess readmissions | Up to **3% of total Medicare payments** |
| Average 30-day readmission rate | **15–20%** |
| Preventable readmissions | **27–50%** of all readmissions |

The **CMS Hospital Readmissions Reduction Program (HRRP)** penalizes hospitals with higher-than-expected readmission rates for conditions like heart failure, pneumonia, and hip/knee replacement. A predictive model that identifies high-risk patients **before discharge** enables targeted interventions:

- **Transitional care** coordination
- **Post-discharge follow-up** calls within 48 hours
- **Medication reconciliation** before leaving the hospital
- **Social determinant** screening (does the patient have transportation to follow-up appointments?)

### The Gen AI Advantage

Traditional machine learning for readmission prediction requires a clinical data scientist to spend **2–3 weeks** manually researching and engineering features.  A recent healthcare hackathon demonstrated that using **Gen AI as a feature engineering assistant** can:

- Suggest **47 candidate features** across 6 clinical categories
- Draw on medical literature knowledge (LACE index, HOSPITAL score, Charlson Comorbidity Index)
- Improve AUC-ROC from **0.72 → 0.81** (12.5% improvement over manual features)
- Reduce development time by **73%**

In this module, you'll replicate this approach on your Fabric Lakehouse data.

---

## What You Will Do

1. 🤖 **Ask Fabric's built-in AI endpoint** to analyze your data schema and suggest predictive features for readmission
2. 🔧 **Build 25+ features** across 6 categories (Demographics, Comorbidity, Utilization, Clinical, Financial, Temporal) using PySpark
3. 📊 **Train an XGBoost model** to predict 30-day readmissions with class imbalance handling
4. 📈 **Evaluate the model** with AUC-ROC, Precision-Recall curves, and feature importance charts
5. 🏥 **Score all patients** with a readmission risk probability and classify into Low/Medium/High risk tiers
6. 💡 **Ask the AI endpoint** to interpret the results and generate clinical recommendations

---

## Prerequisites

Before starting this module, ensure you have:

- ✅ **Completed Modules 1–2**: All Silver and Gold tables populated in your Lakehouse
- ✅ **Fabric capacity**: The built-in Fabric AI endpoint provides access to Azure OpenAI models (like `gpt-4.1`) directly from Fabric notebooks, with no separate Azure OpenAI resource or API key needed
- ✅ **Fabric notebook environment**: Same workspace from previous modules

> **📋 Key table needed:** `gold_readmissions` (created in Notebook 03). This table contains the target variable `was_readmitted` — a boolean flag indicating whether a patient was readmitted within 30 days of discharge.

---

## Part A: Create the Notebook and Configure AI Endpoint

### Step 1: Create a New Notebook

1. Open your **Fabric workspace**
2. Click **+ New item** → **Notebook**
3. Rename it to: **"06 - Predictive Readmission Model"**
4. In the **Explorer** pane on the left, click **Add data items** → **From OneLake catalog**
5. Search for `HealthcareLakehouse`. You will see **two items** with the same name — one is the **Lakehouse** and the other is the **SQL Analytics Endpoint**. **Select the Lakehouse** (blue house/database icon). Click on the item details if needed to confirm the type is **Lakehouse**.
6. Click **Add**

> ⚠️ **Session Note:** If your Spark session expires or is stopped at any point, you will need to re-run all cells from the top using **Run all**. Fabric does not preserve variables, imports, or DataFrames across session restarts.

### Step 2: Install the OpenAI SDK

Paste in Cell 1:

```python
%pip install -U openai -q
```

> **Expected warnings — safe to ignore:**
> - `ERROR: pip's dependency resolver...` — This is a pre-installed Fabric package with a stale dependency constraint. It does **not** affect the `openai` package or this lab.
> - `A new release of pip is available` — Informational only.
> - `PySpark kernel has been restarted` — Expected. Fabric restarts the kernel after `%pip install` so the new package is available. **Wait for the restart to complete, then continue with the next cell.**

### Step 3: Initialize the Fabric AI Client

Fabric provides a **built-in AI endpoint** that gives you access to Azure OpenAI models (like `gpt-4.1`) directly — no API keys, no endpoint URLs, no separate Azure OpenAI resource needed. Authentication is handled automatically through your Fabric credentials.

Paste in Cell 2:

```python
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
```

You should see: `✅ Connection successful`

---

## Part B: Use Gen AI for Feature Engineering

### Step 4: Ask the AI Endpoint to Suggest Predictive Features

This is the core innovation — rather than manually researching clinical features from published readmission risk models, we ask the AI endpoint to analyze our data schema and suggest features grounded in medical literature.

Create a new code cell (Cell 3) and paste the following:

```python
import json

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
```

**What happens:**
- We send a detailed description of all our table schemas to the LLM
- The system prompt positions the LLM as a senior clinical data scientist
- The LLM suggests ~25 features with computation logic, clinical rationale, and expected importance
- Each feature includes the clinical reasoning — critical for model interpretability

**Expected output:**
```
✅ AI endpoint suggested 25 features:

#   Feature                                  Category        Importance
──────────────────────────────────────────────────────────────────────────
1   prior_admissions_12m                     Utilization     high
2   chronic_condition_count                  Comorbidity     high
3   age                                      Demographics    high
4   index_los                                Clinical        high
5   is_polypharmacy                          Medication      high
...
```

> **💡 Key Insight:** The LLM draws on its training data — which includes medical literature, clinical informatics textbooks, and ML research — to suggest features that would take a domain expert weeks to identify. Categories span: Demographics, Comorbidity, Utilization, Clinical, Financial, and Temporal patterns.

### Step 5: Build the Feature Training Dataset

> ⚠️ **Before running this cell:** Make sure your **HealthcareLakehouse** is attached to the notebook. In the **Explorer** pane on the left, check that you see your tables listed under **HealthcareLakehouse → Tables**. If not, click **Add data items** → **From OneLake catalog** → search for `HealthcareLakehouse` → select the **Lakehouse** item (not the SQL Analytics Endpoint) → **Add**.

Now we implement the AI-suggested features using PySpark. Create a new code cell (Cell 4) and paste the following:

```python
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
```

This cell builds features across 6 categories:

| Category | Features | Source Table | Clinical Basis |
|----------|----------|-------------|----------------|
| **Demographics** | age, gender, insurance, risk_score | silver_patients | LACE index "A" component |
| **Comorbidity** | chronic_condition_count, has_diabetes, has_chf, etc. | silver_conditions | Charlson Comorbidity Index |
| **Prior Utilization** | prior_admissions_12m, prior_ed_visits_12m | silver_encounters | #1 predictor in published models |
| **Clinical** | SIRS count, avg heart rate, max pain | silver_vitals | Clinical instability markers |
| **Financial** | payment_ratio, denied_claims_count | silver_claims | Care complexity proxy |
| **Temporal** | is_weekend_discharge, discharge_month | gold_readmissions | "Weekend effect" in literature |
| **Medications** | medication count, is_polypharmacy | silver_medications | Drug interaction risk |

**Expected output:**
```
Base: 302 index admissions
  Readmitted: 48
  Not readmitted: 254
✓ Demographics features added
✓ Comorbidity features added
✓ Prior utilization features added
✓ Clinical vitals features added
✓ Financial features added
✓ Temporal features added
✓ Medication features added

📊 Training dataset: 302 rows × 42 features
   Label distribution: 48 readmitted (15.9%), 254 not readmitted (84.1%)
✓ Saved to gold_readmission_training
```

> ✅ **Checkpoint:** The `gold_readmission_training` table should appear in your Lakehouse Tables list. Each row is one inpatient index admission with 42 feature columns and a binary label.

---

## Part C: Train and Evaluate the Prediction Model

### Step 6: Install ML Libraries

Create a new code cell (Cell 5):

```python
%pip install xgboost scikit-learn matplotlib -q
```

### Step 7: Train the XGBoost Model

Create a new code cell (Cell 6) and paste the following:

```python
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score, classification_report, confusion_matrix,
    roc_curve, precision_recall_curve, average_precision_score
)
from xgboost import XGBClassifier
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
```

**What this does:**
- Loads the feature-engineered data from `gold_readmission_training`
- Splits 80% train / 20% test (stratified to preserve readmission rate)
- Handles class imbalance with `scale_pos_weight` (readmissions are ~15% of cases)
- Trains an XGBoost gradient-boosted classifier with 200 trees, max depth 4
- Evaluates with AUC-ROC, classification report, and feature importance

**Expected output:**
```
════════════════════════════════════════════════════════
📊 MODEL EVALUATION RESULTS
════════════════════════════════════════════════════════
  AUC-ROC:            0.XXXX
  Average Precision:  0.XXXX

  Interpretation:
  ✅ Good discriminative ability (0.75-0.80)

═══════════════════════════════════════════════════════
🔑 TOP 15 FEATURES BY IMPORTANCE (XGBoost gain)
═══════════════════════════════════════════════════════
   1. prior_admissions_12m                  0.XXXX  ████████████████████
   2. chronic_condition_count               0.XXXX  ██████████████
   3. age                                   0.XXXX  ████████████
   ...
```

> **📊 AUC-ROC Interpretation:**
> - **≥ 0.80**: Excellent — competitive with published models
> - **0.75–0.80**: Good — comparable to the HOSPITAL score
> - **0.70–0.75**: Acceptable — similar to LACE index
> - **< 0.70**: Needs improvement (more features or data)

### Step 8: Visualize Results

Create a new code cell (Cell 7) and paste the following:

```python
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
display(fig)
print("📈 Plots generated — see above for ROC curve, PR curve, and feature importance")
```

This generates three visualizations:

1. **ROC Curve** — The gold standard for classifier evaluation. The curve should bow toward the upper-left corner (higher = better).
2. **Precision-Recall Curve** — More informative than ROC for imbalanced datasets. Shows the tradeoff between catching all readmissions (recall) and not false-alarming (precision).
3. **Feature Importance Bar Chart** — The top 15 features driving predictions. This is the most important chart for clinical stakeholders: "WHY does the model think this patient is high-risk?"

> ✅ **Checkpoint:** Review the feature importance chart. The top features should align with clinical intuition — prior utilization, comorbidity burden, and length of stay are consistently the strongest predictors in published literature.

---

## Part D: Score Patients and Generate Clinical Insights

### Step 9: Generate Risk Scores for All Patients

Create a new code cell (Cell 8) and paste the following:

```python
from pyspark.sql.functions import col, count, avg, sum, round

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
```

This cell:
- Scores every index admission with a readmission probability (0.0 to 1.0)
- Classifies patients into risk tiers:

| Risk Tier | Score Range | Clinical Action |
|-----------|-------------|-----------------|
| **Low** | < 0.2 | Routine discharge, standard follow-up |
| **Medium** | 0.2 – 0.5 | Enhanced follow-up within 48 hours |
| **High** | ≥ 0.5 | Active intervention before discharge |

**Expected output:**
```
✅ Saved XXX risk scores to gold_readmission_risk_scores

Risk Tier Distribution:
+---------+--------+--------------+-------------------+
|risk_tier|patients|avg_risk_score|actual_readmissions|
+---------+--------+--------------+-------------------+
|     High|      XX|         0.XXX|                 XX|
|      Low|     XXX|         0.XXX|                 XX|
|   Medium|      XX|         0.XXX|                 XX|
+---------+--------+--------------+-------------------+
```

> ✅ **Checkpoint:** The `gold_readmission_risk_scores` table should appear in your Lakehouse. High-risk patients should have a much higher actual readmission rate than low-risk patients.

### Step 10: Ask Gen AI to Interpret the Results

Create a new code cell (Cell 9) and paste the following:

```python
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
```

This closes the loop — Gen AI suggested the features, we trained the model, and now Gen AI interprets the results for clinical stakeholders:

- **Clinical Interpretation**: What the model found in plain language
- **Actionable Recommendations**: Specific interventions based on top features
- **Model Limitations**: Caveats for clinicians
- **Comparison to Published Models**: How our AUC-ROC compares to LACE, HOSPITAL score, etc.

> **💡 Key Insight:** This step transforms raw ML metrics (AUC-ROC, feature importance) into a **clinician-readable report** that a Chief Medical Officer or VP of Quality can act on. The LLM bridges the gap between data science output and clinical decision-making.

---

## ✅ Module 9 Checklist

Confirm you have completed:

- [ ] Fabric AI endpoint suggested 25+ features across 6 clinical categories
- [ ] Built a training dataset with 42 features in `gold_readmission_training`
- [ ] Trained an XGBoost model with class imbalance handling
- [ ] AUC-ROC evaluated (target: ≥ 0.70)
- [ ] Reviewed feature importance chart — top features align with clinical literature
- [ ] ROC curve and Precision-Recall curve generated
- [ ] All patients scored and classified into risk tiers in `gold_readmission_risk_scores`
- [ ] Fabric AI endpoint generated clinical interpretation and recommendations
- [ ] Understand how Gen AI accelerates ML feature engineering

---

## What You Built

| Component | Description |
|-----------|-------------|
| **Gen AI Feature Discovery** | Used Fabric's built-in AI endpoint to suggest clinically-grounded features |
| **Feature Engineering Pipeline** | Built 42 features from 7 Silver/Gold tables using PySpark |
| **XGBoost Prediction Model** | Gradient-boosted classifier with class imbalance handling |
| **Risk Scoring System** | Every patient scored 0.0–1.0 and classified Low/Medium/High |
| **Clinical Interpretation** | AI-generated insights for non-technical stakeholders |

### How This Connects to Other Modules

- **Module 2** (Data Engineering): The Silver/Gold tables you built are the foundation for all 42 ML features
- **Module 3** (Power BI): Add the `gold_readmission_risk_scores` table to your semantic model for a "Readmission Risk" dashboard page
- **Module 5** (Gen AI): The same Fabric built-in AI endpoint is used here, but for feature engineering and result interpretation instead of clinical note analysis
- **Module 7** (Data Agent): The Data Agent can now answer questions like *"Which high-risk patients are being discharged this week?"* using the risk scores table

---

**[← Module 8: VS Code Agent Mode](Module08_VSCode_Agent_Mode.md)** | **[Back to Overview](../README.md)**
