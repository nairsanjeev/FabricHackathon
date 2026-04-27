# Module 9: Predicting Hospital Readmissions with Gen AI (Optional)

| Duration | 45–60 minutes |
|----------|---------------|
| Objective | Use Azure OpenAI as a feature engineering assistant to build a machine learning model that predicts 30-day hospital readmissions |
| Tools | Fabric Notebook, Azure OpenAI, PySpark, XGBoost |

> **⚠️ This module is OPTIONAL.** It builds on the data created in Modules 1–2 and the Gen AI skills from Module 5. You will use Gen AI not just for text analysis, but as an **ML feature engineering assistant** — a technique that reduced development time by 73% in a recent healthcare hackathon.

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

1. 🤖 **Ask Azure OpenAI** to analyze your data schema and suggest predictive features for readmission
2. 🔧 **Build 25+ features** across 6 categories (Demographics, Comorbidity, Utilization, Clinical, Financial, Temporal) using PySpark
3. 📊 **Train an XGBoost model** to predict 30-day readmissions with class imbalance handling
4. 📈 **Evaluate the model** with AUC-ROC, Precision-Recall curves, and feature importance charts
5. 🏥 **Score all patients** with a readmission risk probability and classify into Low/Medium/High risk tiers
6. 💡 **Ask Azure OpenAI** to interpret the results and generate clinical recommendations

---

## Prerequisites

Before starting this module, ensure you have:

- ✅ **Completed Modules 1–2**: All Silver and Gold tables populated in your Lakehouse
- ✅ **Azure OpenAI resource**: Same deployment used in Module 5 (GPT-4o recommended)
- ✅ **Azure OpenAI endpoint URL and API key**: From Module 5 configuration
- ✅ **Fabric notebook environment**: Same workspace from previous modules

> **📋 Key table needed:** `gold_readmissions` (created in Notebook 03). This table contains the target variable `was_readmitted` — a boolean flag indicating whether a patient was readmitted within 30 days of discharge.

---

## Part A: Create the Notebook and Configure Azure OpenAI

### Step 1: Create a New Notebook

1. Open your **Fabric workspace**
2. Click **+ New item** → **Notebook**
3. Rename it to: **"06 - Predictive Readmission Model"**
4. In the **Explorer** pane on the left, click **Lakehouses** → **Add** → Select your **HealthcareLakehouse**

### Step 2: Add the Configuration Cell

Create the first code cell and paste the following configuration:

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json

# ── Azure OpenAI Configuration ────────────────────────────────
# ⚠️ REPLACE with your Azure OpenAI resource details
AZURE_OPENAI_ENDPOINT = "https://<your-resource-name>.openai.azure.com/"
AZURE_OPENAI_KEY = "<your-api-key>"
AZURE_OPENAI_DEPLOYMENT = "<your-deployment-name>"
AZURE_OPENAI_API_VERSION = "2024-06-01"

print("✅ Configuration ready")
```

> ⚠️ Replace the three placeholder values with your Azure OpenAI resource details (same values used in Module 5).

### Step 3: Install the OpenAI SDK

Create a second code cell:

```python
%pip install openai -q
```

> After pip finishes, the kernel may restart. If so, re-run Cell 1 (Configuration).

---

## Part B: Use Gen AI for Feature Engineering

### Step 4: Ask Azure OpenAI to Suggest Predictive Features

This is the core innovation — rather than manually researching clinical features from published readmission risk models, we ask Azure OpenAI to analyze our data schema and suggest features grounded in medical literature.

Create a new code cell and paste the code from **Cell 5** in the notebook file (`notebooks/06_Predictive_Readmission_Model.py`).

**What happens:**
- We send a detailed description of all our table schemas to the LLM
- The system prompt positions the LLM as a senior clinical data scientist
- The LLM suggests ~25 features with computation logic, clinical rationale, and expected importance
- Each feature includes the clinical reasoning — critical for model interpretability

**Expected output:**
```
✅ Azure OpenAI suggested 25 features:

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

Now we implement the AI-suggested features using PySpark. Create a new code cell and paste the code from **Cell 7** in the notebook file.

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

Create a new code cell:

```python
%pip install xgboost scikit-learn matplotlib -q
```

### Step 7: Train the XGBoost Model

Create a new code cell and paste the code from **Cell 10** in the notebook file.

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

Create a new code cell and paste the code from **Cell 12** in the notebook file.

This generates three visualizations:

1. **ROC Curve** — The gold standard for classifier evaluation. The curve should bow toward the upper-left corner (higher = better).
2. **Precision-Recall Curve** — More informative than ROC for imbalanced datasets. Shows the tradeoff between catching all readmissions (recall) and not false-alarming (precision).
3. **Feature Importance Bar Chart** — The top 15 features driving predictions. This is the most important chart for clinical stakeholders: "WHY does the model think this patient is high-risk?"

> ✅ **Checkpoint:** Review the feature importance chart. The top features should align with clinical intuition — prior utilization, comorbidity burden, and length of stay are consistently the strongest predictors in published literature.

---

## Part D: Score Patients and Generate Clinical Insights

### Step 9: Generate Risk Scores for All Patients

Create a new code cell and paste the code from **Cell 14** in the notebook file.

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

Create a new code cell and paste the code from **Cell 16** in the notebook file.

This closes the loop — Gen AI suggested the features, we trained the model, and now Gen AI interprets the results for clinical stakeholders:

- **Clinical Interpretation**: What the model found in plain language
- **Actionable Recommendations**: Specific interventions based on top features
- **Model Limitations**: Caveats for clinicians
- **Comparison to Published Models**: How our AUC-ROC compares to LACE, HOSPITAL score, etc.

> **💡 Key Insight:** This step transforms raw ML metrics (AUC-ROC, feature importance) into a **clinician-readable report** that a Chief Medical Officer or VP of Quality can act on. The LLM bridges the gap between data science output and clinical decision-making.

---

## ✅ Module 9 Checklist

Confirm you have completed:

- [ ] Azure OpenAI suggested 25+ features across 6 clinical categories
- [ ] Built a training dataset with 42 features in `gold_readmission_training`
- [ ] Trained an XGBoost model with class imbalance handling
- [ ] AUC-ROC evaluated (target: ≥ 0.70)
- [ ] Reviewed feature importance chart — top features align with clinical literature
- [ ] ROC curve and Precision-Recall curve generated
- [ ] All patients scored and classified into risk tiers in `gold_readmission_risk_scores`
- [ ] Azure OpenAI generated clinical interpretation and recommendations
- [ ] Understand how Gen AI accelerates ML feature engineering

---

## What You Built

| Component | Description |
|-----------|-------------|
| **Gen AI Feature Discovery** | Used Azure OpenAI to suggest clinically-grounded features |
| **Feature Engineering Pipeline** | Built 42 features from 7 Silver/Gold tables using PySpark |
| **XGBoost Prediction Model** | Gradient-boosted classifier with class imbalance handling |
| **Risk Scoring System** | Every patient scored 0.0–1.0 and classified Low/Medium/High |
| **Clinical Interpretation** | AI-generated insights for non-technical stakeholders |

### How This Connects to Other Modules

- **Module 2** (Data Engineering): The Silver/Gold tables you built are the foundation for all 42 ML features
- **Module 3** (Power BI): Add the `gold_readmission_risk_scores` table to your semantic model for a "Readmission Risk" dashboard page
- **Module 5** (Gen AI): The same Azure OpenAI deployment is used here, but for feature engineering and result interpretation instead of clinical note analysis
- **Module 7** (Data Agent): The Data Agent can now answer questions like *"Which high-risk patients are being discharged this week?"* using the risk scores table

---

**[← Module 8: VS Code Agent Mode](Module08_VSCode_Agent_Mode.md)** | **[Back to Overview](../README.md)**
