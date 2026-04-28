# Module 6: Prep Data for AI

| Duration | 30 minutes |
|----------|------------|
| Objective | Prepare healthcare data for AI consumption by validating quality, creating feature-ready views, and building AI-optimized tables that the Data Agent and future ML models can use effectively |
| Fabric Features | Spark Notebooks, Delta Lake, Data Quality Checks |

---

## Why Prep Data for AI?

AI models — whether they're LLMs answering questions or ML models predicting outcomes — are only as good as the data they're fed. In healthcare, poor data quality leads to:

- **Wrong answers** from Data Agents ("Our readmission rate is 2%" when it's really 15% because nulls were miscounted)
- **Biased predictions** (a model trained on incomplete demographic data may underperform for certain populations)
- **Compliance risks** (AI decisions based on inaccurate data can trigger CMS audit flags)

This module ensures your Gold layer tables are **AI-ready** — clean, complete, well-documented, and optimized for natural language querying.

---

## What You Will Do

1. Run **data quality checks** across all Gold tables
2. Create **AI-friendly summary views** that simplify complex joins
3. Build a **data dictionary table** that helps the Data Agent understand your data
4. Validate **referential integrity** across tables
5. Create **materialized features** for common AI/ML use cases

---

## Part A: Data Quality Assessment

### Step 1: Create a New Notebook

1. Go to your workspace
2. Click **+ New item** → **Notebook**
3. Rename to: `06 - Prep Data for AI`
4. Attach the notebook to your `HealthcareLakehouse`

> ⚠️ **Session Note:** If your Spark session expires or is stopped at any point, you will need to re-run all cells from the top using **Run all**. Fabric does not preserve variables, imports, or DataFrames across session restarts.

### Step 2: Run Comprehensive Data Quality Checks

Paste in Cell 1:

```python
# =============================================================
# Cell 1: Comprehensive Data Quality Assessment
# =============================================================
# Before feeding data to AI, we need to understand:
#   - Completeness: How many nulls are in key columns?
#   - Uniqueness: Are IDs actually unique?
#   - Validity: Are values within expected ranges?
#   - Consistency: Do related tables agree?
# =============================================================

from pyspark.sql.functions import *

# List of Gold tables to check
gold_tables = [
    "gold_readmissions",
    "gold_ed_utilization",
    "gold_encounter_summary",
    "gold_alos",
    "gold_financial",
    "gold_population_health"
]

print("=" * 70)
print("📊 DATA QUALITY ASSESSMENT — GOLD LAYER")
print("=" * 70)

quality_results = []

for table_name in gold_tables:
    try:
        df = spark.table(table_name)
        row_count = df.count()
        col_count = len(df.columns)
        
        # Calculate null percentages for each column
        null_counts = {}
        for c in df.columns:
            null_pct = df.filter(col(c).isNull()).count() / row_count * 100 if row_count > 0 else 0
            if null_pct > 0:
                null_counts[c] = round(null_pct, 1)
        
        print(f"\n🔍 {table_name}")
        print(f"   Rows: {row_count:,} | Columns: {col_count}")
        
        if null_counts:
            print(f"   ⚠️  Columns with nulls:")
            for col_name, pct in sorted(null_counts.items(), key=lambda x: -x[1]):
                status = "🔴" if pct > 20 else "🟡" if pct > 5 else "🟢"
                print(f"      {status} {col_name}: {pct}% null")
        else:
            print(f"   ✅ No null values found")
        
        quality_results.append({
            "table": table_name,
            "rows": row_count,
            "columns": col_count,
            "null_columns": len(null_counts),
            "max_null_pct": max(null_counts.values()) if null_counts else 0
        })
    except Exception as e:
        print(f"\n❌ {table_name}: {str(e)}")

print("\n" + "=" * 70)
print("📋 QUALITY SUMMARY")
print("=" * 70)
for r in quality_results:
    status = "✅" if r["max_null_pct"] < 5 else "⚠️" if r["max_null_pct"] < 20 else "🔴"
    print(f"  {status} {r['table']}: {r['rows']:,} rows, {r['null_columns']} columns with nulls (max {r['max_null_pct']}%)")
```

---

## Part B: Referential Integrity Checks

### Step 3: Validate Cross-Table References

Paste in Cell 2:

```python
# =============================================================
# Cell 2: Referential Integrity Checks
# =============================================================
# AI models (especially Data Agents) join tables to answer 
# questions. If foreign keys don't match, joins produce nulls 
# and the AI gives wrong answers.
# =============================================================

print("=" * 70)
print("🔗 REFERENTIAL INTEGRITY CHECKS")
print("=" * 70)

# Check 1: All encounter_ids in gold tables exist in silver_encounters
encounters = spark.table("silver_encounters")
encounter_ids = set(encounters.select("encounter_id").rdd.flatMap(lambda x: x).collect())

for table_name in ["gold_readmissions", "gold_financial"]:
    try:
        df = spark.table(table_name)
        if "encounter_id" in df.columns:
            id_col = "encounter_id"
        elif "index_encounter_id" in df.columns:
            id_col = "index_encounter_id"
        else:
            continue
        
        table_ids = set(df.select(id_col).rdd.flatMap(lambda x: x).collect())
        orphans = table_ids - encounter_ids
        
        if orphans:
            print(f"⚠️  {table_name}.{id_col}: {len(orphans)} orphan IDs (no match in silver_encounters)")
        else:
            print(f"✅ {table_name}.{id_col}: All IDs match silver_encounters")
    except Exception as e:
        print(f"❌ {table_name}: {str(e)}")

# Check 2: All patient_ids in gold tables exist in silver_patients
patients = spark.table("silver_patients")
patient_ids = set(patients.select("patient_id").rdd.flatMap(lambda x: x).collect())

for table_name in ["gold_population_health", "gold_ed_utilization", "gold_readmissions"]:
    try:
        df = spark.table(table_name)
        if "patient_id" in df.columns:
            table_pids = set(df.select("patient_id").rdd.flatMap(lambda x: x).collect())
            orphans = table_pids - patient_ids
            if orphans:
                print(f"⚠️  {table_name}.patient_id: {len(orphans)} orphan patient IDs")
            else:
                print(f"✅ {table_name}.patient_id: All IDs match silver_patients")
    except Exception as e:
        print(f"❌ {table_name}: {str(e)}")

print("\n✅ Referential integrity checks complete")
```

---

## Part C: Create a Data Dictionary Table

### Step 4: Build a Data Dictionary for the Data Agent

One of the most effective ways to improve Data Agent accuracy is to provide a structured reference of all available tables and their column definitions. 

Paste in Cell 3:

```python
# =============================================================
# Cell 3: Create a Data Dictionary Table
# =============================================================
# The Data Agent can reference this table to understand what 
# data is available and what each column means. This dramatically 
# improves query accuracy.
# =============================================================

from pyspark.sql.types import StructType, StructField, StringType

# Define the data dictionary
data_dict = [
    # Gold Readmissions
    ("gold_readmissions", "index_encounter_id", "string", "Unique ID of the original (index) admission", "Primary key"),
    ("gold_readmissions", "patient_id", "string", "Patient identifier", "Foreign key → silver_patients"),
    ("gold_readmissions", "index_admission_date", "date", "Date of the original admission", "Use for time-based analysis"),
    ("gold_readmissions", "index_discharge_date", "date", "Date of discharge from original admission", "Used in 30-day calculation"),
    ("gold_readmissions", "index_diagnosis", "string", "Primary diagnosis of the index admission", "Group readmissions by diagnosis"),
    ("gold_readmissions", "index_facility", "string", "Facility of the original admission", "Metro General, Community Medical, Riverside Health"),
    ("gold_readmissions", "was_readmitted", "boolean", "TRUE if patient returned within 30 days", "Core readmission flag"),
    ("gold_readmissions", "days_to_readmission", "integer", "Days between discharge and readmission", "NULL if not readmitted"),
    
    # Gold Encounter Summary
    ("gold_encounter_summary", "encounter_id", "string", "Unique encounter identifier", "Primary key"),
    ("gold_encounter_summary", "encounter_type", "string", "Type of encounter", "Values: Inpatient, ED, Outpatient, Observation"),
    ("gold_encounter_summary", "facility_name", "string", "Hospital or facility name", "3 facilities in our network"),
    ("gold_encounter_summary", "length_of_stay_days", "integer", "Duration of inpatient stay", "NULL for non-inpatient encounters"),
    ("gold_encounter_summary", "total_charges", "decimal", "Total charges for the encounter", "Billed amount, not collected"),
    ("gold_encounter_summary", "age_group", "string", "Patient age category", "Values: 0-17, 18-34, 35-49, 50-64, 65+"),
    ("gold_encounter_summary", "insurance_type", "string", "Patient insurance type", "Values: Medicare, Medicaid, Commercial, Self-Pay"),
    ("gold_encounter_summary", "risk_category", "string", "Patient risk stratification", "Values: Low, Moderate, High, Critical"),
    
    # Gold ED Utilization
    ("gold_ed_utilization", "patient_id", "string", "Patient identifier", "Foreign key → silver_patients"),
    ("gold_ed_utilization", "ed_visit_count", "integer", "Number of ED visits in the period", "Frequent flyer threshold: ≥4"),
    ("gold_ed_utilization", "is_frequent_flyer", "boolean", "TRUE if 4+ ED visits", "Key metric for care management"),
    ("gold_ed_utilization", "total_ed_charges", "decimal", "Total charges across all ED visits", "Financial impact of ED utilization"),
    
    # Gold Financial
    ("gold_financial", "claim_status", "string", "Current status of the claim", "Values: Paid, Denied, Pending"),
    ("gold_financial", "claim_amount", "decimal", "Billed amount on the claim", "Original charge"),
    ("gold_financial", "paid_amount", "decimal", "Amount actually paid by payer", "May be less than claim_amount"),
    ("gold_financial", "payer", "string", "Insurance company or payer", "Group financial metrics by payer"),
    ("gold_financial", "payment_ratio", "decimal", "Ratio of paid to billed", "1.0 = full payment, 0.0 = denied"),
    
    # Gold Population Health
    ("gold_population_health", "patient_id", "string", "Patient identifier", "Foreign key → silver_patients"),
    ("gold_population_health", "chronic_condition_count", "integer", "Number of chronic conditions", "0 = healthy, 3+ = high multimorbidity"),
    ("gold_population_health", "has_diabetes", "boolean", "Patient has diabetes", "Common chronic condition flag"),
    ("gold_population_health", "has_heart_failure", "boolean", "Patient has heart failure", "High-cost condition flag"),
    ("gold_population_health", "has_copd", "boolean", "Patient has COPD", "Readmission risk factor"),
    ("gold_population_health", "multimorbidity", "string", "Multimorbidity tier", "Values: None, Moderate (1-2), High (3+)"),
]

schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("data_type", StringType(), False),
    StructField("description", StringType(), False),
    StructField("notes", StringType(), True)
])

df_dict = spark.createDataFrame(data_dict, schema=schema)
df_dict.write.mode("overwrite").format("delta").saveAsTable("data_dictionary")

print(f"✅ Data dictionary created with {df_dict.count()} entries")
df_dict.show(10, truncate=60)
```

---

## Part D: Create AI-Friendly Summary Views

### Step 5: Build a Patient 360° View

The Data Agent often needs to answer questions like "Tell me about patient X" which requires joining many tables. Creating a pre-joined view makes this instant.

Paste in Cell 4:

```python
# =============================================================
# Cell 4: Patient 360° View — AI-Ready Summary
# =============================================================
# This table pre-joins patient demographics, conditions, 
# encounters, and financial data into a single AI-queryable 
# table. The Data Agent can answer complex cross-domain 
# questions from this one table instead of joining 5+ tables.
# =============================================================

patients = spark.table("silver_patients")
encounters = spark.table("silver_encounters")
conditions = spark.table("silver_conditions")
claims = spark.table("silver_claims")
pop_health = spark.table("gold_population_health")

# Encounter summary per patient
encounter_summary = encounters.groupBy("patient_id").agg(
    count("*").alias("total_encounters"),
    sum(when(col("encounter_type") == "Inpatient", 1).otherwise(0)).alias("inpatient_count"),
    sum(when(col("encounter_type") == "ED", 1).otherwise(0)).alias("ed_count"),
    sum(when(col("encounter_type") == "Outpatient", 1).otherwise(0)).alias("outpatient_count"),
    round(avg("length_of_stay_days"), 1).alias("avg_los"),
    round(sum("total_charges"), 2).alias("total_charges"),
    min("encounter_date").alias("first_encounter"),
    max("encounter_date").alias("last_encounter")
)

# Claims summary per patient (join claims through encounters)
patient_claims = encounters.select("encounter_id", "patient_id") \
    .join(claims, "encounter_id") \
    .groupBy("patient_id") \
    .agg(
        count("*").alias("total_claims"),
        sum(when(col("claim_status") == "Denied", 1).otherwise(0)).alias("denied_claims"),
        round(sum("claim_amount"), 2).alias("total_billed"),
        round(sum("paid_amount"), 2).alias("total_paid")
    )

# Condition list per patient
patient_conditions = conditions.groupBy("patient_id").agg(
    count("*").alias("total_conditions"),
    collect_set("condition_description").alias("condition_list_raw")
)

# Build the 360° view
patient_360 = patients \
    .join(encounter_summary, "patient_id", "left") \
    .join(patient_claims, "patient_id", "left") \
    .join(
        pop_health.select("patient_id", "chronic_condition_count", "multimorbidity",
                         "has_diabetes", "has_heart_failure", "has_copd", 
                         "has_hypertension", "has_ckd"),
        "patient_id", "left"
    ) \
    .withColumn("is_high_utilizer", 
        when((col("ed_count") >= 4) | (col("inpatient_count") >= 3), True).otherwise(False)) \
    .withColumn("denial_rate",
        when(col("total_claims") > 0, round(col("denied_claims") / col("total_claims") * 100, 1)).otherwise(0))

patient_360.write.mode("overwrite").format("delta").saveAsTable("gold_patient_360")

print(f"✅ gold_patient_360 created: {patient_360.count()} patients")
print("\nSample high-utilizer patients:")
patient_360.filter(col("is_high_utilizer") == True) \
    .select("patient_id", "first_name", "last_name", "age", "insurance_type",
            "total_encounters", "ed_count", "chronic_condition_count", "multimorbidity") \
    .show(10, truncate=False)
```

### Step 6: Create a Facility Performance Summary

Paste in Cell 5:

```python
# =============================================================
# Cell 5: Facility Performance Summary — AI-Ready
# =============================================================
# Common Data Agent questions compare facilities. This table 
# pre-computes all key metrics per facility for instant answers.
# =============================================================

encounters = spark.table("silver_encounters")
readmissions = spark.table("gold_readmissions")
claims_data = spark.table("gold_financial")

# Encounter metrics by facility
facility_encounters = encounters.groupBy("facility_name").agg(
    count("*").alias("total_encounters"),
    sum(when(col("encounter_type") == "Inpatient", 1).otherwise(0)).alias("inpatient_count"),
    sum(when(col("encounter_type") == "ED", 1).otherwise(0)).alias("ed_count"),
    round(avg(when(col("encounter_type") == "Inpatient", col("length_of_stay_days"))), 1).alias("avg_inpatient_los"),
    round(sum("total_charges"), 2).alias("total_charges"),
    countDistinct("patient_id").alias("unique_patients")
)

# Readmission rate by facility
facility_readmissions = readmissions.groupBy("index_facility").agg(
    count("*").alias("index_admissions"),
    sum(when(col("was_readmitted") == True, 1).otherwise(0)).alias("readmissions_count"),
    round(sum(when(col("was_readmitted") == True, 1).otherwise(0)) / count("*") * 100, 1).alias("readmission_rate_pct")
)

# Join
facility_summary = facility_encounters \
    .join(facility_readmissions,
          facility_encounters.facility_name == facility_readmissions.index_facility,
          "left") \
    .drop("index_facility")

facility_summary.write.mode("overwrite").format("delta").saveAsTable("gold_facility_summary")

print("✅ gold_facility_summary created:")
facility_summary.show(truncate=False)
```

---

## Part E: Validate AI Readiness

### Step 7: Run the AI Readiness Scorecard

Paste in Cell 6:

```python
# =============================================================
# Cell 6: AI Readiness Scorecard
# =============================================================
# Final check: are all tables ready for Data Agent consumption?
# =============================================================

print("=" * 70)
print("🎯 AI READINESS SCORECARD")
print("=" * 70)

ai_tables = [
    ("gold_readmissions", "30-day readmission analysis"),
    ("gold_ed_utilization", "ED frequent flyer identification"),
    ("gold_encounter_summary", "Central encounter fact table"),
    ("gold_alos", "Length of stay by diagnosis"),
    ("gold_financial", "Revenue cycle / claims analysis"),
    ("gold_population_health", "Chronic disease prevalence"),
    ("gold_patient_360", "Patient-level comprehensive view"),
    ("gold_facility_summary", "Facility comparison metrics"),
    ("data_dictionary", "Self-describing metadata"),
]

if spark.catalog.tableExists("gold_clinical_ai_insights"):
    ai_tables.append(("gold_clinical_ai_insights", "AI-generated clinical summaries"))

checks_passed = 0
total_checks = len(ai_tables)

for table_name, description in ai_tables:
    try:
        df = spark.table(table_name)
        count = df.count()
        if count > 0:
            print(f"  ✅ {table_name} ({count:,} rows) — {description}")
            checks_passed += 1
        else:
            print(f"  ⚠️  {table_name} (0 rows) — {description}")
    except:
        print(f"  ❌ {table_name} — NOT FOUND — {description}")

print(f"\n{'=' * 70}")
print(f"  Score: {checks_passed}/{total_checks} tables ready")

if checks_passed == total_checks:
    print("  🎉 All tables are AI-ready! Proceed to the Data Agent module.")
else:
    print("  ⚠️  Some tables need attention. Review the issues above.")
print(f"{'=' * 70}")
```

---

## Part F: Prep Data for AI in Power BI (Semantic Model Layer)

In Parts A–E you cleaned and enriched data **at the Lakehouse level** (Delta tables). Power BI offers a complementary feature called **"Prep data for AI"** that works **at the Semantic Model level** — it tells Copilot in Power BI *how* to interpret your model, what business terms mean, and which visuals to return for common questions.

> **Think of it this way:**
> - Parts A–E = "Make the data itself AI-ready" (clean, joined, documented)
> - Part F = "Make the *semantic model* AI-ready" (schema focus, business rules, curated answers)

The **Prep data for AI** button (preview) is available on the **Home ribbon** in Power BI Desktop and on the **Semantic Model page ribbon** in the Power BI service. It provides three features:

### Feature 1: AI Data Schema — Simplify What Copilot Sees

Not every column in your semantic model is relevant for natural language Q&A. The AI Data Schema lets you **select which fields Copilot should reason over**, removing noise and ambiguity.

#### Steps

1. Open your healthcare report in **Power BI Desktop** (or select the semantic model in the Power BI service)
2. Click **Prep data for AI** on the Home ribbon
3. Go to the **Simplify data schema** tab
4. **Deselect** columns that would confuse Copilot — for example:
   - Internal surrogate keys (`encounter_id`, `claim_id`) — keep only human-readable identifiers
   - ETL metadata columns (`_loaded_at`, `_source_file`)
   - Raw codes when you also have descriptions (keep `condition_description`, hide `condition_code`)
5. **Keep selected** the columns that users would naturally ask about:
   - `patient_name`, `age`, `insurance_type`, `facility_name`
   - `total_charges`, `readmission_rate_pct`, `length_of_stay_days`
   - `chronic_condition_count`, `multimorbidity`, `claim_status`
6. Click **Apply**

> **Healthcare example:** A clinician asking "Which patients have the highest ED utilization?" doesn't need to see `encounter_id` or `payer_code`. By hiding those fields, Copilot focuses on the right columns and produces cleaner answers.

---

### Feature 2: AI Instructions — Teach Copilot Your Business Context

AI Instructions let you provide **plain-text guidance** that Copilot uses when interpreting questions. This is where you encode domain knowledge, terminology, and analysis rules.

#### Steps

1. In the **Prep data for AI** dialog, go to the **Add AI instructions** tab
2. Enter instructions that help Copilot understand your healthcare data. Here is a recommended set for our lab:

```
## Healthcare Analytics Context

You are analyzing data for a hospital network with 3 facilities:
Metro General Hospital, Community Medical Center, and Riverside Health System.

## Key Terminology
- "Readmission" = a patient returning to any facility within 30 days of discharge
- "Frequent flyer" = a patient with 4 or more ED visits in the analysis period
- "ALOS" = Average Length of Stay, measured in days for inpatient encounters only
- "Denial rate" = percentage of claims with claim_status = 'Denied'
- "Multimorbidity" = patients with 3 or more chronic conditions (High tier)
- "Payment ratio" = paid_amount / claim_amount (1.0 = fully paid, 0.0 = fully denied)

## Analysis Rules
- When analyzing readmissions, always group by facility and diagnosis
- When showing financial metrics, break down by insurance_type (Medicare, Medicaid, Commercial, Self-Pay)
- For population health questions, prioritize the gold_patient_360 table — it has pre-joined demographics, encounters, and conditions
- A "high-risk" patient has risk_category = 'Critical' or 'High'
- When comparing facilities, use the gold_facility_summary table for pre-computed metrics
- ED utilization analysis should highlight frequent flyers (is_frequent_flyer = TRUE)

## Data Priority
- Use gold_patient_360 as the primary table for patient-level questions
- Use gold_facility_summary for facility comparison questions
- Use gold_readmissions for 30-day readmission analysis
- Use gold_financial for revenue cycle and claims questions
```

3. Click **Apply**

> **Why this matters:** Without instructions, Copilot might not know that "readmission" means a 30-day return, or that "frequent flyer" is a clinical term with a specific threshold. These instructions ground Copilot in your organization's definitions.

---

### Feature 3: Verified Answers — Pin Curated Visuals to Common Questions

Verified Answers let you **pre-approve specific visuals** as the "correct" response to common questions. When a user asks something matching a trigger phrase, Copilot returns your curated visual instead of generating a new one.

#### Steps

1. First, **create a visual** in your report that answers a common question — for example, a bar chart showing *30-Day Readmission Rate by Facility*
2. **Select the visual** on the report canvas
3. Click the **...** menu on the visual header → **Set up a verified answer**
4. Add **trigger phrases** (5–7 recommended per verified answer):
   - "What is the readmission rate?"
   - "Show readmission rates by facility"
   - "Which hospital has the most readmissions?"
   - "30-day readmission comparison"
   - "Compare readmission performance across facilities"
5. Optionally add **filters** (up to 3) — e.g., allow users to filter by `insurance_type` or `diagnosis`
6. Click **Apply**

#### Suggested Verified Answers for Healthcare Lab

| Visual | Trigger Phrases |
|--------|----------------|
| Readmission rate by facility (bar chart) | "readmission rate", "which facility has the most readmissions" |
| ED frequent flyers by insurance type (table) | "frequent flyer patients", "ED high utilizers" |
| Average length of stay by diagnosis (bar chart) | "ALOS by diagnosis", "which diagnoses have the longest stays" |
| Claim denial rate by payer (pie/bar chart) | "denial rate", "which payer denies the most claims" |
| Population health — chronic conditions (stacked bar) | "chronic disease prevalence", "how many patients have diabetes" |

> **Verified answers show a ✅ checkmark** in Copilot, signaling to users that the response was human-reviewed and approved — building trust in the AI output.

---

### Testing Your Prep Data for AI Configuration

After configuring all three features, test them in Power BI Desktop:

1. Open the **Copilot pane** in Power BI Desktop
2. Use the **skill picker** (dropdown in the Copilot chat box) → select **Answer questions about the data**
3. Test your AI Instructions:
   - Ask: *"What is the readmission rate at Metro General?"*
   - Copilot should use the correct 30-day definition and reference the right table
4. Test your Verified Answers:
   - Ask: *"Show readmission rates by facility"*
   - You should see your pinned visual with a ✅ verified checkmark
5. Test your AI Data Schema:
   - Ask a question referencing a hidden field — Copilot should **not** use it
   - Ask a question referencing a visible field — Copilot should answer correctly

> **Tip:** After each change to the Prep data for AI settings, close and reopen the Copilot pane to refresh.

### Mark Your Model as Approved for Copilot

Once you're satisfied with the configuration:

1. Go to the **Power BI service** and find your semantic model
2. Click the **Settings** icon
3. Expand the **Approved for Copilot** section
4. Check the **Approved for Copilot** box → click **Apply**

This removes friction treatments (disclaimers) from Copilot answers for your model, signaling that the data is curated and trusted.

---

## 💡 Discussion: Data Preparation Best Practices for AI

**Why this step matters:**
- Data Agents generate SQL/queries from natural language. If table/column names are cryptic, the AI struggles
- Pre-joined views reduce the chance of incorrect joins
- A data dictionary gives the AI explicit context about the data
- AI Data Schema, Instructions, and Verified Answers provide **semantic model–level** context that Copilot uses for Power BI Q&A

**Production considerations:**
- Schedule data quality notebooks to run daily
- Set up alerts for data quality SLA violations (e.g., null rate > 5%)
- Version your data dictionary alongside schema changes
- Consider implementing Great Expectations or similar frameworks for enterprise-grade data quality
- Keep AI Instructions updated as business rules change (e.g., new facilities, changed readmission window)
- Review and refresh Verified Answers quarterly as dashboards evolve

---

## ✅ Module 6 Checklist

Before moving to Module 7, confirm:

- [ ] Data quality checks ran successfully across all Gold tables
- [ ] Referential integrity validated (no orphan IDs)
- [ ] `data_dictionary` table created with column descriptions
- [ ] `gold_patient_360` table created (patient-level summary)
- [ ] `gold_facility_summary` table created (facility comparison metrics)
- [ ] AI Readiness Scorecard passed with all tables ready
- [ ] *(Optional)* AI Data Schema configured — irrelevant fields hidden from Copilot
- [ ] *(Optional)* AI Instructions added with healthcare terminology and analysis rules
- [ ] *(Optional)* Verified Answers set up for common healthcare questions
- [ ] *(Optional)* Semantic model marked as **Approved for Copilot**

---

**[← Module 5: Gen AI — Clinical Intelligence](Module05_GenAI_Clinical_Intelligence.md)** | **[Module 7: Data Agent →](Module07_Data_Agent.md)**
