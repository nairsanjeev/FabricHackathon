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

# Claims summary per patient
claims_summary = claims.groupBy(
    encounters.select("encounter_id", "patient_id")
    .join(claims, "encounter_id")
    .select("patient_id")
    .distinct()
    .columns[0]  # patient_id
)

# Simpler approach: join claims through encounters
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

## 💡 Discussion: Data Preparation Best Practices for AI

**Why this step matters:**
- Data Agents generate SQL/queries from natural language. If table/column names are cryptic, the AI struggles
- Pre-joined views reduce the chance of incorrect joins
- A data dictionary gives the AI explicit context about the data

**Production considerations:**
- Schedule data quality notebooks to run daily
- Set up alerts for data quality SLA violations (e.g., null rate > 5%)
- Version your data dictionary alongside schema changes
- Consider implementing Great Expectations or similar frameworks for enterprise-grade data quality

---

## ✅ Module 6 Checklist

Before moving to Module 7, confirm:

- [ ] Data quality checks ran successfully across all Gold tables
- [ ] Referential integrity validated (no orphan IDs)
- [ ] `data_dictionary` table created with column descriptions
- [ ] `gold_patient_360` table created (patient-level summary)
- [ ] `gold_facility_summary` table created (facility comparison metrics)
- [ ] AI Readiness Scorecard passed with all tables ready

---

**[← Module 5: Gen AI — Clinical Intelligence](Module05_GenAI_Clinical_Intelligence.md)** | **[Module 7: Data Agent →](Module07_Data_Agent.md)**
