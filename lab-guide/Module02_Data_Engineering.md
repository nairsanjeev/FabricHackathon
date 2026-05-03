# Module 2: Data Engineering — Silver & Gold Layers

| Duration | 45 minutes |
|----------|------------|
| Objective | Transform Bronze data into cleansed Silver tables and compute Gold-layer healthcare analytics |
| Fabric Features | Spark Notebooks, Delta Tables, SQL Analytics |

---

## What You Will Do

In this module, you will:
1. Create **Silver layer** tables — cleansed, validated, and properly typed data
2. Create **Gold layer** tables — business-ready analytics tables including:
   - 30-day hospital readmission calculations
   - Average Length of Stay (ALOS) by diagnosis
   - ED frequent flyer identification
   - Patient risk stratification
   - Claims denial analysis

---

## Part A: Silver Layer Transformations

The Silver layer cleanses and standardizes the raw Bronze data. We'll fix data types, add computed columns, and create relationships.

### Step 1: Create the Silver Transformations Notebook

1. Go to your workspace
2. Click **+ New item** → **Notebook**
3. Rename it to: `02 - Silver Transformations`
4. Attach your `HealthcareLakehouse`:
   - In the **Explorer** pane on the left, click **Add data items** → **From OneLake catalog**
   - Search for `HealthcareLakehouse`
   - ⚠️ You will see **two items** with the same name. One is the **Lakehouse** and the other is the **SQL Analytics Endpoint**. **Select the Lakehouse** (blue house/database icon). Click on the item details if needed to confirm the type is **Lakehouse**, not SQL Analytics Endpoint.
   - Click **Add**

> ⚠️ **Session Note:** If your Spark session expires or is stopped at any point, you will need to re-run all cells from the top using **Run all**. Fabric does not preserve variables, imports, or DataFrames across session restarts.

### Step 2: Silver Patients Table

Paste this code in the first cell and run it:

```python
# =============================================================
# Cell 1: Silver Patients
# Cleanse and enrich patient data
# =============================================================
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read Bronze patients
patients = spark.table("bronze_patients")

# Create Silver patients with proper types and computed columns
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
silver_patients.show(5)
```

### Step 3: Silver Encounters Table

Add a new cell and paste:

```python
# =============================================================
# Cell 2: Silver Encounters
# Cleanse encounters and add computed fields
# =============================================================

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
```

### Step 4: Silver Conditions Table

Add a new cell and paste:

```python
# =============================================================
# Cell 3: Silver Conditions
# Cleanse conditions with proper categorization
# =============================================================

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
```

### Step 5: Silver Claims, Medications, and Vitals

Add a new cell and paste:

```python
# =============================================================
# Cell 4: Silver Claims
# =============================================================

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

# =============================================================
# Silver Medications
# =============================================================

medications = spark.table("bronze_medications")

silver_medications = medications \
    .withColumn("start_date", to_date(col("start_date"))) \
    .withColumn("end_date", to_date(col("end_date")))

silver_medications.write.mode("overwrite").format("delta").saveAsTable("silver_medications")
print(f"✓ silver_medications: {silver_medications.count()} rows")

# =============================================================
# Silver Vitals
# =============================================================

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

# Silver Clinical Notes
clinical_notes = spark.table("bronze_clinical_notes")
silver_clinical_notes = clinical_notes \
    .withColumn("note_date", to_date(col("note_date")))

silver_clinical_notes.write.mode("overwrite").format("delta").saveAsTable("silver_clinical_notes")
print(f"✓ silver_clinical_notes: {silver_clinical_notes.count()} rows")

print("\n✅ All Silver tables created!")
```

---

## Part B: Gold Layer — Healthcare Analytics

The Gold layer contains business-ready tables with computed KPIs and aggregated metrics. This is what the Semantic Model and Power BI dashboards will consume.

### Step 6: Create the Gold Analytics Notebook

1. Go to your workspace
2. Click **+ New item** → **Notebook**
3. Rename it to: `03 - Gold Analytics`
4. Attach your `HealthcareLakehouse`:
   - In the **Explorer** pane on the left, click **Add data items** → **From OneLake catalog**
   - Search for `HealthcareLakehouse`
   - ⚠️ You will see **two items** with the same name. **Select the Lakehouse** (blue house/database icon), not the SQL Analytics Endpoint. Click on the item details if needed to confirm the type.
   - Click **Add**

> ⚠️ **Session Note:** If your Spark session expires or is stopped at any point, you will need to re-run all cells from the top using **Run all**. Fabric does not preserve variables, imports, or DataFrames across session restarts.

### Step 7: Calculate 30-Day Readmissions

This is one of the most important healthcare quality measures. We identify patients who were readmitted to the hospital within 30 days of discharge.

Paste in Cell 1:

```python
# =============================================================
# Cell 1: 30-Day Readmission Calculation
# This is a critical CMS quality measure (HRRP)
# =============================================================
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Get inpatient encounters
encounters = spark.table("silver_encounters") \
    .filter(col("encounter_type") == "Inpatient")

# Self-join to find readmissions within 30 days
# An encounter is a readmission if the same patient has another
# inpatient encounter within 30 days of a prior discharge
index_admissions = encounters.alias("idx")
readmissions = encounters.alias("readmit")

readmission_pairs = index_admissions.join(
    readmissions,
    (col("idx.patient_id") == col("readmit.patient_id")) &
    (col("readmit.encounter_date") > col("idx.discharge_date")) &
    (datediff(col("readmit.encounter_date"), col("idx.discharge_date")) <= 30) &
    (col("idx.encounter_id") != col("readmit.encounter_id")) &
    (col("idx.discharge_disposition") != "Expired") &
    (col("idx.discharge_disposition") != "Against Medical Advice"),
    "left"
)

# Create the readmission fact table
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

# Print summary
total = gold_readmissions.count()
readmitted = gold_readmissions.filter(col("was_readmitted") == True).count()
rate = (readmitted / total * 100) if total > 0 else 0
print(f"✓ gold_readmissions: {total} index admissions")
print(f"  Readmitted within 30 days: {readmitted} ({rate:.1f}%)")

# Show readmission rate by diagnosis
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
```

### Step 8: ED Utilization & Frequent Flyers

Add a new cell:

```python
# =============================================================
# Cell 2: ED Utilization Analysis
# Identify frequent flyers and ED patterns
# =============================================================

encounters = spark.table("silver_encounters")
patients = spark.table("silver_patients")

# ED visits per patient per year
ed_visits = encounters \
    .filter(col("encounter_type") == "ED") \
    .groupBy("patient_id", "encounter_year") \
    .agg(
        count("*").alias("ed_visit_count"),
        countDistinct("facility_name").alias("facilities_visited"),
        sum("total_charges").alias("total_ed_charges")
    )

# Identify frequent flyers (4+ ED visits in a year)
ed_frequent_flyers = ed_visits \
    .withColumn("is_frequent_flyer", when(col("ed_visit_count") >= 4, True).otherwise(False)) \
    .join(patients.select("patient_id", "first_name", "last_name", "age", "insurance_type", "risk_score"),
          "patient_id", "left")

ed_frequent_flyers.write.mode("overwrite").format("delta").saveAsTable("gold_ed_utilization")

# Print summary
total_ed_patients = ed_frequent_flyers.count()
frequent_flyers = ed_frequent_flyers.filter(col("is_frequent_flyer") == True).count()
print(f"✓ gold_ed_utilization: {total_ed_patients} patient-year records")
print(f"  Frequent flyers (4+ visits/year): {frequent_flyers}")

print("\nED Visit Distribution:")
ed_frequent_flyers.groupBy("ed_visit_count").count().orderBy("ed_visit_count").show()
```

### Step 9: Average Length of Stay (ALOS) by Diagnosis

Add a new cell:

```python
# =============================================================
# Cell 3: Average Length of Stay (ALOS)
# Key operational metric for hospital efficiency
# =============================================================

encounters = spark.table("silver_encounters")

# ALOS by diagnosis for inpatient encounters
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
```

### Step 10: Patient Volume Summary

Add a new cell:

```python
# =============================================================
# Cell 4: Patient Volume & Encounter Summary
# Monthly volume trends by facility, type, and department
# =============================================================

encounters = spark.table("silver_encounters")
patients = spark.table("silver_patients")

# Encounter summary with patient demographics
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

# Monthly volume trends
print("\nMonthly Encounter Volumes:")
gold_encounter_summary.groupBy("encounter_month") \
    .agg(
        count("*").alias("total_encounters"),
        sum(when(col("encounter_type") == "ED", 1).otherwise(0)).alias("ed_visits"),
        sum(when(col("encounter_type") == "Inpatient", 1).otherwise(0)).alias("inpatient"),
        sum(when(col("encounter_type") == "Outpatient", 1).otherwise(0)).alias("outpatient")
    ) \
    .orderBy("encounter_month") \
    .show(30)
```

### Step 11: Claims & Financial Analysis

Add a new cell:

```python
# =============================================================
# Cell 5: Claims & Financial Analysis
# Revenue, denials, and payer mix analysis
# =============================================================

claims = spark.table("silver_claims")
encounters = spark.table("silver_encounters")

# Join claims with encounter details
gold_financial = claims \
    .join(
        encounters.select("encounter_id", "encounter_type", "facility_name",
                         "department", "primary_diagnosis_description",
                         "encounter_month", "encounter_year"),
        "encounter_id", "left"
    )

gold_financial.write.mode("overwrite").format("delta").saveAsTable("gold_financial")

# Denial analysis by payer
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
```

### Step 12: Patient Chronic Condition Summary

Add a new cell:

```python
# =============================================================
# Cell 6: Population Health — Chronic Condition Summary
# =============================================================

conditions = spark.table("silver_conditions")
patients = spark.table("silver_patients")

# Count chronic conditions per patient
patient_condition_count = conditions \
    .filter(col("condition_type") == "Chronic") \
    .groupBy("patient_id") \
    .agg(
        count("*").alias("chronic_condition_count"),
        collect_set("condition_category").alias("condition_list")
    )

# Create population health view
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
```

---

## Step 13: Verify Your Gold Tables

Go back to your `HealthcareLakehouse` and refresh the Tables section. You should now have the following Gold tables:

| Table | Description | Key Metric |
|-------|-------------|------------|
| `gold_readmissions` | 30-day readmission tracking | Readmission rate % |
| `gold_ed_utilization` | ED visit patterns per patient | Frequent flyer count |
| `gold_alos` | Avg length of stay by diagnosis/facility | ALOS days |
| `gold_encounter_summary` | All encounters with demographics | Volume trends |
| `gold_financial` | Claims with denial analysis | Denial rate %, collection rate |
| `gold_population_health` | Patient chronic condition profile | Multimorbidity, prevalence |

Plus your Silver tables:
| Table | Description |
|-------|-------------|
| `silver_patients` | Cleansed patients with age groups and risk categories |
| `silver_encounters` | Encounters with computed month, LOS categories |
| `silver_conditions` | Conditions with clinical categories |
| `silver_claims` | Claims with payment ratios |
| `silver_medications` | Medications with proper dates |
| `silver_vitals` | Vitals with SIRS flag |
| `silver_clinical_notes` | Clinical notes with proper dates |

---

## 💡 Discussion: Why These Metrics Matter

Take a moment to discuss with your table:

1. **Readmission Rate:** Your hospital's rate is around 15%. The national average is similar. What would a 1% reduction mean in CMS penalty savings?
2. **ED Frequent Flyers:** These patients often have unmanaged chronic conditions. How could proactive outreach reduce ED burden?
3. **ALOS:** Sepsis patients stay much longer than average. What does this mean for bed capacity and staffing?
4. **Denial Rates:** Notice how Medicare Advantage denial rates are higher than traditional Medicare. What administrative costs does this create?

---

## ✅ Module 2 Checklist

Before moving to Module 3, confirm:

- [ ] 7 Silver tables created and populated
- [ ] 6 Gold tables created with computed metrics
- [ ] Readmission rates are being calculated correctly (~15%)
- [ ] You can see ED frequent flyer patients identified
- [ ] Financial denial analysis shows payer-level differences
- [ ] Notebooks `02 - Silver Transformations` and `03 - Gold Analytics` ran successfully

---

**[← Module 1: Setup & Ingestion](Module01_Setup_and_Data_Ingestion.md)** | **[Module 3: Semantic Model & Dashboard →](Module03_Semantic_Model_and_Dashboard.md)**
