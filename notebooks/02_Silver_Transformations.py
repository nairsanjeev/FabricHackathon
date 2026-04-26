# =============================================================
# Notebook 02: Silver Transformations
# 
# Purpose: Cleanse Bronze data and create Silver layer tables
#          with proper types, computed columns, and standardization
#
# Instructions:
#   1. Create a notebook in Fabric named "02 - Silver Transformations"
#   2. Attach your HealthcareLakehouse
#   3. Paste each section into a separate cell (or all in one cell)
# =============================================================
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =============================================================
# Silver Patients
# =============================================================
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

# =============================================================
# Silver Encounters
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

# =============================================================
# Silver Conditions
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

# =============================================================
# Silver Claims
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

# =============================================================
# Silver Clinical Notes
# =============================================================
clinical_notes = spark.table("bronze_clinical_notes")
silver_clinical_notes = clinical_notes \
    .withColumn("note_date", to_date(col("note_date")))

silver_clinical_notes.write.mode("overwrite").format("delta").saveAsTable("silver_clinical_notes")
print(f"✓ silver_clinical_notes: {silver_clinical_notes.count()} rows")

print("\n✅ All Silver tables created!")
