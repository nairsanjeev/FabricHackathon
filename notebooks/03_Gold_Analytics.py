# =============================================================
# Notebook 03: Gold Analytics
# 
# Purpose: Create Gold layer business-ready analytics tables
#          with healthcare KPIs and aggregated metrics
#
# Instructions:
#   1. Create a notebook in Fabric named "03 - Gold Analytics"
#   2. Attach your HealthcareLakehouse
#   3. Paste each section into a separate cell (or all in one cell)
# =============================================================
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# =============================================================
# Gold Readmissions (30-day)
# Critical CMS quality measure under HRRP
# =============================================================
encounters = spark.table("silver_encounters") \
    .filter(col("encounter_type") == "Inpatient")

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

total = gold_readmissions.count()
readmitted = gold_readmissions.filter(col("was_readmitted") == True).count()
rate = (readmitted / total * 100) if total > 0 else 0
print(f"✓ gold_readmissions: {total} index admissions, {readmitted} readmitted ({rate:.1f}%)")

# =============================================================
# Gold ED Utilization
# Frequent flyer identification
# =============================================================
encounters = spark.table("silver_encounters")
patients = spark.table("silver_patients")

ed_visits = encounters \
    .filter(col("encounter_type") == "ED") \
    .groupBy("patient_id", "encounter_year") \
    .agg(
        count("*").alias("ed_visit_count"),
        countDistinct("facility_name").alias("facilities_visited"),
        sum("total_charges").alias("total_ed_charges")
    )

ed_frequent_flyers = ed_visits \
    .withColumn("is_frequent_flyer", when(col("ed_visit_count") >= 4, True).otherwise(False)) \
    .join(patients.select("patient_id", "first_name", "last_name", "age", "insurance_type", "risk_score"),
          "patient_id", "left")

ed_frequent_flyers.write.mode("overwrite").format("delta").saveAsTable("gold_ed_utilization")
frequent_flyers = ed_frequent_flyers.filter(col("is_frequent_flyer") == True).count()
print(f"✓ gold_ed_utilization: {ed_frequent_flyers.count()} records, {frequent_flyers} frequent flyers")

# =============================================================
# Gold ALOS by Diagnosis
# =============================================================
encounters = spark.table("silver_encounters")

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

# =============================================================
# Gold Encounter Summary
# =============================================================
encounters = spark.table("silver_encounters")
patients = spark.table("silver_patients")

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

# =============================================================
# Gold Financial Analysis
# =============================================================
claims = spark.table("silver_claims")
encounters = spark.table("silver_encounters")

gold_financial = claims \
    .join(
        encounters.select("encounter_id", "encounter_type", "facility_name",
                         "department", "primary_diagnosis_description",
                         "encounter_month", "encounter_year"),
        "encounter_id", "left"
    )

gold_financial.write.mode("overwrite").format("delta").saveAsTable("gold_financial")
print(f"✓ gold_financial: {gold_financial.count()} rows")

# =============================================================
# Gold Population Health
# =============================================================
conditions = spark.table("silver_conditions")
patients = spark.table("silver_patients")

patient_condition_count = conditions \
    .filter(col("condition_type") == "Chronic") \
    .groupBy("patient_id") \
    .agg(
        count("*").alias("chronic_condition_count"),
        collect_set("condition_category").alias("condition_list")
    )

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

print("\n✅ All Gold layer tables created!")
