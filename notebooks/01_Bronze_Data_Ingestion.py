# ================================================================
# NOTEBOOK 01: BRONZE DATA INGESTION
# ================================================================
# 
# ┌─────────────────────────────────────────────────────────────┐
# │  MODULE 1 — LAKEHOUSE & DATA INGESTION                      │
# │  Fabric Capability: Lakehouse, Spark, Delta Lake             │
# └─────────────────────────────────────────────────────────────┘
#
# ── INSTRUCTIONS ──────────────────────────────────────────────
#   1. Create a notebook in Fabric named "01 - Bronze Data Ingestion"
#   2. Attach your HealthcareLakehouse
#   3. Create one cell per section below (each "CELL" block)
#   4. Run cells sequentially
#
# ================================================================


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 1 — MARKDOWN (paste as a Markdown cell in Fabric)       ║
# ╚════════════════════════════════════════════════════════════════╝
# 
# # 🏥 Bronze Data Ingestion — Raw Healthcare Data
# 
# ## What is the Bronze Layer?
# 
# In the **Medallion Architecture** (Bronze → Silver → Gold), the 
# Bronze layer is the **raw landing zone**. Data arrives here exactly 
# as it was received — no transformations, no type changes, no 
# filtering. Think of it as a digital "filing cabinet" for incoming data.
#
# ### Why keep raw data untouched?
# - **Auditability:** Regulators (CMS, Joint Commission) may require 
#   proof of what data was received and when
# - **Reprocessing:** If business logic changes, you can re-derive 
#   Silver/Gold from the original Bronze data
# - **Data lineage:** You can trace any downstream number back to 
#   its exact source record
#
# ### What we're loading
# We have 7 CSV files representing a year of operations at 
# **HealthFirst Medical Group** (3 hospitals, ~200 patients):
#
# | File | Records | Description |
# |------|---------|-------------|
# | patients.csv | 200 | Patient demographics, insurance, risk scores |
# | encounters.csv | ~1,000 | ED, Inpatient, Outpatient, Ambulatory visits |
# | conditions.csv | 428 | ICD-10 coded diagnoses (chronic & acute) |
# | medications.csv | 640 | Prescribed medications with dosages |
# | vitals.csv | 3,800+ | Heart rate, BP, temperature, SpO2, pain |
# | clinical_notes.csv | 150 | Free-text ED notes, discharge summaries |
# | claims.csv | ~1,000 | Billing: charges, payments, denials by payer |
#
# ### Technical approach
# We use **Apache Spark** (built into Fabric) to read each CSV and 
# write it as a **Delta table**. Delta Lake adds ACID transactions, 
# schema enforcement, and time-travel capabilities on top of Parquet 
# files — making it ideal for healthcare data governance.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 2 — CODE: Bronze Ingestion Loop                         ║
# ╚════════════════════════════════════════════════════════════════╝

# Define the 7 source CSV files matching HealthFirst's data domains
# These map to common healthcare data categories from HL7 FHIR resources:
#   patients    → FHIR Patient resource
#   encounters  → FHIR Encounter resource
#   conditions  → FHIR Condition resource
#   medications → FHIR MedicationRequest resource
#   vitals      → FHIR Observation resource (vital-signs category)
#   clinical_notes → FHIR DocumentReference resource
#   claims      → FHIR Claim resource

csv_files = [
    "patients",
    "encounters",
    "conditions",
    "medications",
    "vitals",
    "clinical_notes",
    "claims"
]

# Lakehouse path where we uploaded the raw CSVs
raw_path = "Files/raw"

# Ingest each CSV → Delta table
# We use inferSchema=true for Bronze (accept whatever types Spark detects).
# Proper type casting happens in the Silver layer.
for file_name in csv_files:
    print(f"Ingesting {file_name}...")
    
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .load(f"{raw_path}/{file_name}.csv")
    
    table_name = f"bronze_{file_name}"
    df.write.mode("overwrite").format("delta").saveAsTable(table_name)
    
    count = df.count()
    print(f"  ✓ {table_name}: {count} rows, {len(df.columns)} columns")

print("\n✅ Bronze layer ingestion complete!")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 3 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# # 📊 Quick Data Exploration
#
# Before moving to the Silver layer, let's verify the data loaded 
# correctly and understand the clinical landscape:
# 
# 1. **Patient demographics** — Do we have a realistic payer mix? 
#    Medicare should be ~40% since this patient population skews older
# 2. **Encounter types** — All 4 types? (ED, Inpatient, Outpatient, 
#    Ambulatory)
# 3. **Facility distribution** — Spread across all 3 hospitals?
# 4. **Top diagnoses** — Hypertension and diabetes should dominate, 
#    matching national chronic disease prevalence


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 4 — CODE: Data Exploration                               ║
# ╚════════════════════════════════════════════════════════════════╝

# --- Patient Demographics ---
# Why this matters: Insurance type drives reimbursement rates, 
# denial patterns, and regulatory requirements. Medicare patients
# trigger CMS quality reporting obligations.
print("=== Patient Demographics ===")
patients_df = spark.table("bronze_patients")
patients_df.groupBy("insurance_type").count().orderBy("count", ascending=False).show()
patients_df.groupBy("gender").count().show()

# --- Encounter Types ---
# Why this matters: Different encounter types have vastly different 
# cost profiles and quality measures. A single ED visit can cost  
# $2,000-5,000 vs. $150-300 for an outpatient visit.
print("\n=== Encounter Types ===")
encounters_df = spark.table("bronze_encounters")
encounters_df.groupBy("encounter_type").count().orderBy("count", ascending=False).show()

# --- Top Diagnoses ---
# Why this matters: Top diagnoses drive resource allocation, 
# staffing models, and quality improvement priorities.
print("\n=== Top 10 Diagnoses ===")
encounters_df.groupBy("primary_diagnosis_description") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10, truncate=False)

# --- Facility Distribution ---
# Why this matters: Volume distribution affects capacity planning, 
# nurse-to-patient ratios, and capital investment decisions.
print("\n=== Facilities ===")
encounters_df.groupBy("facility_name").count().orderBy("count", ascending=False).show()

# --- Record Counts Summary ---
print("\n=== Bronze Table Summary ===")
for table in csv_files:
    count = spark.table(f"bronze_{table}").count()
    print(f"  bronze_{table}: {count} rows")
