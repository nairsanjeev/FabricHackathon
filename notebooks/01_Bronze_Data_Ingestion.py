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
# In Fabric, each Lakehouse has a "Files" area (for unstructured/raw files)
# and a "Tables" area (for Delta tables). We read from Files and write to Tables.
raw_path = "Files/raw"

# ── Bronze Ingestion Pattern ──────────────────────────────────
# For each CSV, we:
#   1. Read it with Spark (distributed CSV parsing across the cluster)
#   2. Write it as a Delta table (adds ACID transactions, schema, time travel)
#
# WHY DELTA instead of keeping CSVs?
#   - CSVs have no schema enforcement (a string "abc" can appear in an age column)
#   - CSVs don't support UPDATE/DELETE (needed for GDPR patient data removal)
#   - Delta tables support time travel: spark.table("bronze_patients").version(3)
#   - Delta tables are 3-10x faster to query than CSV (columnar Parquet format)

for file_name in csv_files:
    print(f"Ingesting {file_name}...")
    
    # spark.read.format("csv") — Spark's built-in CSV reader
    # Options explained:
    #   header=true    → First row contains column names, not data
    #   inferSchema    → Spark samples the data to guess types (int, string, double)
    #                    This is fine for Bronze; Silver will cast to exact types
    #   multiLine=true → Some clinical notes contain newlines within a single field
    #   escape='"'     → Handle quoted fields that contain commas (common in notes)
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .load(f"{raw_path}/{file_name}.csv")
    
    # Naming convention: bronze_ prefix clearly marks these as raw, unprocessed
    # This prevents confusion when analysts see both bronze_patients and silver_patients
    table_name = f"bronze_{file_name}"
    
    # mode("overwrite") — Replace the table if it already exists
    # This makes the notebook idempotent (safe to re-run without duplicating data)
    # format("delta") — Write as Delta Lake table (Parquet + transaction log)
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

# ── Exploration Strategy ─────────────────────────────────────
# Before transforming data, always explore it first to understand:
#   1. Data distribution — Is the sample realistic?
#   2. Cardinality — How many unique values per category?
#   3. Potential issues — Nulls, unexpected values, skewed data?
#
# In healthcare, specific distributions tell us if data is realistic:
#   - Insurance: Medicare ~40%, Commercial ~30%, Medicaid ~20%, Self-Pay ~10%
#   - Gender: Roughly 50/50 with slight female majority
#   - Encounter types: Outpatient should be highest volume

# --- Patient Demographics ---
# Why this matters: Insurance type drives reimbursement rates, 
# denial patterns, and regulatory requirements. Medicare patients
# trigger CMS quality reporting obligations.
print("=== Patient Demographics ===")
patients_df = spark.table("bronze_patients")

# groupBy().count() — Spark's equivalent of SQL: SELECT insurance_type, COUNT(*) GROUP BY
# This runs distributed across the Spark cluster (fast even on millions of rows)
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
# We expect chronic conditions (hypertension, diabetes) to dominate,
# matching national prevalence data.
print("\n=== Top 10 Diagnoses ===")
encounters_df.groupBy("primary_diagnosis_description") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10, truncate=False)

# --- Facility Distribution ---
# Why this matters: Volume distribution affects capacity planning, 
# nurse-to-patient ratios, and capital investment decisions.
# We want roughly even distribution across our 3 hospitals.
print("\n=== Facilities ===")
encounters_df.groupBy("facility_name").count().orderBy("count", ascending=False).show()

# --- Record Counts Summary ---
# Quick validation: do all tables have data? Any empty tables 
# would indicate a file upload or ingestion problem.
print("\n=== Bronze Table Summary ===")
for table in csv_files:
    count = spark.table(f"bronze_{table}").count()
    print(f"  bronze_{table}: {count} rows")
