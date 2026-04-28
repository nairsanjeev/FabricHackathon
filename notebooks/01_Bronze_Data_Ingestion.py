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
# ⚠️ SESSION NOTE:
#   If your Spark session expires or is stopped, you will need
#   to re-run all cells from the top. Fabric does not preserve
#   variables, imports, or DataFrames across session restarts.
#
# ================================================================


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 1 — MARKDOWN (paste as a Markdown cell in Fabric)       ║
# ╚════════════════════════════════════════════════════════════════╝
# 
# # 🏥 Bronze Data Ingestion — Raw Healthcare Data
# 
# ## 1. What this notebook does (business meaning)
# 
# This notebook implements the **Bronze layer** of the Medallion 
# Architecture (Bronze → Silver → Gold). The Bronze layer is the 
# **raw landing zone** — data arrives here exactly as received from 
# source systems, with no transformations, no type changes, and no 
# filtering.
# 
# We ingest 7 CSV files representing a year of operations at 
# **HealthFirst Medical Group** (3 hospitals, ~200 patients) and 
# convert them to Delta Lake tables for downstream processing.
# 
# ### Why keep raw data untouched?
# - **Auditability:** Regulators (CMS, Joint Commission) may require 
#   proof of what data was received and when
# - **Reprocessing:** If business logic changes, you can re-derive 
#   Silver/Gold from the original Bronze data
# - **Data lineage:** You can trace any downstream number back to 
#   its exact source record
# 
# ## 2. Step-by-step walkthrough of the logic
# 
# ### Step 1: Define the source file list
#     csv_files = ["patients", "encounters", "conditions", ...]
# - These 7 files map to common healthcare data categories from 
#   HL7 FHIR (Fast Healthcare Interoperability Resources):
#   - `patients` → FHIR Patient resource (demographics, insurance)
#   - `encounters` → FHIR Encounter resource (visits, admissions)
#   - `conditions` → FHIR Condition resource (diagnoses, ICD-10)
#   - `medications` → FHIR MedicationRequest (prescriptions)
#   - `vitals` → FHIR Observation (heart rate, BP, temp, SpO2)
#   - `clinical_notes` → FHIR DocumentReference (free-text notes)
#   - `claims` → FHIR Claim (billing, payments, denials)
# 
# ### Step 2: Set the source path
#     raw_path = "Files/raw"
# - In Fabric, each Lakehouse has two areas:
#   - **Files** — for unstructured/raw files (CSVs, PDFs, images)
#   - **Tables** — for Delta tables (structured, queryable)
# - We read from Files and write to Tables
# 
# ### Step 3: Read each CSV with Spark
#     df = spark.read.format("csv")
#         .option("header", "true")      → First row = column names
#         .option("inferSchema", "true")  → Auto-detect data types
#         .option("multiLine", "true")    → Handle newlines in fields
#         .option("escape", '"')          → Handle commas inside quotes
#         .load(f"{raw_path}/{file_name}.csv")
# - `header=true`: Without this, Spark treats the first row as data
# - `inferSchema=true`: Spark samples the data to guess types 
#   (int, string, double). This is fine for Bronze; Silver will 
#   cast to exact types
# - `multiLine=true`: Clinical notes may contain newlines within 
#   a single CSV field — without this, Spark splits one note across 
#   multiple rows
# - `escape='"'`: Fields containing commas are wrapped in quotes; 
#   this tells Spark to treat `"hello, world"` as one field
# 
# ### Step 4: Write as a Delta table
#     df.write.mode("overwrite").format("delta").saveAsTable(table_name)
# - `mode("overwrite")`: Replaces the table if it already exists. 
#   This makes the notebook **idempotent** — safe to re-run without 
#   duplicating data
# - `format("delta")`: Writes as Delta Lake (Parquet + transaction log)
# - `saveAsTable()`: Registers the table in the Lakehouse catalog 
#   so it appears in the Tables section and is queryable by SQL
# 
# ### Why Delta instead of keeping CSVs?
# | Feature | CSV | Delta |
# |---------|-----|-------|
# | Schema enforcement | ❌ No | ✅ Yes |
# | UPDATE/DELETE | ❌ No | ✅ Yes (GDPR compliance) |
# | Time travel | ❌ No | ✅ Yes (version history) |
# | Query performance | Slow (row scan) | Fast (columnar Parquet) |
# | ACID transactions | ❌ No | ✅ Yes |
# 
# ## 3. Summary
# 
# Each CSV file is read by Spark with schema inference and 
# multi-line support, then written as a Delta table with a 
# `bronze_` prefix (e.g., `bronze_patients`). Overwrite mode 
# ensures idempotency. The result is 7 Delta tables in the 
# Lakehouse Tables section, ready for Silver transformations.


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
# # 📊 Data Exploration — Validating the Bronze Layer
#
# ## 1. What this cell does (business meaning)
# 
# Before moving to the Silver layer, we validate that the data 
# loaded correctly and understand the clinical landscape of our 
# patient population. This is a critical data quality step — if 
# the raw data is wrong, every downstream metric will be wrong.
#
# ## 2. Step-by-step walkthrough
#
# ### Step 1: Patient Demographics — Insurance Type
#     patients_df.groupBy("insurance_type").count()
# - `groupBy().count()` is Spark's equivalent of SQL:
#   `SELECT insurance_type, COUNT(*) FROM bronze_patients GROUP BY`
# - This runs **distributed** across the Spark cluster (fast even 
#   on millions of rows)
# - **Why insurance type matters:** Insurance drives reimbursement 
#   rates, denial patterns, and regulatory requirements.
#   - Medicare patients trigger CMS quality reporting obligations
#   - Expected distribution: Medicare ~40%, Commercial ~30%, 
#     Medicaid ~20%, Self-Pay ~10%
#
# ### Step 2: Patient Demographics — Gender
#     patients_df.groupBy("gender").count()
# - Should be roughly 50/50 with a slight female majority 
#   (matching US demographics)
# - If 90%+ one gender, the data generator may have a bug
#
# ### Step 3: Encounter Types
#     encounters_df.groupBy("encounter_type").count()
# - Validates all 4 encounter types are present
# - **Cost context:** ED visits cost $2,000-5,000 each vs. 
#   $150-300 for outpatient. Volume distribution matters for 
#   financial planning
# - Expected: Outpatient > Ambulatory > Inpatient > ED
#
# ### Step 4: Top 10 Diagnoses
#     encounters_df.groupBy("primary_diagnosis_description")
#         .count().orderBy("count", ascending=False).show(10)
# - Top diagnoses drive resource allocation, staffing models, 
#   and quality improvement priorities
# - Expected: Chronic conditions (hypertension, diabetes, COPD) 
#   should dominate, matching national prevalence data
# - `truncate=False` shows full diagnosis names without cutting 
#   them off at 20 characters
#
# ### Step 5: Facility Distribution
#     encounters_df.groupBy("facility_name").count()
# - Validates visits are spread across all 3 hospitals
# - Volume distribution affects capacity planning, nurse-to-patient 
#   ratios, and capital investment decisions
# - We want roughly even distribution across facilities
#
# ### Step 6: Record Counts Summary
#     for table in csv_files:
#         count = spark.table(f"bronze_{table}").count()
# - Quick validation: do ALL 7 tables have data?
# - Any table with 0 rows indicates a file upload or ingestion bug
# - This is the most basic data quality check — "is it there?"
#
# ## 3. Summary
# 
# Data exploration queries validate that all 7 Bronze tables were 
# populated correctly, check for realistic distributions in patient 
# demographics, encounter types, diagnoses, and facility volumes, 
# and establish a baseline understanding of the clinical landscape 
# before applying Silver transformations.


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
