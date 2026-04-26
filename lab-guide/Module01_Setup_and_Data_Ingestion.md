# Module 1: Lakehouse Setup & Data Ingestion

| Duration | 60 minutes |
|----------|------------|
| Objective | Create a Fabric Lakehouse and load raw healthcare CSV data into the Bronze layer |
| Fabric Features | Lakehouse, File Upload, Spark Notebook |

---

## What You Will Do

In this module, you will:
1. Navigate to your Fabric workspace
2. Create a Lakehouse called `HealthcareLakehouse`
3. Upload the synthetic healthcare CSV files
4. Create a Spark notebook to read CSVs and write them as Delta tables (the **Bronze layer**)
5. Verify the Bronze tables are created correctly

---

## Step 1: Navigate to Your Fabric Workspace

1. Open your browser and go to [https://app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. Sign in with your lab credentials
3. In the left navigation pane, click **Workspaces**
4. Find and click on your assigned workspace (e.g., `Healthcare-Lab-[YourName]`)

> **Note:** Your workspace should already be created and assigned to a Fabric capacity. If you don't see your workspace, ask your lab instructor for assistance.

---

## Step 2: Create a Lakehouse

1. In your workspace, click **+ New item**
2. In the search box, type **Lakehouse**
3. Click **Lakehouse**
4. In the **Name** field, enter: `HealthcareLakehouse`
5. Click **Create**

You will be taken to the Lakehouse explorer view, which shows two main sections:
- **Tables** — This is where your Delta tables (structured data) will live
- **Files** — This is where you can store raw files (CSV, Parquet, JSON, etc.)

---

## Step 3: Upload the CSV Data Files

Now we'll upload the synthetic healthcare CSV files to the Lakehouse.

1. In the Lakehouse explorer, click on **Files** in the left panel
2. Click the **⋯ (ellipsis)** next to **Files** and select **New subfolder**
3. Name the subfolder: `raw`
4. Click **Create**
5. Click into the `raw` folder
6. Click **Upload** → **Upload files**
7. Navigate to the `data/` folder from the lab materials and select **all 7 CSV files**:
   - `patients.csv`
   - `encounters.csv`
   - `conditions.csv`
   - `medications.csv`
   - `vitals.csv`
   - `clinical_notes.csv`
   - `claims.csv`
8. Click **Upload**

Wait for all files to finish uploading. You should see all 7 files listed in the `raw` folder.

> **Verify:** Click on any CSV file (e.g., `patients.csv`) to preview its contents. You should see columns like `patient_id`, `first_name`, `last_name`, etc.

---

## Step 4: Create the Bronze Ingestion Notebook

Now we'll create a Spark notebook that reads the raw CSV files and saves them as Delta tables — our **Bronze layer**. The Bronze layer contains the data exactly as it arrived, with no transformations.

### 4.1 Create a New Notebook

1. Click on your workspace name in the breadcrumb at the top to go back to the workspace
2. Click **+ New item**
3. Search for and select **Notebook**
4. Click the notebook name at the top (e.g., "Notebook 1") and rename it to: `01 - Bronze Data Ingestion`
5. In the left panel of the notebook, you will see the **Lakehouse** section. Click **Add** to attach your Lakehouse
6. Select **Existing Lakehouse** → **Add**
7. Find and select `HealthcareLakehouse`, then click **Add**

### 4.2 Add the Ingestion Code

In the first cell of your notebook, paste the following code:

```python
# =============================================================
# Cell 1: Bronze Data Ingestion
# Read raw CSV files and save as Delta tables in the Lakehouse
# =============================================================

# Define the list of CSV files to ingest
csv_files = [
    "patients",
    "encounters",
    "conditions",
    "medications",
    "vitals",
    "clinical_notes",
    "claims"
]

# Base path for raw files in the Lakehouse
raw_path = "Files/raw"

# Ingest each CSV file as a Bronze Delta table
for file_name in csv_files:
    print(f"Ingesting {file_name}...")
    
    # Read CSV with header and infer schema
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .load(f"{raw_path}/{file_name}.csv")
    
    # Write as Delta table in the Tables section
    table_name = f"bronze_{file_name}"
    df.write.mode("overwrite").format("delta").saveAsTable(table_name)
    
    # Print summary
    count = df.count()
    print(f"  ✓ {table_name}: {count} rows, {len(df.columns)} columns")

print("\n✅ Bronze layer ingestion complete!")
```

### 4.3 Run the Notebook

1. Click the **▶ Run all** button at the top of the notebook
2. Wait for the notebook to start a Spark session (this may take 1-2 minutes the first time)
3. Watch the output as each table is created

You should see output like:
```
Ingesting patients...
  ✓ bronze_patients: 200 rows, 13 columns
Ingesting encounters...
  ✓ bronze_encounters: 998 rows, 15 columns
Ingesting conditions...
  ✓ bronze_conditions: 428 rows, 8 columns
...
✅ Bronze layer ingestion complete!
```

---

## Step 5: Verify the Bronze Tables

### 5.1 Check in the Lakehouse Explorer

1. Go back to your `HealthcareLakehouse`
2. In the left panel under **Tables**, you should now see 7 tables:
   - `bronze_patients`
   - `bronze_encounters`
   - `bronze_conditions`
   - `bronze_medications`
   - `bronze_vitals`
   - `bronze_clinical_notes`
   - `bronze_claims`
3. Click on any table to preview its data

> **Tip:** If you don't see the tables, click the **Refresh** icon (🔄) in the Tables section header.

### 5.2 Quick Data Exploration (Optional)

Add a new cell to your notebook and run the following to explore the data:

```python
# =============================================================
# Cell 2: Quick Data Exploration
# =============================================================

# Check patient demographics
print("=== Patient Demographics ===")
patients_df = spark.table("bronze_patients")
patients_df.groupBy("insurance_type").count().orderBy("count", ascending=False).show()
patients_df.groupBy("gender").count().show()

print("\n=== Encounter Types ===")
encounters_df = spark.table("bronze_encounters")
encounters_df.groupBy("encounter_type").count().orderBy("count", ascending=False).show()

print("\n=== Top 10 Diagnoses ===")
encounters_df.groupBy("primary_diagnosis_description") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10, truncate=False)

print("\n=== Facilities ===")
encounters_df.groupBy("facility_name").count().orderBy("count", ascending=False).show()
```

You should see a mix of insurance types (Medicare ~40%, Commercial ~30%, Medicaid ~20%), encounter types (ED, Inpatient, Outpatient, Ambulatory), and common diagnoses like hypertension, diabetes, and heart failure.

---

## Understanding the Medallion Architecture

In this lab, we follow the **Medallion Architecture** (Bronze → Silver → Gold), a proven pattern for organizing data in a Lakehouse:

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│              │    │              │    │              │
│    BRONZE    │───▶│    SILVER    │───▶│     GOLD     │
│              │    │              │    │              │
│  Raw data    │    │  Cleansed,   │    │  Business-   │
│  as-is from  │    │  validated,  │    │  ready       │
│  source      │    │  conformed   │    │  aggregates  │
│              │    │              │    │  & metrics   │
└──────────────┘    └──────────────┘    └──────────────┘
  ↑                                       ↑
  You are here                            Module 2 builds this
```

- **Bronze:** Raw data exactly as ingested — what you just created
- **Silver:** Cleaned, validated, and joined data with proper data types and relationships
- **Gold:** Business-level aggregates, KPIs, and analytics-ready tables

---

## ✅ Module 1 Checklist

Before moving to Module 2, confirm:

- [ ] Lakehouse `HealthcareLakehouse` is created
- [ ] 7 CSV files are uploaded to `Files/raw/`
- [ ] 7 Bronze Delta tables exist in the Tables section
- [ ] You can preview data in each table
- [ ] Your notebook `01 - Bronze Data Ingestion` ran successfully

---

**[← Back to Overview](../README.md)** | **[Module 2: Data Engineering →](Module02_Data_Engineering.md)**
