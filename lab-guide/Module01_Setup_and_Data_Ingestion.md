# Module 1: Lakehouse Setup & Data Ingestion

| Duration | 30 minutes |
|----------|------------|
| Objective | Create a Fabric Lakehouse and load raw healthcare CSV data into the Bronze layer |
| Fabric Features | Lakehouse, File Upload, Spark Notebook |

---

## Pre-Requisite: Download the Lab Data Files from GitHub

Before starting this module, you need to download the data files from the lab's GitHub repository to your local machine.

### Option A: Clone the Repository (Recommended)

If you have **Git** installed, open a terminal or command prompt and run:

```bash
git clone https://github.com/nairsanjeev/FabricHackathon.git
```

The CSV data files will be in the `FabricHackathon/data/` folder.

### Option B: Download as ZIP

If you don't have Git installed:

1. Open your browser and go to [https://github.com/nairsanjeev/FabricHackathon](https://github.com/nairsanjeev/FabricHackathon)
2. Click the green **<> Code** button
3. Select **Download ZIP**
4. Extract the ZIP file to a location on your machine (e.g., `C:\FabricHackathon`)
5. Verify that the `data/` folder contains the following 7 CSV files:
   - `patients.csv`
   - `encounters.csv`
   - `conditions.csv`
   - `medications.csv`
   - `vitals.csv`
   - `clinical_notes.csv`
   - `claims.csv`

> **Note:** If you have already downloaded or received the lab materials from your instructor, you can skip this step.

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
5. **Important:** If you see a checkbox for **Enable Schemas (Public Preview)**, leave it **unchecked**. This lab uses simple table names and enabling schemas will cause errors.
6. Click **Create**

You will be taken to the Lakehouse explorer view, which shows two main sections:
- **Tables** — This is where your Delta tables (structured data) will live
- **Files** — This is where you can store raw files (CSV, Parquet, JSON, etc.)

---

## Step 3: Upload the CSV Data Files to the Lakehouse

Now we'll upload the synthetic healthcare CSV files to the Lakehouse `Files/raw/` folder. Choose **one** of the two options below:

---

### Option A: Manual Upload (Browser)

1. In the Lakehouse explorer, click on **Files** in the left panel
2. Click the **⋯ (ellipsis)** next to **Files** and select **New subfolder**
3. Name the subfolder: `raw`
4. Click **Create**
5. Click into the `raw` folder
6. Click **Upload** → **Upload files**
7. Navigate to `C:\FabricHackathon\data` on your local machine and select **all 7 CSV files**:
   - `patients.csv`
   - `encounters.csv`
   - `conditions.csv`
   - `medications.csv`
   - `vitals.csv`
   - `clinical_notes.csv`
   - `claims.csv`
8. Click **Upload**

Wait for all files to finish uploading. You should see all 7 files listed in the `raw` folder.

---

### Option B: Use a Pipeline to Copy Files (No-Code)

Instead of manually uploading through the browser, you can use a **Pipeline** with the **Copy Data assistant** to move files from your local machine into the Lakehouse. This mirrors how production data ingestion works.

> **Reference:** This approach follows the same pattern as the Microsoft Learn quickstart: [Create your first pipeline to copy data](https://learn.microsoft.com/en-us/fabric/data-factory/create-first-pipeline-with-sample-data).

#### B.1 Create a Pipeline

1. Go back to your workspace (click the workspace name in the breadcrumb)
2. Click **+ New item**
3. Search for and select **Pipeline**
4. Enter the name: `Upload Raw Files`
5. Click **Create**

#### B.2 Launch the Copy Data Assistant

1. In the pipeline canvas, click **Copy data assistant**

#### B.3 Configure the Source (Your Local Files)

1. On the **Choose data source** page, search for and select **File system** as the data source
2. Create a new connection:
   - **Connection name**: `LocalFiles`
   - **Host**: `localhost`  
   - **User name / Password**: Your Windows credentials (or leave blank if running locally with the on-premises data gateway)
3. Click **Connect**
4. For **File path or folder**, browse to or enter: `C:\FabricHackathon\data`
5. Check **Recursively** (not strictly needed here, but harmless)
6. Set **File format** to **DelimitedText** (CSV)
7. Check **First row as header**
8. Click **Next** — you'll see a preview of the files

> **Note:** Connecting to a local file system requires the **On-premises Data Gateway** to be installed on your machine. If you don't have a gateway configured, use **Option A** (manual upload) instead. See [Install an on-premises data gateway](https://learn.microsoft.com/en-us/data-integration/gateway/service-gateway-install) for setup instructions.

#### B.4 Configure the Destination (Lakehouse Files)

1. Select **Lakehouse** as the destination
2. Select your `HealthcareLakehouse`
3. Set **Root folder** to **Files**
4. Set **Folder path** to: `raw`
5. Set **File format** to **DelimitedText**
6. Click **Next**

#### B.5 Review and Run

1. On the **Review + create** page, verify:
   - **Source**: Local file system → `C:\FabricHackathon\data`
   - **Destination**: `Files/raw/` in `HealthcareLakehouse`
2. Leave **Start data transfer immediately** checked
3. Click **Save + Run**

The pipeline will copy all 7 CSV files from your local folder into the `Files/raw/` folder in the Lakehouse. This typically completes in under a minute.

---

### Verify the Upload

Regardless of which option you chose:

1. Go to your `HealthcareLakehouse`
2. Navigate to **Files** → **raw**
3. Confirm all 7 CSV files are present
4. Click on any CSV file (e.g., `patients.csv`) to preview its contents — you should see columns like `patient_id`, `first_name`, `last_name`, etc.

---

## Step 4: Create the Bronze Ingestion Notebook

Now we'll create a Spark notebook that reads the raw CSV files and saves them as Delta tables — our **Bronze layer**. The Bronze layer contains the data exactly as it arrived, with no transformations.

### 4.1 Create a New Notebook

1. Click on your workspace name in the breadcrumb at the top to go back to the workspace
2. Click **+ New item**
3. Search for and select **Notebook**
4. Click the notebook name at the top (e.g., "Notebook 1") and rename it to: `01 - Bronze Data Ingestion`
5. In the **Explorer** pane on the left, click **Add data items** → **From OneLake catalog**
6. Search for `HealthcareLakehouse` in the OneLake catalog
7. You will see **two items** with the same name — one is the **Lakehouse** and the other is the **SQL Analytics Endpoint** (shown with a different icon). **Select the Lakehouse item** (it has a blue house/database icon, not the SQL endpoint icon). If unsure, click on the item to view its details and confirm the type is **Lakehouse**.
8. Click **Add** to attach it

> ⚠️ **Session Note:** If your Spark session expires or is stopped at any point, you will need to re-run all cells from the top using **Run all**. Fabric does not preserve variables, imports, or DataFrames across session restarts.

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
