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

## Alternate Path: Pipeline with Copy Data Assistant (No-Code Bronze Ingestion)

Instead of writing a Spark notebook for Bronze ingestion, you can use a **Fabric Pipeline** with the **Copy Data assistant** — a no-code/low-code approach. This method is common in production environments where data engineers want visual, schedulable, and monitorable pipelines.

> **Assumption:** You have already downloaded the lab data files to your local machine at `C:\FabricHackathon\data` (see the Pre-Requisite section at the top of this module).

### Why Choose a Pipeline?

| Approach | Best For |
|----------|----------|
| **Notebook** (Steps 4–5 above) | Learning, flexibility, custom logic, rapid prototyping |
| **Pipeline** (this section) | Production workloads, scheduling, monitoring, no-code preference |

Both approaches produce identical Bronze Delta tables.

> **Reference:** This approach follows the same pattern as the Microsoft Learn quickstart: [Create your first pipeline to copy data](https://learn.microsoft.com/en-us/fabric/data-factory/create-first-pipeline-with-sample-data).

### Alt Step 1: Upload CSV Files to the Lakehouse

Before creating the pipeline, upload your local CSV files to the Lakehouse so the pipeline can access them as a source.

1. Go to your `HealthcareLakehouse`
2. In the Lakehouse explorer, click on **Files** in the left panel
3. Click the **⋯ (ellipsis)** next to **Files** and select **New subfolder**
4. Name the subfolder: `raw`
5. Click **Create**
6. Click into the `raw` folder
7. Click **Upload** → **Upload files**
8. Navigate to `C:\FabricHackathon\data` on your local machine and select **all 7 CSV files**:
   - `patients.csv`
   - `encounters.csv`
   - `conditions.csv`
   - `medications.csv`
   - `vitals.csv`
   - `clinical_notes.csv`
   - `claims.csv`
9. Click **Upload**

Wait for all files to finish uploading. You should see all 7 files listed in the `raw` folder.

### Alt Step 2: Create a Pipeline

1. Go to your workspace
2. Click **+ New item**
3. Search for and select **Pipeline**
4. Enter the name: `Bronze Ingestion Pipeline`
5. Click **Create**

You will be taken to the pipeline canvas where you can build your data flow.

### Alt Step 3: Copy the Entire Folder in One Shot

The fastest approach is a single **Copy activity** that points at the entire `raw/` folder. The Copy activity will automatically create one table per CSV file — no loops, no duplication, no expressions.

#### 3.1 Add a Copy Data Activity

1. In the pipeline canvas, click **Copy data assistant** (the button at the top of the canvas)

#### 3.2 Configure the Source (Folder-Level)

1. On the **Choose data source** page:
   - **Data store type**: Select **Workspace**
   - **Workspace data store type**: Select **Lakehouse**
   - **Lakehouse**: Select `HealthcareLakehouse`
   - **Root folder**: Select **Files**
   - Browse to the **`raw`** folder — select the **folder itself** (do NOT drill into individual files)
   - **File format**: Select **DelimitedText**
   - Check **First row as header**
2. Click **Next** — you'll see a preview showing all CSV files in the folder

#### 3.3 Configure the Destination

1. Select **Lakehouse** as the destination
2. Select your `HealthcareLakehouse`
3. Set **Root folder** to **Tables**
4. For **Load settings**, select **Load to new table**
5. Set **Table name** to: **`bronze_`** (a prefix — the Copy activity will append the source file name automatically)
   
   > **Note:** If the wizard does not allow a prefix, you can leave the table name mapping as-is. The activity will create tables named after the source files (e.g., `patients`, `encounters`). You can rename them later, or simply use `patients` instead of `bronze_patients` for the rest of the lab.

6. Click **Next**

#### 3.4 Review and Run

1. On the **Review + create** page, verify:
   - **Source**: `Files/raw/` (folder, multiple files)
   - **Destination**: `Tables/` in `HealthcareLakehouse`
2. Leave **Start data transfer immediately** checked
3. Click **Save + Run**

The single Copy activity will process all 7 CSV files and create the corresponding tables in your Lakehouse. This typically completes in under 2 minutes.

### Alt Step 4: Verify Results

1. Go to your `HealthcareLakehouse`
2. Under **Tables**, verify that tables have been created for all 7 datasets:
   - `bronze_patients` (or `patients`)
   - `bronze_encounters` (or `encounters`)
   - `bronze_conditions` (or `conditions`)
   - `bronze_medications` (or `medications`)
   - `bronze_vitals` (or `vitals`)
   - `bronze_clinical_notes` (or `clinical_notes`)
   - `bronze_claims` (or `claims`)
3. Click any table to preview the data

> **Tip:** If tables don't appear immediately, click the **Refresh** icon (🔄) in the Tables section header.

> **Note:** If the tables were created without the `bronze_` prefix, you can either rename them manually or simply use the file-name-based table names throughout the rest of the lab. The downstream notebooks reference `bronze_*` tables — adjust the table names in Module 2 if needed.

### Alt Step 5: Schedule the Pipeline (Optional)

In a production setting, you'd schedule this pipeline to run on a recurring basis:

1. Click **Schedule** in the pipeline toolbar
2. Set the frequency (e.g., Daily at 2:00 AM)
3. Enable the schedule

> **Key Takeaway:** The folder-level Copy activity is the simplest possible no-code approach — one activity, one configuration, all files ingested. No loops, no expressions, no duplication.

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
