# Module 6: Prep Data for AI

| Duration | 30 minutes |
|----------|------------|
| Objective | Prepare healthcare data for AI consumption by validating quality, creating feature-ready views, and building AI-optimized tables that the Data Agent and future ML models can use effectively |
| Fabric Features | Spark Notebooks, Delta Lake, Data Quality Checks, Semantic Link, Fabric AI Services |

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
3. Validate **referential integrity** across tables
4. Create **materialized features** for common AI/ML use cases
5. **Audit your Semantic Model** for AI readiness using Semantic Link and Fabric's built-in LLM

---

## Part A: Data Quality Assessment

### Step 1: Create a New Notebook

1. Go to your workspace
2. Click **+ New item** → **Notebook**
3. Rename to: `06 - Prep Data for AI`
4. Attach the notebook to your `HealthcareLakehouse`:
   - In the **Explorer** pane on the left, click **Add data items** → **From OneLake catalog**
   - Search for `HealthcareLakehouse`
   - ⚠️ You will see **two items** with the same name. **Select the Lakehouse** (blue house/database icon), not the SQL Analytics Endpoint. Click on the item details if needed to confirm the type.
   - Click **Add**

> ⚠️ **Session Note:** If your Spark session expires or is stopped at any point, you will need to re-run all cells from the top using **Run all**. Fabric does not preserve variables, imports, or DataFrames across session restarts.

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

## Part C: Create AI-Friendly Summary Views

### Step 4: Build a Patient 360° View

The Data Agent often needs to answer questions like "Tell me about patient X" which requires joining many tables. Creating a pre-joined view makes this instant.

Paste in Cell 3:

```python
# =============================================================
# Cell 3: Patient 360° View — AI-Ready Summary
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

# Claims summary per patient (join claims through encounters)
patient_claims = encounters.select("encounter_id") \
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

### Step 5: Create a Facility Performance Summary

Paste in Cell 4:

```python
# =============================================================
# Cell 4: Facility Performance Summary — AI-Ready
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

### Step 5B: Create a Chronic Conditions Summary (Copilot-Friendly)

The `gold_population_health` table stores chronic conditions as **boolean flag columns** (`has_diabetes`, `has_heart_failure`, etc.). This is efficient for storage but hard for Copilot to query — when a user asks "What are the most common chronic conditions?", Copilot can't easily count across multiple boolean columns.

This cell **unpivots** those flags into a simple table with one row per patient-condition, making it trivially queryable.

Paste in Cell 4B:

```python
# =============================================================
# Cell 5B: Chronic Conditions Summary — Copilot-Friendly
# =============================================================
# Unpivots boolean condition flags into rows so Copilot can
# answer "most common conditions" with a simple GROUP BY.
# =============================================================

from pyspark.sql.functions import *
from functools import reduce

pop_health = spark.table("gold_population_health")
patients = spark.table("silver_patients")

# Define the condition flag columns and their display names
condition_flags = {
    "has_diabetes": "Diabetes",
    "has_heart_failure": "Heart Failure",
    "has_copd": "COPD",
    "has_hypertension": "Hypertension",
    "has_ckd": "Chronic Kidney Disease"
}

# Unpivot: one row per patient per condition they have
condition_dfs = []
for flag_col, condition_name in condition_flags.items():
    if flag_col in pop_health.columns:
        df = pop_health.filter(col(flag_col) == True) \
            .select("patient_id") \
            .withColumn("condition_name", lit(condition_name))
        condition_dfs.append(df)

if condition_dfs:
    chronic_conditions = reduce(lambda a, b: a.unionAll(b), condition_dfs)
    
    # Join patient demographics for richer analysis
    chronic_conditions = chronic_conditions.join(
        patients.select("patient_id", "age", "gender", "insurance_type"),
        "patient_id", "left"
    )
    
    chronic_conditions.write.mode("overwrite").format("delta").saveAsTable("gold_chronic_conditions")
    
    # Show summary
    print("✅ gold_chronic_conditions created")
    print("\n📊 Most Common Chronic Conditions:")
    chronic_conditions.groupBy("condition_name") \
        .agg(count("*").alias("patient_count")) \
        .orderBy(col("patient_count").desc()) \
        .show(truncate=False)
else:
    print("⚠️  No condition flag columns found in gold_population_health")
```

---

## Part D: Validate AI Readiness

### Step 6: Run the AI Readiness Scorecard

Paste in Cell 5:

```python
# =============================================================
# Cell 5: AI Readiness Scorecard
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
    ("gold_chronic_conditions", "Unpivoted chronic conditions for Copilot"),
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

### Step 6B: Update the Semantic Model with New Tables

The semantic model you built in Module 3 only includes the original Gold and Silver tables. You've now created several new AI-ready tables (`gold_patient_360`, `gold_facility_summary`, `gold_chronic_conditions`) that Copilot and the Data Agent need access to.

> **This step is done in the browser, not in the notebook.**

1. Go to your workspace in the Fabric portal
2. Find `HealthcareLakehouse` and click on it to open the **Lakehouse** view
3. Switch to the **SQL analytics endpoint** view (dropdown at the top right of the Lakehouse page)
4. Click **Reporting** → **Manage default semantic model** in the top toolbar
5. In the table selection dialog, check the following new tables:
   - ✅ `gold_patient_360`
   - ✅ `gold_facility_summary`
   - ✅ `gold_chronic_conditions`
   - ✅ `gold_clinical_ai_insights` *(if created in Module 5)*
6. Click **Confirm** to update the semantic model

> ⚠️ **Why this matters:** If these tables aren't in the semantic model, Power BI Copilot and the standalone Copilot **cannot see them**. This is the most common reason Copilot says "I can't answer that" — the data exists in the Lakehouse but isn't exposed through the semantic model.

### Step 6C: Add Relationships for the New Tables

After the new tables appear in the semantic model, you need to create relationships so Copilot can join them correctly.

1. Open the semantic model in the Power BI service (click on it in your workspace)
2. Click **Edit** to enter edit mode (you should see **New measure**, **Manage relationships** in the toolbar)
3. Create the following relationships by dragging columns between tables (same technique as Module 3 Step 4):

| From Table | From Column | To Table | To Column | Cardinality |
|------------|-------------|----------|-----------|-------------|
| `gold_patient_360` | `patient_id` | `silver_patients` | `patient_id` | One-to-One |
| `gold_chronic_conditions` | `patient_id` | `silver_patients` | `patient_id` | Many-to-One |
| `gold_clinical_ai_insights` *(if added)* | `patient_id` | `silver_patients` | `patient_id` | Many-to-One |

> **Notes on tables without relationships:**
> - **`gold_facility_summary`** — This is a pre-aggregated summary with one row per facility. It does not have a foreign key to join to other tables (it already contains all the metrics). Copilot queries it as a standalone table for facility comparison questions.

4. After creating all relationships, verify them by clicking **Manage relationships** in the toolbar — you should see the new relationships listed alongside the original ones from Module 3.

> **💡 Alternative:** If you prefer to use the **Manage relationships** dialog instead of dragging:
> 1. Click **Manage relationships** in the toolbar
> 2. Click **New relationship**
> 3. Select the From and To tables/columns from the dropdowns
> 4. Set the cardinality and click **OK**

> **💡 Alternative:** If you created a separate semantic model (e.g., `HealthcareLakehouse-SemanticModel`) in Module 3, open it in the Power BI service → click **Edit** → **Add tables** → select the new tables → **Save**.

---

## Part E: Semantic Model AI-Readiness Audit (Notebook)

Parts A–D validated the **data layer** (Delta tables). But AI tools like Power BI Copilot and Data Agent operate on the **Semantic Model** — the layer that defines how tables relate, what measures are available, and what each field means. A semantic model with missing descriptions, unnamed measures, or unclear column names forces Copilot to guess — and it often guesses wrong.

In this section you'll use **Semantic Link** (`sempy`) to programmatically extract the model metadata and **Fabric's built-in LLM** to audit it against five AI-readiness dimensions.

### Step 7: Install Semantic Link and OpenAI

Paste in Cell 6:

```python
# =============================================================
# Cell 6: Install Semantic Link and OpenAI
# =============================================================
%pip install -U semantic-link openai -q
```

### Step 8: Extract Semantic Model Metadata

Semantic Link connects directly to the semantic model you built in Module 3 and pulls out tables, columns, measures, and relationships as pandas DataFrames.

Paste in Cell 7:

```python
# =============================================================
# Cell 7: Connect to the Semantic Model and Extract Metadata
# =============================================================
# Semantic Link (sempy) reads the Tabular Object Model (TOM)
# directly — no need for REST APIs or manual inspection.
# =============================================================

import sempy.fabric as fabric
import pandas as pd

# ⚠️ Update this name if you chose a different name in Module 3
DATASET = "HealthcareLakehouse-SemanticModel"

# Pull metadata from the semantic model
tables_df = fabric.list_tables(DATASET)
columns_df = fabric.list_columns(DATASET)
measures_df = fabric.list_measures(DATASET)
relationships_df = fabric.list_relationships(DATASET)

print(f"📊 Semantic Model: {DATASET}")
print(f"   Tables: {len(tables_df)}")
print(f"   Columns: {len(columns_df)}")
print(f"   Measures: {len(measures_df)}")
print(f"   Relationships: {len(relationships_df)}")

# Show tables
print(f"\n{'='*60}")
print("📋 TABLES")
print(f"{'='*60}")
display(tables_df)

# Show measures
print(f"\n{'='*60}")
print("📋 MEASURES")
print(f"{'='*60}")
if len(measures_df) > 0:
    display(measures_df)
else:
    print("⚠️  No DAX measures defined! Go back to Module 3 to add them.")

# Show relationships
print(f"\n{'='*60}")
print("📋 RELATIONSHIPS")
print(f"{'='*60}")
display(relationships_df)
```

> **What to look for:** Scan the output for tables and columns with empty Description fields — those are gaps that Copilot can't interpret. Also check whether the DAX measures from Module 3 appear in the Measures list.

### Step 9 (Optional): Semantic Model Audit via GitHub Copilot + Power BI Modeling MCP

> **This entire step is optional.** It requires a GitHub account with Copilot access and additional software installs. If you prefer, skip to Step 11 to continue with the notebook-based checks instead.

In this step, you use **GitHub Copilot Chat** to audit your semantic model by typing plain-English prompts. Behind the scenes, Copilot connects to your live semantic model through a local **MCP server** (Model Context Protocol) — a small program that acts as a bridge between Copilot and Power BI.

#### What are we installing and why?

You need **one** MCP server for this step:

| What | Why you need it |
|---|---|
| **[Power BI Modeling MCP Server](https://github.com/microsoft/powerbi-modeling-mcp)** | This is the bridge. It runs on your machine and lets Copilot read your semantic model's tables, columns, measures, relationships, and descriptions. It can also *write* changes (add descriptions, create measures, etc.) when you ask it to. |

> **What about Skills for Fabric and MS Learn MCP?**
> - **[Skills for Fabric](https://github.com/microsoft/skills-for-fabric)** is a *separate* project that provides Fabric agent skills for Spark, SQL, KQL, and more. You do **not** need it for this audit step — the Power BI Modeling MCP covers everything we need for semantic model work.
> - **MS Learn MCP** (bundled with Skills for Fabric) lets Copilot search Microsoft documentation. It's nice to have but not required — Copilot already has general knowledge of Power BI best practices.
>
> If you want to explore these later, see the [Skills for Fabric README](https://github.com/microsoft/skills-for-fabric) for installation instructions.

---

#### Step 9A: Create a GitHub Account (if you don't have one)

1. Go to [https://github.com/signup](https://github.com/signup)
2. Create a free account with your email address
3. After account creation, you need **GitHub Copilot** access:
   - **Free tier:** GitHub Copilot Free gives you limited monthly completions — this is enough for this lab. It activates automatically on new accounts.
   - **Paid tier:** If your organization provides GitHub Copilot Business/Enterprise, sign in with your org account.
   - To verify: Go to [https://github.com/settings/copilot](https://github.com/settings/copilot) — you should see Copilot enabled.

---

#### Step 9B: Install Required Software

You need three things installed on your machine. Skip any you already have.

**1. Install Visual Studio Code**

> We said "no VS Code" earlier, but VS Code is the simplest way to use GitHub Copilot with MCP servers. The GitHub Copilot CLI (`gh copilot`) does not yet support MCP servers, so VS Code is required for this step.

- Download from [https://code.visualstudio.com/download](https://code.visualstudio.com/download)
- Run the installer with default settings

**2. Install Node.js (v18 or later)**

The MCP server needs Node.js to run.

- Download from [https://nodejs.org/](https://nodejs.org/) — pick the **LTS** version
- Run the installer with default settings
- Verify: Open a terminal and run `node --version` — you should see `v18.x.x` or higher

**3. Install the Azure CLI**

You need this to sign in to Azure (which gives the MCP server access to your Fabric workspace).

- Download from [https://learn.microsoft.com/cli/azure/install-azure-cli](https://learn.microsoft.com/cli/azure/install-azure-cli)
- Run the installer with default settings
- Verify: Open a terminal and run `az --version`

---

#### Step 9C: Install GitHub Copilot in VS Code

1. Open **VS Code**
2. Click the **Extensions** icon in the left sidebar (or press `Ctrl+Shift+X`)
3. Search for **GitHub Copilot** and click **Install**
4. Search for **GitHub Copilot Chat** and click **Install**
5. VS Code will ask you to **sign in to GitHub** — click the prompt and sign in with your GitHub account from Step 9A
6. After signing in, you should see a **Copilot chat icon** (💬) in the left sidebar or at the top of the VS Code window

---

#### Step 9D: Install the Power BI Modeling MCP Server

1. In VS Code, click the **Extensions** icon (`Ctrl+Shift+X`)
2. Search for **Power BI Modeling MCP**
3. Click **Install** on the extension by Microsoft (or go directly to [this link](https://aka.ms/powerbi-modeling-mcp-vscode))
4. **Verify it's working:**
   - Open Copilot Chat in VS Code (click the 💬 icon or press `Ctrl+Shift+I`)
   - Click the **Agent Mode** toggle at the top of the chat panel (if you see "Ask" or "Edit" mode, switch it to "Agent")
   - Click the **🔧 tools icon** — you should see `powerbi-modeling-mcp` listed with tools like `connection_operations`, `table_operations`, `measure_operations`, etc.

> **If you don't see the tools:** Go to [https://github.com/settings/copilot](https://github.com/settings/copilot), scroll to **"MCP servers in Copilot"**, and make sure it's **enabled**. For enterprise accounts, your admin may need to enable this.

---

#### Step 9E: Sign in to Azure

The MCP server needs to authenticate to your Fabric workspace using your Azure account.

1. Open a terminal in VS Code (`` Ctrl+` ``)
2. Run:
   ```
   az login
   ```
3. A browser window opens — sign in with the **same account** you use for Microsoft Fabric
4. Verify you're on the right subscription:
   ```
   az account show --query "{name:name, id:id}" -o table
   ```

> **Setup is done!** Steps 9A–9E are one-time only. From now on, just open VS Code and start chatting.

---

#### Step 9F: Connect Copilot to Your Semantic Model

1. In VS Code, open **Copilot Chat** (💬 icon or `Ctrl+Shift+I`)
2. Make sure you're in **Agent Mode** (not "Ask" or "Edit")
3. Type this prompt — replace `<your-workspace-name>` with your actual Fabric workspace name from Module 3:

```
Connect to semantic model 'HealthcareLakehouse-SemanticModel'
in Fabric Workspace '<your-workspace-name>'
```

4. Copilot will show a **confirmation dialog** — click **Allow** or **Continue**
5. Wait a few seconds. You should see a response like:

> ✅ "Connected to semantic model 'HealthcareLakehouse-SemanticModel'. Found 9 tables, 5 measures, 8 relationships."

**If you get an error:**
- `Authentication failed` → Re-run `az login` in the terminal
- `Workspace not found` → Double-check your workspace name in the Fabric portal ([https://app.fabric.microsoft.com](https://app.fabric.microsoft.com))
- `MCP server not available` → Make sure the Power BI Modeling MCP extension is installed (Step 9D)

---

#### Step 9G: Run the AI-Readiness Audit

With the connection established, paste this prompt into Copilot Chat:

```
Audit my connected semantic model for AI readiness.

Check these five areas and score each one
(✅ Good, ⚠️ Needs Improvement, or ❌ Critical Gap):

1. Star Schema Design — Are fact tables and dimension tables clearly separated?
2. Descriptions — How many tables, columns, and measures have no description?
3. Naming — Are names clear and consistent, or are there cryptic abbreviations?
4. DAX Measures — Are healthcare KPIs like Readmission Rate, Average LOS,
   and Denial Rate defined? Suggest missing ones with DAX expressions.
5. Relationships — Are all table joins defined? Are there orphan tables
   with no relationships?

End with a prioritized list of the top 5 things to fix first.
```

**What Copilot does (you can watch it work):**

Copilot will make several tool calls — you'll see them in the chat:
1. `connection_operations` — confirms the connection
2. `table_operations` → `list` — gets all tables
3. `column_operations` → `list` — gets all columns and checks for missing descriptions
4. `measure_operations` → `list` — gets all DAX measures
5. `relationship_operations` → `list` — gets all relationships

Then it analyzes everything and produces a report like:

```
📊 SEMANTIC MODEL AI-READINESS AUDIT
======================================

1. Star Schema Design: ✅ Good
   - Fact tables: gold_encounters, gold_claims, gold_vitals
   - Dimension tables: gold_patients, gold_conditions, gold_medications

2. Descriptions: ❌ Critical Gap
   - 9/9 tables have no description
   - 85/92 columns have no description
   - 3/5 measures have no description

3. Naming: ⚠️ Needs Improvement
   - Tables use "gold_" prefix — consider removing for readability
   - Column "los_days" → "Length of Stay (Days)"

4. DAX Measures: ⚠️ Needs Improvement
   - ✅ Found: Total Encounters, Total Claims
   - ❌ Missing: Readmission Rate, Average LOS, Denial Rate

5. Relationships: ✅ Good
   - 8 relationships defined, all many-to-one
   - No orphan tables

🎯 TOP 5 FIXES:
1. Add descriptions to all 9 tables and 85 columns
2. Create Readmission Rate DAX measure
3. Create Average LOS DAX measure
4. Create Denial Rate DAX measure
5. Remove "gold_" prefix from table display names
```

> Your actual output will vary depending on your model's current state.

---

### Step 10 (Optional): Fix Issues Using Copilot Prompts

Still in Copilot Chat with your model connected, you can fix the issues found in the audit. Just type what you want — Copilot applies changes directly to the live semantic model.

> **⚠️ Important:** Copilot always asks for confirmation before making changes. Read the proposed changes carefully before clicking **Allow**.

---

#### Step 10A: Add Missing Descriptions

```
Add a one-sentence description to every table and column that is currently
missing one. Make each description specific to healthcare analytics.
```

Copilot will show you a preview of all the descriptions it plans to add, then ask for confirmation. Review and click **Allow**.

---

#### Step 10B: Add Missing DAX Measures

```
Create these missing DAX measures and place them in the most appropriate table:
- Readmission Rate: percentage of patients readmitted within 30 days
- Average Length of Stay: average number of days per encounter
- Claim Denial Rate: percentage of claims with a denied status
```

Copilot will show the DAX expressions. Review them before approving.

---

#### Step 10C: Fix Missing Relationships

```
Show me if there are any tables with no relationships, and create the
missing joins. Use many-to-one from fact to dimension tables.
```

---

#### Step 10D: Improve Naming

```
Rename any tables or columns that use cryptic abbreviations.
Make names clear for business users. Show me all proposed renames first.
```

---

#### Step 10E: Verify Fixes

Re-run the audit to see improvements:

```
Run the same AI-readiness audit again and compare with the previous results.
Which scores improved?
```

Dimensions that were ❌ or ⚠️ should now show ✅.

---

## Part F: Data Agent Optimization Checks (Notebook)

Microsoft provides a [Semantic Model Data Agent Checklist](https://github.com/microsoft/fabric-toolbox/blob/main/samples/data_agent_checklist_notebooks/Semantic%20Model%20Data%20Agent%20Checklist.md) with companion [Data Agent Utilities notebook](https://github.com/microsoft/fabric-toolbox/blob/main/samples/data_agent_checklist_notebooks/Data%20Agent%20Utilities.ipynb) that outline best practices for preparing semantic models for the Data Agent. This section implements the key automated checks from those references.

> **Why this matters for the Data Agent:**
> - The Data Agent generates DAX queries from natural language. A model with missing descriptions, poor naming, or incorrect summarization forces the AI to guess — and it often guesses wrong.
> - The **Best Practice Analyzer** catches 60+ rules across performance, DAX patterns, error prevention, and formatting.
> - The **Memory Analyzer** reveals oversized columns and tables that slow DAX performance and Agent response time.

### Step 11: Install Semantic Link Labs

The `semantic-link-labs` package extends Semantic Link with enterprise-grade analysis tools including BPA and Memory Analyzer.

Paste in Cell 11:

```python
# =============================================================
# Cell 11: Install Semantic Link Labs
# =============================================================
%pip install -U semantic-link-labs -q
```

### Step 12: Run the Best Practice Analyzer (BPA)

The BPA checks 60+ rules against your semantic model and categorizes findings by severity. For Data Agent accuracy, prioritize **Performance**, **DAX Expressions**, and **Error Prevention** findings.

Paste in Cell 12:

```python
# =============================================================
# Cell 12: Best Practice Analyzer
# =============================================================
# Checks the semantic model against 60+ rules from Microsoft
# experts and the Fabric community. Focus on Performance, DAX
# Expressions, and Error Prevention for Data Agent accuracy.
# =============================================================

import sempy.fabric as fabric

DATASET = "HealthcareLakehouse-SemanticModel"  # Your semantic model name from Module 3

# Run BPA - results are displayed as an interactive HTML report
bpa_results = fabric.run_model_bpa(dataset=DATASET)

print("✅ Review the BPA output above.")
print("   Priority fixes for Data Agent:")
print("   • ⚠️ 'Do not summarize numeric columns' → Set SummarizeBy = None")
print("   • ⚠️ 'Provide format string for measures' → Add format strings")
print("   • ⚠️ 'Visible objects with no description' → Add descriptions (Step 10A via Copilot MCP)")
print("   • ⚠️ 'Relationship columns should be integer' → Verify key types")
```

> **Key BPA rules for Data Agent accuracy:**
> | Rule | Why it matters |
> |------|---------------|
> | Do not summarize numeric columns | Prevents accidental SUM in Copilot — create explicit measures instead |
> | Visible objects with no description | AI uses descriptions for context; missing ones cause guessing |
> | First letter of objects must be capitalized | Consistent naming improves NLQ → DAX mapping |
> | Mark primary keys | Helps the Agent understand table structure and joins |
> | Provide format string for measures | Currency, percentage, and integer formatting prevents ambiguous results |

### Step 13: Run the Memory Analyzer

The Memory Analyzer shows memory and storage statistics for your semantic model objects. Large or cold columns that aren't used by the Data Agent should be removed from the AI Data Schema to improve performance.

Paste in Cell 13:

```python
# =============================================================
# Cell 13: Semantic Model Memory Analyzer
# =============================================================
# Reveals memory consumption by table, column, and partition.
# Use this to identify oversized objects that slow DAX queries
# (and therefore slow Data Agent response time).
# =============================================================

memory_results = fabric.model_memory_analyzer(dataset=DATASET)

print("\n✅ Review the memory analysis above.")
print("   Optimization tips for Data Agent performance:")
print("   • Tables using >50% of model memory may need column pruning")
print("   • Columns with 0 Temperature are never queried — consider hiding from AI Schema")
print("   • High-cardinality string columns consume disproportionate memory")
print("   • For Direct Lake models, ensure V-Order is applied to Parquet files")
```

### Step 14: Check Description Coverage for AI

Tables, columns, and measures without descriptions are the #1 cause of poor Data Agent accuracy. This cell identifies all undescribed objects so you can fix them (either manually or via the Copilot + Power BI Modeling MCP approach in Step 10A).

Paste in Cell 14:

```python
# =============================================================
# Cell 14: Description Coverage Report
# =============================================================
# Checks which tables, columns, and measures lack descriptions.
# The Data Agent uses descriptions to understand what each field
# means — missing descriptions force it to guess from names alone.
# =============================================================

import sempy_labs as labs

# Get full model metadata
df_tables = labs.list_tables(dataset=DATASET, extended=True)
df_columns = labs.list_columns(dataset=DATASET, extended=True)
df_measures = labs.list_measures(dataset=DATASET, extended=True)

# Tables without descriptions
tables_no_desc = df_tables[df_tables['Description'].isna() | (df_tables['Description'] == '')]
print("=" * 70)
print("📋 DESCRIPTION COVERAGE REPORT")
print("=" * 70)

print(f"\n🏷️ Tables WITHOUT description: {len(tables_no_desc)} / {len(df_tables)}")
if len(tables_no_desc) > 0:
    for _, row in tables_no_desc.iterrows():
        print(f"   ❌ {row['Name']}")

# Columns without descriptions
cols_no_desc = df_columns[df_columns['Description'].isna() | (df_columns['Description'] == '')]
print(f"\n📊 Columns WITHOUT description: {len(cols_no_desc)} / {len(df_columns)}")
if len(cols_no_desc) > 0:
    for _, row in cols_no_desc.head(20).iterrows():
        print(f"   ❌ {row['Table Name']}.{row['Column Name']}")
    if len(cols_no_desc) > 20:
        print(f"   ... and {len(cols_no_desc) - 20} more")

# Measures without descriptions
measures_no_desc = df_measures[df_measures['Description'].isna() | (df_measures['Description'] == '')]
print(f"\n📐 Measures WITHOUT description: {len(measures_no_desc)} / {len(df_measures)}")
if len(measures_no_desc) > 0:
    for _, row in measures_no_desc.iterrows():
        print(f"   ❌ {row['Name']}")

# Duplicate column names (confuse the Data Agent)
print(f"\n🔄 Duplicate column names (appear in multiple tables):")
counts = df_columns['Column Name'].value_counts()
duplicates = counts[counts > 1]
if len(duplicates) > 0:
    for col_name, count in duplicates.items():
        tables = df_columns[df_columns['Column Name'] == col_name]['Table Name'].tolist()
        print(f"   ⚠️ '{col_name}' appears in {count} tables: {', '.join(tables)}")
    print("\n   💡 Tip: Add descriptions to distinguish these columns, or rename them")
    print("      to be table-specific (e.g., 'patient_id' vs 'encounter_patient_id')")
else:
    print("   ✅ No duplicate column names found")

# Summary
total_objects = len(df_tables) + len(df_columns) + len(df_measures)
described = total_objects - len(tables_no_desc) - len(cols_no_desc) - len(measures_no_desc)
coverage_pct = (described / total_objects * 100) if total_objects > 0 else 0
print(f"\n{'=' * 70}")
print(f"📈 Overall description coverage: {coverage_pct:.0f}% ({described}/{total_objects} objects)")
if coverage_pct < 80:
    print("   ⚠️ Below 80% — use Step 10A (Copilot + powerbi-modeling-mcp) to add descriptions")
elif coverage_pct < 100:
    print("   ✅ Good coverage — consider filling remaining gaps for best accuracy")
else:
    print("   🎯 Perfect coverage — your model is fully documented for AI")
```

> **Target:** 100% description coverage for all objects visible in the AI Data Schema. At minimum, aim for 80% across the entire model.

### Step 15 (Optional): Data Agent SDK Evaluation

The Fabric Data Agent Python SDK enables **programmatic evaluation** — you can send test questions and compare responses against expected answers. This is essential for regression testing as your model evolves.

> ⚠️ **Prerequisite:** You must have a Data Agent created (from Module 7) before running this cell. If you haven't created one yet, return to this step after completing Module 7. For a more comprehensive evaluation approach, see [Module 7B: Data Agent Evaluation](Module07B_Data_Agent_Evaluation.md).

Paste in Cell 15:

```python
# =============================================================
# Cell 15: Data Agent SDK — Programmatic Evaluation
# =============================================================
# Uses the Fabric Data Agent SDK to send test questions and
# evaluate response accuracy. Run this AFTER Module 7 to test
# your configured Data Agent programmatically.
# =============================================================

# Uncomment and run after completing Module 7:
"""
%pip install fabric-data-agent-sdk -q

from fabric.dataagent.client import FabricDataAgentManagement, FabricOpenAI
import pandas as pd

# Connect to your Data Agent (update the name to match your agent)
DATA_AGENT_NAME = "HealthFirst Clinical Analyst"
data_agent = FabricDataAgentManagement(agent_name=DATA_AGENT_NAME)

# View current configuration
config = data_agent.get_configuration()
print(f"Agent Instructions: {config.instructions[:200]}...")

# View data sources
datasources = data_agent.get_datasources()
for ds in datasources:
    print(f"  📁 {ds.get('name', 'Unknown')} ({ds.get('type', 'Unknown')})")

# Define test questions with expected patterns
test_questions = [
    "What is the overall 30-day readmission rate?",
    "Which facility has the highest readmission rate?",
    "How many patients are classified as high-risk?",
    "What is the average length of stay by diagnosis?",
    "Show me the claim denial rate by insurance type",
]

# Send questions and collect responses
print("\\n" + "=" * 70)
print("🧪 DATA AGENT EVALUATION")
print("=" * 70)

client = FabricOpenAI(agent_name=DATA_AGENT_NAME)
for q in test_questions:
    print(f"\\n❓ {q}")
    try:
        response = client.chat.completions.create(
            messages=[{"role": "user", "content": q}]
        )
        answer = response.choices[0].message.content[:200]
        print(f"   ✅ {answer}...")
    except Exception as e:
        print(f"   ❌ Error: {e}")

print("\\n💡 Review each response for accuracy. If incorrect:")
print("   1. Check AI Data Schema — are the right fields visible?")
print("   2. Check AI Instructions — is the terminology defined?")
print("   3. Check Verified Answers — should this be a pinned response?")
print("   4. Download diagnostics logs for detailed debugging")
"""
print("ℹ️ Cell 14 is commented out — uncomment after completing Module 7")
print("   It will programmatically test your Data Agent responses")
```

> **📖 Reference:** See the full [Data Agent Utilities notebook](https://github.com/microsoft/fabric-toolbox/blob/main/samples/data_agent_checklist_notebooks/Data%20Agent%20Utilities.ipynb) and [Data Agent SDK documentation](https://learn.microsoft.com/en-us/fabric/data-science/fabric-data-agent-sdk) for advanced evaluation patterns including ground truth comparison and automated scoring.

---

## Part G: Prep Data for AI in Power BI (Semantic Model Layer)

In Parts A–F you validated and enriched data **at the Lakehouse and Semantic Model levels**. Power BI offers a complementary feature called **"Prep data for AI"** that works **at the Semantic Model level** — it tells Copilot in Power BI *how* to interpret your model, what business terms mean, and which visuals to return for common questions.

> **Think of it this way:**
> - Parts A–D = "Make the data itself AI-ready" (clean, joined, documented)
> - Part E = "Audit the semantic model programmatically" (LLM-powered gap analysis + auto-fix descriptions)
> - Part F = "Optimize for the Data Agent" (BPA, memory, descriptions, SDK evaluation)
> - Part G = "Make the *semantic model* AI-ready in Power BI" (schema focus, business rules, curated answers)

The **Prep data for AI** button (preview) is available on the **Home ribbon** in Power BI Desktop and on the **Semantic Model page ribbon** in the Power BI service. It provides three features:

### Feature 1: AI Data Schema — Simplify What Copilot Sees

Not every column in your semantic model is relevant for natural language Q&A. The AI Data Schema lets you **select which fields Copilot should reason over**, removing noise and ambiguity.

#### Steps

1. Open your healthcare report in **Power BI Desktop** (or select the semantic model in the Power BI service)
2. Click **Prep data for AI** on the Home ribbon
3. Go to the **Simplify data schema** tab
4. **Deselect** columns that would confuse Copilot — for example:
   - Internal surrogate keys (`encounter_id`, `claim_id`) — keep only human-readable identifiers
   - ETL metadata columns (`_loaded_at`, `_source_file`)
   - Raw codes when you also have descriptions (keep `condition_description`, hide `condition_code`)
5. **Keep selected** the columns that users would naturally ask about:
   - `patient_name`, `age`, `insurance_type`, `facility_name`
   - `total_charges`, `readmission_rate_pct`, `length_of_stay_days`
   - `chronic_condition_count`, `multimorbidity`, `claim_status`
6. Click **Apply**

> **Healthcare example:** A clinician asking "Which patients have the highest ED utilization?" doesn't need to see `encounter_id` or `payer_code`. By hiding those fields, Copilot focuses on the right columns and produces cleaner answers.

> ⚠️ **Critical from the [Data Agent Checklist](https://github.com/microsoft/fabric-toolbox/blob/main/samples/data_agent_checklist_notebooks/Semantic%20Model%20Data%20Agent%20Checklist.md):**
> - **Include all dependent objects** for selected measures — if a measure references columns from other tables, those must also be visible
> - **Ensure selected tables here match what you select in the Data Agent** — mismatches cause the Agent to fail silently
> - Exclude helper measures and intermediate calculation objects not relevant to end users
> - Verify no fields needed for Verified Answers (below) are hidden
> - Use `get_measure_dependencies` from [Semantic Link Labs](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_measure_dependencies) if you have complex measure dependencies

---

### Feature 2: AI Instructions — Teach Copilot Your Business Context

AI Instructions let you provide **plain-text guidance** that Copilot uses when interpreting questions. This is where you encode domain knowledge, terminology, and analysis rules.

#### Steps

1. In the **Prep data for AI** dialog, go to the **Add AI instructions** tab
2. Enter instructions that help Copilot understand your healthcare data. Here is a recommended set for our lab:

```
## Healthcare Analytics Context

You are analyzing data for a hospital network with 3 facilities:
Metro General Hospital, Community Medical Center, and Riverside Health System.

## Key Terminology
- "Readmission" = a patient returning to any facility within 30 days of discharge
- "Frequent flyer" = a patient with 4 or more ED visits in the analysis period
- "ALOS" = Average Length of Stay, measured in days for inpatient encounters only
- "Denial rate" = percentage of claims with claim_status = 'Denied'
- "Multimorbidity" = patients with 3 or more chronic conditions (High tier)
- "Payment ratio" = paid_amount / claim_amount (1.0 = fully paid, 0.0 = fully denied)

## Analysis Rules
- When analyzing readmissions, always group by facility and diagnosis
- When showing financial metrics, break down by insurance_type (Medicare, Medicaid, Commercial, Self-Pay)
- For population health questions, prioritize the gold_patient_360 table — it has pre-joined demographics, encounters, and conditions
- A "high-risk" patient has risk_category = 'Critical' or 'High'
- When comparing facilities, use the gold_facility_summary table for pre-computed metrics
- ED utilization analysis should highlight frequent flyers (is_frequent_flyer = TRUE)

## Data Priority
- Use gold_patient_360 as the primary table for patient-level questions
- Use gold_facility_summary for facility comparison questions
- Use gold_readmissions for 30-day readmission analysis
- Use gold_financial for revenue cycle and claims questions
```

3. Click **Apply**

> **Why this matters:** Without instructions, Copilot might not know that "readmission" means a 30-day return, or that "frequent flyer" is a clinical term with a specific threshold. These instructions ground Copilot in your organization's definitions.

> 💡 **Best Practices from the [Data Agent Checklist](https://github.com/microsoft/fabric-toolbox/blob/main/samples/data_agent_checklist_notebooks/Semantic%20Model%20Data%20Agent%20Checklist.md):**
> - Add **example DAX queries** for complex scenarios to guide AI query patterns
> - Keep instructions **clear and specific** — avoid conflicts and don't be too verbose
> - Ensure instructions don't contradict Verified Answer configurations
> - If you have Calculation Groups, [DAX UDFs](https://learn.microsoft.com/en-us/dax/best-practices/dax-user-defined-functions), or Field Parameters, describe how they should be used
> - These AI Instructions are for the **semantic model layer** — do NOT duplicate them in the Data Agent instructions (Module 7)

---

### Feature 3: Verified Answers — Pin Curated Visuals to Common Questions

Verified Answers let you **pre-approve specific visuals** as the "correct" response to common questions. When a user asks something matching a trigger phrase, Copilot returns your curated visual instead of generating a new one.

#### Steps

1. First, **create a visual** in your report that answers a common question — for example, a bar chart showing *30-Day Readmission Rate by Facility*
2. **Select the visual** on the report canvas
3. Click the **...** menu on the visual header → **Set up a verified answer**
4. Add **trigger phrases** (5–7 recommended per verified answer):
   - "What is the readmission rate?"
   - "Show readmission rates by facility"
   - "Which hospital has the most readmissions?"
   - "30-day readmission comparison"
   - "Compare readmission performance across facilities"
5. Optionally add **filters** (up to 3) — e.g., allow users to filter by `insurance_type` or `diagnosis`
6. Click **Apply**

#### Suggested Verified Answers for Healthcare Lab

| Visual | Trigger Phrases |
|--------|----------------|
| Readmission rate by facility (bar chart) | "readmission rate", "which facility has the most readmissions" |
| ED frequent flyers by insurance type (table) | "frequent flyer patients", "ED high utilizers" |
| Average length of stay by diagnosis (bar chart) | "ALOS by diagnosis", "which diagnoses have the longest stays" |
| Claim denial rate by payer (pie/bar chart) | "denial rate", "which payer denies the most claims" |
| Population health — chronic conditions (stacked bar) | "chronic disease prevalence", "how many patients have diabetes" |

> **Verified answers show a ✅ checkmark** in Copilot, signaling to users that the response was human-reviewed and approved — building trust in the AI output.

> ⚠️ **Verified Answers Checklist** (from [Microsoft Data Agent Checklist](https://github.com/microsoft/fabric-toolbox/blob/main/samples/data_agent_checklist_notebooks/Semantic%20Model%20Data%20Agent%20Checklist.md)):
> - Use **5–7 complete, robust trigger questions** per verified answer — not partial phrases
> - Include both **formal and conversational phrasings** (e.g., "What is the readmission rate?" AND "show me readmissions")
> - Test trigger questions for both **exact and semantic matching** — Copilot uses fuzzy matching
> - Ensure **all fields used in verified answer visuals are visible** (not hidden) in the AI Data Schema
> - If a Verified Answer uses a measure, that measure and its dependencies must be in the AI Data Schema

---

### Testing Your Prep Data for AI Configuration

After configuring all three features, test them in Power BI Desktop:

1. Open the **Copilot pane** in Power BI Desktop
2. Use the **skill picker** (dropdown in the Copilot chat box) → select **Answer questions about the data**
3. Test your AI Instructions:
   - Ask: *"What is the readmission rate at Metro General?"*
   - Copilot should use the correct 30-day definition and reference the right table
4. Test your Verified Answers:
   - Ask: *"Show readmission rates by facility"*
   - You should see your pinned visual with a ✅ verified checkmark
5. Test your AI Data Schema:
   - Ask a question referencing a hidden field — Copilot should **not** use it
   - Ask a question referencing a visible field — Copilot should answer correctly

> **Tip:** After each change to the Prep data for AI settings, close and reopen the Copilot pane to refresh.

> 💡 **Testing & Validation Best Practices** (from [Microsoft Data Agent Checklist](https://github.com/microsoft/fabric-toolbox/blob/main/samples/data_agent_checklist_notebooks/Semantic%20Model%20Data%20Agent%20Checklist.md)):
> - **Review the DAX** in Copilot responses — expand the "How this was calculated" section to validate the generated query logic
> - **Download diagnostic logs** from Copilot to inspect full DAX queries, answer confidence, and model selection details
> - Use the **Data Agent Python SDK** (see Part F, Cell 15) for automated batch evaluation after Module 7
> - **Iterate based on findings** — if Copilot generates incorrect joins or uses wrong measures, update AI Instructions or add Verified Answers
> - Use **Git integration or Deployment Pipelines** to version-control your Prep Data for AI configuration changes

### Mark Your Model as Approved for Copilot

Once you're satisfied with the configuration:

1. Go to the **Power BI service** and find your semantic model
2. Click the **Settings** icon
3. Expand the **Approved for Copilot** section
4. Check the **Approved for Copilot** box → click **Apply**

This removes friction treatments (disclaimers) from Copilot answers for your model, signaling that the data is curated and trusted.

---

### 🔑 Data Agent Configuration — Key Principles (Preview for Module 7)

When you configure the Data Agent in Module 7, keep these principles in mind:

| Principle | Details |
|-----------|---------|
| **DO NOT duplicate semantic model instructions** | AI Instructions you set in Prep Data for AI are already read by the Data Agent — don't repeat them at the Data Agent level |
| **Data Agent instructions = formatting & routing** | Limit Data Agent–level instructions to response formatting, abbreviation definitions, tone, and cross-source routing |
| **Select the same tables** | Tables selected in the Data Agent should match those visible in your AI Data Schema |
| **Test before adding instructions** | A well-configured semantic model (descriptions + AI Instructions + Verified Answers) may need zero Data Agent instructions |
| **Multi-source routing** | If your Data Agent spans multiple semantic models or lakehouses, add routing instructions to help it pick the right source |

> 📌 **Rule of thumb:** The semantic model's Prep Data for AI handles *what* and *how* to calculate. The Data Agent handles *how to respond* and *where to route*.

---

## 💡 Discussion: Data Preparation Best Practices for AI

**Why this step matters:**
- Data Agents generate SQL/queries from natural language. If table/column names are cryptic, the AI struggles
- Pre-joined views reduce the chance of incorrect joins
- Unpivoted tables (like `gold_chronic_conditions`) make boolean flags queryable by Copilot
- AI Data Schema, Instructions, and Verified Answers provide **semantic model–level** context that Copilot uses for Power BI Q&A
- Programmatic audits (Part F) let you **continuously validate** the semantic model as it evolves — catching new tables without descriptions, missing measures, or broken relationships

**Production considerations:**
- Schedule data quality notebooks to run daily
- Set up alerts for data quality SLA violations (e.g., null rate > 5%)
- Consider implementing Great Expectations or similar frameworks for enterprise-grade data quality
- Keep AI Instructions updated as business rules change (e.g., new facilities, changed readmission window)
- Review and refresh Verified Answers quarterly as dashboards evolve
- **Re-run Part F (BPA + Description Coverage) after every model change** — new tables/columns may lack descriptions
- Use the [Microsoft Data Agent Checklist](https://github.com/microsoft/fabric-toolbox/blob/main/samples/data_agent_checklist_notebooks/Semantic%20Model%20Data%20Agent%20Checklist.md) as a periodic audit guide

---

## ✅ Module 6 Checklist

Before moving to Module 7, confirm:

- [ ] Data quality checks ran successfully across all Gold tables
- [ ] Referential integrity validated (no orphan IDs)
- [ ] `gold_patient_360` table created (patient-level summary)
- [ ] `gold_facility_summary` table created (facility comparison metrics)
- [ ] `gold_chronic_conditions` table created (unpivoted conditions for Copilot)
- [ ] AI Readiness Scorecard passed with all tables ready
- [ ] Semantic model updated to include new tables (gold_patient_360, gold_facility_summary, gold_chronic_conditions)
- [ ] Relationships created for new tables (gold_patient_360, gold_chronic_conditions, gold_clinical_ai_insights → silver_patients)
- [ ] Semantic model metadata extracted via Semantic Link (tables, columns, measures, relationships)
- [ ] LLM-powered audit completed — reviewed findings for all 5 dimensions
- [ ] *(Optional)* Descriptions auto-generated and applied via TOM API
- [ ] Best Practice Analyzer (BPA) run — prioritized Performance and DAX rules reviewed
- [ ] Memory Analyzer run — large tables/columns identified for potential AI Schema exclusion
- [ ] Description coverage ≥ 80% across all tables, columns, and measures
- [ ] *(Optional)* AI Data Schema configured — irrelevant fields hidden from Copilot
- [ ] *(Optional)* AI Instructions added with healthcare terminology and analysis rules
- [ ] *(Optional)* Verified Answers set up for common healthcare questions
- [ ] *(Optional)* Semantic model marked as **Approved for Copilot**
- [ ] *(Post-Module 7)* Data Agent SDK evaluation run to validate end-to-end accuracy

---

**[← Module 5: Gen AI — Clinical Intelligence](Module05_GenAI_Clinical_Intelligence.md)** | **[Module 6B: Testing Copilot →](Module06B_Testing_Copilot.md)**
