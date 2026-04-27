# Module 8: Building the Lab with VS Code Agent Mode (Optional)

| Duration | 60–90 minutes |
|----------|---------------|
| Objective | Use GitHub Copilot in VS Code Agent Mode to build the entire healthcare analytics lab through conversational AI — from data generation to notebooks to dashboards |
| Tools | VS Code, GitHub Copilot (Agent Mode), Microsoft Fabric Extension |

> **⚠️ This module is OPTIONAL.** It demonstrates an alternate, AI-assisted approach to building everything you created in Modules 1–7. You can use this as a learning exercise to see how an AI coding agent accelerates Fabric development.

---

## Why VS Code Agent Mode?

In Modules 1–7, you built each component step by step — creating notebooks manually, copying code cell by cell, and configuring each Fabric item through the portal UI. **VS Code Agent Mode** offers a fundamentally different approach:

Instead of you writing the code, you **describe what you want** and the AI agent writes it for you.

| Traditional Approach (Modules 1–7) | Agent Mode Approach (This Module) |
|-------------------------------------|-------------------------------------|
| Copy code from lab guide into notebook cells | Describe the task; agent generates the code |
| Manually create each Fabric item in the portal | Agent creates notebooks and configures items |
| Debug errors by reading error messages | Agent reads errors and fixes them automatically |
| Look up PySpark functions in documentation | Agent already knows PySpark, KQL, DAX, etc. |
| Iterative trial-and-error | Conversational refinement |

### What is Agent Mode?

GitHub Copilot's **Agent Mode** (available in VS Code) is an autonomous AI assistant that can:

- **Read and write files** in your workspace
- **Run terminal commands** (pip install, git, Azure CLI, etc.)
- **Search your codebase** for context
- **Browse documentation** when needed
- **Execute multi-step plans** — breaking complex requests into tasks and completing them sequentially
- **Self-correct** — when something fails, it reads the error and tries a different approach

Think of it as having a senior developer sitting next to you who knows Fabric, PySpark, KQL, DAX, and healthcare data inside and out.

---

## Prerequisites

Before starting this module, you need:

1. **VS Code** installed on your machine
   - Download: [https://code.visualstudio.com](https://code.visualstudio.com)

2. **GitHub Copilot subscription** (Individual, Business, or Enterprise)
   - Sign up: [https://github.com/features/copilot](https://github.com/features/copilot)

3. **GitHub Copilot extension for VS Code**
   - Open VS Code → Extensions (Ctrl+Shift+X) → Search "GitHub Copilot" → Install
   - Also install **GitHub Copilot Chat**

4. **Microsoft Fabric VS Code Extension** (optional but recommended)
   - Extensions → Search "Microsoft Fabric" → Install
   - This allows you to develop Fabric notebooks directly in VS Code

5. **A Microsoft Fabric workspace** with capacity (same as Modules 1–7)

6. **Python 3.9+** installed locally (for data generation)

---

## Part A: Setting Up Your Workspace

### Step 1: Clone the Repository

1. Open VS Code
2. Open the terminal (Ctrl+`)
3. Clone the lab repository:
   ```
   git clone https://github.com/nairsanjeev/FabricHackathon.git
   cd FabricHackathon
   ```
4. Open the folder in VS Code: **File → Open Folder → FabricHackathon**

### Step 2: Open Agent Mode

1. Open GitHub Copilot Chat: press **Ctrl+Shift+I** (or click the Copilot icon in the sidebar)
2. At the top of the chat panel, switch the mode dropdown to **"Agent"**
   - You should see the mode indicator change from "Ask" or "Edit" to **"Agent"**
3. You're now in Agent Mode — the AI can read files, run commands, and make edits autonomously

> **💡 Tip:** Agent Mode is different from regular Copilot Chat. In Agent Mode, the AI can take *actions* (create files, run terminal commands, edit code) — not just answer questions.

### Step 3: Verify Your Setup

Type this into the Agent Mode chat:

```
Look at the files in this repository. What is this project about? 
List the key files and their purposes.
```

The agent should read the README, explore the folder structure, and give you a summary of the healthcare lab. This confirms it can access your workspace.

---

## Part B: Generate and Explore the Data

### Step 4: Generate Synthetic Healthcare Data

Ask the agent:

```
Run the data generation script at data/generate_healthcare_data.py to create 
the synthetic healthcare CSV files. Show me a summary of what was generated — 
how many rows in each file and what columns they have.
```

The agent will:
- Run the Python script
- List each CSV with row counts and column names
- Show you a preview of the data

### Step 5: Understand the Data Model

Ask the agent:

```
Look at all 7 CSV files in the data/ folder. Draw me an entity-relationship 
diagram showing how the tables relate to each other (patients → encounters → 
conditions, claims, vitals, medications, clinical_notes). What are the join 
keys between each table?
```

The agent will analyze the CSVs and explain the data model — something that would take significant manual exploration.

---

## Part C: Build the Bronze Layer (Module 1 Equivalent)

### Step 6: Create the Bronze Ingestion Notebook

Ask the agent:

```
Create a Fabric notebook that ingests all 7 CSV files from Files/raw into 
Bronze Delta tables. Follow the Medallion Architecture pattern. The notebook 
should:
- Read each CSV with proper options (inferSchema, multiLine, escape for quoted fields)
- Write as Delta tables with "bronze_" prefix (e.g., bronze_patients)
- Use overwrite mode for idempotency
- Print row counts and column counts for validation
- Include data exploration queries showing patient demographics, encounter 
  types, top diagnoses, and facility distribution

Add detailed comments explaining why we use Delta format instead of keeping 
CSVs, what each Spark read option does, and the clinical context for the 
data exploration queries.
```

**What happens:** The agent creates a complete notebook with all the code, comments, and exploration queries — equivalent to what you built in Module 1's notebook step.

---

## Part D: Build the Silver Layer (Module 2 Equivalent)

### Step 7: Create the Silver Transformations Notebook

Ask the agent:

```
Create a Fabric notebook for Silver layer transformations. Read from the Bronze 
tables and create Silver tables with these transformations:

1. silver_patients: Parse dates, cast age/risk_score to numeric, compute 
   age_group (18-29, 30-44, 45-59, 60-74, 75+) and risk_category (Low <1.5, 
   Medium 1.5-3.0, High 3.0+)

2. silver_encounters: Parse dates, extract month/year/quarter/day_of_week, 
   add is_weekend flag, add los_category (Same Day, Short 1-2, Medium 3-5, 
   Long 6-10, Extended >10)

3. silver_conditions: Parse dates, map ICD-10 codes to clinical categories 
   (E11=Diabetes, I50=Heart Failure, J44=COPD, I10=Hypertension, etc.)

4. silver_claims: Cast monetary columns, compute payment_ratio 
   (paid/claim_amount), add is_denied flag

5. silver_vitals: Parse timestamps, cast all vital columns to numeric, 
   compute SIRS flag (temp >100.4 or <96.8 AND HR >90 AND RR >20)

6. silver_medications: Parse dates
7. silver_clinical_notes: Parse dates

Include markdown cells explaining each table's business context and why each 
transformation matters for healthcare analytics.
```

---

## Part E: Build the Gold Layer (Module 2 Equivalent)

### Step 8: Create the Gold Analytics Notebook

Ask the agent:

```
Create a Fabric notebook for Gold analytics layer. Build these 6 Gold tables 
from Silver data:

1. gold_readmissions: 30-day readmission calculation using self-join on 
   inpatient encounters. Match same patient, readmission within 30 days of 
   discharge, exclude expired/AMA. Deduplicate. Per CMS HRRP methodology.

2. gold_ed_utilization: Count ED visits per patient per year, flag frequent 
   flyers (4+ visits), join with patient demographics.

3. gold_alos: Average length of stay by diagnosis and facility for inpatient 
   encounters with LOS > 0. Include std dev, min, max, avg charges.

4. gold_encounter_summary: Denormalized encounter + patient demographics for 
   Power BI star schema consumption.

5. gold_financial: Claims joined with encounter context for revenue cycle 
   analysis. Compute denial rates and collection rates by payer.

6. gold_population_health: Patient-level chronic condition counts, boolean 
   flags for diabetes/heart failure/COPD/hypertension/CKD, multimorbidity 
   tiers (None, Moderate 1-2, High 3+).

Include markdown cells with national benchmarks and clinical context. Add 
detailed inline comments explaining the self-join algorithm for readmissions 
and the array_contains pattern for population health.
```

---

## Part F: Build the Gen AI Pipeline (Module 5 Equivalent)

### Step 9: Create the Clinical Intelligence Notebook

Ask the agent:

```
Create a Fabric notebook that uses Azure OpenAI to process clinical notes 
from the Silver layer. It should:

1. Configure Azure OpenAI with placeholder values for endpoint, key, and 
   deployment name. Include detailed comments on how to get these from 
   AI Foundry.

2. Install the openai SDK with %pip install

3. Create 3 AI functions:
   - summarize_clinical_note(): 2-3 sentence summary, temperature 0.3
   - extract_medical_entities(): JSON output with diagnoses, medications, 
     procedures, vitals, allergies, follow_up. Temperature 0.1
   - suggest_icd10_codes(): JSON array with code, description, confidence, 
     evidence. Temperature 0.2

4. Batch process 20 notes with 0.5s rate limiting delay

5. Save results to gold_clinical_ai_insights Delta table

Include detailed comments on prompt engineering strategy, temperature settings, 
HIPAA/BAA considerations, and cost awareness.
```

---

## Part G: Build the Real-Time Simulator (Module 4 Equivalent)

### Step 10: Create the Vitals Simulator Notebook

Ask the agent:

```
Create a Fabric notebook that simulates real-time patient vital signs and 
sends them to an Eventstream Custom endpoint via the Event Hub protocol.

Requirements:
- 20 patients with 3 archetypes: Sepsis Risk (3), Heart Failure (3), Stable (14)
- Sepsis patients: high temp (101.5-104°F), high HR (100-130), high RR (22-32), low SpO2
- Heart failure: elevated HR (90-115), normal temp, low SpO2 (90-95)
- Stable: all normal ranges
- Generate realistic physiologic variation (±8 HR, ±0.5°F temp, etc.)
- Calculate SIRS score (criteria: temp >100.4 or <96.8, HR >90, RR >20)
- Send batches of 20 readings every 2 seconds via azure-eventhub SDK
- Print SIRS alerts to console as they occur
- Include placeholder for connection string with instructions on where to find it

Add detailed comments explaining patient archetypes, SIRS criteria, Event Hub 
protocol, and why batching is more efficient than individual sends.
```

---

## Part H: Create KQL Queries and DAX Measures

### Step 11: Generate KQL Queries for Real-Time Analytics

Ask the agent:

```
Create a KQL query file for analyzing real-time patient vitals in Eventhouse. 
Include queries for:
1. Latest vitals per patient (last 5 minutes)
2. SIRS alert summary
3. Heart rate trends over last 30 minutes
4. Vital sign statistics by department
5. Sepsis risk patients timeline
6. Temperature anomaly detection
7. Department workload by patient count
8. Rolling average vitals per patient

IMPORTANT: The timestamp column is stored as a string, so all queries must 
use todatetime(timestamp) when comparing with ago() functions.
```

### Step 12: Generate DAX Measures for Semantic Model

Ask the agent:

```
Create DAX measures for a healthcare semantic model. Include measures for:
- 30-Day Readmission Rate
- Average Length of Stay  
- Total Encounters and ED Visit Count
- Total Revenue, Total Denied, Denial Rate
- Average Charges per Encounter
- High Risk Patient Count and percentage
- Bed Occupancy Rate

Format each with the measure name, DAX formula, and format string.
```

---

## Part I: Ask the Agent to Explain and Debug

### Step 13: Use Agent Mode for Learning

One of the most powerful uses of Agent Mode is **learning by asking**. Try these prompts:

**Understand the code:**
```
Explain the readmission self-join algorithm in notebook 03. Walk me through 
each join condition step by step and why the LEFT join and dropDuplicates 
are necessary.
```

**Debug issues:**
```
I'm getting an AMBIGUOUS_REFERENCE error when running the Gold notebook. 
Look at the code and tell me which join is causing a duplicate column name, 
and fix it.
```

**Extend the lab:**
```
Add a new Gold table called gold_provider_scorecard that shows each provider's:
- Total patients seen
- Average patient risk score
- Readmission rate
- Average length of stay
- Average charges per encounter
```

**Ask clinical questions:**
```
What is the CMS Hospital Readmissions Reduction Program? How does our 
readmission calculation align with CMS methodology? What simplifications 
did we make?
```

---

## Part J: End-to-End in One Shot

### Step 14: The Single-Prompt Challenge (Advanced)

For the most adventurous, try giving the agent a single comprehensive prompt to build everything at once:

```
I need to build a complete healthcare analytics lab on Microsoft Fabric. 
Look at the CSV files in data/ and the existing notebooks in notebooks/ 
for reference. Create a complete set of Spark notebooks that:

1. Ingest 7 CSV files into Bronze Delta tables
2. Transform Bronze → Silver with type casting, computed columns (age groups, 
   risk categories, ICD-10 mapping, SIRS flags)
3. Compute Gold KPIs (readmissions, ED utilization, ALOS, financial, 
   population health)
4. Process clinical notes with Azure OpenAI for summarization, entity 
   extraction, and ICD-10 coding
5. Simulate real-time patient vitals streaming to Eventstream

Follow the Medallion Architecture. Include detailed comments explaining 
the clinical context and analytics approach for each transformation.
```

> **💡 Note:** This is a complex request. The agent may take several minutes and produce multiple files. Review the output carefully — AI-generated code should always be validated before running against real systems.

---

## Tips for Effective Agent Mode Usage

### 1. Be Specific About Context
The more context you provide, the better the output:
- ❌ "Create a notebook for data transformations"
- ✅ "Create a Fabric notebook that transforms bronze_patients into silver_patients with age_group buckets aligned to CMS reporting brackets (18-29, 30-44, 45-59, 60-74, 75+)"

### 2. Iterate, Don't Restart
If the agent's output isn't quite right, refine it:
```
That's good, but change the risk score thresholds to Low <1.5, Medium 1.5-3.0, 
High 3.0+ and add a comment explaining why these thresholds were chosen.
```

### 3. Ask the Agent to Read Before Writing
```
Read notebooks/02_Silver_Transformations.py and understand the pattern. 
Then create a similar notebook for a new set of transformations.
```

### 4. Use the Agent for Debugging
When something fails in Fabric, paste the error message:
```
I got this error when running the Gold notebook in Fabric:
[TABLE_OR_VIEW_NOT_FOUND] The table or view `gold_readmissions` cannot be found.
What's wrong and how do I fix it?
```

### 5. Ask for Explanations
Agent Mode is an excellent learning tool:
```
Explain what the Window function does in PySpark, and give me 3 healthcare 
examples of when I'd use it.
```

---

## What You Accomplished

By using VS Code Agent Mode, you've experienced a fundamentally different way to build analytics solutions:

| Aspect | Manual Approach | Agent Mode |
|--------|----------------|------------|
| **Time** | 6–7 hours (full lab) | 1–2 hours |
| **Skill required** | PySpark, SQL, KQL, DAX knowledge | Natural language descriptions |
| **Error handling** | Manual debugging | Agent reads errors and self-corrects |
| **Learning** | Read docs, trial-and-error | Ask "why" and get explanations in context |
| **Customization** | Rewrite code manually | "Change this part to do X instead" |

### When to Use Agent Mode vs. Manual Development

| Use Agent Mode When | Use Manual Development When |
|--------------------|-----------------------------|
| Rapid prototyping and POCs | Production-critical code requiring audit trails |
| Learning new frameworks (PySpark, KQL) | You need complete control over every line |
| Boilerplate code generation | Security-sensitive configurations |
| Debugging unfamiliar errors | Highly regulated environments requiring signed-off code |
| Exploring data and building queries | You already know exactly what to type |

### The Key Takeaway

Agent Mode doesn't replace the need to **understand** what the code does — it accelerates **how fast** you get there. The students who benefit most are those who use the agent to generate code *and then ask it to explain what it generated and why*.

---

## ✅ Module 8 Checklist

Confirm you have completed:

- [ ] VS Code with GitHub Copilot installed and Agent Mode enabled
- [ ] Repository cloned and opened in VS Code
- [ ] Successfully used Agent Mode to generate at least one notebook
- [ ] Compared the agent-generated code with the manual lab notebooks
- [ ] Asked the agent to explain at least one complex concept (e.g., readmission self-join)
- [ ] Used the agent to debug or extend the lab in some way
- [ ] Understand when Agent Mode is appropriate vs. manual development

---

**[← Module 7: Data Agent & Power BI Copilot](Module07_Data_Agent.md)** | **[Back to Overview](../README.md)**
