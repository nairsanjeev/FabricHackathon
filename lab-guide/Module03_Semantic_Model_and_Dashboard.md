# Module 3: Semantic Model, Dashboard & Power BI Copilot

| Duration | 45 minutes |
|----------|------------|
| Objective | Build a star-schema Semantic Model on Gold tables, create an interactive Power BI dashboard, and test Power BI Copilot for AI-assisted analytics |
| Fabric Features | Semantic Model, Power BI Report, DAX Measures, Power BI Copilot |

---

## What You Will Do

In this module, you will:
1. Create a **Semantic Model** (star schema) on your Gold layer tables
2. Define **DAX measures** for readmission rate, ALOS, and financial KPIs
3. Build a **Power BI report** with three pages:
   - Page 1: Patient Volume & Flow
   - Page 2: Quality & Readmissions
   - Page 3: Population Health & Financials

---

## Part A: Create the Semantic Model

### Step 1: Navigate to the SQL Analytics Endpoint

To create a semantic model with the ability to select tables, you must start from the **SQL Analytics Endpoint** — not the Lakehouse view directly.

1. Open your **HealthcareLakehouse** in the Fabric portal
2. In the top-right corner of the Lakehouse view, click the dropdown that says **Lakehouse** and switch to **SQL analytics endpoint**
3. You should now see the SQL endpoint view with your tables listed in the left panel

![SQL Analytics Endpoint view with New semantic model button](images/NewSemanticModefromSQLEndpoint.png)

> ⚠️ **Important:** If you try to create a semantic model directly from the Lakehouse view, you may not be able to select individual tables. Always switch to the **SQL analytics endpoint** first.

### Step 2: Create a New Semantic Model

1. In the **SQL analytics endpoint** view, click **New semantic model** in the top toolbar
2. Give it a name: **HealthcareLakehouse-SemanticModel** (or keep the suggested name)
3. For **Storage mode**, select **Direct Lake on SQL**

![New semantic model dialog — select Direct Lake on SQL](images/Modlue3-Semantic%20Mode%20-%20Direct%20Lake%20on%20SQL.png)

> **Note:** When creating the semantic model from the SQL analytics endpoint, choose **Direct Lake on SQL**. This mode reads the tables through the SQL analytics endpoint layer, which ensures reliable table discovery and query routing for Lakehouse tables.

### Step 3: Select Tables for the Model

You'll be prompted to select which Lakehouse tables to include in the semantic model:

1. Check the following **Gold tables**:
   - ✅ `gold_encounter_summary`
   - ✅ `gold_readmissions`
   - ✅ `gold_ed_utilization`
   - ✅ `gold_financial`
   - ✅ `gold_population_health`
   - ✅ `gold_alos`
2. Also include this Silver table for dimension lookups:
   - ✅ `silver_patients`
3. Click **Confirm**

The semantic model opens in the **Model view** (diagram view) where you can see your selected tables.

### Step 4: Create Relationships

> ⚠️ **Important:** Before creating relationships, make sure the semantic model is in **Edit mode**. Look at the top-right corner — if you see **Viewing**, click the pencil icon and select **Editing** to switch to edit mode. You should see the toolbar options change (e.g., **New measure**, **Manage relationships** become available).

![Semantic model in Edit mode showing tables](images/Moldule3-SemanticModel%20Edit%20Mode.png)

Create the following relationships:

| From Table | From Column | To Table | To Column | Cardinality |
|------------|-------------|----------|-----------|-------------|
| `gold_encounter_summary` | `patient_id` | `silver_patients` | `patient_id` | Many-to-One |
| `gold_readmissions` | `patient_id` | `silver_patients` | `patient_id` | Many-to-One |
| `gold_financial` | `encounter_id` | `gold_encounter_summary` | `encounter_id` | One-to-One |
| `gold_ed_utilization` | `patient_id` | `silver_patients` | `patient_id` | Many-to-One |
| `gold_population_health` | `patient_id` | `silver_patients` | `patient_id` | One-to-One |

**To create relationships using Manage Relationships:**

1. In the toolbar, click **Manage relationships**

![Manage relationships button in the toolbar](images/Module%203%20-%20Manage%20Relationship%20in%20Semantic%20Model.png)

2. Click **New relationship**
3. In the **New relationship** dialog:
   - Select the **From table** (e.g., `gold_encounter_summary`)
   - Select the **From column** (e.g., `patient_id`)
   - Select the **To table** (e.g., `silver_patients`)
   - Select the **To column** (e.g., `patient_id`)
   - Set the **Cardinality** (e.g., Many to one)
   - Leave **Cross-filter direction** as **Single**
   - Check **Make this relationship active**
   - Check **Assume referential integrity**
4. Click **OK** to create the relationship
5. Repeat for each relationship in the table above

![New relationship dialog showing configuration](images/Module3-New%20relationshipfrom%20Manage%20relationship%20and%20Assume%20referential%20integrity.png)

> **Tip:** Using **Manage relationships** lets you create all relationships in one place without dragging columns across the diagram.

### Step 5: Create DAX Measures

Now we'll add business measures using DAX (Data Analysis Expressions). These measures calculate KPIs that update dynamically based on filters.

1. Click on the `gold_readmissions` table
2. Right-click the table name (or click the **...** menu on the table) and select **New measure**

![New measure option from table context menu](images/Module3-new%20DAX%20measure.png)

3. Enter each measure below, clicking **New measure** for each one:

#### Readmission Measures (on `gold_readmissions` table)

**Measure 1: Readmission Rate**
```dax
Readmission Rate = 
DIVIDE(
    COUNTROWS(FILTER(gold_readmissions, gold_readmissions[was_readmitted] = TRUE())),
    COUNTROWS(gold_readmissions),
    0
)
```
> Format this measure as **Percentage** with 1 decimal place.

**Measure 2: Total Readmissions**
```dax
Total Readmissions = 
COUNTROWS(FILTER(gold_readmissions, gold_readmissions[was_readmitted] = TRUE()))
```

**Measure 3: Total Index Admissions**
```dax
Total Index Admissions = COUNTROWS(gold_readmissions)
```

#### Encounter Measures (on `gold_encounter_summary` table)

Click on the `gold_encounter_summary` table, then create these measures:

**Measure 4: Total Encounters**
```dax
Total Encounters = COUNTROWS(gold_encounter_summary)
```

**Measure 5: ED Visits**
```dax
ED Visits = 
COUNTROWS(FILTER(gold_encounter_summary, gold_encounter_summary[encounter_type] = "ED"))
```

**Measure 6: Inpatient Admissions**
```dax
Inpatient Admissions = 
COUNTROWS(FILTER(gold_encounter_summary, gold_encounter_summary[encounter_type] = "Inpatient"))
```

**Measure 7: Average Length of Stay**
```dax
Avg Length of Stay = 
CALCULATE(
    AVERAGE(gold_encounter_summary[length_of_stay_days]),
    gold_encounter_summary[encounter_type] = "Inpatient",
    gold_encounter_summary[length_of_stay_days] > 0
)
```

#### Financial Measures (on `gold_financial` table)

Click on the `gold_financial` table:

**Measure 8: Total Charges**
```dax
Total Charges = SUM(gold_financial[claim_amount])
```

**Measure 9: Total Collections**
```dax
Total Collections = SUM(gold_financial[paid_amount])
```

**Measure 10: Collection Rate**
```dax
Collection Rate = 
DIVIDE(
    SUM(gold_financial[paid_amount]),
    SUM(gold_financial[claim_amount]),
    0
)
```
> Format as Percentage.

**Measure 11: Denial Rate**
```dax
Denial Rate = 
DIVIDE(
    COUNTROWS(FILTER(gold_financial, gold_financial[claim_status] = "Denied")),
    COUNTROWS(gold_financial),
    0
)
```
> Format as Percentage.

**Measure 12: Revenue Lost to Denials**
```dax
Revenue Lost to Denials = 
CALCULATE(
    SUM(gold_financial[claim_amount]),
    gold_financial[claim_status] = "Denied"
)
```

---

## Part B: Build the Power BI Report Using Copilot

Instead of manually building each visual, use **Power BI Copilot** to generate report pages using natural language prompts. This is faster and demonstrates the AI-assisted analytics experience.

### Prerequisites
- Power BI Copilot must be enabled in your Fabric tenant (your admin may need to enable this in the Admin Portal → Tenant settings → Copilot)
- Your semantic model must have tables and measures already created (complete Part A first)

### Step 6: Create a New Report and Open Copilot

1. From the Semantic Model view, click **File** → **Create new report**

![File menu showing Create new report option](images/Module3-NewReport.png)

2. You will be taken to the Power BI report editor
3. In the report editor, look for the **Copilot** icon in the toolbar (it looks like a sparkle ✨)
4. Click it to open the Copilot pane on the right side

![Power BI report editor with Copilot pane open](images/Module3-PowerBICopilot.png)

> **Note:** If you don't see the Copilot icon, Copilot may not be enabled for your tenant. Use the **Alternate Path** (manual approach) below instead.

### Step 7: Generate Page 1 — Patient Volume & Flow

In the Copilot chat pane, type the following prompt:

```
Create a dashboard page showing patient volume and flow for our hospital system. Include:
- KPI cards for Total Encounters, ED Visits, Inpatient Admissions, and Average Length of Stay
- A line chart showing monthly encounter volume trends by encounter type 
- A bar chart showing encounters by facility name
- Slicers for facility name, encounter year, and insurance type
Use the gold_encounter_summary table and related measures.
```

Review what Copilot generates. You can refine by asking:

```
Change the line chart to show the last 12 months only.
```

### Step 8: Generate Page 2 — Quality & Readmissions

Add a new page, then prompt Copilot:

```
Create a quality metrics page focused on 30-day hospital readmissions. Include:
- KPI cards for Readmission Rate (as percentage), Total Readmissions, and Total Index Admissions
- A bar chart showing readmission rate by diagnosis (index_diagnosis)
- A column chart comparing readmission rates across facilities (index_facility)
- A detail table with index diagnosis, total admissions, readmissions, and readmission rate
- Slicers for index_facility and encounter_year
Use the gold_readmissions table and readmission measures.
```

### Step 9: Generate Page 3 — Financials & Population Health

Add a new page, then prompt Copilot:

```
Create a financial and population health dashboard page. Include:
- KPI cards for Total Charges, Total Collections, Collection Rate, and Revenue Lost to Denials
- A bar chart of denial rate by payer from gold_financial table 
- A pie chart showing multimorbidity distribution (None, Moderate, High) from gold_population_health
- A matrix showing collection rate by facility and payer type
- Slicers for age group, insurance type, and gender
```

### Copilot Tips

| Do | Don't |
|----|-------|
| Reference specific table and column names | Use vague terms like "the data" |
| Ask for one page at a time | Try to generate the entire report in one prompt |
| Specify chart types explicitly | Let Copilot guess which visual to use |
| Refine iteratively with follow-up prompts | Start over if the first result isn't perfect |
| Mention your measures by name | Expect Copilot to know your custom DAX measures |

> **Key Takeaway:** Copilot is excellent for rapid prototyping and getting 80% of the way there. You'll typically still need to fine-tune layouts, conditional formatting, and interactions manually.

### Step 10: Save the Report

1. Click **File** → **Save**
2. Name: `Healthcare Operations Dashboard`
3. Save to your workspace

---

## Part D: Test Power BI Copilot

Now that your report is saved, let's test **Power BI Copilot** — an AI assistant that can answer questions about your data, create visuals, and summarize report pages.

### Step 11: Open Copilot

1. Open the `Healthcare Operations Dashboard` report you just saved
2. Click the **Copilot** button in the top ribbon — the Copilot pane opens on the right

> **Note:** Power BI Copilot requires Fabric capacity (F64 or higher) and must be enabled by your admin. If you don't see the Copilot button, check with your instructor.

### Step 12: Try These Prompts

Test each prompt in the Copilot pane and observe how it uses your semantic model:

#### Prompt 1: Summarize the Current Page
```
Summarize the key insights from this report page
```
**What to observe:** Copilot reads the visuals on the current page and generates a narrative summary — great for quick meeting prep.

#### Prompt 2: Create a Visual
```
Create a bar chart showing readmission rate by facility
```
**What to observe:** Copilot generates a visual using DAX queries against your semantic model. It should use the `gold_readmissions` table.

#### Prompt 3: Ask a Data Question
```
What is our overall 30-day readmission rate and how does it compare across facilities?
```
**What to observe:** Copilot returns a data-driven answer using your readmission measures.

#### Prompt 4: Request a Board-Ready Narrative
```
Write a summary for the hospital board about our quality metrics, including readmission rates and length of stay
```
**What to observe:** Copilot generates executive-level narrative text suitable for board presentations.

> **Copilot Tips:**
> - Reference specific table and column names for better results
> - Ask for one thing at a time — don't combine multiple requests
> - Copilot can create visuals, change chart types, and add filters, but cannot resize visuals or apply conditional formatting

---

## ✅ Module 3 Checklist

Before moving to Module 4, confirm:

- [ ] Semantic Model has all Gold tables included
- [ ] Relationships are set up between tables
- [ ] 12 DAX measures are created
- [ ] Power BI report has 3 pages:
  - [ ] Patient Volume & Flow
  - [ ] Quality & Readmissions
  - [ ] Population Health & Financials
- [ ] Slicers work and filter the visuals correctly
- [ ] Report is saved as `Healthcare Operations Dashboard`
- [ ] Power BI Copilot tested with at least 2 prompts

---

## 💡 Try These Insights

With your dashboard built, try answering these questions:

1. **Which facility has the highest readmission rate?** Use the Quality page slicers.
2. **What diagnosis drives the most readmissions?** Look at the bar chart on Page 2.
3. **Which payer has the highest denial rate?** Check the Financials section on Page 3.
4. **How many patients have 3+ chronic conditions?** Check the multimorbidity pie chart.
5. **Are weekend admissions different from weekday?** Use the day-of-week chart on Page 1.

---

**[← Module 2: Data Engineering](Module02_Data_Engineering.md)** | **[Module 4: Real-Time Analytics →](Module04_RealTime_Analytics.md)**
