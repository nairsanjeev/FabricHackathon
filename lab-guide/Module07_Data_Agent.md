# Module 7: Data Agent & Power BI Copilot

| Duration | 45 minutes |
|----------|------------|
| Objective | Create a Fabric Data Agent that allows clinical and administrative staff to query healthcare data using plain English, then test Power BI Copilot to compare AI-assisted analytics approaches |
| Fabric Features | Data Agent (AI Skill), Lakehouse integration, Power BI Copilot |

---

## Why a Data Agent?

Healthcare organizations have data everywhere — claims, encounters, patient records, quality metrics — but the people who need answers most (clinicians, quality managers, finance directors) often **can't write SQL queries.**

A Data Agent bridges this gap. Instead of:
```sql
SELECT facility_name, AVG(length_of_stay_days) 
FROM gold_encounter_summary 
WHERE encounter_type = 'Inpatient' 
GROUP BY facility_name
```

A care manager simply asks:
> "What is the average length of stay for inpatient encounters at each facility?"

The Data Agent translates natural language into queries, runs them, and returns the answer — all within the governance framework of Microsoft Fabric.

---

## What You Will Do

1. Create a **Data Agent** in your workspace
2. Configure it to use your Lakehouse tables
3. Add **instructions** so it understands healthcare context
4. Test with clinical and operational questions
5. Share with your team
6. Test **Power BI Copilot** and compare approaches

---

## Part A: Create the Data Agent

### Step 1: Create a New Data Agent

1. Go to your workspace
2. Click **+ New item**
3. Search for and select **Data Agent** (it may appear as **AI Skill** in some tenants)
4. Name: `HealthFirst Clinical Analyst`
5. Click **Create**

### Step 2: Select Data Sources

After the Data Agent is created:

1. You'll see a configuration screen for the agent
2. Under **Data sources**, click **Add data**
3. Select your **HealthcareLakehouse**
4. Choose the following tables (select all Gold and Silver tables):

**Gold layer tables (primary):**
- `gold_readmissions`
- `gold_ed_utilization`
- `gold_encounter_summary`
- `gold_alos`
- `gold_financial`
- `gold_population_health`
- `gold_patient_360` *(created in Module 6)*
- `gold_facility_summary` *(created in Module 6)*

**Silver layer tables (supplemental):**
- `silver_patients`
- `silver_encounters`
- `silver_conditions`
- `silver_medications`
- `silver_claims`

**AI-enriched table (if created in Module 5):**
- `gold_clinical_ai_insights`

**Metadata table (from Module 6):**
- `data_dictionary`

5. Click **Confirm** to add all selected tables

---

## Part B: Configure the Agent Instructions

### Step 3: Add Custom Instructions

The Data Agent performs much better when it understands the healthcare context. In the agent configuration, find the **Instructions** section and paste:

```
You are a clinical data analyst for HealthFirst Medical Group, a healthcare 
network with three facilities: Metro General Hospital, Community Medical Center, 
and Riverside Health Center.

You help clinicians, quality managers, and administrators answer questions about 
patient outcomes, operational metrics, and financial performance.

Key domain knowledge:
- A "readmission" means a patient returned to the hospital within 30 days of 
  a prior inpatient discharge. The national readmission rate is ~15%.
- CMS penalizes hospitals with excessive readmissions under the HRRP program.
- Average Length of Stay (ALOS) is measured in days. The national average for 
  inpatient stays is ~4.5 days.
- ED frequent flyers are patients with 4 or more ED visits in a year.
- SIRS criteria (temperature >100.4°F, HR >90, RR >20) indicate potential sepsis.
- Claims can be Paid, Denied, or Pending. Denial rates vary by payer.
- Insurance types include Medicare, Medicaid, Commercial, and Self-Pay.

Important tables:
- gold_patient_360: Comprehensive patient view with demographics, encounters, 
  conditions, and financial data all in one table. Use this for patient-level questions.
- gold_facility_summary: Pre-computed facility metrics. Use this for facility 
  comparisons instead of aggregating raw tables.
- gold_readmissions: Contains index admission and readmission details with 
  30-day readmission flags
- gold_encounter_summary: One row per encounter with patient demographics, 
  facility, department, length of stay, and outcome details
- gold_ed_utilization: ED encounters with frequent flyer flags
- gold_financial: Claims with payment ratios and denial information
- gold_population_health: Patient-level chronic condition counts and risk info
- gold_alos: Average length of stay broken down by diagnosis
- data_dictionary: Describes all columns in all tables — query this first if 
  you're unsure what a column means

When answering:
- Always specify which facility or facilities the data covers
- Include relevant counts (N=) alongside percentages
- Flag any quality concerns (e.g., readmission rate above 15%)
- If comparing facilities, present results in a table format
- For patient-level queries, use gold_patient_360 first
```

### Step 4: Provide Example Questions (Optional)

If the agent configuration supports example questions/prompts, add these:

| Example Question | Expected Behavior |
|---|---|
| What is our readmission rate? | Query gold_readmissions, calculate % |
| Which facility has the highest ALOS? | Query gold_facility_summary |
| How many ED frequent flyers do we have? | Query gold_ed_utilization, filter is_frequent_flyer |
| What is our claims denial rate by payer? | Query gold_financial, group by payer |
| Show me patients with diabetes and CHF | Query gold_population_health, filter flags |
| Compare all three facilities | Query gold_facility_summary |

---

## Part C: Test the Data Agent

### Step 5: Ask Clinical Questions

In the agent chat interface, try these questions one at a time. Observe how the agent translates your question into a query and returns results.

#### Question 1: Readmission Overview
```
What is the overall 30-day readmission rate, and how does it break down by facility?
```

**Expected:** The agent queries `gold_readmissions`, calculates the readmission rate, and breaks it down by facility_name.

#### Question 2: Length of Stay
```
What is the average length of stay for inpatient encounters? 
Which diagnoses have the longest stays?
```

**Expected:** The agent uses `gold_encounter_summary` or `gold_alos` to show ALOS with a breakdown by diagnosis.

#### Question 3: ED Utilization
```
How many patients visited the ED more than 3 times this year? 
What are their most common diagnoses?
```

**Expected:** The agent queries `gold_ed_utilization` and joins with conditions to identify frequent flyers and their diagnoses.

#### Question 4: Financial Performance
```
What is our claims denial rate? Which payer has the highest denial rate, 
and how much revenue have we lost to denials?
```

**Expected:** The agent queries `gold_financial` to show denial rates and financial impact by payer.

#### Question 5: Population Health
```
How many patients have 3 or more chronic conditions? 
What percentage are on Medicare?
```

**Expected:** The agent queries `gold_population_health` and `silver_patients` for multimorbidity analysis.

#### Question 6: Cross-Domain Analysis (Using Patient 360)
```
Compare Metro General Hospital and Community Medical Center across:
readmission rate, average length of stay, and ED volume
```

**Expected:** The agent queries `gold_facility_summary` and presents a comparative table.

### Step 6: Try Your Own Questions

Think about what a hospital administrator or quality officer would want to know, and ask the agent. Some ideas:

- "Which department has the most encounters?"
- "What percentage of our patients are uninsured?"
- "Are there any diagnoses where our ALOS is significantly above average?"
- "Show me the trend of encounters by month"
- "Which high-risk patients have diabetes AND heart failure?"
- "Tell me everything about patient P-XXX" (uses gold_patient_360)

---

## Part D: Refine and Share

### Step 7: Iterate on Instructions

If the agent gives unexpected or incorrect answers:

1. Note what the agent got wrong
2. Update the **Instructions** with clarifications
3. Re-test the question

For example, if the agent confuses "readmission rate" with "readmission count," add:
```
When asked about "readmission rate," always return a percentage: 
(patients readmitted within 30 days / total index admissions) * 100
```

### Step 8: Share the Agent

1. Click the **Share** button in the agent settings
2. Add colleagues or groups who should have access
3. They can now use the agent from their Fabric workspace

> **Note:** Users accessing the Data Agent will only see data they have permissions to access through Fabric's built-in security model.

---

## Part E: Power BI Copilot — AI-Assisted Visual Analytics

Now that you've tested the Data Agent (text-based Q&A), let's test **Power BI Copilot** — which provides AI-assisted *visual* analytics.

### Data Agent vs. Power BI Copilot

| Capability | Data Agent | Power BI Copilot |
|---|---|---|
| **Output format** | Text answers + tables | Visual charts + narratives |
| **Data source** | Lakehouse tables directly | Semantic model (DAX measures) |
| **Best for** | Ad-hoc data questions | Visual exploration & presentations |
| **User persona** | Analysts, data-savvy managers | Executives, board presentations |
| **Underlying tech** | SQL generation over Lakehouse | DAX generation over semantic model |

### Step 9: Open Your Power BI Report

1. Open the Power BI report you created in Module 3
2. Click the **Copilot** button in the top ribbon (if available)
3. The Copilot pane opens on the right side

> **Note:** Power BI Copilot requires Fabric capacity (F64 or higher) and must be enabled by your admin. If you don't see the Copilot button, check with your instructor.

### Step 10: Test Copilot with Healthcare Questions

Try these prompts in the Copilot pane:

#### Prompt 1: Summary of Current Page
```
Summarize the key insights from this report page
```
**What to observe:** Copilot reads the visuals on the current page and generates a narrative summary. Compare this to the Data Agent — Copilot understands the *visual context*.

#### Prompt 2: Create a New Visual
```
Create a bar chart showing readmission rate by facility
```
**What to observe:** Copilot generates a visual using DAX queries against your semantic model.

#### Prompt 3: Suggest Insights
```
What are the most important trends in patient volume over time?
```
**What to observe:** Copilot identifies patterns and trends from the data, similar to a junior analyst.

#### Prompt 4: Ask a Complex Question
```
Which facility has the best financial performance based on 
collection rates and lowest denial rates?
```
**What to observe:** Copilot may create a comparison visual or provide a narrative answer.

#### Prompt 5: Request a Narrative
```
Write a summary for the hospital board about our quality metrics, 
including readmission rates and length of stay trends
```
**What to observe:** Copilot generates executive-level narrative text suitable for board presentations.

### Step 11: Compare Data Agent vs. Copilot

Ask the **same question** to both tools and compare:

**Question:** *"What is our 30-day readmission rate by facility?"*

| Aspect | Data Agent Answer | Power BI Copilot Answer |
|---|---|---|
| **Format** | | |
| **Detail level** | | |
| **Accuracy** | | |
| **Usefulness** | | |
| **Speed** | | |

Fill in the comparison table based on your experience. Discuss with your table group:

1. When would you use the Data Agent vs. Copilot?
2. Which tool would a CFO prefer? A quality nurse?
3. Could you chain them? (e.g., ask Data Agent for data, then Copilot for visualization)

---

## 💡 Discussion: AI-Powered Analytics in Healthcare

**Impact Scenarios:**
- **Quality Director:** "Show me readmission rates for CHF patients by facility" → Instant insight instead of waiting for IT to run a report
- **CFO:** "How much revenue did we lose to claim denials last quarter?" → Real-time financial visibility via Data Agent text answer OR Copilot visual dashboard
- **Board Presentation:** Use Copilot to generate executive narratives and visuals directly from the data

**Governance Considerations:**
- Data Agents respect Fabric workspace permissions (row-level security, table access)
- All queries are logged and auditable
- PHI (Protected Health Information) stays within the Fabric environment
- HIPAA compliance is maintained through Azure's compliance certifications
- Power BI Copilot uses the semantic model's security policies

**Discussion Questions:**
1. Who in a hospital would benefit most from each tool?
2. What safeguards should exist when non-technical users query patient data?
3. How does this approach compare to traditional BI report distribution?
4. What happens if the AI generates an incorrect response — what guardrails are needed?

---

## ✅ Module 7 Checklist

Confirm you have completed:

- [ ] Data Agent `HealthFirst Clinical Analyst` is created
- [ ] Lakehouse tables are connected as data sources (including gold_patient_360 and data_dictionary)
- [ ] Custom instructions added with healthcare domain context
- [ ] Successfully tested at least 4 natural language queries with the Data Agent
- [ ] The agent returns accurate, well-formatted responses
- [ ] Power BI Copilot tested with at least 3 prompts
- [ ] Compared Data Agent vs. Copilot for the same question
- [ ] You understand when to use each AI tool

---

## 🎉 Lab Complete!

Congratulations! You have built an end-to-end healthcare analytics platform on Microsoft Fabric covering:

| Module | What You Built |
|--------|---------------|
| **Module 1** | Lakehouse with Bronze data layer |
| **Module 2** | Silver (cleansed) and Gold (analytics) data layers |
| **Module 3** | Semantic model with 12 DAX measures and a 3-page Power BI dashboard |
| **Module 4** | Real-time patient vitals monitoring with sepsis SIRS detection |
| **Module 5** | AI-powered clinical note summarization, entity extraction, and ICD-10 coding |
| **Module 6** | Data quality validation and AI-ready data preparation |
| **Module 7** | Natural language Data Agent + Power BI Copilot for self-service analytics |

This represents the full spectrum of what Microsoft Fabric enables for healthcare — from raw data landing to AI-powered insights, all in a unified, governed platform.

> **💡 Want to see an alternate approach?** [Module 8 (Optional)](Module08_VSCode_Agent_Mode.md) shows how to build this entire lab using VS Code Agent Mode — an AI-assisted approach where you describe what you want and GitHub Copilot builds it for you.

> **🔮 Want to build a predictive model?** [Module 9 (Optional)](Module09_Readmission_Prediction.md) uses Gen AI-assisted feature engineering to predict 30-day hospital readmissions with XGBoost — turning your Gold layer data into actionable risk scores.

---

**[← Module 6: Prep Data for AI](Module06_Prep_Data_for_AI.md)** | **[Module 8 (Optional): VS Code Agent Mode →](Module08_VSCode_Agent_Mode.md)** | **[Back to Overview](../README.md)**
