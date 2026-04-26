# Module 6: Data Agent — Natural Language Analytics

| Duration | 30 minutes |
|----------|------------|
| Objective | Create a Fabric Data Agent that allows clinical and administrative staff to query healthcare data using plain English — no SQL, no KQL, no coding required |
| Fabric Features | Data Agent (AI Skill), Lakehouse integration |

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
- `gold_alos_by_diagnosis`
- `gold_financial_analysis`
- `gold_population_health`

**Silver layer tables (supplemental):**
- `silver_patients`
- `silver_encounters`
- `silver_conditions`
- `silver_medications`
- `silver_claims`

**AI-enriched table (if created in Module 5):**
- `gold_clinical_ai_insights`

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
- gold_readmissions: Contains index admission and readmission details with 
  30-day readmission flags
- gold_encounter_summary: One row per encounter with patient demographics, 
  facility, department, length of stay, and outcome details
- gold_ed_utilization: ED encounters with frequent flyer flags
- gold_financial_analysis: Claims with payment ratios and denial information
- gold_population_health: Patient-level chronic condition counts and risk info
- gold_alos_by_diagnosis: Average length of stay broken down by diagnosis
- silver_patients: Patient demographics (age, gender, insurance, zip code)
- silver_conditions: Patient diagnoses with ICD-10 codes

When answering:
- Always specify which facility or facilities the data covers
- Include relevant counts (N=) alongside percentages
- Flag any quality concerns (e.g., readmission rate above 15%)
- If comparing facilities, present results in a table format
```

### Step 4: Provide Example Questions (Optional)

If the agent configuration supports example questions/prompts, add these:

| Example Question | Expected Behavior |
|---|---|
| What is our readmission rate? | Query gold_readmissions, calculate % |
| Which facility has the highest ALOS? | Query gold_encounter_summary, group by facility |
| How many ED frequent flyers do we have? | Query gold_ed_utilization, filter is_frequent_flyer |
| What is our claims denial rate by payer? | Query gold_financial_analysis, group by payer |
| Show me patients with diabetes and CHF | Query silver_conditions, filter by condition codes |

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

**Expected:** The agent uses `gold_encounter_summary` or `gold_alos_by_diagnosis` to show ALOS with a breakdown by diagnosis.

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

**Expected:** The agent queries `gold_financial_analysis` to show denial rates and financial impact by payer.

#### Question 5: Population Health
```
How many patients have 3 or more chronic conditions? 
What percentage are on Medicare?
```

**Expected:** The agent queries `gold_population_health` and `silver_patients` for multimorbidity analysis.

#### Question 6: Cross-Domain Analysis
```
Compare Metro General Hospital and Community Medical Center across:
readmission rate, average length of stay, and ED volume
```

**Expected:** The agent queries multiple Gold tables and presents a comparative table.

### Step 6: Try Your Own Questions

Think about what a hospital administrator or quality officer would want to know, and ask the agent. Some ideas:

- "Which department has the most encounters?"
- "What percentage of our patients are uninsured?"
- "Are there any diagnoses where our ALOS is significantly above average?"
- "Show me the trend of encounters by month"
- "Which medications are most commonly prescribed to diabetic patients?"

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

## 💡 Discussion: Data Agents in Healthcare

**Impact Scenarios:**
- **Quality Director:** "Show me readmission rates for CHF patients by facility" → Instant insight instead of waiting for IT to run a report
- **CFO:** "How much revenue did we lose to claim denials last quarter?" → Real-time financial visibility
- **Care Manager:** "Which patients with diabetes haven't had an encounter in 6 months?" → Proactive outreach

**Governance Considerations:**
- Data Agents respect Fabric workspace permissions (row-level security, table access)
- All queries are logged and auditable
- PHI (Protected Health Information) stays within the Fabric environment
- HIPAA compliance is maintained through Azure's compliance certifications

**Discussion Questions:**
1. Who in a hospital would benefit most from a Data Agent?
2. What safeguards should exist when non-technical users query patient data?
3. How does this approach compare to traditional BI report distribution?
4. What happens if the agent generates an incorrect response — what guardrails are needed?

---

## ✅ Module 6 Checklist

Confirm you have completed:

- [ ] Data Agent `HealthFirst Clinical Analyst` is created
- [ ] Lakehouse tables are connected as data sources
- [ ] Custom instructions added with healthcare domain context
- [ ] Successfully tested at least 4 natural language queries
- [ ] The agent returns accurate, well-formatted responses
- [ ] You understand how to refine instructions based on query results

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
| **Module 6** | Natural language Data Agent for self-service clinical analytics |

This represents the full spectrum of what Microsoft Fabric enables for healthcare — from raw data landing to AI-powered insights, all in a unified, governed platform.

---

**[← Module 5: Gen AI — Clinical Intelligence](Module05_GenAI_Clinical_Intelligence.md)** | **[Back to Overview](../README.md)**
