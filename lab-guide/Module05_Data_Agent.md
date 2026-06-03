# Module 5: Data Agent

| Duration | 40 minutes |
|----------|------------|
| Objective | Prepare your semantic model for AI, then create a Fabric Data Agent that allows clinical and administrative staff to query healthcare data using plain English |
| Fabric Features | Prep Data for AI, Data Agent (AI Skill), Lakehouse integration |

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

1. **Prep your semantic model for AI** — simplify schema, add instructions, and set up verified answers
2. Create a **Data Agent** in your workspace
3. Configure it to use your Lakehouse tables
4. Add **instructions** so it understands healthcare context
5. Test with clinical and operational questions
6. Share with your team

---

## Part A: Prep Data for AI in Power BI (Semantic Model Layer)

Before creating the Data Agent, we'll configure the semantic model so that AI tools (both Copilot and the Data Agent) understand your data better. Power BI offers a feature called **"Prep data for AI"** that works **at the Semantic Model level** — it tells AI *how* to interpret your model, what business terms mean, and which visuals to return for common questions.

The **Prep data for AI** button (preview) is available on the **Home ribbon** in Power BI Desktop and on the **Semantic Model page ribbon** in the Power BI service. It provides three features:

### Feature 1: AI Data Schema — Simplify What AI Sees

Not every column in your semantic model is relevant for natural language Q&A. The AI Data Schema lets you **select which fields AI should reason over**, removing noise and ambiguity.

#### Steps

1. Open your healthcare report in **Power BI Desktop** (or select the semantic model in the Power BI service)
2. Click **Prep data for AI** on the Home ribbon
3. Go to the **Simplify data schema** tab
4. **Deselect** columns that would confuse AI — for example:
   - Internal surrogate keys (`encounter_id`, `claim_id`) — keep only human-readable identifiers
   - ETL metadata columns (`_loaded_at`, `_source_file`)
   - Raw codes when you also have descriptions (keep `condition_description`, hide `condition_code`)
5. **Keep selected** the columns that users would naturally ask about:
   - `patient_name`, `age`, `insurance_type`, `facility_name`
   - `total_charges`, `readmission_rate_pct`, `length_of_stay_days`
   - `chronic_condition_count`, `multimorbidity`, `claim_status`
6. Click **Apply**

> **Healthcare example:** A clinician asking "Which patients have the highest ED utilization?" doesn't need to see `encounter_id` or `payer_code`. By hiding those fields, AI focuses on the right columns and produces cleaner answers.

---

### Feature 2: AI Instructions — Teach AI Your Business Context

AI Instructions let you provide **plain-text guidance** that Copilot and the Data Agent use when interpreting questions. This is where you encode domain knowledge, terminology, and analysis rules.

#### Steps

1. In the **Prep data for AI** dialog, go to the **Add AI instructions** tab
2. Enter instructions that help AI understand your healthcare data. Here is a recommended set for our lab:

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
- A "high-risk" patient has risk_category = 'Critical' or 'High'
- ED utilization analysis should highlight frequent flyers (is_frequent_flyer = TRUE)

## Data Priority
- Use gold_readmissions for 30-day readmission analysis
- Use gold_financial for revenue cycle and claims questions
- Use gold_encounter_summary for encounter-level questions
- Use gold_population_health for chronic disease and risk analysis
```

3. Click **Apply**

> **Why this matters:** Without instructions, AI might not know that "readmission" means a 30-day return, or that "frequent flyer" is a clinical term with a specific threshold. These instructions ground AI in your organization's definitions.

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

### Mark Your Model as Approved for Copilot

Once you're satisfied with the configuration:

1. Go to the **Power BI service** and find your semantic model
2. Click the **Settings** icon
3. Expand the **Approved for Copilot** section
4. Check the **Approved for Copilot** box → click **Apply**

This removes friction treatments (disclaimers) from Copilot answers for your model, signaling that the data is curated and trusted.

---

## Part B: Create the Data Agent

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

**Silver layer tables (supplemental):**
- `silver_patients`
- `silver_encounters`
- `silver_conditions`
- `silver_medications`
- `silver_claims`

**AI-enriched table (if created in Module 6):**
- `gold_clinical_ai_insights`

5. Click **Confirm** to add all selected tables

---

## Part C: Configure the Agent Instructions

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
- gold_financial: Claims with payment ratios and denial information
- gold_population_health: Patient-level chronic condition counts and risk info
- gold_alos: Average length of stay broken down by diagnosis

When answering:
- Always specify which facility or facilities the data covers
- Include relevant counts (N=) alongside percentages
- Flag any quality concerns (e.g., readmission rate above 15%)
- If comparing facilities, present results in a table format
```

### Step 4: Provide Example Questions

If the agent configuration supports example questions/prompts, add these:

| Example Question | Expected Behavior |
|---|---|
| What is our readmission rate? | Query gold_readmissions, calculate % |
| Which facility has the highest ALOS? | Query gold_alos or gold_encounter_summary |
| How many ED frequent flyers do we have? | Query gold_ed_utilization, filter is_frequent_flyer |
| What is our claims denial rate by payer? | Query gold_financial, group by payer |
| Show me patients with diabetes and CHF | Query gold_population_health, filter flags |

---

## Part D: Test the Data Agent

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

#### Question 6: Cross-Facility Comparison
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
- "Which high-risk patients have diabetes AND heart failure?"

---

## Part E: Refine and Share

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

## 💡 Discussion: AI-Powered Analytics in Healthcare

**Impact Scenarios:**
- **Quality Director:** "Show me readmission rates for CHF patients by facility" → Instant insight instead of waiting for IT to run a report
- **CFO:** "How much revenue did we lose to claim denials last quarter?" → Real-time financial visibility
- **Board Presentation:** Use Copilot to generate executive narratives and visuals directly from the data

**Governance Considerations:**
- Data Agents respect Fabric workspace permissions (row-level security, table access)
- All queries are logged and auditable
- PHI (Protected Health Information) stays within the Fabric environment
- HIPAA compliance is maintained through Azure's compliance certifications

**Discussion Questions:**
1. Who in a hospital would benefit most from a Data Agent?
2. What safeguards should exist when non-technical users query patient data?
3. How does this approach compare to traditional BI report distribution?
4. What happens if the AI generates an incorrect response — what guardrails are needed?

---

## ✅ Module 5 Checklist

Confirm you have completed:

- [ ] Prep Data for AI configured (AI Data Schema, AI Instructions, Verified Answers)
- [ ] Semantic model marked as Approved for Copilot
- [ ] Data Agent `HealthFirst Clinical Analyst` is created
- [ ] Lakehouse tables are connected as data sources
- [ ] Custom instructions added with healthcare domain context
- [ ] Successfully tested at least 4 natural language queries with the Data Agent
- [ ] The agent returns accurate, well-formatted responses

---

**[← Module 4: Real-Time Analytics](Module04_RealTime_Analytics.md)** | **[Module 6: Gen AI — Clinical Intelligence →](Module06_GenAI_Clinical_Intelligence.md)**
