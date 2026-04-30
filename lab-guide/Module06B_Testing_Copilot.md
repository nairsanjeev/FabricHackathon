# Module 6B: Testing Power BI Copilot & Standalone Copilot

| Duration | 30–45 minutes |
|----------|------------|
| Objective | Thoroughly test Power BI Copilot (in-report) and the standalone Fabric Copilot with a wide variety of healthcare prompts — building fluency with what AI-assisted analytics can (and cannot) do |
| Fabric Features | Power BI Copilot, Standalone Fabric Copilot, Semantic Model |

---

## Why a Dedicated Copilot Testing Module?

In Module 6 you prepared your data and semantic model for AI. Now it's time to **put that preparation to the test** — not with one or two prompts, but with dozens of varied questions across different categories, personas, and difficulty levels.

The goal is to build **pattern recognition** for what Copilot handles well, where it struggles, and how prompt phrasing changes the output quality. By the end, you'll know exactly when to reach for Copilot and when to use the Data Agent (Module 7) instead.

---

## Prerequisites

- ✅ Completed **Module 3** (Semantic Model with DAX measures and Power BI report)
- ✅ Completed **Module 6** (Data quality checks, Patient 360, data dictionary)
- ✅ Power BI Copilot is **enabled** on your Fabric capacity (F64 or higher, or Fabric Trial)

> **Can't see the Copilot button?** Check with your instructor. Copilot requires Fabric capacity and admin enablement. If unavailable, you can still review the prompts and expected outputs as a reference.

---

## Part A: Open Copilot and Set Up Your Testing Environment

### Step 1: Open Your Power BI Report

1. Go to your Fabric workspace
2. Open the Power BI report you created in **Module 3**
3. Click the **Copilot** button in the top ribbon — the Copilot pane opens on the right

### Step 2: Select the Right Skill

In the Copilot chat box, you'll see a **skill picker** dropdown. Select:

- **"Answer questions about the data"** — for data exploration and Q&A
- **"Create a report page"** — for visual generation
- **"Summarize this page"** — for narrative summaries of what's on the current page

> **💡 Tip:** Try the same prompt with different skills selected to see how the output changes.

### Step 3: Testing Approach

For each prompt below:
1. Type (or copy-paste) the prompt into the Copilot pane
2. Observe the **response type** (visual, narrative, table, or error)
3. Note whether Copilot used the **correct table and measure**
4. Rate the quality: ✅ Accurate, ⚠️ Partially correct, or ❌ Wrong/Unhelpful
5. Try **rephrasing** if the first attempt doesn't work — wording matters!

---

## Part B: Quick Fact Lookups

These prompts test whether Copilot can retrieve straightforward facts from your semantic model. They should produce simple answers — numbers, percentages, or short lists.

#### Prompt B1: Total Volume
```
How many total encounters are in our data?
```
> **Expected:** A single number from `Total Encounters` measure or `COUNTROWS(gold_encounter_summary)`.

#### Prompt B2: Unique Patients
```
How many unique patients do we serve across all facilities?
```
> **Expected:** A distinct count of patient IDs.

#### Prompt B3: Readmission Rate
```
What is our overall 30-day readmission rate?
```
> **Expected:** A percentage calculated from `gold_readmissions`. Compare to the `Readmission Rate` DAX measure if defined.

#### Prompt B4: ED Frequent Flyers
```
How many patients are classified as ED frequent flyers?
```
> **Expected:** Count of patients where `is_frequent_flyer = TRUE` from `gold_ed_utilization`.

#### Prompt B5: Average Length of Stay
```
What is the average length of stay for inpatient admissions?
```
> **Expected:** Average of `length_of_stay_days` filtered to `encounter_type = 'Inpatient'`.

#### Prompt B6: Denial Rate
```
What percentage of our claims were denied?
```
> **Expected:** Percentage of claims with `claim_status = 'Denied'` from `gold_financial`.

#### Prompt B7: Chronic Disease Count
```
How many patients have 3 or more chronic conditions?
```
> **Expected:** Count from `gold_population_health` where `chronic_condition_count >= 3`.

#### Prompt B8: Revenue Total
```
What is our total revenue collected across all payers?
```
> **Expected:** Sum of `paid_amount` from `gold_financial`.

#### Prompt B9: Facility Count
```
How many facilities are in our healthcare network and what are their names?
```
> **Expected:** 3 facilities — Metro General Hospital, Community Medical Center, Riverside Health System.

#### Prompt B10: Insurance Mix
```
What percentage of our patients are on Medicare vs Medicaid vs Commercial vs Self-Pay?
```
> **Expected:** A breakdown by `insurance_type` showing count and percentage for each.

---

## Part C: Visual Creation Prompts

These prompts ask Copilot to **build new visuals**. Observe the chart type it chooses, the fields it maps, and whether the visual is useful without manual edits.

#### Prompt C1: Bar Chart — Readmission by Facility
```
Create a bar chart showing 30-day readmission rate for each facility
```
> **Expected:** A clustered bar chart with facility names on the x-axis and readmission rate (%) on the y-axis.

#### Prompt C2: Pie Chart — Insurance Distribution
```
Create a pie chart showing the distribution of patients by insurance type
```
> **Expected:** A pie chart with slices for Medicare, Medicaid, Commercial, and Self-Pay.

#### Prompt C3: Line Chart — Encounters Over Time
```
Create a line chart showing the number of encounters per month over the past year
```
> **Expected:** A line chart with months on the x-axis and encounter count on the y-axis.

#### Prompt C4: Table — Facility Comparison
```
Create a table comparing each facility with columns for total encounters, 
average length of stay, readmission rate, and ED visit count
```
> **Expected:** A matrix or table visual with one row per facility and the requested KPI columns.

#### Prompt C5: Stacked Bar — Encounter Types by Facility
```
Create a stacked bar chart showing the breakdown of encounter types 
(Inpatient, ED, Outpatient, Observation) for each facility
```
> **Expected:** A stacked bar chart with facilities on the axis, colored segments per encounter type.

#### Prompt C6: KPI Card
```
Create a KPI card showing the overall readmission rate with the target of 15%
```
> **Expected:** A KPI or card visual displaying the readmission rate. Note: Copilot may not support conditional formatting — you'd add red/green thresholds manually.

#### Prompt C7: Scatter Plot — Cost vs. LOS
```
Create a scatter plot of average length of stay vs total charges by facility
```
> **Expected:** A scatter chart with LOS on one axis, charges on the other, and points labeled by facility.

#### Prompt C8: Treemap — Diagnosis Volume
```
Create a treemap showing the top diagnoses by number of encounters
```
> **Expected:** A treemap visual sized by encounter count for each diagnosis category.

#### Prompt C9: Donut Chart — Claim Status
```
Create a donut chart showing the split between Paid, Denied, and Pending claims
```
> **Expected:** A donut/ring chart with 3 segments for claim statuses.

#### Prompt C10: Combo Chart — Encounters and ALOS
```
Create a combination chart with total encounters as bars and average 
length of stay as a line, broken down by month
```
> **Expected:** A combo chart (bars + line on secondary axis) showing volume alongside efficiency.

#### Prompt C11: Funnel — Claims Pipeline
```
Create a funnel chart showing the claims pipeline from Total Billed 
to Paid Amount to show revenue leakage
```
> **Expected:** A funnel visual showing the progression from billed → paid, highlighting the gap.

#### Prompt C12: Map Visual (if geographic data exists)
```
Show encounters by facility on a map
```
> **Expected:** Copilot may not have geographic data — observe how it handles this gracefully.

---

## Part D: Visual Modification & Formatting

Start with an **existing visual** on your report page. Select it, then ask Copilot to modify it.

#### Prompt D1: Change Chart Type
```
Change this bar chart to a line chart
```
> **Expected:** Copilot swaps the visual type while keeping the same data fields.

#### Prompt D2: Add a Field
```
Add insurance type as a legend to this chart
```
> **Expected:** Copilot adds a legend/color grouping by insurance type.

#### Prompt D3: Remove a Field
```
Remove the encounter_id column from this table
```
> **Expected:** Copilot removes the field from the visual.

#### Prompt D4: Change Title
```
Change the title of this chart to "30-Day Readmission Performance by Facility"
```
> **Expected:** Copilot updates the visual title.

#### Prompt D5: Add Filter
```
Filter this visual to show only Medicare patients
```
> **Expected:** Copilot adds a visual-level filter for `insurance_type = 'Medicare'`.

#### Prompt D6: Sort
```
Sort this chart from highest to lowest readmission rate
```
> **Expected:** Copilot applies descending sort.

#### Prompt D7: Relative Date Filter
```
Filter to show only encounters from the last 6 months
```
> **Expected:** Copilot applies a relative date filter on the encounter date field.

#### Prompt D8: Switch Axes
```
Swap the X and Y axes on this chart to make it horizontal
```
> **Expected:** Copilot swaps the axis assignments to create a horizontal layout.

> **⚠️ Reminder — Copilot Formatting Limits:**
> Copilot **cannot** resize visuals, apply conditional formatting, change font sizes, or set custom color palettes. For those, use the **Format** pane manually.

---

## Part E: Facility & Payer Comparisons

Healthcare executives constantly compare facilities and payers. These prompts test Copilot's ability to do **comparative analysis**.

#### Prompt E1: Head-to-Head Facility
```
Compare Metro General Hospital and Community Medical Center on readmission rate, 
average length of stay, and total charges
```

#### Prompt E2: Best and Worst
```
Which facility performs best and worst on 30-day readmission rate?
```

#### Prompt E3: Payer Comparison
```
Compare denial rates across Medicare, Medicaid, Commercial, and Self-Pay payers
```

#### Prompt E4: Revenue by Payer
```
Show total revenue and collection rate for each insurance type
```

#### Prompt E5: Facility Efficiency
```
Which facility has the lowest average length of stay for inpatient encounters?
```

#### Prompt E6: Cross-Metric Comparison
```
Create a table showing each facility with readmission rate, ALOS, 
ED volume, denial rate, and total charges side by side
```

#### Prompt E7: Benchmark Against Target
```
Which facilities have a readmission rate above the 15% national benchmark?
```

#### Prompt E8: Payer Risk Profile
```
Which insurance type has the highest proportion of high-risk patients?
```

---

## Part F: Trend & Time-Based Analysis

These prompts test Copilot's ability to identify **patterns over time** — a common executive ask.

#### Prompt F1: Monthly Trend
```
Show the trend of total encounters per month over the entire dataset
```

#### Prompt F2: Readmission Trend
```
Is our readmission rate improving or getting worse over time? Show the trend
```

#### Prompt F3: Seasonal Patterns
```
Are there seasonal patterns in ED visits? Show ED volume by month
```

#### Prompt F4: LOS Trend
```
Show how average length of stay has changed over the past 12 months
```

#### Prompt F5: Financial Trend
```
Show monthly revenue trends — is our collection rate improving?
```

#### Prompt F6: Year-over-Year
```
Compare this year's readmission rate to last year's by quarter
```

#### Prompt F7: Forecast Ask
```
Based on current trends, what will our encounter volume be next quarter?
```
> **Note:** Power BI Copilot does not do statistical forecasting — observe how it responds to this. It may suggest using the built-in forecast feature or Analytics pane instead.

#### Prompt F8: Rolling Average
```
Show a 3-month rolling average of inpatient admissions
```
> **Note:** Copilot may struggle with calculated rolling averages. This tests its limits.

---

## Part G: Complex Multi-Step Analysis

These prompts require Copilot to **combine multiple data points** or apply multiple filters — testing the depth of its reasoning.

#### Prompt G1: Multi-Filter
```
Show readmission rates for Medicare patients over 65 at Metro General Hospital
```

#### Prompt G2: Cross-Domain
```
Do patients with 3 or more chronic conditions have longer lengths of stay 
than patients with fewer conditions?
```

#### Prompt G3: Root Cause
```
What are the top 5 diagnoses driving readmissions at the facility 
with the highest readmission rate?
```

#### Prompt G4: Correlation
```
Is there a relationship between the number of ED visits and the 
likelihood of readmission?
```

#### Prompt G5: Outlier Detection
```
Which patients have the highest total charges? Are they outliers or 
part of a pattern?
```

#### Prompt G6: Cohort Analysis
```
Compare outcomes for diabetic patients vs non-diabetic patients — 
readmission rate, average LOS, and total charges
```

#### Prompt G7: Risk Stratification
```
Break down our patient population by risk category (Low, Moderate, High, Critical) 
and show the readmission rate and average cost for each tier
```

#### Prompt G8: Multi-Condition
```
How many patients have both diabetes AND heart failure? 
What is their readmission rate compared to patients with neither condition?
```

#### Prompt G9: Resource Utilization
```
Which diagnoses consume the most bed-days (total LOS × admissions)? 
Show as a ranked list
```

#### Prompt G10: Complete Picture
```
Give me a complete picture of Metro General Hospital: patient volume, 
encounter mix, readmission rate, average LOS, top diagnoses, denial rate, 
and chronic disease prevalence
```

---

## Part H: Narrative & Executive Summaries

These prompts ask Copilot to **write text** — useful for board presentations, quality committee reports, and meeting preparation.

#### Prompt H1: Page Summary
```
Summarize the key insights from this report page in 3 bullet points
```

#### Prompt H2: Board Report
```
Write a 3-paragraph summary for the hospital board about our quality 
performance this period, covering readmission rates, length of stay, 
and ED utilization
```

#### Prompt H3: Quality Committee Brief
```
Prepare a quality committee briefing highlighting any metrics that are 
trending in the wrong direction
```

#### Prompt H4: CFO Summary
```
Write an executive summary of our financial performance for the CFO, 
including revenue, collection rate, denial rate, and revenue lost to denials
```

#### Prompt H5: Action Items
```
Based on the data, what are the top 3 actions the hospital should 
take to reduce readmission rates? Support each with specific data
```

#### Prompt H6: Talking Points
```
Give me 5 talking points for a meeting about reducing ED frequent 
flyer visits, backed by data from our reports
```

#### Prompt H7: Compare and Recommend
```
Compare all three facilities and recommend which one should be the 
focus for a readmission reduction initiative. Justify with data
```

#### Prompt H8: Newsletter Blurb
```
Write a short paragraph for a staff newsletter celebrating our 
quality achievements this period
```

---

## Part I: Standalone Fabric Copilot

The **standalone Fabric Copilot** works across your entire workspace — not just the report you have open. It's great for discovery, meeting prep, and cross-report questions.

### Step 4: Open the Standalone Copilot

1. In the Fabric portal, click the **Copilot** icon in the left navigation bar (the ✨ sparkle icon)
2. The standalone Copilot opens with an "Ask a question about your data" prompt

### Discovery Prompts

#### Prompt I1: Find Reports
```
What reports and dashboards are available in my workspace?
```

#### Prompt I2: Topic Search
```
Find reports about patient readmissions
```

#### Prompt I3: Data Inventory
```
What data sources and tables are available in this workspace?
```

### Meeting Prep Prompts

#### Prompt I4: Quick Briefing
```
Prep a 2-minute summary for a meeting about hospital performance
```

#### Prompt I5: Facility Briefing
```
Prepare a briefing about Metro General Hospital's performance 
for a leadership meeting
```

#### Prompt I6: Quality Standup
```
Summarize our quality metrics for a morning standup — 
readmissions, ALOS, and ED volume
```

#### Prompt I7: Financial Review Prep
```
Prepare talking points for a payer contract meeting — 
show our volume by insurance type and denial rates
```

### Cross-Report Questions

#### Prompt I8: Population Overview
```
What chronic conditions are most common in our patient population?
```

#### Prompt I9: Holistic View
```
What are the biggest operational challenges facing our hospital network 
based on the data?
```

#### Prompt I10: Risk Summary
```
Which patient populations are highest risk for readmission?
```

#### Prompt I11: Comparison
```
How does our ED utilization compare across the three facilities?
```

#### Prompt I12: Financial Health
```
Give me an overview of our revenue cycle health — are we collecting 
what we bill?
```

### Visual Creation from Standalone

#### Prompt I13: Create from Scratch
```
Create a visual showing the top 5 diagnoses by total charges
```

#### Prompt I14: Dashboard Request
```
Create a dashboard page showing our key quality metrics: 
readmission rate, ALOS, ED frequent flyers, and denial rate
```

---

## Part J: Testing Copilot Limitations

It's important to know **what Copilot can't do** so you set realistic expectations. Try these prompts to see how Copilot handles edge cases.

#### Prompt J1: Formatting Request
```
Make the readmission rate card red if above 15% and green if below 10%
```
> **Expected:** Copilot will explain it can't apply conditional formatting. Do this manually: select the card → Format pane → Callout value → Color → **fx** → set rules.

#### Prompt J2: Visual Sizing
```
Make this chart twice as large and move it to the top-left corner
```
> **Expected:** Copilot can't resize or reposition visuals. Drag corners manually.

#### Prompt J3: Custom Colors
```
Change the bar colors to our hospital brand colors: #1B4F72 and #27AE60
```
> **Expected:** Copilot can't set custom hex colors. Use Format pane → Data colors manually.

#### Prompt J4: Write DAX
```
Create a DAX measure that calculates the 90-day readmission rate
```
> **Expected:** Copilot may describe the DAX but can't create measures in the semantic model. You'd need to open the model editor and add it manually.

#### Prompt J5: Cross-Report Join
```
Compare readmission data from this report with the financial report 
from a different workspace
```
> **Expected:** Copilot works within a single semantic model. Cross-workspace queries are not supported.

#### Prompt J6: Data Modification
```
Delete all records where encounter_type is 'Observation'
```
> **Expected:** Copilot is read-only — it cannot modify underlying data.

#### Prompt J7: Predictive
```
Predict which patients will be readmitted in the next 30 days
```
> **Expected:** Copilot does not run predictive models. For this, see Module 9 (Readmission Prediction).

---

## Part K: Prompt Engineering Tips for Power BI Copilot

After testing all the prompts above, you've likely noticed that **how you phrase a question** matters enormously. Here are proven techniques:

### Tip 1: Be Specific About the Visual Type
| ❌ Vague | ✅ Specific |
|---|---|
| Show me readmissions | Create a bar chart showing readmission rate by facility |

### Tip 2: Name the Fields
| ❌ Vague | ✅ Specific |
|---|---|
| Show costs by hospital | Show total_charges by facility_name as a column chart |

### Tip 3: Specify Filters Upfront
| ❌ Vague | ✅ Specific |
|---|---|
| Show inpatient data | Show encounters filtered to encounter_type = 'Inpatient' |

### Tip 4: Ask for Comparisons Explicitly
| ❌ Vague | ✅ Specific |
|---|---|
| How is Metro General? | Compare Metro General Hospital to the network average on readmission rate and ALOS |

### Tip 5: Use Domain Terms from Your AI Instructions
If you configured AI Instructions in Module 6 Part G, use those exact terms:
- ✅ "readmission rate" (matches your definition: 30-day return)
- ✅ "frequent flyer" (matches your definition: 4+ ED visits)
- ✅ "multimorbidity" (matches your definition: 3+ chronic conditions)

### Tip 6: Iterate — Don't Start Over
If Copilot creates a visual that's close but not right:
```
Change the sort order to descending
```
```
Add insurance_type as a filter
```
```
Change the title to "Quality Dashboard — Q4"
```

---

## Part L: Score Your Copilot Experience

Use this rubric to rate your overall experience. Fill in after testing.

### In-Report Copilot Scorecard

| Category | Prompts Tested | ✅ Accurate | ⚠️ Partial | ❌ Wrong | Notes |
|---|---|---|---|---|---|
| Quick Facts (Part B) | /10 | | | | |
| Visual Creation (Part C) | /12 | | | | |
| Visual Modification (Part D) | /8 | | | | |
| Comparisons (Part E) | /8 | | | | |
| Trends (Part F) | /8 | | | | |
| Complex Analysis (Part G) | /10 | | | | |
| Narratives (Part H) | /8 | | | | |

### Standalone Copilot Scorecard

| Category | Prompts Tested | ✅ Accurate | ⚠️ Partial | ❌ Wrong | Notes |
|---|---|---|---|---|---|
| Discovery (I1–I3) | /3 | | | | |
| Meeting Prep (I4–I7) | /4 | | | | |
| Cross-Report (I8–I12) | /5 | | | | |
| Visual Creation (I13–I14) | /2 | | | | |

### Reflection Questions

1. Which **category** did Copilot perform best in?
2. Which prompts required **rephrasing** to get the right answer?
3. Did the **AI Instructions** (Module 6 Part G) noticeably improve accuracy?
4. What **types of questions** should you ask Copilot vs. the Data Agent (Module 7)?
5. Where would you **not trust** Copilot without verifying the answer?

---

## 💡 Key Takeaways

| Copilot Capability | In-Report Copilot | Standalone Copilot |
|---|---|---|
| **Best for** | Page-aware analysis, visual creation | Discovery, meeting prep, cross-report |
| **Input context** | Current report page + semantic model | Entire workspace |
| **Output types** | Visuals, narratives, filters | Summaries, answers, visual suggestions |
| **Persona** | Analysts building dashboards | Executives needing quick answers |

**What Copilot excels at:**
- Creating first-draft visuals quickly
- Summarizing report pages in plain English
- Applying filters and swapping chart types
- Answering simple data questions

**Where you'll still need manual work:**
- Conditional formatting (red/green thresholds)
- Visual sizing and layout
- Custom color palettes and branding
- Complex DAX measure creation
- Predictive analytics

---

## ✅ Module 6B Checklist

- [ ] Tested at least **5 Quick Fact** prompts (Part B)
- [ ] Created at least **3 new visuals** using Copilot (Part C)
- [ ] Modified an existing visual with at least **2 change requests** (Part D)
- [ ] Ran **2 facility or payer comparison** prompts (Part E)
- [ ] Asked at least **1 trend question** (Part F)
- [ ] Tested **1 complex multi-step** question (Part G)
- [ ] Generated **1 executive narrative** (Part H)
- [ ] Tested the **standalone Copilot** with at least **3 prompts** (Part I)
- [ ] Tested at least **2 limitation prompts** to understand boundaries (Part J)
- [ ] Filled in the **Copilot Scorecard** (Part L)
- [ ] Discussed prompt engineering tips with your team

---

**[← Module 6: Prep Data for AI](Module06_Prep_Data_for_AI.md)** | **[Module 7: Data Agent →](Module07_Data_Agent.md)**
