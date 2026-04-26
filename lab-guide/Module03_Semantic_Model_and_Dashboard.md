# Module 3: Semantic Model & Power BI Dashboard

| Duration | 90 minutes |
|----------|------------|
| Objective | Build a star-schema Semantic Model on Gold tables and create an interactive Power BI dashboard for healthcare operations |
| Fabric Features | Default Semantic Model, Power BI Report, DAX Measures |

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

### Step 1: Open the Default Semantic Model

Every Lakehouse in Fabric automatically creates a **default Semantic Model** (also called a dataset). We'll configure this model.

1. Navigate to your workspace
2. Find **HealthcareLakehouse** in the list — you'll notice there are multiple items with that name. Look for the one with a **semantic model icon** (it looks like a small database symbol).
   > Alternatively, open your Lakehouse, and in the top-right corner, click on the **Lakehouse** dropdown and switch to **SQL analytics endpoint**
3. Click on the Semantic Model item

### Step 2: Select Tables for the Model

If prompted to select tables to include in the model:

1. Check the following **Gold tables**:
   - ✅ `gold_encounter_summary`
   - ✅ `gold_readmissions`
   - ✅ `gold_ed_utilization`
   - ✅ `gold_financial`
   - ✅ `gold_population_health`
   - ✅ `gold_alos`
2. Also include these Silver tables for dimension lookups:
   - ✅ `silver_patients`
3. Click **Confirm** or **Save**

> **Note:** If you opened the SQL analytics endpoint, you may need to navigate to Model view. Click on the **Model** tab at the bottom of the screen.

### Step 3: Create Relationships

In the Model view (diagram view), create the following relationships by dragging columns between tables:

| From Table | From Column | To Table | To Column | Cardinality |
|------------|-------------|----------|-----------|-------------|
| `gold_encounter_summary` | `patient_id` | `silver_patients` | `patient_id` | Many-to-One |
| `gold_readmissions` | `patient_id` | `silver_patients` | `patient_id` | Many-to-One |
| `gold_financial` | `encounter_id` | `gold_encounter_summary` | `encounter_id` | One-to-One |
| `gold_ed_utilization` | `patient_id` | `silver_patients` | `patient_id` | Many-to-One |
| `gold_population_health` | `patient_id` | `silver_patients` | `patient_id` | One-to-One |

**To create a relationship:**
1. Click and drag the column from one table to the matching column in another table
2. In the dialog, verify the cardinality and cross-filter direction
3. Click **OK**

> **Tip:** If you don't see the Model diagram view, look for a button that says **Open data model** or switch to the **Model** tab.

### Step 4: Create DAX Measures

Now we'll add business measures using DAX (Data Analysis Expressions). These measures calculate KPIs that update dynamically based on filters.

1. Click on the `gold_readmissions` table
2. Click **New measure** in the toolbar
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

## Part B: Build the Power BI Report

### Step 5: Create a New Report

1. From the Semantic Model view, click **New Report** in the toolbar (or go to your workspace and click **+ New item** → **Report** → **Pick a semantic model** → select the HealthcareLakehouse model)
2. You will be taken to the Power BI report editor

### Step 6: Page 1 — Patient Volume & Flow

Rename the first page by double-clicking the page tab and typing: `Patient Volume & Flow`

#### 6a: Title
1. Add a **Text box** from the Insert menu
2. Type: **HealthFirst Medical Group — Patient Volume Dashboard**
3. Format: Bold, size 20, dark blue color

#### 6b: KPI Cards (top row)
Add four **Card** visuals across the top:

| Card | Field |
|------|-------|
| Card 1 | `Total Encounters` (measure) |
| Card 2 | `ED Visits` (measure) |
| Card 3 | `Inpatient Admissions` (measure) |
| Card 4 | `Avg Length of Stay` (measure) |

> To add a Card: Click an empty area → In the Visualizations pane, click the **Card** icon → Drag the measure to the "Fields" well.

#### 6c: Monthly Volume Trend (Line Chart)
1. Add a **Line chart**
2. X-axis: `gold_encounter_summary` → `encounter_month`
3. Y-axis: `Total Encounters` (measure)
4. Legend: `gold_encounter_summary` → `encounter_type`
5. This shows volume trends over time, split by ED vs Inpatient vs Outpatient

#### 6d: Encounters by Facility (Bar Chart)
1. Add a **Clustered bar chart**
2. Y-axis: `gold_encounter_summary` → `facility_name`
3. X-axis: `Total Encounters` (measure)
4. Legend: `gold_encounter_summary` → `encounter_type`

#### 6e: Encounters by Day of Week (Column Chart)
1. Add a **Clustered column chart**
2. X-axis: `gold_encounter_summary` → `day_of_week`
3. Y-axis: `Total Encounters` (measure)
4. This helps identify peak days for staffing

#### 6f: Add Slicers
Add **Slicer** visuals for:
- `gold_encounter_summary` → `facility_name`
- `gold_encounter_summary` → `encounter_year`
- `gold_encounter_summary` → `insurance_type`

### Step 7: Page 2 — Quality & Readmissions

1. Click the **+** icon next to the page tabs to add a new page
2. Rename it to: `Quality & Readmissions`

#### 7a: Title
Add a text box: **Quality Metrics — 30-Day Readmission Analysis**

#### 7b: KPI Cards
Add three cards:

| Card | Field | Format |
|------|-------|--------|
| Card 1 | `Readmission Rate` | % with 1 decimal |
| Card 2 | `Total Readmissions` | Whole number |
| Card 3 | `Total Index Admissions` | Whole number |

#### 7c: Readmission Rate by Diagnosis (Bar Chart)
1. Add a **Clustered bar chart**
2. Y-axis: `gold_readmissions` → `index_diagnosis`
3. X-axis: `Readmission Rate` (measure)
4. Sort by readmission rate descending
5. This reveals which conditions have the highest readmission rates

#### 7d: Readmission Rate by Facility (Column Chart)
1. Add a **Clustered column chart**
2. X-axis: `gold_readmissions` → `index_facility`
3. Y-axis: `Readmission Rate` (measure)
4. Add `Total Index Admissions` as a secondary Y-axis line
5. Compare readmission performance across your three hospitals

#### 7e: Readmissions by Discharge Disposition (Donut Chart)
1. Add a **Donut chart**
2. Legend: `gold_readmissions` → `index_disposition`
3. Values: `Total Readmissions` (measure)
4. This shows which discharge pathways lead to more readmissions

#### 7f: Readmission Detail Table
1. Add a **Table** visual
2. Add columns:
   - `gold_readmissions` → `index_diagnosis`
   - `Total Index Admissions`
   - `Total Readmissions`
   - `Readmission Rate`
3. Sort by `Total Index Admissions` descending

#### 7g: Add Slicers
- `gold_readmissions` → `index_facility`
- `gold_encounter_summary` → `encounter_year`

### Step 8: Page 3 — Population Health & Financials

1. Add a new page
2. Rename to: `Population Health & Financials`

#### 8a: Title
Add a text box: **Population Health & Financial Performance**

#### 8b: Financial KPI Cards
Add four cards:

| Card | Field |
|------|-------|
| Card 1 | `Total Charges` (formatted as currency) |
| Card 2 | `Total Collections` (formatted as currency) |
| Card 3 | `Collection Rate` |
| Card 4 | `Revenue Lost to Denials` |

#### 8c: Denial Rate by Payer (Bar Chart)
1. Add a **Clustered bar chart**
2. Y-axis: `gold_financial` → `payer`
3. X-axis: `Denial Rate` (measure)
4. This identifies which payers are denying the most claims

#### 8d: Chronic Disease Prevalence (Clustered Bar Chart)
1. Add a **Clustered bar chart**
2. Create a visual using `gold_population_health` table
3. Values: Count of patients with each condition
4. Alternatively, use a **Matrix** visual with:
   - Rows: `gold_population_health` → `age_group`
   - Values: Count of patients where `has_diabetes = TRUE`, etc.

> **Tip:** You can also use a **Decomposition tree** visual to explore condition prevalence by age group, gender, and insurance type.

#### 8e: Multimorbidity Distribution (Pie Chart)
1. Add a **Pie chart**
2. Legend: `gold_population_health` → `multimorbidity`
3. Values: Count of `patient_id`
4. This shows what proportion of patients have multiple chronic conditions

#### 8f: Collection Rate by Facility & Payer (Matrix)
1. Add a **Matrix** visual
2. Rows: `gold_financial` → `facility_name`
3. Columns: `gold_financial` → `payer_type`
4. Values: `Collection Rate`
5. This heatmap reveals which facility-payer combinations have the lowest collections

#### 8g: Add Slicers
- `gold_population_health` → `age_group`
- `gold_population_health` → `insurance_type`
- `gold_population_health` → `gender`

### Step 9: Save the Report

1. Click **File** → **Save**
2. Name: `Healthcare Operations Dashboard`
3. Save to your workspace

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
