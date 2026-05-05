# Module 10: Fabric IQ — Ontology & Plan (Optional)

| Duration | 45–60 minutes |
|----------|---------------|
| Objective | Use Fabric IQ to define a healthcare ontology (with LLM-assisted design), connect it to a Data Agent for business-semantic Q&A, and create a Plan for hospital capacity forecasting |
| Fabric Features | Fabric IQ, Ontology (preview), Plan (preview), Data Agent, Azure OpenAI |

---

## Why Fabric IQ?

Healthcare organizations have dozens of tables — claims, encounters, patients, vitals, readmissions — but **business users don't think in tables and columns.** They think in *patients*, *facilities*, *diagnoses*, and *care episodes.*

**Fabric IQ** bridges this gap by layering **business semantics** on top of your data. It includes:

| IQ Item | Purpose | Healthcare Example |
|---------|---------|-------------------|
| **Ontology** | Defines business entities, properties, and relationships | "Patient" has encounters, conditions, and claims |
| **Plan** | Budget/forecast planning with writeback | Hospital bed capacity forecasting by department |
| **Data Agent** | Natural language Q&A grounded in ontology | "Which diabetic patients were readmitted at Mercy Hospital?" |

> **Key insight:** Customers typically do not know how their ontology should be defined. In this module, you'll use an **LLM to generate the ontology design** before creating it in Fabric.

---

## What You Will Do

1. Use an LLM to **generate an ontology definition** from your data schema
2. **Create an Ontology** in Fabric IQ from your semantic model
3. Review and refine entity types, properties, and relationships
4. **Connect the Ontology to a Data Agent** and test with healthcare questions
5. **Create a Plan** for hospital bed capacity forecasting
6. Experience the planning sheet interface with scenario analysis

---

## Prerequisites

- Completed Modules 1–3 (Lakehouse + Silver + Gold tables populated)
- Completed Module 3 (Semantic model exists in your workspace)
- Tenant admin has enabled:
  - **Ontology (preview)** — Admin Portal → Tenant Settings → Fabric IQ
  - **Users can create Plan (preview) items** — Admin Portal → Tenant Settings → Fabric IQ
- For Part A: Azure OpenAI endpoint (same one from Module 5)

---

## Part A: LLM-Assisted Ontology Design

> **Why this step matters:** Defining an ontology requires understanding which tables represent *business entities* vs. *fact tables*, which columns are meaningful properties, and how entities relate. An LLM can accelerate this by analyzing your schema and recommending an ontology design.

### Step 1: Gather Your Schema

Before asking the LLM to design the ontology, collect the tables and columns in your semantic model. You already know these from the lab:

**Gold Tables (Analytics/Fact):**
| Table | Description |
|-------|-------------|
| `gold_readmissions` | 30-day readmission tracking with index admission and readmitting encounter |
| `gold_ed_utilization` | ED frequent flyer identification |
| `gold_alos` | Average length of stay by diagnosis and facility |
| `gold_encounter_summary` | Encounter volumes with demographics |
| `gold_financial` | Revenue, denials, collection rates |
| `gold_population_health` | Chronic disease prevalence by demographics |

**Silver Tables (Cleansed Entities):**
| Table | Description |
|-------|-------------|
| `silver_patients` | Patient demographics (age, gender, race, facility) |
| `silver_encounters` | Hospital encounters (dates, type, facility, LOS) |
| `silver_conditions` | ICD-10 coded diagnoses linked to encounters |
| `silver_claims` | Billing and claims records |
| `silver_medications` | Medication records linked to encounters |
| `silver_vitals` | Vital sign readings for encounters |

### Step 2: Prompt the LLM to Generate an Ontology Design

Open your Azure OpenAI playground (or a Fabric notebook with AI endpoint access) and use this prompt:

```text
You are a healthcare data architect designing a Fabric IQ Ontology for a hospital system.

The ontology should define entity types (business concepts), their properties, keys (unique identifiers), and relationship types.

Here is the data schema:

SILVER TABLES (cleansed entities):
- silver_patients: patient_id, first_name, last_name, date_of_birth, gender, race, city, state, primary_facility
- silver_encounters: encounter_id, patient_id, encounter_type, encounter_start, encounter_end, facility_name, primary_diagnosis, length_of_stay_days
- silver_conditions: condition_id, patient_id, encounter_id, icd10_code, description, onset_date, status
- silver_claims: claim_id, encounter_id, patient_id, claim_type, total_charge, amount_paid, payer_name, status
- silver_medications: medication_id, patient_id, encounter_id, medication_name, dosage, frequency, start_date, end_date
- silver_vitals: vital_id, encounter_id, patient_id, timestamp, heart_rate, systolic_bp, diastolic_bp, temperature, respiratory_rate, o2_saturation

GOLD TABLES (analytics):
- gold_readmissions: patient_id, index_encounter_id, index_admission_date, index_diagnosis, readmission_encounter_id, readmission_date, days_to_readmission, was_readmitted
- gold_encounter_summary: encounter_id, patient_id, encounter_type, facility_name, admission_month, length_of_stay_days, age_at_encounter, gender
- gold_financial: claim_id, encounter_id, total_charge, amount_paid, payer_name, denial_flag, collection_rate
- gold_alos: diagnosis, facility_name, avg_los, encounter_count
- gold_ed_utilization: patient_id, ed_visit_count, is_frequent_flyer, last_ed_visit
- gold_population_health: condition_category, age_group, gender, patient_count, prevalence_rate

Based on this schema, design a Fabric IQ Ontology with:
1. Entity types (business concepts, not raw tables)
2. For each entity: key property, important properties, and which table(s) to bind
3. Relationship types between entities (with cardinality)
4. Brief business justification for each entity

Format as a structured table.
```

### Step 3: Review the LLM Output

The LLM should produce something like:

| Entity Type | Key | Source Table | Properties | Business Justification |
|-------------|-----|-------------|------------|----------------------|
| **Patient** | patient_id | silver_patients | name, DOB, gender, race, city, facility | Core entity — all care revolves around the patient |
| **Encounter** | encounter_id | silver_encounters | type, start/end dates, facility, diagnosis, LOS | Each patient interaction is a billable care episode |
| **Facility** | facility_name | silver_encounters (distinct) | name, city, state | Organizational unit for capacity and quality reporting |
| **Condition** | condition_id | silver_conditions | ICD-10 code, description, onset, status | Clinical diagnoses drive care plans and risk scores |
| **Claim** | claim_id | silver_claims | type, charge, paid, payer, status | Financial entity for revenue cycle management |
| **Medication** | medication_id | silver_medications | name, dosage, frequency, dates | Pharmacy costs are 20% of hospital budgets |

**Relationship Types:**

| From | To | Cardinality | Relationship Name |
|------|-----|-------------|-------------------|
| Patient | Encounter | 1:Many | has_encounter |
| Encounter | Condition | 1:Many | has_diagnosis |
| Encounter | Claim | 1:1 | generates_claim |
| Encounter | Medication | 1:Many | includes_medication |
| Encounter | Facility | Many:1 | occurs_at |
| Patient | Condition | 1:Many | has_condition |

> 💡 **Key takeaway:** The LLM identified that *silver tables map naturally to entity types* while *gold tables are analytics/fact tables* that don't need to be individual entities — their data appears as properties or computed metrics on entities.

### Step 4: Refine the Design (Optional Discussion)

Consider with your team:
- Should **Payer** (insurance company) be its own entity? (Yes, if you plan payer-specific queries)
- Should **Vital Signs** be an entity or a property of Encounter? (Usually a time-series property)
- Should **Readmission** be a relationship type between two Encounters? (Yes — powerful for graph queries)

Save this ontology design — you'll use it as a reference when verifying the auto-generated ontology in Part B.

---

## Part B: Create the Ontology in Fabric IQ

### Step 5: Generate Ontology from Semantic Model

1. In your Fabric workspace, click **+ New item**
2. Search for **Ontology** (under the IQ section)
3. Select **Generate from a semantic model**
4. Choose your **HealthcareLakehouse-SemanticModel**
5. Name the ontology: `Healthcare Ontology`
6. Click **Generate**

> ⏳ Generation takes 30–60 seconds. Fabric reads your semantic model's tables, columns, and relationships and creates entity types automatically.

### Step 6: Verify Entity Types

After generation, you'll see entity types listed in the ontology editor.

1. In the left panel, review each entity type that was generated
2. Compare against your LLM-designed ontology from Step 3:
   - Are the expected entities present? (Patient, Encounter, Facility, etc.)
   - Did it create entities for gold analytics tables? (These may need renaming or removal)

3. **Rename** entity types to match business terminology:
   - `silver_patients` → `Patient`
   - `silver_encounters` → `Encounter`
   - `silver_conditions` → `Condition`
   - `silver_claims` → `Claim`
   - `gold_encounter_summary` → `Encounter Summary` (or remove if redundant)

> To rename: Click the entity type → Click the pencil icon next to the name → Enter new name → Save

### Step 7: Verify Properties and Keys

For each entity type:

1. Click the entity type (e.g., **Patient**)
2. Go to the **Properties** tab
3. Verify that key columns are listed:
   - Patient: `patient_id` should be the key
   - Encounter: `encounter_id` should be the key
   - Condition: `condition_id` should be the key
4. If a key is missing, click **+ Add key** and select the appropriate column
5. Go to the **Bindings** tab to verify the data source mapping is correct

### Step 8: Configure Relationship Types

1. Go to the **Relationship Types** section in the ontology editor
2. Review auto-generated relationships:
   - Check that Patient → Encounter is 1:Many
   - Check that Encounter → Condition is 1:Many
   - Check that Encounter → Claim is 1:1 or 1:Many
3. For each relationship, verify:
   - **Source entity** and **target entity** are correct
   - **Source column** (foreign key) is mapped correctly
   - **Cardinality** matches your expectation
4. If a relationship is missing (e.g., Encounter → Facility), add it:
   - Click **+ Add relationship type**
   - Source: Encounter, Target: Facility
   - Source column: `facility_name`
   - Name: `occurs_at`

### Step 9: Save and Publish

1. Review the full ontology in the graph view (click the graph icon)
2. Verify entities and relationships look correct
3. Click **Save**

> ✅ **Checkpoint:** Your ontology should have 4–6 entity types with keys, properties, and relationship types connecting them as a graph.

---

## Part C: Test the Ontology with a Data Agent

### Step 10: Create a Data Agent Connected to the Ontology

1. In your workspace, click **+ New item**
2. Select **Data Agent**
3. Name: `Healthcare Ontology Agent`
4. In the **Data sources** section, click **+ Add data source**
5. Select **Ontology** as the source type
6. Choose your `Healthcare Ontology`
7. Click **Create**

> 💡 **Why ontology + Data Agent?** When a Data Agent is connected to an ontology, it understands *business concepts and relationships* — not just raw tables. The question "Show me patients with diabetes" maps to the Patient entity with a Condition relationship where the description contains "diabetes."

### Step 11: Add Instructions for the Agent

In the **Instructions** field, add context:

```text
You are a clinical data analyst for a hospital system. When answering questions:
- Use entity relationships to traverse from patients to their encounters, conditions, and claims
- "Readmitted" means a patient had another encounter within 30 days of discharge
- Facility names include: Mercy Hospital, General Hospital, City Medical Center, etc.
- ICD-10 codes follow standard coding (e.g., E11 = Type 2 Diabetes, I10 = Hypertension)
- Length of stay (LOS) is measured in days
- Financial metrics include total_charge, amount_paid, and collection_rate
```

### Step 12: Test with Healthcare Questions

Try these questions that exercise entity relationships:

| Question | Tests |
|----------|-------|
| "How many patients have diabetes?" | Patient → Condition entity traversal |
| "What is the average length of stay for inpatient encounters?" | Encounter entity properties |
| "Which facilities have the highest readmission rates?" | Encounter entity + gold analytics |
| "Show me patients over 65 with more than 3 encounters" | Patient properties + Encounter count |
| "What are the top 5 most expensive conditions by total charges?" | Condition → Claim relationship |
| "List medications prescribed for patients with hypertension" | Patient → Condition → Medication traversal |

For each question:
1. Type it in the Data Agent chat
2. Review the answer
3. Check the generated query to see how the agent used ontology relationships
4. Note any incorrect answers or missing relationships

### Step 13: Compare with Table-Based Agent

Think about how these questions would perform with a table-based Data Agent (Module 7) vs. the ontology-based agent:

| Aspect | Table-Based Agent | Ontology-Based Agent |
|--------|-------------------|---------------------|
| Schema understanding | Must infer joins from foreign keys | Relationships are explicit |
| Business terminology | Depends on column naming | Entity names map to business concepts |
| Multi-hop queries | Requires complex JOIN chains | Follows relationship types naturally |
| Consistency | Different agents may join differently | Single source of truth for relationships |

> ✅ **Checkpoint:** Your ontology-based Data Agent should answer relationship-spanning healthcare questions more accurately than a raw table-based approach.

---

## Part D: Create a Plan for Hospital Capacity Forecasting

### Why This Use Case?

Hospital bed capacity planning is one of the highest-value forecasting problems in healthcare:
- **Under-capacity**: Patient diversions, longer ED wait times, delayed surgeries
- **Over-capacity**: Wasted staffing costs ($80K+ per nurse FTE per year)
- **Seasonal surges**: Flu season, RSV peaks, post-holiday trauma increases

Your `gold_encounter_summary` table already has historical encounter volumes by facility and month — perfect for building a forecast.

### Step 14: Create a Plan Item

1. In your workspace, click **+ New item**
2. Search for **Plan** (under the IQ section)
3. Name: `Hospital Capacity Plan`
4. Click **Create**

> The Plan editor opens with a blank canvas.

### Step 15: Connect to Your Semantic Model

1. In the Plan editor, click **Model** in the top menu
2. Click **+ Add data connection**
3. Select your **HealthcareLakehouse-SemanticModel**
4. The model editor shows available tables and measures
5. Map the following dimensions:
   - **Time**: `admission_month` from `gold_encounter_summary`
   - **Facility**: `facility_name` from `gold_encounter_summary`
   - **Encounter Type**: `encounter_type` from `gold_encounter_summary`
6. Map measures:
   - **Encounter Count**: Count of encounters
   - **Avg Length of Stay**: `length_of_stay_days` average
7. Click **Save model**

### Step 16: Create a Planning Sheet

1. Click **+ New sheet** → Select **Planning sheet**
2. Name: `FY2025 Bed Demand Forecast`
3. Configure the layout:
   - **Rows**: Facility (Mercy Hospital, General Hospital, City Medical Center)
   - **Columns**: Months (Jan 2025 – Dec 2025)
   - **Values**: Projected Encounter Count

4. The planning sheet opens with an Excel-like grid
5. You'll see **Actuals** (from your data) for historical months
6. For future months, **enter forecasted values**:

| Facility | Jan | Feb | Mar | Apr | May | Jun |
|----------|-----|-----|-----|-----|-----|-----|
| Mercy Hospital | 85 | 82 | 90 | 78 | 75 | 72 |
| General Hospital | 110 | 105 | 120 | 100 | 95 | 88 |
| City Medical Center | 65 | 62 | 70 | 60 | 58 | 55 |

> 💡 In a real scenario, these forecasts come from statistical models or department heads. Here, enter representative values to experience the interface.

### Step 17: Scenario Analysis

1. Click **+ Add scenario** (or use version management)
2. Create a scenario: `Flu Season Surge`
3. In this scenario, increase Jan–Mar values by 25%:
   - Mercy Hospital: 85 → 106, 82 → 103, 90 → 113
   - General Hospital: 110 → 138, 105 → 131, 120 → 150
4. Create another scenario: `Post-Pandemic Baseline`
5. Keep values flat or reduced by 10%

This shows the **what-if analysis** capability: leadership can compare scenarios side-by-side and plan staffing/resources accordingly.

### Step 18: Review and Collaborate

1. Add **comments** to cells that need review (right-click → Add comment)
2. Use **@mentions** to tag team members for input on specific forecasts
3. View the **variance** between scenarios using the built-in comparison view
4. If available, explore **Intelligence sheets** for automated trend detection

> ✅ **Checkpoint:** You've created a Plan with a connected semantic model, entered forecasts in a planning sheet, and built multiple scenarios for bed capacity analysis.

---

## Part E: Wrap-Up and Discussion

### What You Built

| Component | Healthcare Value |
|-----------|-----------------|
| **LLM-generated ontology design** | Accelerated the hardest part — defining business concepts — using AI |
| **Fabric IQ Ontology** | Unified business semantics across all healthcare data |
| **Ontology-based Data Agent** | Clinicians get answers in business language, not SQL |
| **Capacity Plan** | Finance and operations align on bed demand forecasts with scenario analysis |

### Real-World Extensions

- **Ontology + Graph**: Once the Graph feature is GA, traverse relationships like "Find all patients connected to a specific attending physician who were readmitted"
- **Plan + InfoBridge**: Pull in external data (CMS benchmarks, market forecasts) to enrich planning sheets
- **Operations Agent**: Automate alerts when actual encounter volume exceeds planned capacity by >15%
- **Multi-department planning**: Extend the Plan to cover OR scheduling, staffing ratios, and supply chain

### Key Lessons

1. **LLMs accelerate ontology design** — Instead of weeks of workshops, get a solid first draft in minutes
2. **Silver tables = Entity types** — Cleansed operational tables map naturally to business entities
3. **Gold tables = Analytics** — These inform measures and properties, not separate entities
4. **Ontology-grounded agents** are more reliable because relationships are explicit, not inferred
5. **Planning sheets** bring the familiar Excel experience with enterprise governance and writeback

---

## 🎉 Congratulations!

You've explored Fabric IQ — Microsoft's vision for unifying data with business semantics. The combination of **AI-designed ontologies**, **semantic Data Agents**, and **collaborative planning** represents the next generation of enterprise analytics.

---

## Additional Resources

- [Fabric IQ Overview](https://learn.microsoft.com/en-us/fabric/iq/overview)
- [Ontology Tutorial Part 1: Create an Ontology](https://learn.microsoft.com/en-us/fabric/iq/ontology/tutorial-1-create-ontology)
- [Generating an Ontology from a Semantic Model](https://learn.microsoft.com/en-us/fabric/iq/ontology/concepts-generate)
- [Plan Overview](https://learn.microsoft.com/en-us/fabric/iq/plan/overview)
- [Planning Sheets Guide](https://learn.microsoft.com/en-us/fabric/iq/plan/planning-overview)
