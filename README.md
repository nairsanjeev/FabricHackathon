# Microsoft Fabric for Healthcare Providers — One-Day Hands-On Lab

## Unified Patient Intelligence Platform

Build an end-to-end analytics solution on **Microsoft Fabric** that helps a hospital system improve patient outcomes, reduce costs, and meet regulatory requirements.

---

## 🏥 Who Is This Lab For?

Healthcare providers — hospital administrators, clinical informaticists, data analysts, IT leaders, and quality improvement teams — who want to understand how Microsoft Fabric can unify their data platform.

## ⏱️ Agenda

| Time | Module | Fabric Capability |
|------|--------|--------------------|
| 9:00 – 9:30 | [Module 0 — Introduction & Healthcare Context](lab-guide/Module00_Introduction.md) | Overview |
| 9:30 – 10:30 | [Module 1 — Lakehouse & Data Ingestion](lab-guide/Module01_Setup_and_Data_Ingestion.md) | Lakehouse, Upload, Data Pipeline |
| 10:30 – 12:00 | [Module 2 — Data Engineering](lab-guide/Module02_Data_Engineering.md) | Spark Notebooks, Pipelines |
| 12:00 – 12:45 | *Lunch Break* | |
| 12:45 – 2:15 | [Module 3 — Semantic Model & Power BI Dashboard](lab-guide/Module03_Semantic_Model_and_Dashboard.md) | Semantic Model, Power BI, Copilot |
| 2:15 – 3:15 | [Module 4 — Real-Time Analytics](lab-guide/Module04_RealTime_Analytics.md) | Eventhouse, KQL |
| 3:15 – 4:00 | [Module 5 — Gen AI: Clinical Intelligence](lab-guide/Module05_GenAI_Clinical_Intelligence.md) | Notebooks, Azure OpenAI |
| 4:00 – 4:25 | [Module 6 — Prep Data for AI](lab-guide/Module06_Prep_Data_for_AI.md) | Data Quality, Feature Engineering |
| 4:25 – 5:00 | [Module 6B — Testing Power BI Copilot](lab-guide/Module06B_Testing_Copilot.md) | Power BI Copilot, Standalone Copilot |
| 5:00 – 5:30 | [Module 7 — Data Agent & Power BI Copilot](lab-guide/Module07_Data_Agent.md) | Fabric Data Agent, Power BI Copilot |
| *(Optional)* | [Module 8 — Building with VS Code Agent Mode](lab-guide/Module08_VSCode_Agent_Mode.md) | GitHub Copilot Agent Mode, VS Code |
| *(Optional)* | [Module 9 — Predicting Readmissions with Gen AI](lab-guide/Module09_Readmission_Prediction.md) | Azure OpenAI, XGBoost, PySpark ML |

## 📂 Repository Structure

```
FabricHackathon/
├── README.md                          ← You are here
├── lab-guide/
│   ├── Module00_Introduction.md       ← Healthcare challenges & lab overview
│   ├── Module01_Setup_and_Data_Ingestion.md  ← Lakehouse + Data Pipeline alt path
│   ├── Module02_Data_Engineering.md
│   ├── Module03_Semantic_Model_and_Dashboard.md  ← + Power BI Copilot alt path
│   ├── Module04_RealTime_Analytics.md
│   ├── Module05_GenAI_Clinical_Intelligence.md
│   ├── Module06_Prep_Data_for_AI.md   ← Data quality & AI-ready preparation
│   ├── Module06B_Testing_Copilot.md   ← Testing Power BI & Standalone Copilot
│   ├── Module07_Data_Agent.md         ← Data Agent + Power BI Copilot testing
│   ├── Module08_VSCode_Agent_Mode.md  ← (Optional) Build entire lab with AI agent
│   └── Module09_Readmission_Prediction.md ← (Optional) GenAI-powered readmission prediction
├── data/
│   ├── generate_healthcare_data.py    ← Python script to regenerate data
│   ├── patients.csv                   ← 200 synthetic patients
│   ├── encounters.csv                 ← ~1,000 hospital encounters
│   ├── conditions.csv                 ← 428 diagnoses (ICD-10 coded)
│   ├── medications.csv                ← 640 medication records
│   ├── vitals.csv                     ← 3,800+ vital sign readings
│   ├── clinical_notes.csv             ← 150 clinical notes (for Gen AI)
│   └── claims.csv                     ← ~1,000 billing/claims records
├── notebooks/
│   ├── 01_Bronze_Data_Ingestion.py    ← With FHIR mapping & data exploration
│   ├── 02_Silver_Transformations.py   ← With ICD-10 & SIRS clinical context
│   ├── 03_Gold_Analytics.py           ← With KPI business rationale
│   ├── 04_GenAI_Clinical_Notes.py     ← With prompt engineering explanations
│   ├── 05_RealTime_Vitals_Simulator.py  ← With patient archetype & SIRS docs
│   └── 06_Predictive_Readmission_Model.py ← GenAI feature engineering + XGBoost
└── resources/
    ├── kql_queries.kql                ← KQL queries for real-time analytics
    └── dax_measures.md                ← DAX measures for semantic model
```

## 🚀 Prerequisites

- A Microsoft Fabric workspace (capacity F64 or higher recommended)
- A web browser (Microsoft Edge or Google Chrome)
- The synthetic CSV data files from the `data/` folder (pre-generated)
- For Module 5 (Gen AI): An Azure OpenAI Service endpoint with a GPT-4o deployment

## 📊 What You Will Build

By the end of this lab, you will have:

1. **A Lakehouse** with Bronze → Silver → Gold data layers containing clinical, operational, and financial healthcare data
2. **Spark Notebooks** that compute hospital quality measures including 30-day readmission rates, average length of stay, and ED utilization
3. **A Semantic Model** (star schema) with measures for readmission rate, bed occupancy, and revenue analysis
4. **A Power BI Dashboard** with pages for Patient Volume & Flow, Quality & Readmissions, and Population Health (with Power BI Copilot alternate path)
5. **A Real-Time Dashboard** monitoring simulated patient vitals with sepsis early-warning detection
6. **A Gen AI Notebook** that summarizes clinical notes and suggests ICD-10 codes
7. **AI-Ready Data** with quality checks, a data dictionary, and pre-built summary views (Patient 360°, Facility Summary)
8. **A Data Agent** that lets you ask questions about your healthcare data in natural language
9. **Power BI Copilot** tested for AI-assisted visual analytics and executive narrative generation
10. **(Optional) VS Code Agent Mode** — experience building the entire lab through conversational AI with GitHub Copilot
11. **(Optional) Readmission Prediction Model** — Gen AI-assisted feature engineering with XGBoost to predict 30-day hospital readmissions and generate patient risk scores

## ⚠️ Important Notes

- All data in this lab is **100% synthetic**. No real patient information (PHI) is used.
- The data was generated using a Python script (`data/generate_healthcare_data.py`) with realistic distributions but fictional names and records.
- This lab is for **educational purposes** and demonstrates Fabric's capabilities. Production healthcare solutions require additional security, compliance (HIPAA), and governance controls.
