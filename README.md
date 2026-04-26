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
| 9:30 – 10:30 | [Module 1 — Lakehouse & Data Ingestion](lab-guide/Module01_Setup_and_Data_Ingestion.md) | Lakehouse, Upload |
| 10:30 – 12:00 | [Module 2 — Data Engineering](lab-guide/Module02_Data_Engineering.md) | Spark Notebooks, Pipelines |
| 12:00 – 12:45 | *Lunch Break* | |
| 12:45 – 2:15 | [Module 3 — Semantic Model & Power BI Dashboard](lab-guide/Module03_Semantic_Model_and_Dashboard.md) | Semantic Model, Power BI |
| 2:15 – 3:15 | [Module 4 — Real-Time Analytics](lab-guide/Module04_RealTime_Analytics.md) | Eventhouse, KQL |
| 3:15 – 4:15 | [Module 5 — Gen AI: Clinical Intelligence](lab-guide/Module05_GenAI_Clinical_Intelligence.md) | Notebooks, Azure OpenAI |
| 4:15 – 4:50 | [Module 6 — Data Agent](lab-guide/Module06_Data_Agent.md) | Fabric Data Agent |
| 4:50 – 5:00 | Wrap-Up & Q&A | |

## 📂 Repository Structure

```
FabricHackathon/
├── README.md                          ← You are here
├── lab-guide/
│   ├── Module00_Introduction.md       ← Healthcare challenges & lab overview
│   ├── Module01_Setup_and_Data_Ingestion.md
│   ├── Module02_Data_Engineering.md
│   ├── Module03_Semantic_Model_and_Dashboard.md
│   ├── Module04_RealTime_Analytics.md
│   ├── Module05_GenAI_Clinical_Intelligence.md
│   └── Module06_Data_Agent.md
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
│   ├── 01_Bronze_Data_Ingestion.py
│   ├── 02_Silver_Transformations.py
│   ├── 03_Gold_Analytics.py
│   ├── 04_GenAI_Clinical_Notes.py
│   └── 05_RealTime_Vitals_Simulator.py
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
4. **A Power BI Dashboard** with pages for Patient Volume & Flow, Quality & Readmissions, and Population Health
5. **A Real-Time Dashboard** monitoring simulated patient vitals with sepsis early-warning detection
6. **A Gen AI Notebook** that summarizes clinical notes and suggests ICD-10 codes
7. **A Data Agent** that lets you ask questions about your healthcare data in natural language

## ⚠️ Important Notes

- All data in this lab is **100% synthetic**. No real patient information (PHI) is used.
- The data was generated using a Python script (`data/generate_healthcare_data.py`) with realistic distributions but fictional names and records.
- This lab is for **educational purposes** and demonstrates Fabric's capabilities. Production healthcare solutions require additional security, compliance (HIPAA), and governance controls.
