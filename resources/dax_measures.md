# DAX Measures for Healthcare Semantic Model

Use these DAX measures in your Direct Lake semantic model in Microsoft Fabric.
Each measure is designed for healthcare KPIs and can be added to the
`gold_encounter_summary` table (or a dedicated Measures table).

---

## Core Volume Measures

### Total Encounters
```dax
Total Encounters = COUNTROWS(gold_encounter_summary)
```

### Total Patients
```dax
Total Patients = DISTINCTCOUNT(gold_encounter_summary[patient_id])
```

### ED Visits
```dax
ED Visits = 
CALCULATE(
    COUNTROWS(gold_encounter_summary),
    gold_encounter_summary[encounter_type] = "ED"
)
```

### Inpatient Admissions
```dax
Inpatient Admissions = 
CALCULATE(
    COUNTROWS(gold_encounter_summary),
    gold_encounter_summary[encounter_type] = "Inpatient"
)
```

---

## Quality Measures

### Readmission Rate
```dax
Readmission Rate = 
DIVIDE(
    CALCULATE(COUNTROWS(gold_readmissions), gold_readmissions[was_readmitted] = TRUE()),
    COUNTROWS(gold_readmissions),
    0
)
```
*Format as percentage. National benchmark: ~15%*

### Avg Length of Stay
```dax
Avg Length of Stay = 
CALCULATE(
    AVERAGE(gold_encounter_summary[length_of_stay_days]),
    gold_encounter_summary[encounter_type] = "Inpatient",
    gold_encounter_summary[length_of_stay_days] > 0
)
```

### Readmitted Patients
```dax
Readmitted Patients = 
CALCULATE(
    COUNTROWS(gold_readmissions),
    gold_readmissions[was_readmitted] = TRUE()
)
```

### Avg Days to Readmission
```dax
Avg Days to Readmission = 
CALCULATE(
    AVERAGE(gold_readmissions[days_to_readmission]),
    gold_readmissions[was_readmitted] = TRUE()
)
```

---

## Financial Measures

### Total Revenue
```dax
Total Revenue = SUM(gold_financial[paid_amount])
```

### Total Billed
```dax
Total Billed = SUM(gold_financial[claim_amount])
```

### Collection Rate
```dax
Collection Rate = 
DIVIDE(
    SUM(gold_financial[paid_amount]),
    SUM(gold_financial[claim_amount]),
    0
)
```
*Format as percentage*

### Denial Rate
```dax
Denial Rate = 
DIVIDE(
    CALCULATE(COUNTROWS(gold_financial), gold_financial[claim_status] = "Denied"),
    COUNTROWS(gold_financial),
    0
)
```
*Format as percentage*

### Revenue Lost to Denials
```dax
Revenue Lost to Denials = 
CALCULATE(
    SUM(gold_financial[claim_amount]),
    gold_financial[claim_status] = "Denied"
)
```

---

## Population Health Measures

### Patients with Multimorbidity
```dax
Patients with Multimorbidity = 
CALCULATE(
    DISTINCTCOUNT(gold_population_health[patient_id]),
    gold_population_health[chronic_condition_count] >= 3
)
```

### Diabetes Prevalence
```dax
Diabetes Prevalence = 
DIVIDE(
    CALCULATE(COUNTROWS(gold_population_health), gold_population_health[has_diabetes] = TRUE()),
    COUNTROWS(gold_population_health),
    0
)
```

---

## Conditional Formatting Thresholds

| Measure | Green | Yellow | Red |
|---------|-------|--------|-----|
| Readmission Rate | < 12% | 12-15% | > 15% |
| Avg LOS (Inpatient) | < 4 days | 4-6 days | > 6 days |
| Collection Rate | > 85% | 70-85% | < 70% |
| Denial Rate | < 10% | 10-15% | > 15% |
| ED Volume (vs prior) | < 5% increase | 5-10% increase | > 10% increase |
