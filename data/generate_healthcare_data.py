"""
Healthcare Synthetic Data Generator
====================================
Generates realistic synthetic healthcare data for the Microsoft Fabric
Healthcare Analytics Lab. All data is fictional - no real patient data is used.

Usage:
    python generate_healthcare_data.py

Output:
    Creates CSV files in the current directory:
    - patients.csv
    - encounters.csv
    - conditions.csv
    - medications.csv
    - vitals.csv
    - clinical_notes.csv
    - claims.csv
"""

import csv
import random
import os
from datetime import datetime, timedelta

# Seed for reproducibility
random.seed(42)

# ============================================================
# REFERENCE DATA
# ============================================================

FIRST_NAMES_MALE = [
    "James", "Robert", "John", "Michael", "David", "William", "Richard",
    "Joseph", "Thomas", "Charles", "Christopher", "Daniel", "Matthew",
    "Anthony", "Mark", "Donald", "Steven", "Paul", "Andrew", "Joshua",
    "Kenneth", "Kevin", "Brian", "George", "Timothy", "Ronald", "Edward",
    "Jason", "Jeffrey", "Ryan", "Jacob", "Gary", "Nicholas", "Eric",
    "Jonathan", "Stephen", "Larry", "Justin", "Scott", "Brandon"
]

FIRST_NAMES_FEMALE = [
    "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth",
    "Susan", "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty",
    "Margaret", "Sandra", "Ashley", "Dorothy", "Kimberly", "Emily",
    "Donna", "Michelle", "Carol", "Amanda", "Melissa", "Deborah",
    "Stephanie", "Rebecca", "Sharon", "Laura", "Cynthia", "Kathleen",
    "Amy", "Angela", "Shirley", "Anna", "Brenda", "Pamela", "Emma",
    "Nicole", "Helen"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts"
]

FACILITIES = [
    {"id": "FAC001", "name": "Metro General Hospital", "type": "Academic Medical Center", "beds": 650},
    {"id": "FAC002", "name": "Community Medical Center", "type": "Community Hospital", "beds": 280},
    {"id": "FAC003", "name": "Riverside Health Center", "type": "Community Hospital", "beds": 180}
]

DEPARTMENTS = ["Emergency", "Internal Medicine", "Cardiology", "Pulmonology",
               "Orthopedics", "Neurology", "General Surgery", "Obstetrics",
               "Pediatrics", "Oncology", "Behavioral Health", "ICU"]

PROVIDERS = [
    "Dr. Sarah Chen", "Dr. Michael Patel", "Dr. James Wilson",
    "Dr. Maria Rodriguez", "Dr. David Kim", "Dr. Emily Johnson",
    "Dr. Robert Singh", "Dr. Lisa Chang", "Dr. William Taylor",
    "Dr. Jennifer Brown", "Dr. Ahmed Hassan", "Dr. Catherine Lee",
    "Dr. Steven Park", "Dr. Rachel Green", "Dr. Thomas Wright",
    "Dr. Karen Martinez", "Dr. John Williams", "Dr. Patricia Clark",
    "Dr. Richard Davis", "Dr. Sandra Lopez"
]

ZIP_CODES = [
    {"zip": "10001", "city": "New York", "state": "NY"},
    {"zip": "10002", "city": "New York", "state": "NY"},
    {"zip": "10003", "city": "New York", "state": "NY"},
    {"zip": "10010", "city": "New York", "state": "NY"},
    {"zip": "10016", "city": "New York", "state": "NY"},
    {"zip": "10029", "city": "New York", "state": "NY"},
    {"zip": "10035", "city": "New York", "state": "NY"},
    {"zip": "10451", "city": "Bronx", "state": "NY"},
    {"zip": "10452", "city": "Bronx", "state": "NY"},
    {"zip": "10453", "city": "Bronx", "state": "NY"},
    {"zip": "11201", "city": "Brooklyn", "state": "NY"},
    {"zip": "11205", "city": "Brooklyn", "state": "NY"},
    {"zip": "11215", "city": "Brooklyn", "state": "NY"},
    {"zip": "11221", "city": "Brooklyn", "state": "NY"},
    {"zip": "07302", "city": "Jersey City", "state": "NJ"},
]

INSURANCE_TYPES = ["Medicare", "Medicaid", "Commercial", "Self-Pay"]
INSURANCE_WEIGHTS = [0.40, 0.20, 0.30, 0.10]  # Weighted distribution

RACES = ["White", "Black or African American", "Asian", "Hispanic or Latino",
         "American Indian", "Native Hawaiian or Pacific Islander", "Other"]
RACE_WEIGHTS = [0.40, 0.25, 0.15, 0.12, 0.03, 0.02, 0.03]

CHRONIC_CONDITIONS = [
    {"code": "E11.9", "desc": "Type 2 diabetes mellitus without complications", "prevalence": 0.35},
    {"code": "I50.9", "desc": "Heart failure, unspecified", "prevalence": 0.22},
    {"code": "J44.1", "desc": "Chronic obstructive pulmonary disease with acute exacerbation", "prevalence": 0.18},
    {"code": "I10", "desc": "Essential (primary) hypertension", "prevalence": 0.50},
    {"code": "E78.5", "desc": "Hyperlipidemia, unspecified", "prevalence": 0.38},
    {"code": "N18.9", "desc": "Chronic kidney disease, unspecified", "prevalence": 0.12},
    {"code": "F32.9", "desc": "Major depressive disorder, single episode, unspecified", "prevalence": 0.15},
    {"code": "J45.909", "desc": "Unspecified asthma, uncomplicated", "prevalence": 0.10},
    {"code": "I25.10", "desc": "Atherosclerotic heart disease of native coronary artery", "prevalence": 0.14},
    {"code": "E66.01", "desc": "Morbid obesity due to excess calories", "prevalence": 0.20},
]

ACUTE_CONDITIONS = [
    {"code": "J18.9", "desc": "Pneumonia, unspecified organism", "dept": "Pulmonology"},
    {"code": "I21.9", "desc": "Acute myocardial infarction, unspecified", "dept": "Cardiology"},
    {"code": "K35.80", "desc": "Unspecified acute appendicitis", "dept": "General Surgery"},
    {"code": "S72.001A", "desc": "Fracture of unspecified part of neck of right femur", "dept": "Orthopedics"},
    {"code": "N39.0", "desc": "Urinary tract infection, site not specified", "dept": "Internal Medicine"},
    {"code": "K92.1", "desc": "Melena (gastrointestinal bleeding)", "dept": "Internal Medicine"},
    {"code": "A41.9", "desc": "Sepsis, unspecified organism", "dept": "ICU"},
    {"code": "I63.9", "desc": "Cerebral infarction, unspecified (Stroke)", "dept": "Neurology"},
    {"code": "J96.01", "desc": "Acute respiratory failure with hypoxia", "dept": "ICU"},
    {"code": "R55", "desc": "Syncope and collapse", "dept": "Emergency"},
    {"code": "R07.9", "desc": "Chest pain, unspecified", "dept": "Emergency"},
    {"code": "R10.9", "desc": "Unspecified abdominal pain", "dept": "Emergency"},
    {"code": "T78.2XXA", "desc": "Anaphylactic shock, unspecified", "dept": "Emergency"},
    {"code": "R56.9", "desc": "Unspecified convulsions (Seizure)", "dept": "Neurology"},
    {"code": "I48.91", "desc": "Unspecified atrial fibrillation", "dept": "Cardiology"},
]

MEDICATIONS = {
    "Diabetes": [
        {"name": "Metformin 500mg", "class": "Biguanide", "freq": "Twice daily"},
        {"name": "Insulin Glargine 100 units/mL", "class": "Insulin", "freq": "Once daily"},
        {"name": "Glipizide 5mg", "class": "Sulfonylurea", "freq": "Once daily"},
        {"name": "Empagliflozin 10mg", "class": "SGLT2 Inhibitor", "freq": "Once daily"},
    ],
    "Heart Failure": [
        {"name": "Lisinopril 10mg", "class": "ACE Inhibitor", "freq": "Once daily"},
        {"name": "Carvedilol 12.5mg", "class": "Beta Blocker", "freq": "Twice daily"},
        {"name": "Furosemide 40mg", "class": "Loop Diuretic", "freq": "Once daily"},
        {"name": "Spironolactone 25mg", "class": "Aldosterone Antagonist", "freq": "Once daily"},
    ],
    "COPD": [
        {"name": "Albuterol Inhaler 90mcg", "class": "SABA", "freq": "As needed"},
        {"name": "Tiotropium 18mcg", "class": "LAMA", "freq": "Once daily"},
        {"name": "Fluticasone/Salmeterol 250/50mcg", "class": "ICS/LABA", "freq": "Twice daily"},
        {"name": "Prednisone 20mg", "class": "Corticosteroid", "freq": "Once daily"},
    ],
    "Hypertension": [
        {"name": "Amlodipine 5mg", "class": "CCB", "freq": "Once daily"},
        {"name": "Losartan 50mg", "class": "ARB", "freq": "Once daily"},
        {"name": "Hydrochlorothiazide 25mg", "class": "Thiazide Diuretic", "freq": "Once daily"},
    ],
    "General": [
        {"name": "Acetaminophen 500mg", "class": "Analgesic", "freq": "Every 6 hours as needed"},
        {"name": "Omeprazole 20mg", "class": "PPI", "freq": "Once daily"},
        {"name": "Ceftriaxone 1g IV", "class": "Antibiotic", "freq": "Once daily"},
        {"name": "Enoxaparin 40mg", "class": "Anticoagulant", "freq": "Once daily"},
        {"name": "Ondansetron 4mg", "class": "Antiemetic", "freq": "Every 8 hours as needed"},
    ]
}

DISCHARGE_DISPOSITIONS = [
    "Home", "Home with Home Health", "Skilled Nursing Facility",
    "Rehabilitation Facility", "Against Medical Advice", "Expired",
    "Transfer to Another Facility"
]
DISCHARGE_WEIGHTS_INPATIENT = [0.50, 0.18, 0.12, 0.08, 0.03, 0.02, 0.07]
DISCHARGE_WEIGHTS_ED = [0.82, 0.05, 0.02, 0.01, 0.05, 0.01, 0.04]

DENIAL_REASONS = [
    "Medical necessity not established",
    "Prior authorization not obtained",
    "Out-of-network provider",
    "Duplicate claim submitted",
    "Timely filing limit exceeded",
    "Incorrect procedure code",
    "Coordination of benefits issue",
    "Insufficient documentation"
]

PAYERS = ["Medicare FFS", "Medicare Advantage", "Medicaid", "BlueCross BlueShield",
          "Aetna", "UnitedHealthcare", "Cigna", "Humana", "Oscar Health", "Self-Pay"]

# Clinical note templates
NOTE_TEMPLATES = {
    "ED Note": [
        "Patient is a {age}-year-old {gender} who presents to the Emergency Department with chief complaint of {complaint}. "
        "Symptoms began {onset}. Past medical history is significant for {pmh}. "
        "Current medications include {meds}. "
        "Vital signs on arrival: HR {hr}, BP {bp}, Temp {temp}F, RR {rr}, SpO2 {spo2}%. "
        "Physical examination reveals {exam}. "
        "Labs obtained including CBC, BMP, and {additional_labs}. "
        "Assessment: {assessment}. "
        "Plan: {plan}. Patient was {disposition}.",
    ],
    "Discharge Summary": [
        "DISCHARGE SUMMARY\n\n"
        "Patient: {age}-year-old {gender}\n"
        "Admission Date: {admit_date}\n"
        "Discharge Date: {discharge_date}\n"
        "Length of Stay: {los} days\n"
        "Admitting Diagnosis: {admit_dx}\n"
        "Discharge Diagnosis: {discharge_dx}\n\n"
        "HOSPITAL COURSE:\n"
        "Patient was admitted for {reason}. {pmh_text}. "
        "During hospitalization, patient was treated with {treatment}. "
        "{complications}"
        "Patient showed {progress} and was deemed safe for discharge.\n\n"
        "DISCHARGE MEDICATIONS:\n{discharge_meds}\n\n"
        "DISCHARGE INSTRUCTIONS:\n"
        "1. Follow up with {follow_up} within {follow_up_days} days\n"
        "2. {instruction1}\n"
        "3. {instruction2}\n"
        "4. Return to ED if experiencing {warning_signs}\n\n"
        "DISCHARGE DISPOSITION: {disposition}",
    ],
    "Progress Note": [
        "DAILY PROGRESS NOTE - Day {day} of hospitalization\n\n"
        "Subjective: Patient reports {subjective}. Pain level {pain}/10. "
        "{sleep_text}. {appetite_text}.\n\n"
        "Objective:\n"
        "Vital Signs: HR {hr}, BP {bp}, Temp {temp}F, RR {rr}, SpO2 {spo2}%\n"
        "General: {general}\n"
        "Cardiovascular: {cardio}\n"
        "Respiratory: {resp}\n"
        "Abdomen: {abdomen}\n"
        "Extremities: {extremities}\n\n"
        "Assessment/Plan:\n"
        "1. {problem1}: {plan1}\n"
        "2. {problem2}: {plan2}\n"
        "{additional_plans}"
        "Continue to monitor. Anticipated discharge {discharge_plan}.",
    ]
}

# ============================================================
# DATA GENERATION FUNCTIONS
# ============================================================

def random_date(start, end):
    """Generate a random date between start and end."""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

def weighted_choice(options, weights):
    """Choose from options with given weights."""
    return random.choices(options, weights=weights, k=1)[0]

def generate_patients(n=200):
    """Generate synthetic patient records."""
    patients = []
    for i in range(1, n + 1):
        gender = random.choice(["Male", "Female"])
        if gender == "Male":
            first_name = random.choice(FIRST_NAMES_MALE)
        else:
            first_name = random.choice(FIRST_NAMES_FEMALE)
        last_name = random.choice(LAST_NAMES)

        # Age distribution skewed older (healthcare population)
        age_group = random.choices(
            ["18-30", "31-45", "46-60", "61-75", "76-90"],
            weights=[0.08, 0.12, 0.25, 0.35, 0.20],
            k=1
        )[0]
        age_ranges = {
            "18-30": (18, 30), "31-45": (31, 45), "46-60": (46, 60),
            "61-75": (61, 75), "76-90": (76, 90)
        }
        age = random.randint(*age_ranges[age_group])
        dob = datetime(2026, 1, 1) - timedelta(days=age * 365 + random.randint(0, 364))

        location = random.choice(ZIP_CODES)

        # Older patients more likely to have Medicare
        if age >= 65:
            insurance = weighted_choice(
                INSURANCE_TYPES, [0.70, 0.10, 0.15, 0.05]
            )
        else:
            insurance = weighted_choice(
                INSURANCE_TYPES, [0.10, 0.25, 0.50, 0.15]
            )

        pcp = random.choice(PROVIDERS)
        race = weighted_choice(RACES, RACE_WEIGHTS)

        patients.append({
            "patient_id": f"P{i:04d}",
            "first_name": first_name,
            "last_name": last_name,
            "date_of_birth": dob.strftime("%Y-%m-%d"),
            "age": age,
            "gender": gender,
            "race": race,
            "zip_code": location["zip"],
            "city": location["city"],
            "state": location["state"],
            "insurance_type": insurance,
            "primary_care_provider": pcp,
            "risk_score": round(random.uniform(0.5, 4.5), 2)
        })
    return patients

def generate_conditions(patients):
    """Generate chronic and acute conditions for patients."""
    conditions = []
    cond_id = 1
    patient_conditions = {}  # Track which conditions each patient has

    for patient in patients:
        pid = patient["patient_id"]
        age = patient["age"]
        patient_conditions[pid] = []

        # Chronic conditions - probability increases with age
        age_factor = min(age / 65.0, 1.5)
        for cc in CHRONIC_CONDITIONS:
            adjusted_prevalence = cc["prevalence"] * age_factor
            if random.random() < adjusted_prevalence:
                diag_date = random_date(
                    datetime(2020, 1, 1),
                    datetime(2025, 6, 1)
                )
                conditions.append({
                    "condition_id": f"C{cond_id:05d}",
                    "patient_id": pid,
                    "encounter_id": "",  # Will be linked later
                    "condition_code": cc["code"],
                    "condition_description": cc["desc"],
                    "condition_type": "Chronic",
                    "date_diagnosed": diag_date.strftime("%Y-%m-%d"),
                    "status": "Active"
                })
                patient_conditions[pid].append(cc)
                cond_id += 1

    return conditions, patient_conditions

def generate_encounters(patients, patient_conditions):
    """Generate encounter records with realistic patterns."""
    encounters = []
    enc_id = 1
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2026, 3, 31)

    # Track encounters per patient for readmission logic
    patient_encounters = {p["patient_id"]: [] for p in patients}

    for patient in patients:
        pid = patient["patient_id"]
        age = patient["age"]
        conditions = patient_conditions.get(pid, [])
        num_chronic = len(conditions)

        # Number of encounters depends on chronic conditions and age
        base_encounters = random.randint(1, 4)
        chronic_bonus = num_chronic * random.randint(0, 2)
        age_bonus = 1 if age > 70 else 0
        total_encounters = min(base_encounters + chronic_bonus + age_bonus, 15)

        # Determine if this is an ED frequent flyer (4+ ED visits)
        is_frequent_flyer = num_chronic >= 3 and random.random() < 0.3

        for _ in range(total_encounters):
            # Encounter type distribution
            if is_frequent_flyer and random.random() < 0.6:
                enc_type = "ED"
            else:
                enc_type = weighted_choice(
                    ["ED", "Inpatient", "Outpatient", "Ambulatory"],
                    [0.25, 0.20, 0.35, 0.20]
                )

            enc_date = random_date(start_date, end_date)
            facility = random.choice(FACILITIES)

            # Determine diagnosis
            if enc_type in ("ED", "Inpatient"):
                if conditions and random.random() < 0.5:
                    # Related to chronic condition
                    cond = random.choice(conditions)
                    dx_code = cond["code"]
                    dx_desc = cond["desc"]
                    dept = random.choice(["Internal Medicine", "Cardiology", "Pulmonology", "ICU"])
                else:
                    # Acute condition
                    acute = random.choice(ACUTE_CONDITIONS)
                    dx_code = acute["code"]
                    dx_desc = acute["desc"]
                    dept = acute["dept"]
            else:
                if conditions:
                    cond = random.choice(conditions)
                    dx_code = cond["code"]
                    dx_desc = cond["desc"]
                else:
                    dx_code = "Z00.00"
                    dx_desc = "General adult medical examination"
                dept = random.choice(["Internal Medicine", "Cardiology", "Pulmonology"])

            # Length of stay
            if enc_type == "ED":
                los_hours = random.choices(
                    [2, 4, 6, 8, 12, 18, 24],
                    weights=[0.15, 0.25, 0.25, 0.15, 0.10, 0.05, 0.05],
                    k=1
                )[0]
                discharge_date = enc_date + timedelta(hours=los_hours)
                los_days = 0
            elif enc_type == "Inpatient":
                # Sicker patients stay longer
                base_los = random.choices(
                    [1, 2, 3, 4, 5, 7, 10, 14, 21],
                    weights=[0.10, 0.20, 0.25, 0.15, 0.10, 0.08, 0.06, 0.04, 0.02],
                    k=1
                )[0]
                if dx_code == "A41.9":  # Sepsis
                    base_los = max(base_los, random.randint(5, 21))
                los_days = base_los
                discharge_date = enc_date + timedelta(days=los_days)
            else:
                discharge_date = enc_date
                los_days = 0

            # Charges and disposition
            if enc_type == "ED":
                charges = round(random.uniform(1500, 15000), 2)
                disposition = weighted_choice(
                    DISCHARGE_DISPOSITIONS, DISCHARGE_WEIGHTS_ED
                )
            elif enc_type == "Inpatient":
                charges = round(random.uniform(8000, 120000), 2)
                if dx_code == "A41.9":
                    charges = round(random.uniform(50000, 250000), 2)
                disposition = weighted_choice(
                    DISCHARGE_DISPOSITIONS, DISCHARGE_WEIGHTS_INPATIENT
                )
            else:
                charges = round(random.uniform(200, 3000), 2)
                disposition = "Home"

            provider = random.choice(PROVIDERS)

            encounter = {
                "encounter_id": f"E{enc_id:05d}",
                "patient_id": pid,
                "encounter_date": enc_date.strftime("%Y-%m-%d"),
                "encounter_time": f"{random.randint(0,23):02d}:{random.randint(0,59):02d}",
                "discharge_date": discharge_date.strftime("%Y-%m-%d"),
                "encounter_type": enc_type,
                "facility_id": facility["id"],
                "facility_name": facility["name"],
                "department": dept,
                "primary_diagnosis_code": dx_code,
                "primary_diagnosis_description": dx_desc,
                "attending_provider": provider,
                "discharge_disposition": disposition,
                "length_of_stay_days": los_days,
                "total_charges": charges,
            }
            encounters.append(encounter)
            patient_encounters[pid].append(encounter)
            enc_id += 1

    # Generate explicit readmissions with RISK-ADJUSTED probability
    # Instead of a flat 15% rate, readmission probability depends on
    # patient risk factors — creating realistic correlations that an
    # ML model can learn from.
    inpatient_encounters = [e for e in encounters if e["encounter_type"] == "Inpatient"
                           and e["discharge_disposition"] not in ("Expired", "Against Medical Advice")]

    # Build a lookup for quick patient access
    patient_lookup = {p["patient_id"]: p for p in patients}

    # High-risk diagnosis codes for readmission (from CMS HRRP)
    high_risk_dx = {"I50.9", "J44.1", "A41.9", "J96.01", "I21.9"}

    for enc in inpatient_encounters:
        pid = enc["patient_id"]
        pt = patient_lookup[pid]
        conds = patient_conditions.get(pid, [])
        cond_codes = {c["code"] for c in conds}
        num_chronic = len(conds)

        # ── Compute risk-adjusted readmission probability ─────────
        # Base rate 8% (below national average for healthy patients)
        readmit_prob = 0.08

        # Age: older patients have higher readmission risk
        if pt["age"] >= 75:
            readmit_prob += 0.10
        elif pt["age"] >= 65:
            readmit_prob += 0.05

        # Comorbidity burden: more chronic conditions → higher risk
        if num_chronic >= 5:
            readmit_prob += 0.14
        elif num_chronic >= 3:
            readmit_prob += 0.08
        elif num_chronic >= 1:
            readmit_prob += 0.03

        # Specific high-risk conditions (from clinical literature)
        if "I50.9" in cond_codes:   # Heart failure
            readmit_prob += 0.10
        if "J44.1" in cond_codes:   # COPD
            readmit_prob += 0.07
        if "N18.9" in cond_codes:   # CKD
            readmit_prob += 0.05
        if "F32.9" in cond_codes:   # Depression
            readmit_prob += 0.04

        # Index stay diagnosis: acute high-risk diagnoses
        if enc["primary_diagnosis_code"] in high_risk_dx:
            readmit_prob += 0.08

        # Length of stay: longer stays indicate higher acuity
        los = enc["length_of_stay_days"]
        if los >= 14:
            readmit_prob += 0.10
        elif los >= 7:
            readmit_prob += 0.05

        # Discharge disposition: SNF/rehab patients have higher risk
        if enc["discharge_disposition"] == "Skilled Nursing Facility":
            readmit_prob += 0.06
        elif enc["discharge_disposition"] == "Home with Home Health":
            readmit_prob += 0.03

        # Risk score (from patient record, 0.5-4.5 scale)
        if pt["risk_score"] > 3.5:
            readmit_prob += 0.08
        elif pt["risk_score"] > 2.5:
            readmit_prob += 0.03

        # Insurance: Medicare/Medicaid patients have higher rates
        if pt["insurance_type"] == "Medicare":
            readmit_prob += 0.04
        elif pt["insurance_type"] == "Medicaid":
            readmit_prob += 0.03

        # Cap at 70% to keep it realistic
        readmit_prob = min(readmit_prob, 0.70)

        if random.random() < readmit_prob:
            readmit_days = random.randint(1, 29)
            discharge_dt = datetime.strptime(enc["discharge_date"], "%Y-%m-%d")
            readmit_date = discharge_dt + timedelta(days=readmit_days)

            if readmit_date > end_date:
                continue

            facility = random.choice(FACILITIES)
            provider = random.choice(PROVIDERS)
            dx_code = enc["primary_diagnosis_code"]
            dx_desc = enc["primary_diagnosis_description"]
            los_days = random.randint(2, 10)

            readmission = {
                "encounter_id": f"E{enc_id:05d}",
                "patient_id": enc["patient_id"],
                "encounter_date": readmit_date.strftime("%Y-%m-%d"),
                "encounter_time": f"{random.randint(0,23):02d}:{random.randint(0,59):02d}",
                "discharge_date": (readmit_date + timedelta(days=los_days)).strftime("%Y-%m-%d"),
                "encounter_type": "Inpatient",
                "facility_id": facility["id"],
                "facility_name": facility["name"],
                "department": random.choice(["Internal Medicine", "Cardiology", "Pulmonology", "ICU"]),
                "primary_diagnosis_code": dx_code,
                "primary_diagnosis_description": dx_desc,
                "attending_provider": provider,
                "discharge_disposition": weighted_choice(
                    DISCHARGE_DISPOSITIONS, DISCHARGE_WEIGHTS_INPATIENT
                ),
                "length_of_stay_days": los_days,
                "total_charges": round(random.uniform(10000, 90000), 2),
            }
            encounters.append(readmission)
            enc_id += 1

    # Sort by date
    encounters.sort(key=lambda x: x["encounter_date"])
    return encounters

def generate_medications(patients, patient_conditions, encounters):
    """Generate medication records."""
    meds = []
    med_id = 1

    for patient in patients:
        pid = patient["patient_id"]
        conditions = patient_conditions.get(pid, [])
        patient_encs = [e for e in encounters if e["patient_id"] == pid]

        if not patient_encs:
            continue

        for cond in conditions:
            code = cond["code"]
            # Map condition to medication category
            if code.startswith("E11"):
                med_category = "Diabetes"
            elif code.startswith("I50"):
                med_category = "Heart Failure"
            elif code.startswith("J44"):
                med_category = "COPD"
            elif code == "I10":
                med_category = "Hypertension"
            else:
                continue

            # Assign 1-2 medications per condition
            num_meds = random.randint(1, 2)
            selected_meds = random.sample(
                MEDICATIONS[med_category],
                min(num_meds, len(MEDICATIONS[med_category]))
            )

            for med in selected_meds:
                enc = random.choice(patient_encs)
                start_dt = datetime.strptime(enc["encounter_date"], "%Y-%m-%d")
                end_dt = start_dt + timedelta(days=random.randint(30, 365))

                meds.append({
                    "medication_id": f"M{med_id:05d}",
                    "patient_id": pid,
                    "encounter_id": enc["encounter_id"],
                    "medication_name": med["name"],
                    "medication_class": med["class"],
                    "dosage": med["name"].split()[-1] if "mg" in med["name"] or "mcg" in med["name"] else "As directed",
                    "frequency": med["freq"],
                    "prescriber": enc["attending_provider"],
                    "start_date": start_dt.strftime("%Y-%m-%d"),
                    "end_date": end_dt.strftime("%Y-%m-%d"),
                    "status": random.choice(["Active", "Active", "Active", "Completed", "Discontinued"])
                })
                med_id += 1

        # Add general medications for some encounters
        for enc in patient_encs:
            if enc["encounter_type"] in ("ED", "Inpatient") and random.random() < 0.6:
                gen_med = random.choice(MEDICATIONS["General"])
                start_dt = datetime.strptime(enc["encounter_date"], "%Y-%m-%d")
                meds.append({
                    "medication_id": f"M{med_id:05d}",
                    "patient_id": pid,
                    "encounter_id": enc["encounter_id"],
                    "medication_name": gen_med["name"],
                    "medication_class": gen_med["class"],
                    "dosage": gen_med["name"].split()[-1] if "mg" in gen_med["name"] else "As directed",
                    "frequency": gen_med["freq"],
                    "prescriber": enc["attending_provider"],
                    "start_date": start_dt.strftime("%Y-%m-%d"),
                    "end_date": (start_dt + timedelta(days=random.randint(5, 30))).strftime("%Y-%m-%d"),
                    "status": "Completed"
                })
                med_id += 1

    return meds

def generate_vitals(encounters):
    """Generate vital sign records for real-time analytics simulation."""
    vitals = []
    vital_id = 1

    for enc in encounters:
        if enc["encounter_type"] not in ("ED", "Inpatient"):
            if random.random() > 0.3:
                continue

        enc_date = datetime.strptime(enc["encounter_date"], "%Y-%m-%d")
        dx_code = enc["primary_diagnosis_code"]

        # Number of vital sign readings
        if enc["encounter_type"] == "Inpatient":
            num_readings = random.randint(4, enc["length_of_stay_days"] * 4 + 4)
            num_readings = min(num_readings, 30)
        elif enc["encounter_type"] == "ED":
            num_readings = random.randint(2, 6)
        else:
            num_readings = random.randint(1, 2)

        # Base vitals (normal ranges with condition-specific adjustments)
        base_hr = 75
        base_systolic = 120
        base_diastolic = 80
        base_temp = 98.6
        base_rr = 16
        base_spo2 = 97

        # Adjust for specific conditions
        if dx_code == "A41.9":  # Sepsis
            base_hr = random.randint(95, 130)
            base_temp = round(random.uniform(101.0, 104.0), 1)
            base_rr = random.randint(22, 32)
            base_spo2 = random.randint(88, 95)
            base_systolic = random.randint(80, 100)
        elif dx_code.startswith("I50"):  # Heart failure
            base_hr = random.randint(85, 110)
            base_spo2 = random.randint(90, 96)
        elif dx_code.startswith("J44") or dx_code.startswith("J96"):  # COPD/resp failure
            base_rr = random.randint(22, 30)
            base_spo2 = random.randint(85, 93)
        elif dx_code == "I10":  # Hypertension
            base_systolic = random.randint(150, 190)
            base_diastolic = random.randint(90, 110)
        elif dx_code == "I21.9":  # MI
            base_hr = random.randint(90, 120)
            base_systolic = random.randint(90, 140)

        for reading_num in range(num_readings):
            timestamp = enc_date + timedelta(
                hours=reading_num * random.randint(2, 8),
                minutes=random.randint(0, 59)
            )

            # Add some variation to each reading
            hr = max(40, min(180, base_hr + random.randint(-10, 10)))
            systolic = max(70, min(220, base_systolic + random.randint(-10, 10)))
            diastolic = max(40, min(130, base_diastolic + random.randint(-8, 8)))
            temp = round(max(95.0, min(106.0, base_temp + random.uniform(-0.5, 0.5))), 1)
            rr = max(8, min(40, base_rr + random.randint(-3, 3)))
            spo2 = max(70, min(100, base_spo2 + random.randint(-2, 2)))
            pain = random.choices(
                [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                weights=[0.15, 0.10, 0.10, 0.12, 0.10, 0.10, 0.10, 0.08, 0.07, 0.05, 0.03],
                k=1
            )[0]

            vitals.append({
                "vital_id": f"V{vital_id:06d}",
                "patient_id": enc["patient_id"],
                "encounter_id": enc["encounter_id"],
                "facility_name": enc["facility_name"],
                "department": enc["department"],
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "heart_rate": hr,
                "systolic_bp": systolic,
                "diastolic_bp": diastolic,
                "temperature_f": temp,
                "respiratory_rate": rr,
                "spo2_percent": spo2,
                "pain_level": pain
            })
            vital_id += 1

    return vitals

def generate_clinical_notes(patients, encounters, patient_conditions):
    """Generate clinical notes for Gen AI processing."""
    notes = []
    note_id = 1

    # Focus on ED and Inpatient encounters
    relevant_encounters = [e for e in encounters
                          if e["encounter_type"] in ("ED", "Inpatient")]

    # Select a subset for notes (not every encounter gets a full note)
    selected = random.sample(relevant_encounters, min(150, len(relevant_encounters)))

    complaints = [
        "shortness of breath", "chest pain", "abdominal pain",
        "severe headache", "dizziness and near-syncope", "high fever and chills",
        "worsening leg swelling", "confusion and altered mental status",
        "difficulty breathing", "palpitations", "nausea and vomiting",
        "weakness and fatigue", "back pain", "fall with hip pain",
        "cough with blood-tinged sputum"
    ]

    onsets = [
        "approximately 2 hours ago", "this morning", "3 days ago with progressive worsening",
        "yesterday evening", "1 week ago", "suddenly 30 minutes ago",
        "gradually over the past 2 weeks", "acutely 4 hours ago"
    ]

    exam_findings = [
        "alert and oriented, mild respiratory distress",
        "anxious-appearing, diaphoretic, tachycardic",
        "lethargic but arousable, oriented to person only",
        "comfortable at rest, no acute distress",
        "ill-appearing, febrile, tachycardic with dry mucous membranes",
        "moderate respiratory distress with accessory muscle use",
        "bilateral lower extremity edema 2+, JVP elevated",
        "clear lungs bilaterally, regular rate and rhythm",
        "diffuse abdominal tenderness without rebound or guarding",
        "confused, unable to follow commands consistently"
    ]

    treatments = [
        "IV fluids, broad-spectrum antibiotics, and vasopressor support",
        "oxygen supplementation, nebulizer treatments, and IV steroids",
        "IV heparin drip, cardiac monitoring, and serial troponins",
        "IV diuretics with strict I&O monitoring",
        "pain management, orthopedic consultation, and surgical intervention",
        "IV antibiotics, blood cultures, and lactate monitoring",
        "antihypertensive medications and continuous monitoring",
        "anticoagulation therapy and neurology consultation",
        "insulin drip protocol and electrolyte replacement",
        "fluid resuscitation, electrolyte correction, and nutritional support"
    ]

    complications_options = [
        "Hospital course was complicated by a urinary tract infection treated with antibiotics. ",
        "Patient developed atrial fibrillation on hospital day 3, managed with rate control. ",
        "No significant complications during hospitalization. ",
        "Patient experienced a fall on hospital day 2 without injury. Falls precautions were initiated. ",
        "Hospital course was complicated by acute kidney injury which resolved with IV hydration. ",
        "Patient developed hospital-acquired pneumonia requiring escalation of antibiotics. ",
        "",  # No complications
        "",
        ""
    ]

    warning_signs = [
        "worsening shortness of breath, chest pain, or fever above 101F",
        "severe headache, vision changes, or confusion",
        "increased swelling, redness, or drainage from surgical site",
        "inability to tolerate oral medications or fluids",
        "new or worsening symptoms, or any concerns about recovery",
        "chest pain, difficulty breathing, or loss of consciousness"
    ]

    for enc in selected:
        pid = enc["patient_id"]
        patient = next(p for p in patients if p["patient_id"] == pid)
        conditions = patient_conditions.get(pid, [])

        pmh = ", ".join([c["desc"] for c in conditions[:4]]) if conditions else "No significant past medical history"

        if enc["encounter_type"] == "ED":
            note_type = "ED Note"
            note_text = NOTE_TEMPLATES["ED Note"][0].format(
                age=patient["age"],
                gender=patient["gender"].lower(),
                complaint=random.choice(complaints),
                onset=random.choice(onsets),
                pmh=pmh,
                meds="see medication list" if conditions else "no regular medications",
                hr=random.randint(60, 130),
                bp=f"{random.randint(90, 180)}/{random.randint(60, 110)}",
                temp=round(random.uniform(97.0, 103.5), 1),
                rr=random.randint(12, 30),
                spo2=random.randint(88, 100),
                exam=random.choice(exam_findings),
                additional_labs=random.choice(["troponin", "lipase", "urinalysis", "blood cultures", "D-dimer", "lactate"]),
                assessment=enc["primary_diagnosis_description"],
                plan=random.choice(treatments),
                disposition=f"admitted to {enc['department']}" if random.random() < 0.3 else "discharged home with follow-up instructions"
            )
        elif enc["encounter_type"] == "Inpatient":
            note_type = random.choice(["Discharge Summary", "Progress Note"])
            if note_type == "Discharge Summary":
                discharge_meds = "\n".join([f"  - {random.choice(MEDICATIONS['General'])['name']}"
                                           for _ in range(random.randint(3, 7))])
                note_text = NOTE_TEMPLATES["Discharge Summary"][0].format(
                    age=patient["age"],
                    gender=patient["gender"].lower(),
                    admit_date=enc["encounter_date"],
                    discharge_date=enc["discharge_date"],
                    los=enc["length_of_stay_days"],
                    admit_dx=enc["primary_diagnosis_description"],
                    discharge_dx=enc["primary_diagnosis_description"],
                    reason=enc["primary_diagnosis_description"].lower(),
                    pmh_text=f"Past medical history significant for {pmh}" if conditions else "No significant PMH",
                    treatment=random.choice(treatments),
                    complications=random.choice(complications_options),
                    progress="gradual improvement" if random.random() > 0.2 else "slow but steady improvement",
                    discharge_meds=discharge_meds,
                    follow_up=random.choice(["primary care physician", "cardiologist", "pulmonologist", "surgeon"]),
                    follow_up_days=random.choice([3, 5, 7, 14]),
                    instruction1=random.choice(["Monitor blood pressure daily", "Continue wound care as instructed",
                                               "Weigh yourself daily and report gains > 3 lbs",
                                               "Complete full course of antibiotics"]),
                    instruction2=random.choice(["Resume regular diet as tolerated", "Limit sodium intake to <2g/day",
                                               "Activity as tolerated, no heavy lifting > 10 lbs",
                                               "Take medications as prescribed"]),
                    warning_signs=random.choice(warning_signs),
                    disposition=enc["discharge_disposition"]
                )
            else:
                note_text = NOTE_TEMPLATES["Progress Note"][0].format(
                    day=random.randint(1, max(1, enc["length_of_stay_days"])),
                    subjective=random.choice(["feeling better today", "some improvement in symptoms",
                                             "persistent discomfort", "mild nausea but improved overall",
                                             "feeling weak, requesting to go home"]),
                    pain=random.randint(1, 8),
                    sleep_text=random.choice(["Slept well overnight", "Sleep interrupted by vital checks",
                                             "Poor sleep due to pain", "Fair sleep with medication assistance"]),
                    appetite_text=random.choice(["Appetite improving", "Poor appetite, ate 25% of meals",
                                                "Good oral intake", "Tolerating clear liquids"]),
                    hr=random.randint(60, 110),
                    bp=f"{random.randint(100, 160)}/{random.randint(60, 95)}",
                    temp=round(random.uniform(97.5, 100.5), 1),
                    rr=random.randint(14, 24),
                    spo2=random.randint(92, 99),
                    general=random.choice(["Alert, oriented, comfortable", "Appears fatigued but cooperative",
                                          "Mild distress, resting in bed"]),
                    cardio=random.choice(["Regular rate and rhythm, no murmurs", "Irregular rhythm, no gallops",
                                         "Tachycardic, S1/S2 normal"]),
                    resp=random.choice(["Clear to auscultation bilaterally", "Reduced breath sounds at bases",
                                       "Scattered wheezes bilaterally", "Crackles at bilateral bases"]),
                    abdomen=random.choice(["Soft, non-tender, non-distended", "Mild tenderness in RLQ",
                                          "Distended, mildly tender diffusely"]),
                    extremities=random.choice(["No edema", "1+ bilateral lower extremity edema",
                                              "2+ bilateral lower extremity edema", "Warm, well-perfused"]),
                    problem1=enc["primary_diagnosis_description"].split(",")[0],
                    plan1=random.choice(["continue current management", "adjust medications as below",
                                       "consult specialist", "monitor labs q12h"]),
                    problem2=random.choice(["Pain management", "DVT prophylaxis", "Nutrition", "Mobility"]),
                    plan2=random.choice(["continue current regimen", "advance diet as tolerated",
                                       "PT/OT consultation", "reassess in AM"]),
                    additional_plans=random.choice(["", "3. Fall prevention: maintain bed alarm and assist with ambulation\n",
                                                   "3. Diabetes management: continue sliding scale insulin\n"]),
                    discharge_plan=random.choice(["in 1-2 days if continues to improve",
                                                 "tomorrow pending lab results",
                                                 "when able to ambulate independently",
                                                 "pending insurance authorization for SNF placement"])
                )

        notes.append({
            "note_id": f"N{note_id:04d}",
            "patient_id": pid,
            "encounter_id": enc["encounter_id"],
            "note_date": enc["encounter_date"],
            "note_type": note_type,
            "provider": enc["attending_provider"],
            "note_text": note_text.replace("\n", "\\n")  # Escape newlines for CSV
        })
        note_id += 1

    return notes

def generate_claims(encounters, patients):
    """Generate claims/billing data."""
    claims = []
    claim_id = 1

    patient_insurance = {p["patient_id"]: p["insurance_type"] for p in patients}

    for enc in encounters:
        pid = enc["patient_id"]
        insurance = patient_insurance.get(pid, "Self-Pay")

        # Map insurance type to payer
        if insurance == "Medicare":
            payer = random.choice(["Medicare FFS", "Medicare Advantage"])
        elif insurance == "Medicaid":
            payer = "Medicaid"
        elif insurance == "Commercial":
            payer = random.choice(["BlueCross BlueShield", "Aetna", "UnitedHealthcare", "Cigna", "Humana", "Oscar Health"])
        else:
            payer = "Self-Pay"

        claim_amount = enc["total_charges"]

        # Payment and denial logic
        denial_chance = 0.0
        if payer == "Medicare Advantage":
            denial_chance = 0.17  # 17% denial rate per AHA data
            payment_ratio = random.uniform(0.75, 0.88)
        elif payer == "Medicare FFS":
            denial_chance = 0.05
            payment_ratio = random.uniform(0.78, 0.85)
        elif payer == "Medicaid":
            denial_chance = 0.08
            payment_ratio = random.uniform(0.60, 0.80)
        elif payer == "Self-Pay":
            denial_chance = 0.0
            payment_ratio = random.uniform(0.10, 0.40)
        else:  # Commercial
            denial_chance = 0.10
            payment_ratio = random.uniform(0.85, 1.10)

        is_denied = random.random() < denial_chance

        if is_denied:
            denied_amount = claim_amount
            paid_amount = 0
            denial_reason = random.choice(DENIAL_REASONS)
            # Some denials get overturned on appeal
            if random.random() < 0.57:  # 57% overturn rate per AHA
                status = "Paid on Appeal"
                paid_amount = round(claim_amount * payment_ratio, 2)
                denied_amount = round(claim_amount - paid_amount, 2)
            else:
                status = "Denied"
        else:
            paid_amount = round(claim_amount * payment_ratio, 2)
            denied_amount = 0
            denial_reason = ""
            status = "Paid"

        claims.append({
            "claim_id": f"CL{claim_id:05d}",
            "patient_id": pid,
            "encounter_id": enc["encounter_id"],
            "claim_date": enc["encounter_date"],
            "payer": payer,
            "payer_type": insurance,
            "claim_amount": round(claim_amount, 2),
            "paid_amount": round(paid_amount, 2),
            "denied_amount": round(denied_amount, 2),
            "patient_responsibility": round(claim_amount - paid_amount - denied_amount, 2) if status == "Paid" else 0,
            "denial_reason": denial_reason,
            "claim_status": status,
            "days_to_payment": random.randint(15, 90) if status in ("Paid", "Paid on Appeal") else 0
        })
        claim_id += 1

    return claims

def write_csv(filename, data, fieldnames):
    """Write data to CSV file."""
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"  Created {filename}: {len(data)} records")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    print("=" * 60)
    print("Healthcare Synthetic Data Generator")
    print("=" * 60)
    print()

    print("Generating patients...")
    patients = generate_patients(200)
    write_csv("patients.csv", patients,
              ["patient_id", "first_name", "last_name", "date_of_birth", "age",
               "gender", "race", "zip_code", "city", "state", "insurance_type",
               "primary_care_provider", "risk_score"])

    print("Generating conditions...")
    conditions, patient_conditions = generate_conditions(patients)
    write_csv("conditions.csv", conditions,
              ["condition_id", "patient_id", "encounter_id", "condition_code",
               "condition_description", "condition_type", "date_diagnosed", "status"])

    print("Generating encounters...")
    encounters = generate_encounters(patients, patient_conditions)
    write_csv("encounters.csv", encounters,
              ["encounter_id", "patient_id", "encounter_date", "encounter_time",
               "discharge_date", "encounter_type", "facility_id", "facility_name",
               "department", "primary_diagnosis_code", "primary_diagnosis_description",
               "attending_provider", "discharge_disposition", "length_of_stay_days",
               "total_charges"])

    print("Generating medications...")
    medications = generate_medications(patients, patient_conditions, encounters)
    write_csv("medications.csv", medications,
              ["medication_id", "patient_id", "encounter_id", "medication_name",
               "medication_class", "dosage", "frequency", "prescriber",
               "start_date", "end_date", "status"])

    print("Generating vitals...")
    vitals = generate_vitals(encounters)
    write_csv("vitals.csv", vitals,
              ["vital_id", "patient_id", "encounter_id", "facility_name",
               "department", "timestamp", "heart_rate", "systolic_bp",
               "diastolic_bp", "temperature_f", "respiratory_rate",
               "spo2_percent", "pain_level"])

    print("Generating clinical notes...")
    clinical_notes = generate_clinical_notes(patients, encounters, patient_conditions)
    write_csv("clinical_notes.csv", clinical_notes,
              ["note_id", "patient_id", "encounter_id", "note_date",
               "note_type", "provider", "note_text"])

    print("Generating claims...")
    claims = generate_claims(encounters, patients)
    write_csv("claims.csv", claims,
              ["claim_id", "patient_id", "encounter_id", "claim_date",
               "payer", "payer_type", "claim_amount", "paid_amount",
               "denied_amount", "patient_responsibility", "denial_reason",
               "claim_status", "days_to_payment"])

    print()
    print("=" * 60)
    print("Data generation complete!")
    print(f"  Patients:       {len(patients)}")
    print(f"  Conditions:     {len(conditions)}")
    print(f"  Encounters:     {len(encounters)}")
    print(f"  Medications:    {len(medications)}")
    print(f"  Vitals:         {len(vitals)}")
    print(f"  Clinical Notes: {len(clinical_notes)}")
    print(f"  Claims:         {len(claims)}")
    print("=" * 60)

if __name__ == "__main__":
    main()
