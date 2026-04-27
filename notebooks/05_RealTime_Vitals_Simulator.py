# ================================================================
# NOTEBOOK 05: REAL-TIME VITALS SIMULATOR
# ================================================================
# 
# ┌─────────────────────────────────────────────────────────────┐
# │  MODULE 4 — REAL-TIME ANALYTICS (Simulator Component)       │
# │  Fabric Capability: Eventstream Custom Endpoint, Event Hubs │
# └─────────────────────────────────────────────────────────────┘
#
# ── INSTRUCTIONS ──────────────────────────────────────────────
#   1. Create a notebook in Fabric named "05 - RealTime Vitals Simulator"
#   2. Attach your HealthcareLakehouse
#   3. Create one cell per section below (each "CELL" block)
#   4. Run cells sequentially
#
# ⚠️ PREREQUISITES:
#   - Eventstream created with a Custom endpoint source
#   - Connection string copied from Keys tab of the Custom endpoint
#   - Eventhouse created as a destination in the Eventstream
#
# ================================================================


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 1 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# # 📡 Real-Time Vitals Simulator
#
# ## What This Notebook Does
#
# This notebook simulates a **real-time patient monitoring system** 
# — the kind found in ICUs, EDs, and telemetry floors. It generates 
# synthetic vital sign readings and streams them to your Fabric 
# Eventstream via the Event Hub protocol.
#
# ## Real-World Context
#
# In a hospital, bedside monitors (Philips, GE, Medtronic) 
# continuously capture:
# - Heart rate (ECG-derived)
# - Blood pressure (arterial line or cuff)
# - Temperature (oral, rectal, or axillary)
# - Respiratory rate (impedance or capnography)
# - SpO2 (pulse oximetry)
#
# These readings stream to a central monitoring system at 
# intervals of 1-5 minutes. **Early Warning Scores** calculated 
# from these vitals can predict clinical deterioration 6-12 hours 
# before it becomes obvious to clinicians.
#
# ## Patient Archetypes
#
# We simulate 3 types of patients:
#
# | Archetype | Count | Vital Pattern | Clinical Significance |
# |-----------|-------|---------------|----------------------|
# | Sepsis Risk | 3 | High temp (>101.5°F), high HR (>100), high RR (>22), low SpO2 | SIRS criteria → sepsis screening |
# | Heart Failure | 3 | Elevated HR (90-115), normal temp, low SpO2 (90-95) | Fluid overload monitoring |
# | Stable | 14 | Normal ranges | Baseline comparison group |
#
# ## SIRS Criteria (Systemic Inflammatory Response Syndrome)
#
# SIRS is a clinical screening tool for sepsis. Meeting ≥2 
# of these 4 criteria triggers a sepsis alert:
#
# 1. **Temperature** > 100.4°F (38°C) or < 96.8°F (36°C)
# 2. **Heart rate** > 90 bpm
# 3. **Respiratory rate** > 20 breaths/min
# 4. **WBC** > 12,000 or < 4,000 (not available in vitals)
#
# Since we only have vitals (not WBC), we check the first 3.
# Meeting ≥2 means SIRS_ALERT = TRUE.
#
# **Why SIRS matters:** Sepsis kills ~270,000 Americans annually. 
# Every hour of delayed treatment increases mortality by 7.6%.
# Real-time SIRS monitoring enables "Code Sepsis" activation 
# within minutes of clinical deterioration.


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 2 — CODE: Configuration                                  ║
# ╚════════════════════════════════════════════════════════════════╝

# ⚠️ IMPORTANT: Replace with YOUR Eventstream connection string
#
# HOW TO GET THIS VALUE:
#   1. First, make sure you've PUBLISHED the Eventstream
#   2. Click your Custom endpoint source node (e.g., "VitalsSimulator")
#   3. Click "Details" (bottom/right pane) → look for "Event Hub" section
#   4. Under "SAS key authentication", copy the full Connection string
#
# The connection string looks like:
#   Endpoint=sb://xxx.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=...
#
# ⚠️ If you DON'T see the Event Hub section, the Eventstream may not be published yet.
#    Click "Publish" in the Eventstream toolbar first.
CONNECTION_STR = "<PASTE_YOUR_EVENTSTREAM_CONNECTION_STRING_HERE>"

# Simulation parameters
NUM_PATIENTS = 20    # Number of patients to simulate
INTERVAL_SEC = 2     # Seconds between each batch of readings
NUM_BATCHES = 100    # Total number of batches to send (set high for demo)

print(f"📡 Simulator configured:")
print(f"   Patients: {NUM_PATIENTS}")
print(f"   Interval: {INTERVAL_SEC}s between batches")
print(f"   Total batches: {NUM_BATCHES}")
print(f"   Total readings: {NUM_PATIENTS * NUM_BATCHES}")
print(f"   Estimated run time: {NUM_BATCHES * INTERVAL_SEC / 60:.1f} minutes")


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 3 — MARKDOWN                                            ║
# ╚════════════════════════════════════════════════════════════════╝
#
# ## The Event Hub Protocol
#
# Fabric Eventstream's Custom endpoint source exposes an 
# **Event Hub-compatible endpoint**. This is the same protocol 
# used by:
# - Azure IoT Hub (medical devices)
# - Azure Event Hubs (application telemetry)
# - Kafka (via the Kafka protocol surface)
#
# The `azure-eventhub` Python SDK sends events as:
# 1. Create a **producer client** from the connection string
# 2. Create an **event batch** (groups events for efficient transport)
# 3. Add **EventData** objects (JSON-serialized readings)
# 4. **Send the batch** — delivery is guaranteed (at-least-once)
#
# Each batch contains one reading per patient (20 events). 
# Events are automatically routed through the Eventstream 
# to the Eventhouse destination.
#
# ## How the Simulator Generates Realistic Variation
#
# Each patient has a **base profile** (e.g., base HR = 110 for 
# sepsis-risk patients). Each reading adds random variation:
# - Heart rate: ±8 bpm
# - Temperature: ±0.5°F
# - Blood pressure: ±10/8 mmHg
# - Respiratory rate: ±2 breaths/min
# - SpO2: ±2%
#
# This creates realistic physiologic variation while maintaining 
# the clinical pattern (sepsis patients stay abnormal, stable 
# patients stay normal).


# ╔════════════════════════════════════════════════════════════════╗
# ║  CELL 4 — CODE: Vitals Simulator                               ║
# ╚════════════════════════════════════════════════════════════════╝
import json
import random
import time
from datetime import datetime

# Install the Event Hubs SDK if not available
try:
    from azure.eventhub import EventHubProducerClient, EventData
except ImportError:
    import subprocess
    subprocess.check_call(["pip", "install", "azure-eventhub", "-q"])
    from azure.eventhub import EventHubProducerClient, EventData

# ── Build Patient Profiles ─────────────────────────────────
# We define 3 clinical archetypes with distinct vital sign baselines.
# Each archetype represents a typical patient scenario that generates
# different alert patterns in the real-time dashboard:
#
#   Sepsis Risk (3 patients): Will trigger SIRS alerts continuously
#     → This tests the real-time alerting pipeline end-to-end
#   Heart Failure (3 patients): Borderline vitals, occasional alerts
#     → This tests threshold-based detection sensitivity
#   Stable (14 patients): Normal vitals, no alerts
#     → This provides baseline comparison and realistic volume

patient_profiles = []
facilities = ["Metro General Hospital", "Community Medical Center", "Riverside Health Center"]
departments = ["ICU", "Emergency", "Internal Medicine", "Cardiology", "Pulmonology"]

for i in range(NUM_PATIENTS):
    if i < 3:
        # SEPSIS RISK: High temp, high HR, high RR, low SpO2
        # These patients should trigger SIRS alerts (≥2 criteria met)
        profile = {
            "patient_id": f"RT-P{i+1:03d}",
            "facility": random.choice(facilities),
            "department": random.choice(["ICU", "Emergency"]),
            "condition": "Sepsis Risk",
            "base_hr": random.randint(100, 130),      # Tachycardic
            "base_temp": round(random.uniform(101.5, 104.0), 1),  # Febrile
            "base_rr": random.randint(22, 32),         # Tachypneic
            "base_spo2": random.randint(88, 94),       # Hypoxic
            "base_systolic": random.randint(75, 95),   # Hypotensive
            "base_diastolic": random.randint(45, 60)
        }
    elif i < 6:
        # HEART FAILURE: Elevated HR, normal temp, low SpO2
        # Represents fluid-overloaded patients with compensatory tachycardia
        profile = {
            "patient_id": f"RT-P{i+1:03d}",
            "facility": random.choice(facilities),
            "department": random.choice(["ICU", "Cardiology"]),
            "condition": "Heart Failure",
            "base_hr": random.randint(90, 115),
            "base_temp": round(random.uniform(97.5, 99.0), 1),
            "base_rr": random.randint(18, 24),
            "base_spo2": random.randint(90, 95),
            "base_systolic": random.randint(100, 130),
            "base_diastolic": random.randint(60, 80)
        }
    else:
        # STABLE: Normal vital signs — baseline comparison group
        profile = {
            "patient_id": f"RT-P{i+1:03d}",
            "facility": random.choice(facilities),
            "department": random.choice(departments),
            "condition": "Stable",
            "base_hr": random.randint(65, 85),
            "base_temp": round(random.uniform(97.5, 99.0), 1),
            "base_rr": random.randint(14, 18),
            "base_spo2": random.randint(96, 99),
            "base_systolic": random.randint(110, 135),
            "base_diastolic": random.randint(65, 85)
        }
    patient_profiles.append(profile)


def generate_vital_reading(profile):
    """Generate a single vital sign reading with realistic physiologic variation.
    
    Each reading adds random variation around the patient's baseline.
    Values are clamped to physiologically possible ranges to avoid
    nonsensical data (e.g., HR of -5 or SpO2 of 110%).
    """
    reading = {
        "patient_id": profile["patient_id"],
        "facility_name": profile["facility"],
        "department": profile["department"],
        "condition": profile["condition"],
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "heart_rate": max(40, min(180, profile["base_hr"] + random.randint(-8, 8))),
        "systolic_bp": max(60, min(220, profile["base_systolic"] + random.randint(-10, 10))),
        "diastolic_bp": max(40, min(130, profile["base_diastolic"] + random.randint(-8, 8))),
        "temperature_f": round(max(95.0, min(106.0, profile["base_temp"] + random.uniform(-0.5, 0.5))), 1),
        "respiratory_rate": max(8, min(40, profile["base_rr"] + random.randint(-2, 2))),
        "spo2_percent": max(70, min(100, profile["base_spo2"] + random.randint(-2, 2))),
        "pain_level": random.randint(0, 8)
    }
    
    # ── SIRS Scoring ────────────────────────────────────────
    # Check 3 of 4 SIRS criteria (WBC not available from vitals)
    sirs_count = 0
    if reading["temperature_f"] > 100.4 or reading["temperature_f"] < 96.8:
        sirs_count += 1  # Criterion 1: Abnormal temperature
    if reading["heart_rate"] > 90:
        sirs_count += 1  # Criterion 2: Tachycardia
    if reading["respiratory_rate"] > 20:
        sirs_count += 1  # Criterion 3: Tachypnea
    
    reading["sirs_criteria_met"] = sirs_count
    reading["sirs_alert"] = sirs_count >= 2  # ≥2 = potential sepsis
    
    return reading


# ── Send Data to Eventstream via Event Hub Protocol ─────────
# The EventHubProducerClient connects to the Eventstream's Custom
# endpoint using the same protocol that Azure IoT Hub and Event Hubs
# use. This means production medical device data flows through the
# exact same pipeline as our simulated data.
#
# Batching: We group all patient readings into one batch per interval.
# This is more efficient than sending 20 individual events because:
#   - Fewer network round-trips (1 batch vs 20 individual sends)
#   - Amortized connection overhead
#   - Event Hubs charges per operation, not per event in a batch
print("🚀 Starting vitals simulation...")
print(f"   Patients: {NUM_PATIENTS} ({sum(1 for p in patient_profiles if p['condition']=='Sepsis Risk')} sepsis risk, "
      f"{sum(1 for p in patient_profiles if p['condition']=='Heart Failure')} heart failure, "
      f"{sum(1 for p in patient_profiles if p['condition']=='Stable')} stable)")
print(f"   Interval: {INTERVAL_SEC}s between batches")
print(f"   Batches: {NUM_BATCHES}")
print()

producer = EventHubProducerClient.from_connection_string(CONNECTION_STR)

try:
    for batch_num in range(1, NUM_BATCHES + 1):
        event_batch = producer.create_batch()
        alerts = []
        
        for profile in patient_profiles:
            reading = generate_vital_reading(profile)
            event_batch.add(EventData(json.dumps(reading)))
            
            # Collect SIRS alerts for console output
            if reading["sirs_alert"]:
                alerts.append(f"⚠️  {reading['patient_id']} ({reading['department']}): "
                            f"Temp={reading['temperature_f']}°F, HR={reading['heart_rate']}, "
                            f"RR={reading['respiratory_rate']}")
        
        producer.send_batch(event_batch)
        
        # Print status with alert details
        timestamp = datetime.utcnow().strftime("%H:%M:%S")
        print(f"[{timestamp}] Batch {batch_num}/{NUM_BATCHES}: Sent {NUM_PATIENTS} readings", end="")
        if alerts:
            print(f" | {len(alerts)} SIRS ALERTS:")
            for alert in alerts:
                print(f"    {alert}")
        else:
            print(" | No alerts")
        
        if batch_num < NUM_BATCHES:
            time.sleep(INTERVAL_SEC)

finally:
    producer.close()

print(f"\n✅ Simulation complete! Sent {NUM_BATCHES * NUM_PATIENTS} total readings.")
print("   Switch to Eventhouse to run KQL queries on the streaming data.")
