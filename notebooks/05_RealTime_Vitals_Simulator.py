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
# ## 1. What this notebook does (business meaning)
#
# This notebook simulates a **real-time patient monitoring system** 
# — the kind found in ICUs, EDs, and telemetry floors. It generates 
# synthetic vital sign readings and streams them to your Fabric 
# Eventstream via the Event Hub protocol.
#
# ### Real-world context
# In a hospital, bedside monitors (Philips, GE, Medtronic) 
# continuously capture heart rate, blood pressure, temperature, 
# respiratory rate, and SpO2 at 1–5 minute intervals. **Early 
# Warning Scores** calculated from these vitals can predict 
# clinical deterioration 6–12 hours before it becomes obvious 
# to clinicians.
#
# ## 2. Patient archetypes and vital sign patterns
#
# We simulate 3 types of patients with distinct clinical behaviors:
#
# | Archetype | Count | HR | Temp (°F) | RR | SpO2 | Clinical Significance |
# |-----------|-------|----|-----------|----|----- |----------------------|
# | Sepsis Risk | 3 | 100–130 | 101.5–104 | 22–32 | 88–94% | ≥ 2 SIRS criteria → sepsis alert |
# | Heart Failure | 3 | 90–115 | 97.5–99 | 18–24 | 90–95% | Compensatory tachycardia, borderline alerts |
# | Stable | 14 | 65–85 | 97.5–99 | 14–18 | 96–99% | Normal baseline group |
#
# ## 3. SIRS criteria (Systemic Inflammatory Response Syndrome)
#
# SIRS is a clinical screening tool for sepsis. Meeting ≥2 of 
# these criteria triggers a sepsis alert:
#
# 1. **Temperature** > 100.4°F (38°C) OR < 96.8°F (36°C)
# 2. **Heart rate** > 90 bpm
# 3. **Respiratory rate** > 20 breaths/min
# 4. **WBC** > 12,000 or < 4,000 (not available in vitals)
#
# We check the first 3 (WBC is a lab value, not a vital sign). 
# Meeting ≥2 means `SIRS_ALERT = TRUE`.
#
# **Why SIRS matters:** Sepsis kills ~270,000 Americans annually. 
# Every hour of delayed treatment increases mortality by 7.6%. 
# Real-time SIRS monitoring enables "Code Sepsis" activation 
# within minutes of clinical deterioration.
#
# ## 4. Step-by-step walkthrough of the simulator logic
#
# #### Step 1: Build patient profiles
#     patient_profiles = []
#     for i in range(NUM_PATIENTS):
#         if i < 3:    # Sepsis Risk archetype
#         elif i < 6:  # Heart Failure archetype
#         else:        # Stable archetype
# - Creates 20 patients with baseline vital signs matching their 
#   clinical archetype
# - Each profile stores: patient_id, facility, department, condition, 
#   and base values for HR, temp, RR, SpO2, systolic/diastolic BP
# - Random facility/department assignment provides dimensional variety 
#   for KQL dashboard analysis
#
# #### Step 2: Generate vital readings with variation
#     def generate_vital_reading(profile):
#         reading = {
#             "heart_rate": max(40, min(180, profile["base_hr"] + random.randint(-8, 8))),
#             "temperature_f": round(max(95, min(106, profile["base_temp"] + random.uniform(-0.5, 0.5))), 1),
#             ...}
# - Each reading adds random variation around the patient's baseline:
#   - ✅ Heart rate: ±8 bpm
#   - ✅ Temperature: ±0.5°F
#   - ✅ Blood pressure: ±10/8 mmHg
#   - ✅ Respiratory rate: ±2 breaths/min
#   - ✅ SpO2: ±2%
# - Values are clamped with `max()/min()` to physiologically possible 
#   ranges (e.g., HR 40–180, SpO2 70–100) to avoid nonsensical data
# - This creates realistic variation while maintaining the clinical 
#   pattern (sepsis patients stay consistently abnormal)
#
# #### Step 3: Compute SIRS score per reading
#     sirs_count = 0
#     if reading["temperature_f"] > 100.4 or reading["temperature_f"] < 96.8:
#         sirs_count += 1
#     if reading["heart_rate"] > 90:    sirs_count += 1
#     if reading["respiratory_rate"] > 20:  sirs_count += 1
#     reading["sirs_alert"] = sirs_count >= 2
# - Checks 3 of 4 SIRS criteria (WBC unavailable from vitals)
# - `sirs_criteria_met` stores the raw count (0–3)
# - `sirs_alert` is a boolean: TRUE when ≥2 criteria are met
# - Sepsis-risk patients should trigger alerts on nearly every reading; 
#   stable patients should almost never trigger
#
# #### Step 4: Send batches via Event Hub protocol
#     producer = EventHubProducerClient.from_connection_string(CONNECTION_STR)
#     for batch_num in range(1, NUM_BATCHES + 1):
#         event_batch = producer.create_batch()
#         for profile in patient_profiles:
#             reading = generate_vital_reading(profile)
#             event_batch.add(EventData(json.dumps(reading)))
#         producer.send_batch(event_batch)
#         time.sleep(INTERVAL_SEC)
# - `EventHubProducerClient` connects to the Eventstream's Custom 
#   endpoint using the same protocol Azure IoT Hub uses
# - Each batch contains 20 readings (one per patient)
# - `json.dumps()` serializes each reading to JSON for transport
# - `send_batch()` delivers all 20 events in a single network call 
#   (more efficient than 20 individual sends)
# - `time.sleep(INTERVAL_SEC)` paces delivery every 2 seconds
# - The `finally: producer.close()` block ensures the connection is 
#   properly cleaned up even if an error occurs
#
# ## 5. Summary
#
# The simulator creates 20 patient profiles across 3 clinical 
# archetypes (sepsis risk, heart failure, stable), generates vital 
# sign readings with realistic physiologic variation, computes SIRS 
# criteria per reading, and streams batches of 20 events to the 
# Fabric Eventstream every 2 seconds via the Event Hub protocol.


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
# ## The Event Hub Protocol & Send Loop
#
# ### 1. What this protocol is (business meaning)
#
# Fabric Eventstream's Custom endpoint source exposes an 
# **Event Hub-compatible endpoint** — the same protocol used by:
# - Azure IoT Hub (medical device ingestion)
# - Azure Event Hubs (application telemetry)
# - Kafka (via the Kafka protocol surface)
#
# This means the simulator code below is identical to what you'd 
# use in production to ingest data from real bedside monitors.
#
# ### 2. Step-by-step walkthrough of the send loop
#
# #### Step 1: Create the producer client
#     producer = EventHubProducerClient.from_connection_string(CONNECTION_STR)
# - `from_connection_string()` parses the connection string to extract:
#   - ✅ `Endpoint=sb://xxx.servicebus.windows.net/` → The Eventstream broker
#   - ✅ `SharedAccessKeyName=...` / `SharedAccessKey=...` → Authentication
#   - ✅ `EntityPath=...` → Which Eventstream topic to send to
# - The client manages the AMQP connection, authentication, and retry logic
#
# #### Step 2: Create an event batch
#     event_batch = producer.create_batch()
# - Groups multiple events for efficient transport
# - One batch per interval = one network round-trip per 20 readings
# - Event Hubs charges per operation, not per event in a batch, 
#   so batching is both faster AND cheaper
#
# #### Step 3: Add events to the batch
#     for profile in patient_profiles:
#         reading = generate_vital_reading(profile)
#         event_batch.add(EventData(json.dumps(reading)))
# - `generate_vital_reading()` produces a Python dict with all vital signs
# - `json.dumps()` serializes the dict to a JSON string
# - `EventData()` wraps the string as an Event Hub message
# - `event_batch.add()` appends it to the batch (raises if batch full)
#
# #### Step 4: Send and pace
#     producer.send_batch(event_batch)
#     time.sleep(INTERVAL_SEC)
# - `send_batch()` delivers all 20 events in a single AMQP operation
# - Delivery is **at-least-once** (guaranteed, may occasionally duplicate)
# - `time.sleep(2)` paces batches every 2 seconds for steady stream
#
# #### Step 5: Cleanup
#     finally:
#         producer.close()
# - `try/finally` ensures the AMQP connection is properly closed even 
#   if an error occurs during sending
# - Unclosed connections can leak resources in long-running notebooks
#
# ### 3. Summary
#
# The send loop creates an Event Hub producer from the Eventstream 
# connection string, batches 20 vital readings per interval, sends 
# each batch via AMQP, and paces delivery every 2 seconds. The 
# `try/finally` block ensures proper connection cleanup.


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
