# =============================================================
# Notebook 05: Real-Time Vitals Simulator
# 
# Purpose: Simulate real-time patient vital signs streaming
#          to a Fabric Eventstream via Event Hub protocol
#
# Prerequisites:
#   - Eventstream created with a Custom App source
#   - Connection string from the Custom App source
#
# Instructions:
#   1. Create a notebook in Fabric named "05 - RealTime Vitals Simulator"
#   2. Replace CONNECTION_STR with your Eventstream connection string
#   3. Run Cell 1 (config), then Cell 2 (simulator)
# =============================================================

# --- Cell 1: Configuration ---
# Replace with YOUR Eventstream Custom App connection string
CONNECTION_STR = "<PASTE_YOUR_EVENTSTREAM_CONNECTION_STRING_HERE>"

# Simulation parameters
NUM_PATIENTS = 20    # Number of patients to simulate
INTERVAL_SEC = 2     # Seconds between each batch of readings
NUM_BATCHES = 100    # Total number of batches to send

# --- Cell 2: Simulator ---
import json
import random
import time
from datetime import datetime

try:
    from azure.eventhub import EventHubProducerClient, EventData
except ImportError:
    import subprocess
    subprocess.check_call(["pip", "install", "azure-eventhub", "-q"])
    from azure.eventhub import EventHubProducerClient, EventData

# Define simulated patients with different clinical profiles
patient_profiles = []
facilities = ["Metro General Hospital", "Community Medical Center", "Riverside Health Center"]
departments = ["ICU", "Emergency", "Internal Medicine", "Cardiology", "Pulmonology"]

for i in range(NUM_PATIENTS):
    if i < 3:  # Sepsis-risk patients
        profile = {
            "patient_id": f"RT-P{i+1:03d}",
            "facility": random.choice(facilities),
            "department": random.choice(["ICU", "Emergency"]),
            "condition": "Sepsis Risk",
            "base_hr": random.randint(100, 130),
            "base_temp": round(random.uniform(101.5, 104.0), 1),
            "base_rr": random.randint(22, 32),
            "base_spo2": random.randint(88, 94),
            "base_systolic": random.randint(75, 95),
            "base_diastolic": random.randint(45, 60)
        }
    elif i < 6:  # Heart failure patients
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
    else:  # Normal/stable patients
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
    """Generate a single vital sign reading with realistic variation."""
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
    
    # Calculate SIRS criteria
    sirs_count = 0
    if reading["temperature_f"] > 100.4 or reading["temperature_f"] < 96.8:
        sirs_count += 1
    if reading["heart_rate"] > 90:
        sirs_count += 1
    if reading["respiratory_rate"] > 20:
        sirs_count += 1
    
    reading["sirs_criteria_met"] = sirs_count
    reading["sirs_alert"] = sirs_count >= 2
    
    return reading

# Send data to Eventstream
print("Starting vitals simulation...")
print(f"  Patients: {NUM_PATIENTS}")
print(f"  Interval: {INTERVAL_SEC}s between batches")
print(f"  Batches: {NUM_BATCHES}")
print()

producer = EventHubProducerClient.from_connection_string(CONNECTION_STR)

try:
    for batch_num in range(1, NUM_BATCHES + 1):
        event_batch = producer.create_batch()
        alerts = []
        
        for profile in patient_profiles:
            reading = generate_vital_reading(profile)
            event_batch.add(EventData(json.dumps(reading)))
            
            if reading["sirs_alert"]:
                alerts.append(f"⚠️  {reading['patient_id']} ({reading['department']}): "
                            f"Temp={reading['temperature_f']}°F, HR={reading['heart_rate']}, "
                            f"RR={reading['respiratory_rate']}")
        
        producer.send_batch(event_batch)
        
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
