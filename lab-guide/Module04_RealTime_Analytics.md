# Module 4: Real-Time Analytics — Patient Vitals Monitoring

| Duration | 60 minutes |
|----------|------------|
| Objective | Build a real-time clinical surveillance system using Fabric Eventhouse that monitors patient vitals and detects sepsis early-warning signs |
| Fabric Features | Eventhouse, KQL Database, Eventstream, Real-Time Dashboard |

---

## Why Real-Time Matters in Healthcare

Traditional batch analytics tells you what happened yesterday. Real-time analytics tells you what's happening **right now** — which in a clinical setting can be the difference between life and death.

**Sepsis example:** Every hour of delay in antibiotic administration for sepsis increases mortality. A real-time system that detects SIRS criteria (Systemic Inflammatory Response Syndrome) can alert clinicians within minutes, not hours.

**SIRS Criteria (at least 2 of the following):**
- Temperature > 100.4°F (38°C) or < 96.8°F (36°C)
- Heart Rate > 90 bpm
- Respiratory Rate > 20 breaths/min
- White Blood Cell count > 12,000 or < 4,000 (not in our vitals data, but a real clinical criterion)

In this module, we'll monitor for **temperature + heart rate + respiratory rate** as our simplified sepsis early-warning trigger.

---

## What You Will Do

1. Create an **Eventhouse** with a KQL database
2. Create an **Eventstream** to receive vitals data
3. Build a **Real-Time Dashboard** with auto-refresh (before any data flows — so you can watch it come alive)
4. Run a **simulator notebook** that pushes vitals data to the Eventstream
5. **Watch the dashboard update in real time** as the simulator runs
6. Write **KQL queries** for deeper clinical analysis

---

## Part A: Set Up the Eventhouse

### Step 1: Create an Eventhouse

1. Go to your workspace
2. Click **+ New item**
3. Search for and select **Eventhouse**
4. Name: `PatientVitalsEventhouse`
5. Click **Create**

This will automatically create a KQL database inside the Eventhouse.

### Step 2: Verify the KQL Database

1. After the Eventhouse is created, you'll see a KQL database (usually named the same as the Eventhouse)
2. Click on the database to open it
3. You should see an empty database ready to receive data

---

## Part B: Create the Eventstream

### Step 3: Create an Eventstream

1. Go to your workspace
2. Click **+ New item**
3. Search for and select **Eventstream**
4. Name: `PatientVitalsStream`
5. Click **Create**

### Step 4: Add a Custom Endpoint Source

The Eventstream needs a source that your simulator notebook can send data to. We'll use a **Custom endpoint**, which provides an Event Hub-compatible connection string.

1. In the Eventstream editor, click **Add source** (in the toolbar)
2. From the dropdown, select **Custom endpoint**
3. Name: `VitalsSimulator`
4. Click **Add**

### Step 5: Add the Eventhouse as a Destination

Now we need to route the incoming data to our Eventhouse for storage and KQL querying.

1. In the Eventstream canvas, click **Add destination** (in the toolbar)
2. Select **Eventhouse** from the list
3. Configure the destination:
   - **Data ingestion mode**: Select **Direct ingestion** (not "Event processing before ingestion")
   - **Destination name**: `PatientVitalsDB`
   - **Workspace**: Select your workspace
   - **Eventhouse**: Select `PatientVitalsEventhouse`
   - **KQL Database**: Select the database (same name as the Eventhouse)
   - **Destination table**: Click **Create new** and name it: `PatientVitals`
   - **Input data format**: Select **JSON**
4. Click **Save**
5. On the canvas, you should now see the flow: **VitalsSimulator** → **PatientVitalsDB**

### Step 6: Publish the Eventstream and Get the Connection String

You **must publish the Eventstream before the connection string becomes available**.

1. Click **Publish** in the toolbar to activate the Eventstream
2. Wait for the publish to complete — the nodes should show a green status indicator
3. After publishing, click on the **VitalsSimulator** source node in the canvas
4. In the detail pane that opens at the bottom, click the **Details** link (or look for the information panel)
5. You will see connection options — select **Event Hub** with **SAS key authentication**
6. You should now see:
   - **Event Hub name** — e.g., `es_xxxxxxxx`
   - **Connection string–primary key** — starts with `Endpoint=sb://...`
7. Click the **copy** icon next to the **Connection string–primary key** to copy it
8. **Save this connection string** — you'll paste it into the simulator notebook later

> **The connection string looks like:**
> `Endpoint=sb://....servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=...`
>
> **⚠️ If you don't see the connection string:** Make sure you have **published** the Eventstream first (Step 6.1). The connection string is only available after publishing.

> **Troubleshooting:** If you don't see the **Add destination** button in the toolbar, try clicking on an empty area of the canvas first, or look for a **"+"** icon on the right edge of the source node that lets you connect directly to a new destination.

---

## Part C: Build the Real-Time Dashboard (Before Simulation)

We build the dashboard **first** so you can watch it come alive the moment the simulator starts sending data.

> **Note:** The dashboard tiles will show "No data" or empty visuals initially — that's expected. They will populate as soon as the simulator starts.

### Step 7: Create a Real-Time Dashboard

1. Go to your workspace
2. Click **+ New item** → **Real-Time Dashboard**
3. Name: `ICU Command Center Dashboard`
4. Click **Create**

### Step 8: Add Dashboard Tiles

> ⚠️ **Note:** All the tile queries below reference a table named `PatientVitals`. This is the table that gets created when the Eventstream destination ingests data. If your table has a different name (you can check later by running `.show tables` in the KQL database), replace `PatientVitals` with your actual table name in each query. The tiles may show errors until you start the simulator and data begins flowing.

#### Tile 1: SIRS Alert Count (Big Number)
1. Click **+ Add tile**
2. Select your KQL database as the data source
3. Enter this query:
```kql
PatientVitals
| where todatetime(timestamp) > ago(5m)
| where sirs_alert == true
| summarize alert_patients = dcount(patient_id)
```
4. Visual type: **Stat** (big number)
5. Title: `Active SIRS Alerts`
6. Add conditional formatting: Red if > 0

#### Tile 2: Patient Vitals Grid
1. Add a new tile with this query:
```kql
PatientVitals
| summarize arg_max(timestamp, *) by patient_id
| project patient_id, department, facility_name,
    heart_rate, temperature_f, respiratory_rate, spo2_percent, sirs_alert
| order by sirs_alert desc, patient_id
```
2. Visual type: **Table**
3. Title: `Current Patient Vitals`
4. Add conditional formatting on `sirs_alert` column (highlight TRUE in red)

#### Tile 3: Heart Rate Trend
1. Add a tile:
```kql
PatientVitals
| where patient_id in ("RT-P001", "RT-P002", "RT-P003")
| where todatetime(timestamp) > ago(10m)
| project timestamp, patient_id, heart_rate
| render timechart
```
2. Visual type: **Time chart**
3. Title: `Heart Rate — High-Risk Patients`

#### Tile 4: SpO2 Monitor
1. Add a tile:
```kql
PatientVitals
| where todatetime(timestamp) > ago(10m)
| summarize avg_spo2 = round(avg(spo2_percent), 1) by bin(timestamp, 30s), facility_name
| render timechart
```
2. Visual type: **Time chart**
3. Title: `Average SpO2 by Facility`

#### Tile 5: Department Alert Heat Map
1. Add a tile:
```kql
PatientVitals
| where todatetime(timestamp) > ago(5m)
| summarize 
    total_patients = dcount(patient_id),
    sirs_alerts = dcountif(patient_id, sirs_alert == true)
    by facility_name, department
| extend alert_pct = round(todouble(sirs_alerts) / todouble(total_patients) * 100, 1)
```
2. Visual type: **Table** or **Heatmap**
3. Title: `Department Alert Summary`

### Step 9: Set Continuous Auto-Refresh

This is the key step that makes the dashboard update automatically as data flows in.

1. At the top of the dashboard, click the **Manage** tab in the ribbon
2. Look for the **Auto refresh** button and click it (or find it under the dashboard toolbar area)
3. Toggle **Auto refresh** to **On**
4. Set the **Minimum time interval** to **30 seconds**
5. Click **Apply**
6. You should now see a small refresh indicator at the top of the dashboard showing the countdown to the next refresh

> **Alternative method:** If you don't see the Auto refresh button in the Manage tab:
> 1. Click the **pencil/Edit** icon at the top-right of the dashboard
> 2. In editing mode, click the **⚙️ gear icon** (Settings) at the top
> 3. Look for **Auto refresh** in the settings panel
> 4. Toggle it **On** and set the interval to **30 seconds**
> 5. Click **Apply** and then **Save** the dashboard

> **Note:** The dashboard will now automatically re-run all tile queries every 30 seconds. When you start the simulator in the next step, you'll see values appear and update without manually refreshing.

**Leave this dashboard tab open** — you'll come back to it after starting the simulator.

---

## Part D: Simulate Real-Time Vitals Data

### Step 10: Create the Simulator Notebook

We'll create a notebook that simulates real-time patient vitals streaming into the Eventstream.

1. Go to your workspace (**open this in a new browser tab** — keep the dashboard tab open)
2. Click **+ New item** → **Notebook**
3. Rename to: `05 - RealTime Vitals Simulator`

> ⚠️ **Session Note:** If your Spark session expires or is stopped at any point, you will need to re-run all cells from the top using **Run all**. Fabric does not preserve variables, imports, or DataFrames across session restarts.

### Step 11: Configure the Simulator

Paste the following code in Cell 1:

```python
# =============================================================
# Cell 1: Configuration
# Set up the connection to the Eventstream
# =============================================================

# ⚠️ IMPORTANT: Replace with YOUR Eventstream connection string
# To find it:
#   1. Go to your PatientVitalsStream Eventstream (must be published)
#   2. Click the VitalsSimulator source node
#   3. Click "Details" in the lower pane
#   4. Select Event Hub → SAS key authentication
#   5. Copy the "Connection string–primary key"
CONNECTION_STR = "<PASTE_YOUR_EVENTSTREAM_CONNECTION_STRING_HERE>"

# Simulation parameters
NUM_PATIENTS = 20    # Number of patients to simulate
INTERVAL_SEC = 2     # Seconds between each batch of readings
NUM_BATCHES = 100    # Total number of batches to send (set high for demo)
```

Paste the following in Cell 2:

```python
# =============================================================
# Cell 2: Vitals Simulator
# Generates and sends realistic patient vital signs
# =============================================================
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

# Define simulated patients with different clinical profiles
patient_profiles = []
facilities = ["Metro General Hospital", "Community Medical Center", "Riverside Health Center"]
departments = ["ICU", "Emergency", "Internal Medicine", "Cardiology", "Pulmonology"]

for i in range(NUM_PATIENTS):
    # Some patients are sepsis-risk (abnormal vitals)
    if i < 3:  # 3 patients with sepsis-like vitals
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
    elif i < 6:  # 3 patients with heart failure vitals
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
    else:  # Normal patients
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
    
    # Add SIRS flag
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
        
        # Print status
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
```

### Step 12: Run the Simulator

1. First, replace the `CONNECTION_STR` value in Cell 1 with the connection string you copied from the Eventstream (see Step 6)
2. Run Cell 1 to set the configuration
3. Run Cell 2 to start the simulation

You should see output like:
```
Starting vitals simulation...
  Patients: 20
  Interval: 2s between batches
  Batches: 100

[14:05:01] Batch 1/100: Sent 20 readings | 3 SIRS ALERTS:
    ⚠️  RT-P001 (ICU): Temp=103.2°F, HR=125, RR=28
    ⚠️  RT-P002 (Emergency): Temp=102.1°F, HR=118, RR=25
    ⚠️  RT-P003 (ICU): Temp=101.8°F, HR=105, RR=24
[14:05:03] Batch 2/100: Sent 20 readings | 2 SIRS ALERTS:
...
```

### Step 13: Watch the Real-Time Dashboard Update

Now that the simulator is sending data, switch to the dashboard tab and watch it come alive:

1. **Switch to the browser tab** with your `ICU Command Center Dashboard`
2. Wait up to 30 seconds for the next auto-refresh cycle (or click the **Refresh** button manually)
3. You should see:
   - **SIRS Alert Count** tile showing 2–3 active alerts
   - **Patient Vitals Grid** populating with 20 patients, SIRS alert patients in red at the top
   - **Heart Rate Trend** chart drawing a live line as new data arrives for RT-P001, RT-P002, RT-P003
   - **SpO2 Monitor** showing oxygen saturation trends by facility
   - **Department Alert Summary** showing which departments have the most alerts
4. **Keep watching** — every 30 seconds the dashboard will refresh and you'll see the charts extend with new data points
5. Notice how the **sepsis-risk patients** (RT-P001, RT-P002, RT-P003) consistently show elevated vitals, while stable patients remain in normal ranges

> **This is the power of real-time analytics:** In a real clinical setting, a nurse or charge nurse would have this dashboard on a monitor at the nursing station, instantly seeing which patients are deteriorating without waiting for a batch report.

> **Tip:** Let the simulator run for at least 3-5 minutes while you explore the dashboard, then continue with the KQL queries in the next section (in another tab).

---

## Part E: KQL Queries for Clinical Analysis

### Step 14: Open the KQL Database

1. Open a **new browser tab** (keep the simulator running and the dashboard open)
2. Go to your workspace
3. Click on `PatientVitalsEventhouse`
4. Click on the KQL database
5. Click **Explore data** or open a new KQL queryset

### Step 15: Verify the Table Exists

Before running any queries, confirm data is flowing and check the actual table name:

```kql
.show tables
```

> ⚠️ **Important:** You must run the simulator (Step 12) first and let it send at least 1–2 batches before the table will appear. If `.show tables` returns no results, go back and make sure the simulator is running and sending data.
>
> Also verify the table name matches `PatientVitals`. If the table has a different name (e.g., `PatientVitalsDB` or something else), use that name in all the queries below instead of `PatientVitals`.

You can also verify data is present by running:

```kql
PatientVitals
| count
```

If this returns a count greater than 0, you're ready to proceed.

### Step 16: Write Clinical KQL Queries

Paste and run each of the following queries:

#### Query 1: Latest Vitals for All Patients
```kql
PatientVitals
| summarize arg_max(timestamp, *) by patient_id
| project timestamp, patient_id, facility_name, department, 
    heart_rate, systolic_bp, diastolic_bp, temperature_f, 
    respiratory_rate, spo2_percent, sirs_alert
| order by sirs_alert desc, patient_id asc
```

#### Query 2: SIRS/Sepsis Alerts — Active Patients Meeting Criteria
```kql
PatientVitals
| where todatetime(timestamp) > ago(5m)
| where sirs_alert == true
| summarize 
    latest_reading = max(timestamp),
    avg_hr = round(avg(heart_rate), 0),
    avg_temp = round(avg(temperature_f), 1),
    avg_rr = round(avg(respiratory_rate), 0),
    avg_spo2 = round(avg(spo2_percent), 0),
    alert_count = count()
    by patient_id, facility_name, department
| order by alert_count desc
```

#### Query 3: Vital Sign Trends for a Specific Patient
```kql
PatientVitals
| where patient_id == "RT-P001"
| order by timestamp asc
| project timestamp, heart_rate, temperature_f, respiratory_rate, spo2_percent, sirs_alert
```

#### Query 4: Department-Level Census & Alert Summary
```kql
PatientVitals
| where todatetime(timestamp) > ago(5m)
| summarize 
    patient_count = dcount(patient_id),
    sirs_alerts = dcountif(patient_id, sirs_alert == true),
    avg_hr = round(avg(heart_rate), 0),
    avg_spo2 = round(avg(spo2_percent), 0),
    critical_spo2 = dcountif(patient_id, spo2_percent < 90)
    by facility_name, department
| order by sirs_alerts desc
```

#### Query 5: Vital Sign Distribution (for anomaly detection)
```kql
PatientVitals
| where todatetime(timestamp) > ago(10m)
| summarize 
    hr_p50 = percentile(heart_rate, 50),
    hr_p95 = percentile(heart_rate, 95),
    temp_p50 = percentile(temperature_f, 50),
    temp_p95 = percentile(temperature_f, 95),
    spo2_p5 = percentile(spo2_percent, 5),
    spo2_p50 = percentile(spo2_percent, 50)
    by facility_name
```

#### Query 6: Alert Timeline (last 30 minutes)
```kql
PatientVitals
| where todatetime(timestamp) > ago(30m)
| where sirs_alert == true
| summarize alert_count = count() by bin(timestamp, 1m), facility_name
| order by timestamp asc
| render timechart
```

---

## Step 17: Stop the Simulator

Once you've explored the dashboard, go back to your simulator notebook and stop the cell execution (click the stop button ■ next to the running cell).

---

## 💡 Clinical Scenario Discussion

Imagine this scenario:
> It's 2 AM in the ICU. A nurse notices on the real-time dashboard that patient RT-P001 has triggered SIRS criteria for the past 15 minutes — temperature 103°F, heart rate 125, respiratory rate 28. The SpO2 is trending down.

**Discussion questions:**
1. What actions should the clinical team take immediately?
2. How is this different from waiting for a batch report the next morning?
3. What additional data points (labs, medications) would improve the alerting?
4. How could this system integrate with nurse call systems or EHR alerts?

---

## ✅ Module 4 Checklist

Before moving to Module 5, confirm:

- [ ] Eventhouse `PatientVitalsEventhouse` is created
- [ ] Eventstream `PatientVitalsStream` is configured with source and destination
- [ ] Simulator notebook successfully sent vitals data
- [ ] KQL queries return results (vitals, SIRS alerts, trends)
- [ ] Real-Time Dashboard shows live patient monitoring tiles
- [ ] You can see SIRS alerts flagged for the high-risk simulated patients

---

**[← Module 3: Semantic Model & Dashboard](Module03_Semantic_Model_and_Dashboard.md)** | **[Module 5: Gen AI — Clinical Intelligence →](Module05_GenAI_Clinical_Intelligence.md)**
