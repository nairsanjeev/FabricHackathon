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
2. Create an **Eventstream** to receive vitals data and configure the destination table
3. Build a **Real-Time Dashboard** with auto-refresh (tiles show empty until data flows)
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

Now we need to route the incoming data to our Eventhouse for storage and KQL querying. **This step also creates the destination table in the KQL database** — if you skip the table creation here, data will not land in the Eventhouse.

1. In the Eventstream canvas, click **Add destination** (in the toolbar)
2. Select **Eventhouse** from the list
3. Configure the destination:
   - **Data ingestion mode**: Select **Event processing before ingestion** (not "Direct ingestion" — Direct ingestion does not allow creating new tables)
   - **Destination name**: `PatientVitalsDB`
   - **Workspace**: Select your workspace
   - **Eventhouse**: Select `PatientVitalsEventhouse`
   - **KQL Database**: Select the database (same name as the Eventhouse)
   - **Destination table**: Click **Create new** → type `PatientVitals` → click **Done**
   - **Input data format**: Select **JSON**

> ⚠️ **Critical:** You **must** select **Event processing before ingestion** as the ingestion mode — this is what enables the **Create new** button for the destination table. If you select "Direct ingestion" instead, you will not be able to create a new table and the **Save** button will remain disabled. Make sure the table name `PatientVitals` is confirmed before proceeding.

4. Click **Save** (the Save button only becomes active after you create a new table)
5. On the canvas, you should now see the flow: **VitalsSimulator** → **PatientVitalsDB**

### Step 6: Verify the Table Was Created

Before publishing, confirm the destination table was set up correctly:

1. Click on the **destination node** (`PatientVitalsDB`) on the canvas
2. In the detail pane, verify you see:
   - Eventhouse: `PatientVitalsEventhouse`
   - Database: `PatientVitalsEventhouse` (or your database name)
   - Table: `PatientVitals`
   - Data format: `JSON`
3. If the table field is empty or shows an error, delete the destination and re-add it following Step 5 again

### Step 7: Publish the Eventstream and Get the Connection String

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

### Step 8: Seed the PatientVitals Table

Even though we configured the destination table in Step 5, the table may not actually be created in the KQL database until data flows through the Eventstream. To avoid `Failed to resolve table` errors when building the dashboard, we'll create a small notebook that creates the table and inserts a few sample rows directly in the KQL database.

1. Go to your workspace
2. Click **+ New item** → **Notebook**
3. Rename to: `Seed PatientVitals Table`

Paste the following in **Cell 1**:

```python
# =============================================================
# Cell 1: Configuration
# =============================================================

# ⚠️ IMPORTANT: Replace these with YOUR values
KUSTO_URI = "https://<your-eventhouse-uri>.kusto.fabric.microsoft.com"
DATABASE_NAME = "PatientVitalsEventhouse"  # Usually same name as the Eventhouse
```

> **How to find your Eventhouse URI:**
> 1. Go to your workspace → click on `PatientVitalsEventhouse`
> 2. Click on the KQL database
> 3. Click **Copy URI** in the toolbar, and select **Query URI**
> 4. It will look like: `https://xyz123abc.kusto.fabric.microsoft.com`

Paste the following in **Cell 2**:

```python
# =============================================================
# Cell 2: Create table and insert sample data
# Uses the Kusto REST API — no extra packages needed
# =============================================================
import requests
import json

# Get an access token using Fabric's built-in credentials
access_token = mssparkutils.credentials.getToken(KUSTO_URI)

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

def run_kql_command(uri, db, csl):
    """Execute a KQL management command via the REST API."""
    url = f"{uri}/v1/rest/mgmt"
    body = {"db": db, "csl": csl}
    resp = requests.post(url, headers=headers, json=body)
    resp.raise_for_status()
    return resp.json()

def run_kql_query(uri, db, csl):
    """Execute a KQL query via the REST API."""
    url = f"{uri}/v1/rest/query"
    body = {"db": db, "csl": csl}
    resp = requests.post(url, headers=headers, json=body)
    resp.raise_for_status()
    return resp.json()

# --- Step 1: Create the table with the expected schema ---
create_table_cmd = """.create-merge table PatientVitals (
    patient_id: string,
    facility_name: string,
    department: string,
    ['condition']: string,
    timestamp: string,
    heart_rate: int,
    systolic_bp: int,
    diastolic_bp: int,
    temperature_f: real,
    respiratory_rate: int,
    spo2_percent: int,
    pain_level: int,
    sirs_criteria_met: int,
    sirs_alert: bool
)"""

run_kql_command(KUSTO_URI, DATABASE_NAME, create_table_cmd)
print("✅ Table 'PatientVitals' created (or already exists)")

# --- Step 2: Insert sample rows so dashboard tiles can render ---
ingest_cmd = """.ingest inline into table PatientVitals <|
RT-P001,Metro General Hospital,ICU,Sepsis Risk,2025-01-01T00:00:00Z,125,85,55,103.2,28,91,6,3,true
RT-P002,Community Medical Center,Emergency,Sepsis Risk,2025-01-01T00:00:00Z,118,90,58,102.1,25,93,5,3,true
RT-P003,Riverside Health Center,ICU,Sepsis Risk,2025-01-01T00:00:00Z,105,92,57,101.8,24,92,4,2,true
RT-P004,Metro General Hospital,Internal Medicine,Stable,2025-01-01T00:00:00Z,75,120,75,98.2,16,98,2,0,false
RT-P005,Community Medical Center,Cardiology,Stable,2025-01-01T00:00:00Z,80,125,78,98.6,15,97,1,0,false"""

run_kql_command(KUSTO_URI, DATABASE_NAME, ingest_cmd)
print("✅ Inserted 5 sample rows into PatientVitals")

# --- Step 3: Verify ---
result = run_kql_query(KUSTO_URI, DATABASE_NAME, "PatientVitals | count")
count = result["Tables"][0]["Rows"][0][0]
print(f"✅ PatientVitals table has {count} rows")
```

4. **Run Cell 1**, then **Run Cell 2**
5. You should see:
   ```
   ✅ Table 'PatientVitals' created (or already exists)
   ✅ Inserted 5 sample rows into PatientVitals
   ✅ PatientVitals table has 5 rows
   ```

> **Note:** This uses the Kusto REST API with `mssparkutils.credentials.getToken()` for authentication — no extra packages to install. The `.create-merge` command is safe to run multiple times. The sample data ensures dashboard tiles render without errors. Once the simulator runs, real data will flow in alongside these seed rows.

---

## Part C: Build the Real-Time Dashboard

The `PatientVitals` table now exists in the KQL database with sample data, so dashboard KQL queries will work immediately. Once we start the simulator in Part D, you'll watch the dashboard come alive with real-time data.

### Step 9: Create a Real-Time Dashboard

1. Go to your workspace
2. Click **+ New item** → **Real-Time Dashboard**
3. Name: `ICU Command Center Dashboard`
4. Click **Create**
5. After the dashboard opens, you will see a prompt: **"The Workspace contains a single KQL database, would you like to use it as a data source?"** — click **Yes**
   - This automatically connects the dashboard to the `PatientVitalsEventhouse` KQL database
   - If you don't see this prompt, you can add the data source manually in Step 10 when adding your first tile

### Step 10: Add Dashboard Tiles

> ⚠️ **Note:** If your table has a different name than `PatientVitals` (as shown by `.show tables`), replace `PatientVitals` with your actual table name in each query below.

> 💡 **Tip:** For each tile, after entering the query, click **Apply changes** first. The visual formatting options (visual type, title, conditional formatting) only become available after you apply the query. Apply changes → then configure the visual settings.

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
| summarize avg_spo2 = round(avg(spo2_percent), 1) by bin(todatetime(timestamp), 30s), facility_name
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

### Step 11: Set Continuous Auto-Refresh

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

> **Note:** The tiles will show empty or zero results until you start the simulator in Part D. This is expected.

---

## Part D: Simulate Real-Time Vitals Data

Now we'll run the simulator, which pushes live patient vital signs to the Eventstream. Once data starts flowing, switch back to your dashboard tab and **watch it come alive** with real-time data.

### Step 12: Create the Simulator Notebook

1. Go to your workspace
2. Click **+ New item** → **Notebook**
3. Rename to: `05 - RealTime Vitals Simulator`

> ⚠️ **Session Note:** If your Spark session expires or is stopped at any point, you will need to re-run all cells from the top using **Run all**. Fabric does not preserve variables, imports, or DataFrames across session restarts.

### Step 13: Configure the Simulator

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

### Step 14: Run the Simulator

1. First, replace the `CONNECTION_STR` value in Cell 1 with the connection string you copied from the Eventstream (see Step 7)
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

4. **Let the simulator run for at least 2–3 batches** so the `PatientVitals` table gets created and populated in the Eventhouse

### Step 15: Verify Data is Flowing

While the simulator is running, verify data has arrived in the Eventhouse:

1. **Open a new browser tab** (keep the simulator notebook running)
2. Go to your workspace → click on `PatientVitalsEventhouse` → open the KQL database
3. Click **Explore data** or open a new KQL queryset
4. Run this query to confirm the table exists:

```kql
.show tables
```

5. Then verify data is present:

```kql
PatientVitals
| count
```

> ⚠️ If the table name shown by `.show tables` is different from `PatientVitals`, use that name in all subsequent queries and dashboard tiles.

> #### 🛠️ Troubleshooting: Simulator runs but `.show tables` returns nothing
>
> If the simulator is sending batches successfully but no table appears in the Eventhouse, the issue is in the **Eventstream pipeline** — data is reaching the Eventstream but not being delivered to the Eventhouse. Check the following:
>
> 1. **Verify the destination table was created in Step 5:**
>    - Go to your workspace → open `PatientVitalsStream`
>    - Click the destination node (`PatientVitalsDB`) and confirm the table is set to `PatientVitals`. If it's empty, delete the destination and re-add it following Step 5.
>
> 2. **Verify the Eventstream was published after adding the destination:**
>    - Check that the Eventstream was **published** (Step 7). If the destination was added after publishing, click **Publish** again.
>
> 3. **Check for errors on the Eventstream canvas:**
>    - Look at the destination node. If it shows a **red icon** or **error indicator**, click on it for error details.
>
> 4. **Verify the source-to-destination connection:**
>    - Make sure there is a **line/arrow** connecting `VitalsSimulator` to `PatientVitalsDB`. If not connected, drag from the source output to the destination input, then **Publish** again.
>
> 5. **Wait and retry:**
>    - After publishing, it can take **1–2 minutes** for the Eventstream to begin delivering data. Run `.show tables` again after waiting.

### Step 16: Watch the Real-Time Dashboard Update

Now switch back to your **ICU Command Center Dashboard** tab:

1. You should see:
   - **SIRS Alert Count** tile showing 2–3 active alerts
   - **Patient Vitals Grid** populating with 20 patients, SIRS alert patients in red at the top
   - **Heart Rate Trend** chart drawing a live line as new data arrives for RT-P001, RT-P002, RT-P003
   - **SpO2 Monitor** showing oxygen saturation trends by facility
   - **Department Alert Summary** showing which departments have the most alerts
2. **Keep watching** — every 30 seconds the dashboard will refresh and you'll see the charts extend with new data points
3. Notice how the **sepsis-risk patients** (RT-P001, RT-P002, RT-P003) consistently show elevated vitals, while stable patients remain in normal ranges

> **This is the power of real-time analytics:** In a real clinical setting, a nurse or charge nurse would have this dashboard on a monitor at the nursing station, instantly seeing which patients are deteriorating without waiting for a batch report.

> **Tip:** Let the simulator run for at least 3-5 minutes while you explore the dashboard, then continue with the KQL queries in the next section (in another tab).

---

## Part E: KQL Queries for Clinical Analysis

### Step 17: Open the KQL Database

1. Open a **new browser tab** (keep the simulator running and the dashboard open)
2. Go to your workspace
3. Click on `PatientVitalsEventhouse`
4. Click on the KQL database
5. Click **Explore data** or open a new KQL queryset

### Step 18: Write Clinical KQL Queries

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
| summarize alert_count = count() by bin(todatetime(timestamp), 1m), facility_name
| order by timestamp asc
| render timechart
```

---

## Step 19: Stop the Simulator

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
