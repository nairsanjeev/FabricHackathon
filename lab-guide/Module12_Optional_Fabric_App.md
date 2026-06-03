# Module 12 (Optional): Build a Fabric App — Clinical Quality Command Center

| Duration | 60 minutes |
|----------|------------|
| Objective | Use VS Code Agent Mode (vibe coding) to build and deploy a full-stack web application on Microsoft Fabric using the Rayfin CLI — a Clinical Quality Command Center for hospital executives |
| Fabric Features | Fabric Apps (Preview), Rayfin CLI, GitHub Copilot Agent Mode, TypeScript, GraphQL API, Fabric SSO |

---

## Why a Fabric App?

In the previous modules, you built:
- A **Semantic Model** with DAX measures for readmission rates, ALOS, and financial KPIs
- A **Data Agent** that answers natural language questions about patient outcomes

But hospital executives and quality directors need **a dedicated, always-on application** — not just a dashboard or a chatbot. They need:

- A **Quality Command Center** that shows real-time facility scorecards
- A **Care Gap Tracker** where quality teams log and follow up on identified risks
- **Action items** assigned to specific team members with due dates and status

A Fabric App combines your existing Lakehouse data with a purpose-built application layer — complete with its own database, APIs, authentication, and a polished UI — all running within the Fabric governance framework.

---

## What You Will Build

A **Clinical Quality Command Center** application with:

1. **Facility Scorecards** — Real-time quality metrics for each hospital
2. **Care Gap Tracker** — A CRUD system for logging care gaps with priority, assignee, and status
3. **Quality Alerts** — Threshold-based alerts when metrics exceed targets

**How you'll build it:** Using **vibe coding** with VS Code and GitHub Copilot Agent Mode. Instead of copying and pasting code, you'll describe what you want in natural language and let Copilot generate the implementation.

---

## Prerequisites

- Completed Modules 1–5 (Lakehouse with Gold tables populated)
- **VS Code** with **GitHub Copilot** extension installed
- Node.js 18+ installed
- Docker Desktop running (for local Rayfin backend)
- Fabric Apps workload enabled in your tenant (see Step 1)

---

## Part A: Enable and Create the Fabric App

### Step 1: Verify Fabric Apps Is Enabled

Fabric Apps (Preview) must be enabled by a tenant administrator.

1. Have your **Fabric admin** go to the [Admin Portal](https://app.fabric.microsoft.com/admin-portal)
2. Navigate to **Tenant settings**
3. Under **Fabric Apps (preview)**, toggle to **Enabled**
4. Choose whether to enable for the entire organization or specific security groups
5. Click **Apply**

> **Note:** Changes may take a few minutes to propagate.

### Step 2: Create the Fabric App Item

1. Go to your **HLS-FabricHack** workspace
2. Click **+ New item**
3. Search for **App** in the item type list
4. Name: `HealthFirst-QualityCenter`
5. Click **Create**

After creation, the portal shows your app with a CLI command to download it locally.

### Step 3: Scaffold the Project Locally

Open a terminal and run:

```bash
npm create @microsoft/rayfin@latest -- "HealthFirst-QualityCenter" --template todoapp --workspace "HLS-FabricHack"
```

Then:

```bash
cd HealthFirst-QualityCenter
npm install
```

### Step 4: Open in VS Code

```bash
code .
```

You now have a scaffolded Fabric App project. Take a moment to explore the structure:

```
HealthFirst-QualityCenter/
├── rayfin/
│   ├── data/
│   │   ├── schema.ts        ← Entity registry
│   │   └── Todo.ts          ← Template entity (we'll replace)
│   ├── rayfin.yml           ← Backend configuration
│   └── .env                 ← Environment variables
├── src/                     ← React frontend
├── package.json
└── tsconfig.json
```

---

## Part B: Vibe Code the Data Model

Now the fun begins. Open **GitHub Copilot Chat** in VS Code (`Ctrl+Shift+I` or click the Copilot icon) and switch to **Agent Mode** (select "Agent" from the mode dropdown at the top of the chat panel).

### Step 5: Generate the CareGap Entity

Type the following prompt into Copilot Agent Mode:

> **Prompt:**
> ```
> I'm building a Fabric App using Rayfin. Delete the Todo.ts file in rayfin/data/ 
> and create a new entity file rayfin/data/CareGap.ts for tracking clinical care gaps 
> in a hospital network.
>
> Use @entity() decorator from @microsoft/rayfin-core. The entity should have:
> - id (uuid)
> - title (text, required, max 200 chars)
> - description (text, max 1000 chars)
> - patient_id (text, required)
> - facility (text, required) — one of our 3 hospitals
> - priority (set: Critical, High, Medium, Low)
> - status (set: Open, In Progress, Resolved, Escalated)
> - assigned_to (text, optional, max 100)
> - diagnosis (text, optional, max 100)
> - resolution_notes (text, optional, max 500)
> - created_at (date, required)
> - resolved_at (date, optional)
> - due_date (date, optional)
> - user_id (text, required — for row-level access)
> ```

**Review** what Copilot generates. It should create a TypeScript class with Rayfin decorators. Accept the changes.

### Step 6: Generate the QualityAlert Entity

> **Prompt:**
> ```
> Create another Rayfin entity file rayfin/data/QualityAlert.ts for tracking 
> quality metric alerts. When a hospital metric exceeds a threshold, an alert is created.
>
> Fields:
> - id (uuid)
> - metric_name (text, required, max 200)
> - facility (text, required)
> - current_value (decimal)
> - threshold_value (decimal)
> - metric_type (set: Readmission Rate, ALOS, Denial Rate, ED Volume, Mortality)
> - alert_status (set: Active, Acknowledged, Resolved)
> - notes (text, optional, max 500)
> - triggered_at (date, required)
> - acknowledged_at (date, optional)
> - user_id (text, required)
> ```

### Step 7: Update the Schema Registry

> **Prompt:**
> ```
> Update rayfin/data/schema.ts to register my CareGap and QualityAlert entities 
> instead of the Todo entity. Export a type called QualityCenterSchema.
> ```

---

## Part C: Vibe Code the Backend Configuration

### Step 8: Configure rayfin.yml

> **Prompt:**
> ```
> Update rayfin/rayfin.yml to configure this as a healthcare quality center app:
> - id: healthfirst-qualitycenter
> - name: HealthFirst-QualityCenter
> - Enable auth with both password (for local dev) and fabric SSO (for production)
> - Set token expiry to 60 minutes, refresh token to 30 days
> - Allow redirects from http://localhost:5173 and http://localhost:5173/auth/callback
> - Enable data service with mssql dialect
> - Enable static hosting from the dist folder with "npm run build" as build command
> ```

---

## Part D: Vibe Code the Frontend

### Step 9: Install the Rayfin Client

Run in the terminal:

```bash
npm install @microsoft/rayfin-client
```

### Step 10: Generate the Rayfin Client Module

> **Prompt:**
> ```
> Create src/lib/rayfin.ts that initializes a RayfinClient from @microsoft/rayfin-client.
> Import the CareGap and QualityAlert types from the rayfin/data folder.
> Use VITE_RAYFIN_API_URL env var (default http://localhost:5168) and VITE_RAYFIN_PUBLISHABLE_KEY.
> ```

### Step 11: Build the Quality Command Center UI

This is where vibe coding really shines. Give Copilot a rich, descriptive prompt:

> **Prompt:**
> ```
> Replace src/App.tsx with a Clinical Quality Command Center application for 
> HealthFirst Medical Group (a 3-hospital network: Metro General Hospital, 
> Community Medical Center, Riverside Health Center).
>
> The app should have:
> 
> 1. A professional header with blue gradient background, title "HealthFirst 
>    Clinical Quality Command Center" and subtitle about real-time monitoring.
>
> 2. Three tabbed views:
>
>    TAB 1 - Facility Scorecards:
>    - Card for each facility showing: 30-day readmission rate, avg length of stay,
>      ED volume (30d), and denial rate
>    - Color-coded metrics: green if within target, yellow for warning, red for danger
>    - Targets: readmission ≤15%, ALOS ≤4.5 days, denial rate ≤12%
>    - Show trend indicator (improving/worsening/stable) on each card
>    - Use hardcoded sample data matching our Lakehouse results:
>      Metro General: 20% readmit, 4.8 ALOS, 145 ED, 12.3% denial
>      Community Medical: 24.7% readmit, 5.2 ALOS, 112 ED, 15.1% denial
>      Riverside: 16.2% readmit, 3.9 ALOS, 89 ED, 9.8% denial
>
>    TAB 2 - Care Gap Tracker:
>    - Table showing all care gaps from the database (use the Rayfin client)
>    - Columns: Priority (color badge), Title, Facility, Diagnosis, Status, 
>      Assigned To, Created date
>    - "+ Log Care Gap" button that opens an inline form with fields for all 
>      required CareGap entity fields
>    - Form has dropdowns for priority and facility
>    - Show count of open gaps in the tab label
>
>    TAB 3 - Quality Alerts:
>    - Card list of alerts from the database
>    - Each card shows metric_name, facility, current vs threshold value
>    - Active alerts have red left border, acknowledged have gray
>    - "Acknowledge" button on active alerts that updates status
>    - Show count of active alerts in tab label
>    - If no alerts, show green success message
>
> 3. Use inline styles with Segoe UI font, Microsoft Fluent-inspired design 
>    (not Material, not Tailwind). Light background #f4f6f8, white cards with 
>    subtle shadows.
>
> 4. Load data on mount using the Rayfin client from src/lib/rayfin.ts.
>    Use client.data.CareGap.select([...]).orderBy({created_at:'desc'}).execute()
>    and similar for QualityAlert.
>
> Make it look impressive — this is a demo for hospital executives.
> ```

**Review** the generated code. Copilot should produce a complete React component with all three views. You may need to iterate:

> **Follow-up prompts if needed:**
> - "The tab badges should show counts of active items"
> - "Add a MetricCard subcomponent that takes label, value, status, and target"  
> - "The create form should call client.data.CareGap.create() and refresh the list"

### Step 12: Iterate and Refine (Optional)

This is the beauty of vibe coding — you can keep refining:

> **Example iteration prompts:**
> - "Add a dark mode toggle in the header"
> - "Make the facility scorecard data come from an API call instead of hardcoded values"
> - "Add a chart showing readmission rate trend over time"
> - "Add status change dropdown on each care gap row so users can update status inline"
> - "Add export to CSV button on the care gap table"

Each prompt generates more features. Accept what works, reject what doesn't, and keep iterating until you're happy with the result.

---

## Part E: Run Locally and Test

### Step 13: Start the Local Development Stack

```bash
npm run dev
```

This starts:
- The Rayfin backend (database + GraphQL API) via Docker
- The Vite development server for the frontend

Open `http://localhost:5173` in your browser.

### Step 14: Sign Up for Local Testing

Since we're running locally, use email/password authentication:

1. Click **Sign Up** (or navigate to the sign-up form)
2. Enter any email (e.g., `admin@healthfirst.local`) and a password
3. You're now authenticated and can use the app

### Step 15: Test the Application

1. **Facility Scorecards** — Verify the three facility cards display with color-coded metrics
2. **Care Gap Tracker** — Click **+ Log Care Gap** and create a test entry:
   - Title: "CHF patient discharged without follow-up appointment"
   - Priority: Critical
   - Patient ID: P-001
   - Facility: Metro General Hospital
   - Diagnosis: Heart Failure
   - Description: "Patient with 3 prior admissions for CHF exacerbation discharged without scheduled cardiology follow-up within 7 days"
3. Verify the care gap appears in the table

### Step 16: Seed Quality Alerts with Copilot

Use Copilot to generate a seed script:

> **Prompt:**
> ```
> Create a seed-alerts.ts script in the project root that:
> 1. Signs in with email/password (admin@healthfirst.local)
> 2. Creates 3 sample QualityAlert records using the Rayfin client:
>    - "CHF 30-Day Readmission Rate Exceeded" at Community Medical Center 
>      (current: 24.7%, threshold: 15%)
>    - "ALOS Above National Benchmark" at Metro General Hospital 
>      (current: 4.8, threshold: 4.5)
>    - "Claims Denial Rate Spike" at Community Medical Center 
>      (current: 15.1%, threshold: 12%)
> All should be Active status.
> ```

Run it:
```bash
npx tsx seed-alerts.ts
```

Refresh the app — you should see three active quality alerts with red indicators.

---

## Part F: Fix Issues with Copilot

If anything doesn't work (compile errors, runtime issues, UI glitches), use Copilot to fix it:

> **Example fix prompts:**
> - "I'm getting a TypeScript error on line 42 — the types don't match the Rayfin client response"
> - "The care gap form isn't submitting — debug the handleCreate function"
> - "The alerts aren't loading — check if the select fields match the QualityAlert entity"

This is the real workflow of vibe coding: **prompt → generate → test → fix → iterate**.

---

## Part G: Deploy to Fabric

### Step 17: Deploy the Application

When you're satisfied with local testing, deploy to Fabric:

```bash
npx rayfin up
```

The CLI will:
1. Authenticate you with Microsoft Entra ID (browser sign-in prompt)
2. Create/update the Fabric App item in your workspace
3. Apply the database schema (CareGap and QualityAlert tables)
4. Build and upload the React frontend
5. Print the live URL

### Step 18: Access the Deployed App

After deployment, the CLI outputs:
- **App URL** — The public URL where your app is hosted (requires Fabric SSO)
- **Fabric portal link** — Manage the app from the Fabric portal

1. Open the **App URL** in your browser
2. Sign in with your Microsoft Entra ID credentials (Fabric SSO)
3. The app loads with the same UI, now backed by a Fabric SQL Database

### Step 19: Verify in the Fabric Portal

1. Go to your **HLS-FabricHack** workspace
2. Find the **HealthFirst-QualityCenter** app item
3. Click it to see:
   - **App Backend URL** — The GraphQL endpoint
   - **App URL** — The hosted frontend
   - **Child items:** SQL Database (view your CareGap and QualityAlert tables)

---

## Part H: Share and Extend

### Step 20: Share the App

1. In the Fabric portal, select your app
2. Click **Share** or manage permissions
3. Grant **Run and interact** permission to colleagues who should use the app
4. They can now open the App URL and sign in with their Fabric SSO credentials

### Step 21: Keep Vibe Coding — Extension Ideas

Now that the app is deployed, try adding more features using Copilot prompts:

| Prompt Idea | What It Adds |
|-------------|--------------|
| "Add a fourth tab called Insights that shows a natural language Q&A interface where users type questions and the app calls our Data Agent API" | AI-powered analytics in the app |
| "Add a dashboard summary at the top of the Scorecards tab showing total open care gaps, active alerts, and network-wide readmission rate as KPI cards" | Executive summary view |
| "Add email notification setup — when a Critical care gap is created, show a toast notification" | Action workflow |
| "Add a patient timeline view that shows all care gaps for a specific patient ID in chronological order" | Patient-centric view |
| "Add role-based access — only users with 'quality_admin' claim can acknowledge alerts" | RBAC with Rayfin permissions |

---

## 💡 Discussion: Vibe Coding + Fabric Apps

**What just happened:**
- You built a full-stack, production-ready application in ~60 minutes
- You wrote **zero code by hand** — every line was generated by Copilot
- The app has authentication, a database, APIs, and a hosted frontend
- It runs on Microsoft Fabric with enterprise governance

**Why this matters for healthcare IT:**

| Traditional Development | Vibe Coding + Fabric Apps |
|------------------------|---------------------------|
| 3-6 month dev cycle | 60-minute prototype → production |
| Dedicated dev team needed | Clinical informaticist + Copilot |
| Separate infra, auth, DB setup | All bundled in Fabric |
| Compliance review for each component | Built on compliant Fabric platform |

**Discussion Questions:**
1. What other clinical workflows could be built this way? (Discharge planning? Care coordination?)
2. How does the speed of vibe coding change the ROI calculation for custom apps?
3. What governance guardrails should exist when AI generates production code?
4. How could this app connect to your Data Agent to provide both structured views AND natural language queries?

---

## ✅ Module 12 Checklist

Confirm you have completed:

- [ ] Fabric Apps enabled in tenant settings
- [ ] `HealthFirst-QualityCenter` app created in workspace
- [ ] Data models generated via Copilot (CareGap, QualityAlert)
- [ ] Backend configured via Copilot (rayfin.yml)
- [ ] Frontend generated via Copilot (Scorecards, Care Gaps, Alerts)
- [ ] App runs locally with test data
- [ ] App deployed to Fabric with `rayfin up`
- [ ] App accessible via Fabric SSO
- [ ] At least one extension feature added via vibe coding

---

## 📚 Reference Links

- [Fabric Apps Overview](https://learn.microsoft.com/en-us/fabric/apps/overview)
- [Create Your First Fabric App](https://learn.microsoft.com/en-us/fabric/apps/create-app)
- [Data Models & Decorators](https://learn.microsoft.com/en-us/fabric/apps/data-models)
- [Read & Write Data with GraphQL](https://learn.microsoft.com/en-us/fabric/apps/read-write-data-graphql)
- [Deploy to Fabric](https://learn.microsoft.com/en-us/fabric/apps/deploy-app)
- [Configure Authentication](https://learn.microsoft.com/en-us/fabric/apps/authentication)
- [Project Structure](https://learn.microsoft.com/en-us/fabric/apps/project-structure)
- [GitHub Copilot Agent Mode](https://code.visualstudio.com/docs/copilot/chat/chat-agent-mode)
