# Module 13 (Optional): Fabric App — Readmission Risk Tracker (Rayfin + React)

| Duration | 45 minutes |
|----------|------------|
| Objective | Build and deploy a production-ready Fabric App using the Rayfin CLI with ready-to-use React code — a Readmission Risk Tracker that connects to the data you built in earlier modules |
| Fabric Features | Fabric Apps (Preview), Rayfin CLI, React, Fabric SQL Database, Fabric SSO |

---

## Why This Module?

In earlier modules you built a complete data pipeline — from raw CSVs to Gold-layer analytics tables — and surfaced them through Power BI dashboards and Data Agents. But hospital quality teams need more than read-only dashboards:

- **CMS penalizes** hospitals up to 3% of Medicare payments for excess 30-day readmissions (Module 0)
- Quality teams need to **track individual high-risk patients** and assign follow-up actions
- Discharge coordinators need a **shared worklist** — not just charts

This module delivers a **Readmission Risk Tracker** — a lightweight Fabric App where quality teams can:

1. View facility-level readmission KPIs (sourced from your Gold tables)
2. Log and track high-risk patient cases that need follow-up
3. Mark cases as resolved and record resolution notes

The app code is **ready to copy and deploy** — no vibe coding required.

---

## What You Will Build

A **Readmission Risk Tracker** with three sections:

| Section | Purpose | Data Source |
|---------|---------|-------------|
| **Facility Scorecards** | Readmission rate, ALOS, ED volume per hospital | Hardcoded from Gold table results |
| **Risk Case Tracker** | CRUD worklist for high-risk patients | Rayfin SQL Database (live) |
| **About** | Context on CMS HRRP program and app purpose | Static content |

---

## Prerequisites

- Completed Modules 1–3 (Lakehouse with Gold tables populated)
- **Node.js 18+** installed ([download](https://nodejs.org))
- **VS Code** with a terminal
- Fabric Apps workload enabled in your tenant (see Step 1)

---

## Part A: Enable Fabric Apps and Scaffold the Project

### Step 1: Verify Fabric Apps Is Enabled

Fabric Apps (Preview) must be enabled by a tenant administrator.

1. Have your **Fabric admin** go to the [Admin Portal](https://app.fabric.microsoft.com/admin-portal)
2. Navigate to **Tenant settings**
3. Under **Fabric Apps (preview)**, toggle to **Enabled**
4. Click **Apply**

> **Note:** Changes may take a few minutes to propagate.

### Step 2: Scaffold the Project

Open a terminal and run:

```bash
npm create @microsoft/rayfin@latest -- "ReadmissionTracker" --workspace "HLS-FabricHack"
```

> Replace `"HLS-FabricHack"` with your actual workspace name.

Then navigate into the project:

```bash
cd ReadmissionTracker
npm install
```

### Step 3: Open in VS Code

```bash
code .
```

You should see the scaffolded structure:

```
ReadmissionTracker/
├── rayfin/
│   ├── data/
│   │   ├── schema.ts
│   │   └── Todo.ts         ← We'll replace this
│   ├── rayfin.yml
│   └── .env
├── src/                    ← React frontend
├── package.json
└── tsconfig.json
```

---

## Part B: Set Up the Data Model

We need one entity: **RiskCase** — a record for each high-risk patient that needs follow-up.

### Step 4: Delete the Template Entity

Delete the file `rayfin/data/Todo.ts` (the default template entity).

### Step 5: Create the RiskCase Entity

Create a new file `rayfin/data/RiskCase.ts` with the following content:

```typescript
import { entity, field, type } from "@anthropic/rayfin-core";

@entity()
export class RiskCase {
  @field({ type: type.uuid() })
  id!: string;

  @field({ type: type.text({ maxLength: 200 }), required: true })
  patient_name!: string;

  @field({ type: type.text({ maxLength: 50 }), required: true })
  patient_id!: string;

  @field({ type: type.text({ maxLength: 100 }), required: true })
  facility!: string;

  @field({ type: type.text({ maxLength: 100 }) })
  primary_diagnosis!: string;

  @field({
    type: type.set(["Critical", "High", "Medium", "Low"]),
    required: true,
  })
  priority!: string;

  @field({
    type: type.set([
      "Open",
      "In Progress",
      "Scheduled Follow-Up",
      "Resolved",
    ]),
    required: true,
  })
  status!: string;

  @field({ type: type.text({ maxLength: 100 }) })
  assigned_to!: string;

  @field({ type: type.text({ maxLength: 500 }) })
  notes!: string;

  @field({ type: type.text({ maxLength: 500 }) })
  resolution_notes!: string;

  @field({ type: type.date(), required: true })
  created_at!: Date;

  @field({ type: type.date() })
  resolved_at!: Date;

  @field({ type: type.date() })
  due_date!: Date;

  @field({ type: type.integer() })
  prior_admissions_12m!: number;

  @field({ type: type.text({ maxLength: 100 }), required: true })
  user_id!: string;
}
```

> **Note:** The exact import path for `@entity`, `@field`, and `type` depends on your Rayfin CLI version. If `@anthropic/rayfin-core` doesn't resolve, check the `Todo.ts` template file that was scaffolded — use the same import path it used (e.g., `@microsoft/rayfin-core`).

### Step 6: Update the Schema Registry

Replace the contents of `rayfin/data/schema.ts` with:

```typescript
import { RiskCase } from "./RiskCase";

export type AppSchema = {
  RiskCase: RiskCase;
};

export { RiskCase };
```

> **Important:** Match the pattern from the original scaffolded `schema.ts`. If it uses a `createSchema()` function or different export style, adapt accordingly — the key is to register `RiskCase` and remove `Todo`.

---

## Part C: Configure the Backend

### Step 7: Update rayfin.yml

Open `rayfin/rayfin.yml` and update the `name` and `id` fields to match your app:

```yaml
id: readmission-tracker
name: ReadmissionTracker
```

Keep the rest of the generated configuration (auth, data service, static hosting) as-is — the scaffold defaults work for both local development and Fabric deployment.

---

## Part D: Build the Frontend

### Step 8: Install the Rayfin Client

```bash
npm install @microsoft/rayfin-client
```

### Step 9: Create the Rayfin Client Module

Create the file `src/lib/rayfin.ts`:

```typescript
import { createClient } from "@microsoft/rayfin-client";
import type { AppSchema } from "../../rayfin/data/schema";

const apiUrl =
  import.meta.env.VITE_RAYFIN_API_URL ?? "http://localhost:5168";
const publishableKey =
  import.meta.env.VITE_RAYFIN_PUBLISHABLE_KEY ?? "";

export const client = createClient<AppSchema>({
  apiUrl,
  publishableKey,
});
```

> **Note:** Adjust the import path for `createClient` if your Rayfin version uses a different package name. Check the scaffolded `package.json` for the correct client package.

### Step 10: Replace the App Component

Copy the **entire contents** of the file `fabric-app/App.tsx` from this repository into `src/App.tsx`, replacing whatever the template generated.

> The source file is provided at: [`fabric-app/App.tsx`](../fabric-app/App.tsx)

### Step 11: Replace the App Styles

Copy the **entire contents** of the file `fabric-app/App.css` from this repository into `src/App.css`.

> The source file is provided at: [`fabric-app/App.css`](../fabric-app/App.css)

---

## Part E: Run Locally and Test

### Step 12: Start the Development Server

```bash
npm run dev
```

This starts:
- The Rayfin backend (deployed to Fabric or running locally via Docker, depending on your CLI version)
- The Vite development server for the React frontend

Open the URL shown in the terminal (typically `http://localhost:5173`).

### Step 13: Sign In

If prompted, sign in with your Microsoft Entra ID credentials (for Fabric-backed dev) or create a local account (for Docker-backed dev).

### Step 14: Test the Application

1. **Facility Scorecards tab** — Verify three hospital cards with color-coded metrics:
   - Metro General Hospital: 20.0% readmission rate (red)
   - Community Medical Center: 24.7% readmission rate (red)
   - Riverside Health Center: 16.2% readmission rate (yellow)

2. **Risk Case Tracker tab** — Click **+ New Case** and create a test entry:
   - Patient Name: John Smith
   - Patient ID: P-001
   - Facility: Metro General Hospital
   - Primary Diagnosis: Heart Failure
   - Priority: Critical
   - Notes: "3 prior admissions in 12 months, no follow-up scheduled"

3. Verify the case appears in the table and can be edited

---

## Part F: Deploy to Fabric

### Step 15: Sign In to Fabric CLI

```bash
npx rayfin login
```

A browser window opens for Microsoft Entra ID authentication.

### Step 16: Deploy

```bash
npx rayfin up
```

The CLI will:
1. Create/update the Fabric App item in your workspace
2. Apply the database schema (RiskCase table)
3. Build and upload the React frontend
4. Print the live URL

### Step 17: Verify Deployment

```bash
npx rayfin up status
```

After a successful deploy, the CLI prints:
- **App URL** — The hosted frontend (Fabric SSO protected)
- **Fabric portal link** — Manage the app from the Fabric portal

### Step 18: Access the Live App

1. Open the **App URL** in your browser
2. Sign in with your Microsoft Entra ID credentials
3. The app loads with Fabric SSO — same identity as your Power BI dashboards

### Step 19: Verify in the Fabric Portal

1. Go to your **HLS-FabricHack** workspace
2. Find the **ReadmissionTracker** app item
3. Click it to see:
   - **App URL** — The hosted frontend
   - **Child items** — SQL Database with your `RiskCase` table

---

## What You Built — Connecting the Dots

| Lab Pain Point | How This App Addresses It |
|----------------|--------------------------|
| **CMS Readmission Penalties** (Module 0) | Facility scorecards show at-a-glance which hospitals exceed the 15% readmission threshold |
| **Gold Analytics** (Module 2) | Scorecard KPIs are derived from `gold_readmissions`, `gold_alos`, and `gold_ed_utilization` results |
| **Workforce Efficiency** (Module 0) | Case tracker replaces spreadsheets — quality teams have a shared, always-on worklist |
| **Complex Patients** (Module 0) | Each risk case tracks prior admissions and diagnosis, enabling targeted interventions |
| **Financial Pressure** (Module 0) | Preventing one readmission saves ~$13,000 in avoidable costs |

---

## Extending the App (Optional Ideas)

- Connect the Facility Scorecards to **live Lakehouse data** via the Fabric SQL endpoint instead of hardcoded values
- Add a **GraphQL query** to pull `gold_readmissions` aggregate data into the scorecards
- Integrate the **Data Agent** (Module 5) for natural language queries within the app
- Add **email notifications** when a Critical case is past its due date
- Build a **trend chart** showing readmission rates over time using a charting library

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `npm create` fails | Ensure Node.js 18+ is installed: `node --version` |
| Sign-in fails or 401/403 | Run `npx rayfin login` again, then retry `npx rayfin up` |
| Entity import path doesn't resolve | Check the scaffolded `Todo.ts` for the correct import path and match it in `RiskCase.ts` |
| Schema changes fail on deploy | Use `npx rayfin up db apply --force` for destructive schema changes |
| Frontend doesn't load | Check that `VITE_RAYFIN_API_URL` in `rayfin/.env` points to the correct backend |
| Deployment takes too long | Run `npx rayfin up --dry-run` first to preview what will be deployed |

---

## Summary

In 45 minutes, you went from a scaffolded template to a **deployed Fabric App** that:

- Shows **readmission KPIs** color-coded against CMS thresholds
- Provides a **case management worklist** for quality teams
- Runs on **Fabric infrastructure** with SSO, SQL Database, and governance
- Addresses the **#1 financial pain point** in US healthcare — preventable readmissions

This is the bridge between analytics and action — turning the insights from your Gold tables and Power BI dashboards into a tool that quality teams use every day.
