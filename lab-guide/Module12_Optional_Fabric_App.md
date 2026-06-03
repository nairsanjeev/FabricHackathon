# Module 12 (Optional): Build a Fabric App — Clinical Quality Command Center

| Duration | 60 minutes |
|----------|------------|
| Objective | Build and deploy a full-stack web application on Microsoft Fabric using the Rayfin CLI that provides a Clinical Quality Command Center for hospital executives |
| Fabric Features | Fabric Apps (Preview), Rayfin CLI, TypeScript data models, GraphQL API, Fabric SSO, Static Hosting |

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

1. **Facility Scorecards** — Real-time quality metrics pulled from your Gold tables
2. **Care Gap Tracker** — A system for logging care gaps (e.g., "CHF patient discharged without follow-up scheduled") with priority, assignee, and status
3. **Quality Alerts** — Configurable thresholds that flag when metrics exceed targets (e.g., readmission rate > 15%)

The app uses:
- **Rayfin data models** (TypeScript decorators) for the Care Gap and Alert entities
- **GraphQL API** (auto-generated) for CRUD operations
- **React frontend** with a rich dashboard UI
- **Fabric SSO** for production authentication

---

## Prerequisites

- Completed Modules 1–5 (Lakehouse with Gold tables populated)
- Node.js 18+ installed on your machine
- A code editor (VS Code recommended)
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

Then navigate to the project:

```bash
cd HealthFirst-QualityCenter
```

Install dependencies:

```bash
npm install
```

You now have a scaffolded Fabric App project. The key structure is:

```
HealthFirst-QualityCenter/
├── rayfin/
│   ├── data/
│   │   ├── schema.ts        ← Register all entities here
│   │   └── Todo.ts          ← Template entity (we'll replace)
│   ├── rayfin.yml           ← Backend configuration
│   └── .env                 ← Environment variables
├── src/                     ← React frontend
├── package.json
└── tsconfig.json
```

---

## Part B: Define the Data Model

We'll replace the template Todo entity with healthcare-specific entities for our Quality Command Center.

### Step 4: Create the CareGap Entity

Create a new file `rayfin/data/CareGap.ts`:

```typescript
import { entity, uuid, text, date, set, boolean } from '@microsoft/rayfin-core';

@entity()
export class CareGap {
  @uuid() id!: string;

  @text({ min: 1, max: 200 }) title!: string;

  @text({ max: 1000 }) description!: string;

  @text() patient_id!: string;

  @text() facility!: string;

  @set('Critical', 'High', 'Medium', 'Low') priority!: 'Critical' | 'High' | 'Medium' | 'Low';

  @set('Open', 'In Progress', 'Resolved', 'Escalated') status!: 'Open' | 'In Progress' | 'Resolved' | 'Escalated';

  @text({ optional: true, max: 100 }) assigned_to?: string;

  @text({ optional: true, max: 100 }) diagnosis?: string;

  @text({ optional: true, max: 500 }) resolution_notes?: string;

  @date() created_at!: Date;

  @date({ optional: true }) resolved_at?: Date;

  @date({ optional: true }) due_date?: Date;

  @text() user_id!: string;
}
```

### Step 5: Create the QualityAlert Entity

Create a new file `rayfin/data/QualityAlert.ts`:

```typescript
import { entity, uuid, text, date, set, decimal } from '@microsoft/rayfin-core';

@entity()
export class QualityAlert {
  @uuid() id!: string;

  @text({ min: 1, max: 200 }) metric_name!: string;

  @text() facility!: string;

  @decimal() current_value!: number;

  @decimal() threshold_value!: number;

  @set('Readmission Rate', 'ALOS', 'Denial Rate', 'ED Volume', 'Mortality')
  metric_type!: 'Readmission Rate' | 'ALOS' | 'Denial Rate' | 'ED Volume' | 'Mortality';

  @set('Active', 'Acknowledged', 'Resolved')
  alert_status!: 'Active' | 'Acknowledged' | 'Resolved';

  @text({ optional: true, max: 500 }) notes?: string;

  @date() triggered_at!: Date;

  @date({ optional: true }) acknowledged_at?: Date;

  @text() user_id!: string;
}
```

### Step 6: Register Entities in the Schema

Replace the contents of `rayfin/data/schema.ts` with:

```typescript
import type { CareGap } from './CareGap.js';
import type { QualityAlert } from './QualityAlert.js';

export type QualityCenterSchema = {
  CareGap: CareGap;
  QualityAlert: QualityAlert;
};
```

### Step 7: Delete the Template Entity

Remove the template file that came with the scaffold:

```bash
rm rayfin/data/Todo.ts
```

(On Windows: `del rayfin\data\Todo.ts`)

---

## Part C: Configure the Backend

### Step 8: Update rayfin.yml

Open `rayfin/rayfin.yml` and update it to:

```yaml
id: healthfirst-qualitycenter
name: HealthFirst-QualityCenter
version: 1.0.0
services:
  auth:
    enabled: true
    expiryInMinutes: 60
    refreshToken:
      lifetimeInDays: 30
    allowedRedirectUris:
      - http://localhost:5173
      - http://localhost:5173/auth/callback
    password:
      enabled: true
    fabric:
      enabled: true
  data:
    enabled: true
    dialect: mssql
  staticHosting:
    enabled: true
    root: .
    folder: dist
    buildCommand: npm run build
    indexDocument: index.html
```

Key points:
- **`password.enabled: true`** — allows local development with email/password
- **`fabric.enabled: true`** — enables Fabric SSO for production deployment
- **`staticHosting`** — hosts your React frontend alongside the backend

---

## Part D: Build the Frontend

### Step 9: Install Frontend Dependencies

```bash
npm install @microsoft/rayfin-client
```

### Step 10: Create the Rayfin Client

Create `src/lib/rayfin.ts`:

```typescript
import { RayfinClient } from '@microsoft/rayfin-client';
import type { CareGap } from '../../rayfin/data/CareGap';
import type { QualityAlert } from '../../rayfin/data/QualityAlert';

type AppSchema = {
  CareGap: CareGap;
  QualityAlert: QualityAlert;
};

export const client = new RayfinClient<AppSchema>({
  baseUrl: import.meta.env.VITE_RAYFIN_API_URL ?? 'http://localhost:5168',
  publishableKey: import.meta.env.VITE_RAYFIN_PUBLISHABLE_KEY ?? '',
});
```

### Step 11: Build the Quality Command Center UI

Replace `src/App.tsx` with the following application shell. This creates a tabbed interface with three views:

```tsx
import { useState, useEffect } from 'react';
import { client } from './lib/rayfin';

// Types from our data model
interface CareGap {
  id: string;
  title: string;
  description: string;
  patient_id: string;
  facility: string;
  priority: 'Critical' | 'High' | 'Medium' | 'Low';
  status: 'Open' | 'In Progress' | 'Resolved' | 'Escalated';
  assigned_to?: string;
  diagnosis?: string;
  due_date?: Date;
  created_at: Date;
}

interface QualityAlert {
  id: string;
  metric_name: string;
  facility: string;
  current_value: number;
  threshold_value: number;
  metric_type: string;
  alert_status: 'Active' | 'Acknowledged' | 'Resolved';
  triggered_at: Date;
}

type Tab = 'scorecards' | 'care-gaps' | 'alerts';

export default function App() {
  const [activeTab, setActiveTab] = useState<Tab>('scorecards');
  const [careGaps, setCareGaps] = useState<CareGap[]>([]);
  const [alerts, setAlerts] = useState<QualityAlert[]>([]);
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  useEffect(() => {
    // Check auth state and load data
    loadData();
  }, []);

  async function loadData() {
    try {
      const gaps = await client.data.CareGap.select([
        'id', 'title', 'description', 'patient_id', 'facility',
        'priority', 'status', 'assigned_to', 'diagnosis', 'due_date', 'created_at'
      ]).orderBy({ created_at: 'desc' }).execute();
      setCareGaps(gaps);

      const alertData = await client.data.QualityAlert.select([
        'id', 'metric_name', 'facility', 'current_value',
        'threshold_value', 'metric_type', 'alert_status', 'triggered_at'
      ]).orderBy({ triggered_at: 'desc' }).execute();
      setAlerts(alertData);

      setIsAuthenticated(true);
    } catch (error) {
      console.error('Failed to load data:', error);
    }
  }

  return (
    <div style={{ fontFamily: 'Segoe UI, sans-serif', minHeight: '100vh', background: '#f4f6f8' }}>
      {/* Header */}
      <header style={{
        background: 'linear-gradient(135deg, #0078D4 0%, #004578 100%)',
        color: 'white', padding: '20px 32px',
        boxShadow: '0 2px 8px rgba(0,0,0,0.15)'
      }}>
        <h1 style={{ margin: 0, fontSize: '24px', fontWeight: 600 }}>
          🏥 HealthFirst Clinical Quality Command Center
        </h1>
        <p style={{ margin: '4px 0 0', opacity: 0.85, fontSize: '14px' }}>
          Real-time quality monitoring across Metro General, Community Medical Center, and Riverside Health
        </p>
      </header>

      {/* Navigation Tabs */}
      <nav style={{ background: 'white', borderBottom: '1px solid #e1e5e8', padding: '0 32px' }}>
        {(['scorecards', 'care-gaps', 'alerts'] as Tab[]).map(tab => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            style={{
              padding: '14px 20px', border: 'none', background: 'none',
              cursor: 'pointer', fontSize: '14px', fontWeight: 500,
              color: activeTab === tab ? '#0078D4' : '#605E5C',
              borderBottom: activeTab === tab ? '3px solid #0078D4' : '3px solid transparent',
            }}
          >
            {tab === 'scorecards' && '📊 Facility Scorecards'}
            {tab === 'care-gaps' && `🔍 Care Gaps (${careGaps.filter(g => g.status !== 'Resolved').length})`}
            {tab === 'alerts' && `⚠️ Quality Alerts (${alerts.filter(a => a.alert_status === 'Active').length})`}
          </button>
        ))}
      </nav>

      {/* Content */}
      <main style={{ padding: '24px 32px', maxWidth: '1400px', margin: '0 auto' }}>
        {activeTab === 'scorecards' && <FacilityScoreCards />}
        {activeTab === 'care-gaps' && <CareGapTracker gaps={careGaps} onRefresh={loadData} />}
        {activeTab === 'alerts' && <AlertPanel alerts={alerts} onRefresh={loadData} />}
      </main>
    </div>
  );
}

// ─── Facility Scorecards ──────────────────────────────────────────
function FacilityScoreCards() {
  const facilities = [
    {
      name: 'Metro General Hospital',
      readmissionRate: 20.0, alos: 4.8, edVolume: 145,
      denialRate: 12.3, trend: 'improving'
    },
    {
      name: 'Community Medical Center',
      readmissionRate: 24.7, alos: 5.2, edVolume: 112,
      denialRate: 15.1, trend: 'worsening'
    },
    {
      name: 'Riverside Health Center',
      readmissionRate: 16.2, alos: 3.9, edVolume: 89,
      denialRate: 9.8, trend: 'stable'
    },
  ];

  return (
    <div>
      <h2 style={{ fontSize: '20px', marginBottom: '16px', color: '#323130' }}>
        Facility Performance Scorecards
      </h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(380px, 1fr))', gap: '20px' }}>
        {facilities.map(f => (
          <div key={f.name} style={{
            background: 'white', borderRadius: '8px', padding: '24px',
            boxShadow: '0 2px 6px rgba(0,0,0,0.08)', border: '1px solid #e8eaed'
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h3 style={{ fontSize: '16px', margin: 0, color: '#323130' }}>{f.name}</h3>
              <span style={{
                fontSize: '12px', padding: '2px 8px', borderRadius: '12px',
                background: f.trend === 'improving' ? '#DFF6DD' : f.trend === 'worsening' ? '#FDE7E9' : '#F3F2F1',
                color: f.trend === 'improving' ? '#107C10' : f.trend === 'worsening' ? '#D13438' : '#605E5C',
              }}>
                {f.trend === 'improving' ? '↗ Improving' : f.trend === 'worsening' ? '↘ Worsening' : '→ Stable'}
              </span>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px', marginTop: '20px' }}>
              <MetricCard
                label="30-Day Readmission"
                value={`${f.readmissionRate}%`}
                status={f.readmissionRate > 15 ? 'danger' : 'good'}
                target="≤ 15%"
              />
              <MetricCard
                label="Avg Length of Stay"
                value={`${f.alos} days`}
                status={f.alos > 4.5 ? 'warning' : 'good'}
                target="≤ 4.5 days"
              />
              <MetricCard
                label="ED Volume (30d)"
                value={`${f.edVolume}`}
                status="neutral"
                target=""
              />
              <MetricCard
                label="Denial Rate"
                value={`${f.denialRate}%`}
                status={f.denialRate > 12 ? 'warning' : 'good'}
                target="≤ 12%"
              />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function MetricCard({ label, value, status, target }: {
  label: string; value: string; status: 'good' | 'warning' | 'danger' | 'neutral'; target: string;
}) {
  const colors = {
    good: { bg: '#DFF6DD', text: '#107C10' },
    warning: { bg: '#FFF4CE', text: '#797600' },
    danger: { bg: '#FDE7E9', text: '#D13438' },
    neutral: { bg: '#F3F2F1', text: '#323130' },
  };
  return (
    <div style={{ background: colors[status].bg, borderRadius: '6px', padding: '12px' }}>
      <div style={{ fontSize: '12px', color: '#605E5C', marginBottom: '4px' }}>{label}</div>
      <div style={{ fontSize: '22px', fontWeight: 700, color: colors[status].text }}>{value}</div>
      {target && <div style={{ fontSize: '11px', color: '#605E5C', marginTop: '2px' }}>Target: {target}</div>}
    </div>
  );
}

// ─── Care Gap Tracker ──────────────────────────────────────────────
function CareGapTracker({ gaps, onRefresh }: { gaps: CareGap[]; onRefresh: () => void }) {
  const [showForm, setShowForm] = useState(false);

  async function handleCreate(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget;
    const formData = new FormData(form);

    await client.data.CareGap.create({
      title: formData.get('title') as string,
      description: formData.get('description') as string,
      patient_id: formData.get('patient_id') as string,
      facility: formData.get('facility') as string,
      priority: formData.get('priority') as CareGap['priority'],
      status: 'Open',
      assigned_to: formData.get('assigned_to') as string || undefined,
      diagnosis: formData.get('diagnosis') as string || undefined,
      created_at: new Date(),
      user_id: 'current-user',
    });

    setShowForm(false);
    onRefresh();
  }

  const priorityColors: Record<string, string> = {
    Critical: '#D13438', High: '#CA5010', Medium: '#8764B8', Low: '#0078D4'
  };

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
        <h2 style={{ fontSize: '20px', color: '#323130', margin: 0 }}>Care Gap Tracker</h2>
        <button
          onClick={() => setShowForm(!showForm)}
          style={{
            background: '#0078D4', color: 'white', border: 'none', borderRadius: '4px',
            padding: '8px 16px', cursor: 'pointer', fontWeight: 500
          }}
        >
          + Log Care Gap
        </button>
      </div>

      {showForm && (
        <form onSubmit={handleCreate} style={{
          background: 'white', padding: '20px', borderRadius: '8px',
          marginBottom: '16px', boxShadow: '0 2px 6px rgba(0,0,0,0.08)'
        }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
            <input name="title" placeholder="Care gap title" required style={inputStyle} />
            <select name="priority" required style={inputStyle}>
              <option value="Critical">Critical</option>
              <option value="High">High</option>
              <option value="Medium" selected>Medium</option>
              <option value="Low">Low</option>
            </select>
            <input name="patient_id" placeholder="Patient ID" required style={inputStyle} />
            <select name="facility" required style={inputStyle}>
              <option value="Metro General Hospital">Metro General Hospital</option>
              <option value="Community Medical Center">Community Medical Center</option>
              <option value="Riverside Health Center">Riverside Health Center</option>
            </select>
            <input name="diagnosis" placeholder="Diagnosis (optional)" style={inputStyle} />
            <input name="assigned_to" placeholder="Assigned to (optional)" style={inputStyle} />
            <textarea name="description" placeholder="Description" required
              style={{ ...inputStyle, gridColumn: '1 / -1' }} rows={3} />
          </div>
          <div style={{ marginTop: '12px', display: 'flex', gap: '8px' }}>
            <button type="submit" style={{
              background: '#0078D4', color: 'white', border: 'none',
              borderRadius: '4px', padding: '8px 20px', cursor: 'pointer'
            }}>Save</button>
            <button type="button" onClick={() => setShowForm(false)} style={{
              background: '#F3F2F1', color: '#323130', border: 'none',
              borderRadius: '4px', padding: '8px 20px', cursor: 'pointer'
            }}>Cancel</button>
          </div>
        </form>
      )}

      {/* Care Gap Table */}
      <div style={{ background: 'white', borderRadius: '8px', overflow: 'hidden', boxShadow: '0 2px 6px rgba(0,0,0,0.08)' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px' }}>
          <thead>
            <tr style={{ background: '#F3F2F1' }}>
              <th style={thStyle}>Priority</th>
              <th style={thStyle}>Title</th>
              <th style={thStyle}>Facility</th>
              <th style={thStyle}>Diagnosis</th>
              <th style={thStyle}>Status</th>
              <th style={thStyle}>Assigned To</th>
              <th style={thStyle}>Created</th>
            </tr>
          </thead>
          <tbody>
            {gaps.map(gap => (
              <tr key={gap.id} style={{ borderBottom: '1px solid #EDEBE9' }}>
                <td style={tdStyle}>
                  <span style={{
                    background: priorityColors[gap.priority] + '20',
                    color: priorityColors[gap.priority],
                    padding: '2px 8px', borderRadius: '12px', fontSize: '12px', fontWeight: 600
                  }}>{gap.priority}</span>
                </td>
                <td style={tdStyle}>{gap.title}</td>
                <td style={tdStyle}>{gap.facility}</td>
                <td style={tdStyle}>{gap.diagnosis || '—'}</td>
                <td style={tdStyle}>{gap.status}</td>
                <td style={tdStyle}>{gap.assigned_to || 'Unassigned'}</td>
                <td style={tdStyle}>{new Date(gap.created_at).toLocaleDateString()}</td>
              </tr>
            ))}
            {gaps.length === 0 && (
              <tr><td colSpan={7} style={{ ...tdStyle, textAlign: 'center', color: '#605E5C' }}>
                No care gaps logged yet. Click "+ Log Care Gap" to get started.
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Quality Alerts Panel ──────────────────────────────────────────
function AlertPanel({ alerts, onRefresh }: { alerts: QualityAlert[]; onRefresh: () => void }) {
  async function acknowledgeAlert(id: string) {
    await client.data.QualityAlert.update(
      { id },
      { alert_status: 'Acknowledged', acknowledged_at: new Date() }
    );
    onRefresh();
  }

  return (
    <div>
      <h2 style={{ fontSize: '20px', color: '#323130', marginBottom: '16px' }}>Quality Alerts</h2>
      <div style={{ display: 'grid', gap: '12px' }}>
        {alerts.map(alert => (
          <div key={alert.id} style={{
            background: 'white', borderRadius: '8px', padding: '16px 20px',
            boxShadow: '0 2px 6px rgba(0,0,0,0.08)',
            borderLeft: `4px solid ${alert.alert_status === 'Active' ? '#D13438' : '#A19F9D'}`,
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div>
                <div style={{ fontWeight: 600, fontSize: '15px', color: '#323130' }}>
                  {alert.metric_name}
                </div>
                <div style={{ fontSize: '13px', color: '#605E5C', marginTop: '4px' }}>
                  {alert.facility} • {alert.metric_type}
                </div>
                <div style={{ fontSize: '14px', marginTop: '8px' }}>
                  Current: <strong style={{ color: '#D13438' }}>{alert.current_value}%</strong>
                  {' '} | Threshold: {alert.threshold_value}%
                </div>
              </div>
              {alert.alert_status === 'Active' && (
                <button onClick={() => acknowledgeAlert(alert.id)} style={{
                  background: '#FFF4CE', color: '#797600', border: '1px solid #797600',
                  borderRadius: '4px', padding: '6px 12px', cursor: 'pointer', fontSize: '12px'
                }}>Acknowledge</button>
              )}
            </div>
          </div>
        ))}
        {alerts.length === 0 && (
          <div style={{
            background: '#DFF6DD', borderRadius: '8px', padding: '24px',
            textAlign: 'center', color: '#107C10'
          }}>
            ✅ No active quality alerts. All metrics within acceptable thresholds.
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Shared Styles ─────────────────────────────────────────────────
const inputStyle: React.CSSProperties = {
  padding: '8px 12px', borderRadius: '4px', border: '1px solid #C8C6C4',
  fontSize: '14px', fontFamily: 'Segoe UI, sans-serif'
};
const thStyle: React.CSSProperties = {
  textAlign: 'left', padding: '10px 12px', fontWeight: 600, fontSize: '12px', color: '#605E5C'
};
const tdStyle: React.CSSProperties = {
  padding: '10px 12px'
};
```

> **Tip:** This is a single-file app for simplicity in the lab. In production, you would split components into separate files under `src/components/`.

---

## Part E: Run Locally and Test

### Step 12: Start the Local Development Stack

```bash
npm run dev
```

This starts:
- The Rayfin backend (database + GraphQL API) via Docker
- The Vite development server for the frontend

Open `http://localhost:5173` in your browser.

### Step 13: Sign Up for Local Testing

Since we're running locally, use email/password authentication:

1. Click **Sign Up** (or navigate to the sign-up form)
2. Enter any email (e.g., `admin@healthfirst.local`) and a password
3. You're now authenticated and can use the app

### Step 14: Test the Application

1. **Facility Scorecards** — Verify the three facility cards display with color-coded metrics
2. **Care Gap Tracker** — Click **+ Log Care Gap** and create a test entry:
   - Title: "CHF patient discharged without follow-up appointment"
   - Priority: Critical
   - Patient ID: P-001
   - Facility: Metro General Hospital
   - Diagnosis: Heart Failure
   - Description: "Patient with 3 prior admissions for CHF exacerbation discharged without scheduled cardiology follow-up within 7 days"
3. Verify the care gap appears in the table
4. **Quality Alerts** — (Alerts are empty initially — we'll seed data in the next step)

### Step 15: Seed Sample Quality Alerts

Open a second terminal and run a quick script to create sample alerts. Create a file `seed-alerts.ts` in the project root:

```typescript
// seed-alerts.ts — Run with: npx tsx seed-alerts.ts
import { client } from './src/lib/rayfin';

async function seed() {
  // Sign in first (local dev)
  await client.auth.signIn({ email: 'admin@healthfirst.local', password: 'your-password' });

  const alerts = [
    {
      metric_name: 'CHF 30-Day Readmission Rate Exceeded',
      facility: 'Community Medical Center',
      current_value: 24.7,
      threshold_value: 15.0,
      metric_type: 'Readmission Rate' as const,
      alert_status: 'Active' as const,
      triggered_at: new Date(),
      user_id: 'system',
    },
    {
      metric_name: 'ALOS Above National Benchmark',
      facility: 'Metro General Hospital',
      current_value: 4.8,
      threshold_value: 4.5,
      metric_type: 'ALOS' as const,
      alert_status: 'Active' as const,
      triggered_at: new Date(Date.now() - 86400000), // yesterday
      user_id: 'system',
    },
    {
      metric_name: 'Claims Denial Rate Spike',
      facility: 'Community Medical Center',
      current_value: 15.1,
      threshold_value: 12.0,
      metric_type: 'Denial Rate' as const,
      alert_status: 'Active' as const,
      triggered_at: new Date(Date.now() - 172800000), // 2 days ago
      user_id: 'system',
    },
  ];

  for (const alert of alerts) {
    await client.data.QualityAlert.create(alert);
    console.log(`Created alert: ${alert.metric_name}`);
  }
}

seed().catch(console.error);
```

Run it:
```bash
npx tsx seed-alerts.ts
```

Refresh the app — you should now see three active quality alerts with red indicators.

---

## Part F: Deploy to Fabric

### Step 16: Deploy the Application

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

### Step 17: Access the Deployed App

After deployment, the CLI outputs:
- **App URL** — The public URL where your app is hosted (requires Fabric SSO)
- **Fabric portal link** — Manage the app from the Fabric portal

1. Open the **App URL** in your browser
2. Sign in with your Microsoft Entra ID credentials (Fabric SSO)
3. The app loads with the same UI, now backed by a Fabric SQL Database

### Step 18: Verify in the Fabric Portal

1. Go to your **HLS-FabricHack** workspace
2. Find the **HealthFirst-QualityCenter** app item
3. Click it to see:
   - **App Backend URL** — The GraphQL endpoint
   - **App URL** — The hosted frontend
   - **Child items:** SQL Database (view your CareGap and QualityAlert tables)

---

## Part G: Share and Discuss

### Step 19: Share the App

1. In the Fabric portal, select your app
2. Click **Share** or manage permissions
3. Grant **Run and interact** permission to colleagues who should use the app
4. They can now open the App URL and sign in with their Fabric SSO credentials

### Step 20: Discussion — The Value of a Fabric App

**Why this matters for healthcare:**

| Traditional Approach | Fabric App Approach |
|---------------------|---------------------|
| Power BI report emailed weekly | Live app updated in real-time |
| Care gaps tracked in spreadsheets | Structured database with audit trail |
| Quality alerts via email — easily missed | Centralized command center with acknowledgment workflow |
| Separate systems for data + actions | Single platform: data → insight → action |

**Real-World Extensions:**
- Connect the Care Gap entity to your Lakehouse data using a scheduled pipeline that auto-creates gaps when metrics exceed thresholds
- Add push notifications when Critical alerts fire
- Embed Power BI visuals from your semantic model directly in the app
- Integrate with the Data Agent API so users can ask natural language questions within the command center

---

## ✅ Module 12 Checklist

Confirm you have completed:

- [ ] Fabric Apps enabled in tenant settings
- [ ] `HealthFirst-QualityCenter` app created in workspace
- [ ] Data models defined (CareGap, QualityAlert)
- [ ] Backend configured with auth + static hosting
- [ ] Frontend built with Facility Scorecards, Care Gap Tracker, and Alert Panel
- [ ] App runs locally with test data
- [ ] App deployed to Fabric with `rayfin up`
- [ ] App accessible via Fabric SSO

---

## 📚 Reference Links

- [Fabric Apps Overview](https://learn.microsoft.com/en-us/fabric/apps/overview)
- [Create Your First Fabric App](https://learn.microsoft.com/en-us/fabric/apps/create-app)
- [Data Models & Decorators](https://learn.microsoft.com/en-us/fabric/apps/data-models)
- [Read & Write Data with GraphQL](https://learn.microsoft.com/en-us/fabric/apps/read-write-data-graphql)
- [Deploy to Fabric](https://learn.microsoft.com/en-us/fabric/apps/deploy-app)
- [Configure Authentication](https://learn.microsoft.com/en-us/fabric/apps/authentication)
- [Project Structure](https://learn.microsoft.com/en-us/fabric/apps/project-structure)
