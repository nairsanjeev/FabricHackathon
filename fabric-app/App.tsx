import { useState, useEffect, useCallback } from "react";
import { client } from "./lib/rayfin";
import "./App.css";

/* ── Types ── */
interface RiskCase {
  id: string;
  patient_name: string;
  patient_id: string;
  facility: string;
  primary_diagnosis: string;
  priority: string;
  status: string;
  assigned_to: string;
  notes: string;
  resolution_notes: string;
  created_at: string;
  resolved_at: string | null;
  due_date: string | null;
  prior_admissions_12m: number;
  user_id: string;
}

interface FacilityData {
  name: string;
  readmissionRate: number;
  alos: number;
  edVolume30d: number;
  denialRate: number;
  trend: "improving" | "stable" | "worsening";
}

/* ── Facility KPI data (from Gold tables) ── */
const FACILITIES: FacilityData[] = [
  {
    name: "Metro General Hospital",
    readmissionRate: 20.0,
    alos: 4.8,
    edVolume30d: 145,
    denialRate: 12.3,
    trend: "stable",
  },
  {
    name: "Community Medical Center",
    readmissionRate: 24.7,
    alos: 5.2,
    edVolume30d: 112,
    denialRate: 15.1,
    trend: "worsening",
  },
  {
    name: "Riverside Health Center",
    readmissionRate: 16.2,
    alos: 3.9,
    edVolume30d: 89,
    denialRate: 9.8,
    trend: "improving",
  },
];

const THRESHOLDS = {
  readmissionRate: { green: 12, yellow: 15 },
  alos: { green: 4, yellow: 6 },
  denialRate: { green: 10, yellow: 15 },
};

const PRIORITIES = ["Critical", "High", "Medium", "Low"] as const;
const STATUSES = [
  "Open",
  "In Progress",
  "Scheduled Follow-Up",
  "Resolved",
] as const;
const FACILITY_NAMES = FACILITIES.map((f) => f.name);

/* ── Helpers ── */
function getStatus(value: number, thresholds: { green: number; yellow: number }) {
  if (value <= thresholds.green) return "green";
  if (value <= thresholds.yellow) return "yellow";
  return "red";
}

function trendIcon(trend: FacilityData["trend"]) {
  if (trend === "improving") return "▼ Improving";
  if (trend === "worsening") return "▲ Worsening";
  return "● Stable";
}

function priorityClass(p: string) {
  return `priority-badge priority-${p.toLowerCase()}`;
}

function statusClass(s: string) {
  const key = s.toLowerCase().replace(/\s+/g, "-");
  return `status-badge status-${key}`;
}

function toDateInput(d: string | null | undefined): string {
  if (!d) return "";
  return d.substring(0, 10);
}

/* ── Components ── */

function MetricCard({
  label,
  value,
  unit,
  status,
  target,
}: {
  label: string;
  value: string;
  unit: string;
  status: string;
  target: string;
}) {
  return (
    <div className={`metric-card metric-${status}`}>
      <div className="metric-label">{label}</div>
      <div className="metric-value">
        {value}
        <span className="metric-unit">{unit}</span>
      </div>
      <div className="metric-target">Target: {target}</div>
    </div>
  );
}

function FacilityScorecard({ facility }: { facility: FacilityData }) {
  return (
    <div className="facility-card">
      <div className="facility-header">
        <h3>{facility.name}</h3>
        <span className={`trend trend-${facility.trend}`}>
          {trendIcon(facility.trend)}
        </span>
      </div>
      <div className="metrics-grid">
        <MetricCard
          label="30-Day Readmission Rate"
          value={facility.readmissionRate.toFixed(1)}
          unit="%"
          status={getStatus(facility.readmissionRate, THRESHOLDS.readmissionRate)}
          target="≤ 15%"
        />
        <MetricCard
          label="Avg Length of Stay"
          value={facility.alos.toFixed(1)}
          unit=" days"
          status={getStatus(facility.alos, THRESHOLDS.alos)}
          target="≤ 4 days"
        />
        <MetricCard
          label="ED Volume (30d)"
          value={facility.edVolume30d.toString()}
          unit=" visits"
          status="neutral"
          target="—"
        />
        <MetricCard
          label="Denial Rate"
          value={facility.denialRate.toFixed(1)}
          unit="%"
          status={getStatus(facility.denialRate, THRESHOLDS.denialRate)}
          target="≤ 10%"
        />
      </div>
    </div>
  );
}

/* ── Empty form state ── */
const EMPTY_FORM = {
  patient_name: "",
  patient_id: "",
  facility: FACILITY_NAMES[0],
  primary_diagnosis: "",
  priority: "Medium" as string,
  status: "Open" as string,
  assigned_to: "",
  notes: "",
  due_date: "",
  prior_admissions_12m: 0,
};

/* ── Main App ── */
export default function App() {
  const [tab, setTab] = useState<"scorecards" | "tracker" | "about">(
    "scorecards"
  );
  const [cases, setCases] = useState<RiskCase[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ ...EMPTY_FORM });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const openCount = cases.filter(
    (c) => c.status !== "Resolved"
  ).length;

  /* ── Load cases ── */
  const loadCases = useCallback(async () => {
    try {
      setLoading(true);
      const result = await (client.data as any).RiskCase.select([
        "id",
        "patient_name",
        "patient_id",
        "facility",
        "primary_diagnosis",
        "priority",
        "status",
        "assigned_to",
        "notes",
        "resolution_notes",
        "created_at",
        "resolved_at",
        "due_date",
        "prior_admissions_12m",
        "user_id",
      ])
        .orderBy({ created_at: "desc" })
        .execute();
      setCases(result.data ?? []);
      setError(null);
    } catch (err: any) {
      console.error("Failed to load cases:", err);
      setError("Could not load cases. Make sure the backend is running.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadCases();
  }, [loadCases]);

  /* ── Create case ── */
  const handleCreate = async () => {
    try {
      await (client.data as any).RiskCase.create({
        patient_name: form.patient_name,
        patient_id: form.patient_id,
        facility: form.facility,
        primary_diagnosis: form.primary_diagnosis,
        priority: form.priority,
        status: form.status,
        assigned_to: form.assigned_to,
        notes: form.notes,
        due_date: form.due_date || undefined,
        prior_admissions_12m: form.prior_admissions_12m,
        created_at: new Date().toISOString(),
        user_id: "current-user",
      });
      setForm({ ...EMPTY_FORM });
      setShowForm(false);
      loadCases();
    } catch (err: any) {
      console.error("Create failed:", err);
      setError("Failed to create case: " + (err.message ?? "Unknown error"));
    }
  };

  /* ── Update status ── */
  const handleStatusChange = async (id: string, newStatus: string) => {
    try {
      const updateData: Record<string, any> = { status: newStatus };
      if (newStatus === "Resolved") {
        updateData.resolved_at = new Date().toISOString();
      }
      await (client.data as any).RiskCase.update(id, updateData);
      loadCases();
    } catch (err: any) {
      console.error("Update failed:", err);
    }
  };

  return (
    <div className="app">
      {/* Header */}
      <header className="app-header">
        <div className="header-content">
          <h1>Readmission Risk Tracker</h1>
          <p>HealthFirst Medical Group — Quality Improvement Platform</p>
        </div>
      </header>

      {/* Tabs */}
      <nav className="tab-bar">
        <button
          className={`tab-btn ${tab === "scorecards" ? "active" : ""}`}
          onClick={() => setTab("scorecards")}
        >
          Facility Scorecards
        </button>
        <button
          className={`tab-btn ${tab === "tracker" ? "active" : ""}`}
          onClick={() => setTab("tracker")}
        >
          Risk Case Tracker
          {openCount > 0 && <span className="tab-badge">{openCount}</span>}
        </button>
        <button
          className={`tab-btn ${tab === "about" ? "active" : ""}`}
          onClick={() => setTab("about")}
        >
          About
        </button>
      </nav>

      {/* Error banner */}
      {error && (
        <div className="error-banner">
          {error}
          <button onClick={() => setError(null)}>✕</button>
        </div>
      )}

      <main className="main-content">
        {/* ── Tab 1: Scorecards ── */}
        {tab === "scorecards" && (
          <section>
            <h2>Facility Quality Scorecards</h2>
            <p className="section-subtitle">
              Metrics derived from Gold-layer analytics tables.
              Red = exceeds CMS threshold. Yellow = approaching. Green = within target.
            </p>
            <div className="scorecards-container">
              {FACILITIES.map((f) => (
                <FacilityScorecard key={f.name} facility={f} />
              ))}
            </div>
          </section>
        )}

        {/* ── Tab 2: Risk Case Tracker ── */}
        {tab === "tracker" && (
          <section>
            <div className="tracker-header">
              <div>
                <h2>Risk Case Tracker</h2>
                <p className="section-subtitle">
                  Track high-risk patients to prevent readmissions.
                  {openCount > 0 && ` ${openCount} open case${openCount > 1 ? "s" : ""}.`}
                </p>
              </div>
              <button
                className="btn-primary"
                onClick={() => setShowForm(!showForm)}
              >
                {showForm ? "Cancel" : "+ New Case"}
              </button>
            </div>

            {/* Create form */}
            {showForm && (
              <div className="create-form">
                <h3>Log New Risk Case</h3>
                <div className="form-grid">
                  <label>
                    Patient Name *
                    <input
                      value={form.patient_name}
                      onChange={(e) =>
                        setForm({ ...form, patient_name: e.target.value })
                      }
                      placeholder="e.g. Jane Doe"
                    />
                  </label>
                  <label>
                    Patient ID *
                    <input
                      value={form.patient_id}
                      onChange={(e) =>
                        setForm({ ...form, patient_id: e.target.value })
                      }
                      placeholder="e.g. P-001"
                    />
                  </label>
                  <label>
                    Facility *
                    <select
                      value={form.facility}
                      onChange={(e) =>
                        setForm({ ...form, facility: e.target.value })
                      }
                    >
                      {FACILITY_NAMES.map((n) => (
                        <option key={n}>{n}</option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Primary Diagnosis
                    <input
                      value={form.primary_diagnosis}
                      onChange={(e) =>
                        setForm({ ...form, primary_diagnosis: e.target.value })
                      }
                      placeholder="e.g. Heart Failure"
                    />
                  </label>
                  <label>
                    Priority *
                    <select
                      value={form.priority}
                      onChange={(e) =>
                        setForm({ ...form, priority: e.target.value })
                      }
                    >
                      {PRIORITIES.map((p) => (
                        <option key={p}>{p}</option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Assigned To
                    <input
                      value={form.assigned_to}
                      onChange={(e) =>
                        setForm({ ...form, assigned_to: e.target.value })
                      }
                      placeholder="e.g. Dr. Martinez"
                    />
                  </label>
                  <label>
                    Due Date
                    <input
                      type="date"
                      value={form.due_date}
                      onChange={(e) =>
                        setForm({ ...form, due_date: e.target.value })
                      }
                    />
                  </label>
                  <label>
                    Prior Admissions (12 mo)
                    <input
                      type="number"
                      min={0}
                      value={form.prior_admissions_12m}
                      onChange={(e) =>
                        setForm({
                          ...form,
                          prior_admissions_12m: parseInt(e.target.value) || 0,
                        })
                      }
                    />
                  </label>
                  <label className="span-2">
                    Notes
                    <textarea
                      value={form.notes}
                      onChange={(e) =>
                        setForm({ ...form, notes: e.target.value })
                      }
                      placeholder="Clinical context, risk factors, follow-up plan..."
                      rows={3}
                    />
                  </label>
                </div>
                <button
                  className="btn-primary"
                  onClick={handleCreate}
                  disabled={!form.patient_name || !form.patient_id}
                >
                  Save Case
                </button>
              </div>
            )}

            {/* Cases table */}
            {loading ? (
              <p className="loading">Loading cases...</p>
            ) : cases.length === 0 ? (
              <div className="empty-state">
                <p>No risk cases yet. Click <strong>+ New Case</strong> to log one.</p>
              </div>
            ) : (
              <div className="table-wrapper">
                <table className="cases-table">
                  <thead>
                    <tr>
                      <th>Priority</th>
                      <th>Patient</th>
                      <th>Facility</th>
                      <th>Diagnosis</th>
                      <th>Status</th>
                      <th>Assigned To</th>
                      <th>Due</th>
                      <th>Prior Admits</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cases.map((c) => (
                      <tr key={c.id} className={c.status === "Resolved" ? "resolved-row" : ""}>
                        <td>
                          <span className={priorityClass(c.priority)}>
                            {c.priority}
                          </span>
                        </td>
                        <td>
                          <strong>{c.patient_name}</strong>
                          <br />
                          <small className="muted">{c.patient_id}</small>
                        </td>
                        <td>{c.facility}</td>
                        <td>{c.primary_diagnosis || "—"}</td>
                        <td>
                          <select
                            className={statusClass(c.status)}
                            value={c.status}
                            onChange={(e) =>
                              handleStatusChange(c.id, e.target.value)
                            }
                          >
                            {STATUSES.map((s) => (
                              <option key={s}>{s}</option>
                            ))}
                          </select>
                        </td>
                        <td>{c.assigned_to || "—"}</td>
                        <td>{c.due_date ? toDateInput(c.due_date) : "—"}</td>
                        <td className="center">
                          {c.prior_admissions_12m ?? "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        )}

        {/* ── Tab 3: About ── */}
        {tab === "about" && (
          <section className="about-section">
            <h2>About This Application</h2>
            <div className="about-card">
              <h3>CMS Hospital Readmissions Reduction Program (HRRP)</h3>
              <p>
                The HRRP penalizes hospitals with excess 30-day readmission rates
                for six conditions: heart failure, heart attack, pneumonia, COPD,
                hip/knee replacement, and CABG surgery. Penalties can reach{" "}
                <strong>3% of all Medicare FFS base operating DRG payments</strong>.
              </p>
              <p>
                The national readmission benchmark is approximately <strong>15%</strong>.
                Each preventable readmission costs an average of <strong>$13,000</strong>.
              </p>
            </div>
            <div className="about-card">
              <h3>How This App Fits the Platform</h3>
              <ul>
                <li>
                  <strong>Data Source:</strong> Gold-layer analytics tables built
                  in Modules 1–2 (Medallion Architecture: Bronze → Silver → Gold)
                </li>
                <li>
                  <strong>Semantic Model:</strong> DAX measures for readmission
                  rate, ALOS, and denial rate (Module 3)
                </li>
                <li>
                  <strong>Real-Time Monitoring:</strong> Eventhouse + KQL for
                  vitals and sepsis detection (Module 4)
                </li>
                <li>
                  <strong>AI Agent:</strong> Natural language queries against
                  patient data (Module 5)
                </li>
                <li>
                  <strong>This App:</strong> Actionable worklist for quality teams
                  to track and resolve high-risk patients
                </li>
              </ul>
            </div>
            <div className="about-card">
              <h3>Technology Stack</h3>
              <ul>
                <li>
                  <strong>Frontend:</strong> React + TypeScript (Vite)
                </li>
                <li>
                  <strong>Backend:</strong> Rayfin (Fabric Apps) — SQL Database +
                  GraphQL API
                </li>
                <li>
                  <strong>Auth:</strong> Microsoft Entra ID (Fabric SSO)
                </li>
                <li>
                  <strong>Hosting:</strong> Microsoft Fabric workspace
                </li>
              </ul>
            </div>
          </section>
        )}
      </main>

      {/* Footer */}
      <footer className="app-footer">
        <p>
          HealthFirst Medical Group — Unified Patient Intelligence Platform •
          Built on Microsoft Fabric
        </p>
      </footer>
    </div>
  );
}
