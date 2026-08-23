import { useState, useEffect, useRef, useCallback } from 'react';
import './App.css';

// ── API configuration ─────────────────────────────────────────────
const API_BASE = import.meta.env.VITE_API_URL ?? (import.meta.env.DEV ? 'http://localhost:8000' : '');
const WS_BASE = API_BASE
  ? API_BASE.replace(/^http/, 'ws')
  : `ws://${window.location.host}`;

// ── Auth token storage ────────────────────────────────────────────
// localStorage rather than memory so a page reload does not log you out, and
// rather than a cookie because the API authenticates with a bearer header.
// The tradeoff is XSS reach: any script on this origin can read it.
const TOKEN_KEY = 'jobplatform.token';

const readToken = () => {
  try {
    return localStorage.getItem(TOKEN_KEY);
  } catch {
    // Private mode or blocked site data — run without persistence.
    return null;
  }
};

const writeToken = (token) => {
  try {
    if (token) localStorage.setItem(TOKEN_KEY, token);
    else localStorage.removeItem(TOKEN_KEY);
  } catch {
    // Non-fatal: the session still works, it just won't survive a reload.
  }
};

// ── Status badge colours ──────────────────────────────────────────
const STATUS_STYLES = {
  PENDING:    { bg: '#fef3c7', color: '#92400e', label: 'PENDING' },
  QUEUED:     { bg: '#e0e7ff', color: '#3730a3', label: 'QUEUED' },
  PROCESSING: { bg: '#dbeafe', color: '#1e40af', label: 'PROCESSING' },
  COMPLETED:  { bg: '#d1fae5', color: '#065f46', label: 'COMPLETED' },
  FAILED:     { bg: '#fee2e2', color: '#991b1b', label: 'FAILED' },
  // Neutral, not red: the user stopped this job, nothing broke.
  CANCELLED:  { bg: '#e5e7eb', color: '#4b5563', label: 'CANCELLED' },
};

// Statuses a job never moves out of. Mirrors app/state.py TERMINAL_STATUSES —
// a status missing from here leaves its WebSocket open forever and the card
// stuck showing progress.
const TERMINAL_STATUSES = new Set(['COMPLETED', 'FAILED', 'CANCELLED']);
const isTerminal = (status) => TERMINAL_STATUSES.has(status);

function StatusBadge({ status }) {
  const style = STATUS_STYLES[status] || { bg: '#f3f4f6', color: '#374151', label: status };
  return (
    <span className="status-badge" style={{ backgroundColor: style.bg, color: style.color }}>
      {style.label}
    </span>
  );
}

// ── WebSocket connection states ───────────────────────────────────
const WS_STATE = {
  IDLE:         'IDLE',
  CONNECTING:   'CONNECTING',
  CONNECTED:    'CONNECTED',
  DISCONNECTED: 'DISCONNECTED',
  ERROR:        'ERROR',
};

const MAX_RECONNECT_ATTEMPTS = 3;
const RECONNECT_DELAY_MS = 1500;

// WebSocket close codes the server uses to reject a connection. Both are
// permanent for this token, so reconnecting would just fail again in a loop.
const WS_CLOSE_UNAUTHORISED = 4401;
const WS_CLOSE_NOT_FOUND = 4404;

// ── Auth screen ───────────────────────────────────────────────────

function AuthGate({ onAuthenticated }) {
  const [mode, setMode] = useState('login');   // 'login' | 'register'
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState(null);
  const [busy, setBusy] = useState(false);

  const isRegister = mode === 'register';

  const submit = async (e) => {
    e.preventDefault();
    setBusy(true);
    setError(null);

    try {
      if (isRegister) {
        const res = await fetch(`${API_BASE}/api/v1/auth/register`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, password }),
        });
        if (!res.ok) {
          const data = await res.json().catch(() => ({}));
          throw new Error(detailToMessage(data.detail) || `Registration failed (${res.status})`);
        }
      }

      // Login uses form encoding, not JSON: the API implements the OAuth2
      // password flow, which specifies application/x-www-form-urlencoded and
      // names the identifier field "username".
      const body = new URLSearchParams({ username: email, password });
      const res = await fetch(`${API_BASE}/api/v1/auth/login`, { method: 'POST', body });

      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(detailToMessage(data.detail) || `Login failed (${res.status})`);
      }

      const { access_token } = await res.json();
      writeToken(access_token);
      onAuthenticated(access_token);
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>⚡ Job Processing Platform</h1>
        <p className="subtitle">Sign in to submit jobs and view your ingestion history</p>
      </header>

      <section className="submit-section">
        <div className="submit-card auth-card">
          <label htmlFor="auth-email">{isRegister ? 'Create an account' : 'Sign in'}</label>

          <form onSubmit={submit} className="auth-form">
            <input
              id="auth-email"
              type="email"
              className="auth-input"
              placeholder="you@example.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              autoComplete="username"
              required
            />
            <input
              id="auth-password"
              type="password"
              className="auth-input"
              placeholder="Password (8+ characters)"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete={isRegister ? 'new-password' : 'current-password'}
              minLength={8}
              required
            />

            <button type="submit" className="auth-submit" disabled={busy}>
              {busy ? 'Working…' : isRegister ? 'Register & sign in' : 'Sign in'}
            </button>
          </form>

          {error && (
            <div className="ws-error auth-error">
              <span className="ws-error-icon">⚠️</span>
              {error}
            </div>
          )}

          <button
            type="button"
            className="raw-toggle-btn auth-toggle"
            onClick={() => { setMode(isRegister ? 'login' : 'register'); setError(null); }}
          >
            {isRegister ? 'Already have an account? Sign in' : 'Need an account? Register'}
          </button>
        </div>
      </section>
    </div>
  );
}

// FastAPI returns `detail` as a string for HTTPException and as a list of error
// objects for a 422 validation failure. Rendering the raw list gives the user
// "[object Object]", so flatten it.
function detailToMessage(detail) {
  if (!detail) return null;
  if (typeof detail === 'string') return detail;
  if (Array.isArray(detail)) {
    return detail.map(d => d.msg || JSON.stringify(d)).join('; ');
  }
  return JSON.stringify(detail);
}

export default function App() {
  const [token, setToken] = useState(readToken);

  // Remounting on token change resets every piece of dashboard state at once —
  // jobs, sockets, expanded panels. Without the key, logging in as a second
  // user would leave the first user's job cards on screen.
  if (!token) {
    return <AuthGate onAuthenticated={setToken} />;
  }

  return (
    <Dashboard
      key={token}
      token={token}
      onSignOut={() => { writeToken(null); setToken(null); }}
    />
  );
}

function Dashboard({ token, onSignOut }) {
  const [selectedFile, setSelectedFile] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [jobs, setJobs] = useState([]);
  const [expandedTables, setExpandedTables] = useState({});
  const [expandedRaw, setExpandedRaw] = useState({});
  const [generatingReportFor, setGeneratingReportFor] = useState(null);
  const [authError, setAuthError] = useState(null);

  const wsRefs = useRef({});
  const terminalJobsRef = useRef(new Set());
  const fileInputRef = useRef(null);

  // ── Authenticated fetch ─────────────────────────────────────────
  // Every call to the API goes through this. A 401 means the token expired or
  // was revoked, and the only useful response is to sign out — otherwise an
  // expired session presents as a dashboard where nothing works and nothing
  // says why.
  const authFetch = useCallback(async (path, options = {}) => {
    const headers = new Headers(options.headers || {});
    headers.set('Authorization', `Bearer ${token}`);

    const res = await fetch(`${API_BASE}${path}`, { ...options, headers });

    if (res.status === 401) {
      setAuthError('Your session expired. Please sign in again.');
      onSignOut();
      throw new Error('Session expired');
    }

    return res;
  }, [token, onSignOut]);

  const toggleTable = (jobId) => {
    setExpandedTables(prev => ({ ...prev, [jobId]: !prev[jobId] }));
  };

  const toggleRaw = (jobId) => {
    setExpandedRaw(prev => ({ ...prev, [jobId]: !prev[jobId] }));
  };

  // ── Open WebSocket for a job ────────────────────────────────────
  const openWebSocket = useCallback((jobId, attempt = 0) => {
    if (terminalJobsRef.current.has(jobId)) return;

    const existing = wsRefs.current[jobId];
    if (existing?.ws) {
      existing.ws.onclose = null;
      existing.ws.onerror = null;
      existing.ws.onmessage = null;
      existing.ws.close();
    }
    if (existing?.timer) clearTimeout(existing.timer);

    setJobs(prev => prev.map(j => (j.id === jobId ? { ...j, wsState: WS_STATE.CONNECTING, wsError: null } : j)));

    // The token goes in the query string because a browser WebSocket cannot
    // send an Authorization header — the constructor takes a URL and
    // subprotocols, nothing else. See the module docstring in app/api/v1/ws.py.
    const ws = new WebSocket(`${WS_BASE}/ws/jobs/${jobId}?token=${encodeURIComponent(token)}`);
    wsRefs.current[jobId] = { ws, attempts: attempt, timer: null };

    ws.onopen = () => {
      setJobs(prev => prev.map(j => (j.id === jobId ? { ...j, wsState: WS_STATE.CONNECTED, wsError: null } : j)));
      if (wsRefs.current[jobId]) wsRefs.current[jobId].attempts = 0;
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);

        if (isTerminal(msg.status)) {
          terminalJobsRef.current.add(jobId);
        }

        setJobs(prev =>
          prev.map(j => {
            if (j.id !== jobId) return j;

            const terminal = isTerminal(msg.status);
            const updated = { ...j };

            if (msg.status && msg.status !== j.status) {
              updated.status = msg.status;
              updated.events = [
                ...j.events,
                { time: new Date().toLocaleTimeString(), status: msg.status },
              ];
            }

            const progressData = msg.result || (typeof msg.processed === 'number' ? msg : null);
            if (progressData && typeof progressData.processed === 'number') {
              const processed = progressData.processed;
              const total = progressData.total || (j.progress ? j.progress.total : 0);
              const percentage = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;
              updated.progress = {
                processed,
                total,
                percentage,
                totalAmount: progressData.total_amount || j.progress?.totalAmount,
              };
            }

            if (msg.result) updated.result = msg.result;
            if (terminal) {
              updated.wsState = WS_STATE.IDLE;
              updated.wsError = null;
            }
            if (msg.error) updated.wsError = msg.error;

            return updated;
          })
        );
      } catch (err) {
        console.error('Failed to parse WebSocket message:', err);
      }
    };

    ws.onclose = (event) => {
      const ref = wsRefs.current[jobId];
      if (!ref) return;

      // 4401 / 4404 are the server's rejections: bad token, or a job this user
      // does not own. Neither improves by retrying, so surface it and stop —
      // reconnecting would spend three attempts arriving at the same answer.
      if (event.code === WS_CLOSE_UNAUTHORISED || event.code === WS_CLOSE_NOT_FOUND) {
        const message = event.code === WS_CLOSE_UNAUTHORISED
          ? 'Live stream refused: session is no longer valid.'
          : 'Live stream refused: job not found.';
        setJobs(prev => prev.map(j => (
          j.id === jobId ? { ...j, wsState: WS_STATE.ERROR, wsError: message } : j
        )));
        delete wsRefs.current[jobId];
        return;
      }

      const isTerminal = terminalJobsRef.current.has(jobId) || event.code === 1000;

      if (isTerminal) {
        setJobs(prev => prev.map(j => (j.id === jobId ? { ...j, wsState: WS_STATE.IDLE, wsError: null } : j)));
        delete wsRefs.current[jobId];
        return;
      }

      const nextAttempt = (ref.attempts || 0) + 1;
      if (nextAttempt <= MAX_RECONNECT_ATTEMPTS) {
        setJobs(prev =>
          prev.map(j =>
            j.id === jobId
              ? {
                  ...j,
                  wsState: WS_STATE.DISCONNECTED,
                  wsError: `Reconnecting stream (${nextAttempt}/${MAX_RECONNECT_ATTEMPTS})…`,
                }
              : j
          )
        );

        const timer = setTimeout(() => {
          openWebSocket(jobId, nextAttempt);
        }, RECONNECT_DELAY_MS * nextAttempt);

        wsRefs.current[jobId] = { ...ref, timer, attempts: nextAttempt };
      } else {
        setJobs(prev =>
          prev.map(j =>
            j.id === jobId
              ? { ...j, wsState: WS_STATE.ERROR, wsError: 'Live stream disconnected.' }
              : j
          )
        );
        delete wsRefs.current[jobId];
      }
    };

    ws.onerror = () => {
      console.error(`WebSocket error for job ${jobId}`);
    };
    // token is a dependency: a reconnect must carry the *current* token, not the
    // one captured when this component first rendered.
  }, [token]);

  // ── Fetch existing jobs from backend ────────────────────────────
  const fetchJobs = useCallback(async () => {
    try {
      const res = await authFetch('/api/v1/jobs?limit=25');
      if (!res.ok) return;
      const data = await res.json();
      if (Array.isArray(data.items)) {
        setJobs(
          data.items.map(item => {
            if (isTerminal(item.status)) terminalJobsRef.current.add(item.id);

            return {
              id: item.id,
              status: item.status,
              jobType: item.job_type,
              fileName: item.payload?.file_name || null,
              events: [{ time: new Date(item.created_at).toLocaleTimeString(), status: item.status }],
              result: item.result,
              progress: item.result?.total
                ? {
                    processed: item.result.processed || item.result.total,
                    total: item.result.total,
                    percentage: 100,
                    totalAmount: item.result.summary?.total_amount,
                  }
                : null,
              wsState: WS_STATE.IDLE,
              wsError: null,
            };
          })
        );
      }
    } catch (err) {
      console.warn('Could not load jobs:', err);
    }
  }, [authFetch]);

  useEffect(() => {
    fetchJobs();
    return () => {
      Object.values(wsRefs.current).forEach(conn => {
        if (conn?.timer) clearTimeout(conn.timer);
        if (conn?.ws) conn.ws.close();
      });
    };
  }, [fetchJobs]);

  // ── Handle file selection ───────────────────────────────────────
  const handleFileChange = (e) => {
    const file = e.target.files?.[0];
    if (file && file.name.toLowerCase().endsWith('.csv')) {
      setSelectedFile(file);
    } else {
      setSelectedFile(null);
      if (file) alert('Please select a valid .csv file.');
    }
  };

  // ── Submit CSV for ingestion ────────────────────────────────────
  const submitCsvJob = async () => {
    if (!selectedFile) return;

    setSubmitting(true);
    try {
      const formData = new FormData();
      formData.append('file', selectedFile);

      const res = await authFetch('/api/v1/jobs/ingest', {
        method: 'POST',
        body: formData,
      });

      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(detailToMessage(errorData.detail) || `HTTP ${res.status}`);
      }

      const data = await res.json();

      const newJob = {
        id: data.id,
        status: data.status,
        jobType: 'csv_ingestion',
        fileName: selectedFile.name,
        events: [{ time: new Date().toLocaleTimeString(), status: data.status }],
        result: null,
        progress: { processed: 0, total: 100, percentage: 0 },
        wsState: WS_STATE.CONNECTING,
        wsError: null,
      };

      setJobs(prev => [newJob, ...prev]);

      setSelectedFile(null);
      if (fileInputRef.current) fileInputRef.current.value = '';

      openWebSocket(data.id);
    } catch (err) {
      console.error('Failed to submit CSV:', err);
      alert(`Error submitting CSV: ${err.message}`);
    } finally {
      setSubmitting(false);
    }
  };

  // ── Post-processing action: export the caller's transactions ────
  const triggerReportForJob = async (job) => {
    const summary = job.result?.summary;

    setGeneratingReportFor(job.id);
    try {
      // Date range only. The report is always scoped to the signed-in user —
      // the server reads the owner off the job row and there is no user_id field
      // on this request to set. Sending one would be a 422.
      const startDate = summary?.start_date ? summary.start_date.split('T')[0] : '2024-01-01';
      const endDate = summary?.end_date ? summary.end_date.split('T')[0] : '2026-12-31';

      const res = await authFetch('/api/v1/jobs/reports', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          start_date: startDate,
          end_date: endDate,
        }),
      });

      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(detailToMessage(errorData.detail) || `HTTP ${res.status}`);
      }
      const data = await res.json();

      const reportJob = {
        id: data.id,
        status: data.status,
        jobType: 'bulk_csv_report',
        fileName: `Export ${startDate} → ${endDate}`,
        events: [{ time: new Date().toLocaleTimeString(), status: data.status }],
        result: null,
        progress: null,
        wsState: WS_STATE.CONNECTING,
        wsError: null,
      };

      setJobs(prev => [reportJob, ...prev]);
      openWebSocket(data.id);
    } catch (err) {
      console.error('Failed to generate report:', err);
      alert(`Could not start report generation: ${err.message}`);
    } finally {
      setGeneratingReportFor(null);
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-bar">
          <div className="header-titles">
            <h1>⚡ Job Processing Platform</h1>
            <p className="subtitle">Async CSV Bulk Ingestion, Database Storage & Live Analytics</p>
          </div>
          <button className="signout-btn" onClick={onSignOut} title="Sign out">
            Sign out
          </button>
        </div>
      </header>

      {authError && (
        <div className="ws-error auth-error">
          <span className="ws-error-icon">⚠️</span>
          {authError}
        </div>
      )}

      {/* ── Submit Card ─────────────────────────────────────────── */}
      <section className="submit-section">
        <div className="submit-card">
          <label htmlFor="csv-file">Bulk CSV File Ingestion</label>
          <div className="submit-row">
            <div className="file-input-wrapper">
              <input
                ref={fileInputRef}
                id="csv-file"
                type="file"
                accept=".csv"
                onChange={handleFileChange}
                className="file-input"
              />
              <div className="file-input-display">
                <span className="file-icon">📄</span>
                <span className="file-name">
                  {selectedFile ? selectedFile.name : 'Choose a .csv file to ingest…'}
                </span>
              </div>
            </div>
            <button
              id="submit-btn"
              onClick={submitCsvJob}
              disabled={submitting || !selectedFile}
            >
              {submitting ? 'Uploading…' : 'Upload & Ingest'}
            </button>
          </div>
          {selectedFile && (
            <p className="file-meta">
              Selected: <strong>{selectedFile.name}</strong> ({(selectedFile.size / 1024).toFixed(1)} KB)
            </p>
          )}
        </div>
      </section>

      {/* ── Jobs List ───────────────────────────────────────────── */}
      <section className="jobs-section">
        <div className="jobs-section-header">
          <h2>Processed Jobs & Ingestion Reports</h2>
          <button className="refresh-btn" onClick={fetchJobs} title="Refresh jobs">
            🔄 Refresh
          </button>
        </div>

        {jobs.length === 0 && (
          <p className="empty-state">No jobs found. Upload a CSV file above to start processing.</p>
        )}

        {jobs.map(job => {
          const isCompleted = job.status === 'COMPLETED';
          const isFailed = job.status === 'FAILED';
          const isCancelled = job.status === 'CANCELLED';
          const isProcessing = !isTerminal(job.status);
          const totalRows = job.result?.total || job.progress?.total || 0;
          const processedRows = job.result?.processed || job.progress?.processed || 0;
          const pct = isCompleted ? 100 : (job.progress?.percentage ?? 0);
          const summary = job.result?.summary;
          const sampleRows = job.result?.sample_rows || [];

          return (
            <div key={job.id} className={`job-card ${isCompleted ? 'job-card-completed' : ''} ${isFailed ? 'job-card-failed' : ''}`}>
              <div className="job-header">
                <div className="job-header-left">
                  <div className="job-title-row">
                    <code className="job-id" title={job.id}>{job.id}</code>
                    {job.fileName && <span className="job-file-tag">📄 {job.fileName}</span>}
                    {job.jobType && <span className="job-type-pill">{job.jobType}</span>}
                  </div>
                </div>
                <StatusBadge status={job.status} />
              </div>

              {/* ── Live Progress Section ────────────────────────── */}
              {(isProcessing || job.progress) && (
                <div className="progress-section">
                  <div className="progress-info">
                    <span className="progress-label">
                      {isCompleted
                        ? `✓ Ingested & Saved all ${totalRows.toLocaleString()} records`
                        : `Processing & inserting: ${processedRows.toLocaleString()} / ${totalRows.toLocaleString()} rows`}
                    </span>
                    <span className="progress-pct">{pct}%</span>
                  </div>
                  <progress
                    className={`progress-bar ${isCompleted ? 'progress-bar-completed' : ''}`}
                    value={isCompleted ? (totalRows || 100) : processedRows}
                    max={totalRows || 100}
                  />
                </div>
              )}

              {/* ── WebSocket Connection Status ─────────────────── */}
              {job.wsError && (
                <div className="ws-error">
                  <span className="ws-error-icon">⚠️</span>
                  {job.wsError}
                </div>
              )}
              {job.wsState === WS_STATE.CONNECTING && (
                <div className="ws-status ws-connecting">
                  <span className="pulse-dot" />
                  Connecting to live database stream…
                </div>
              )}
              {job.wsState === WS_STATE.CONNECTED && isProcessing && (
                <div className="ws-status ws-connected">
                  <span className="pulse-dot connected" />
                  Streaming live database ingestion updates
                </div>
              )}

              {/* ── COMPLETED: Rich Analytics & Post-Processing ─── */}
              {isCompleted && (
                <div className="result-card result-success">
                  <div className="result-card-header">
                    <div>
                      <span className="result-card-title">✅ Database Ingestion Complete</span>
                      <span className="result-card-sub">
                        {job.result?.message || `Successfully committed ${totalRows.toLocaleString()} records to PostgreSQL`}
                      </span>
                    </div>
                  </div>

                  {/* ── Financial & Ingestion KPI Grid ─────────── */}
                  {summary && (
                    <div className="analytics-overview">
                      <div className="kpi-grid">
                        <div className="kpi-card">
                          <span className="kpi-label">Total Volume Ingested</span>
                          <span className="kpi-value kpi-green">
                            ${Number(summary.total_amount || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                          </span>
                        </div>
                        <div className="kpi-card">
                          <span className="kpi-label">Records Committed</span>
                          <span className="kpi-value">{totalRows.toLocaleString()}</span>
                        </div>
                        <div className="kpi-card">
                          <span className="kpi-label">Avg Transaction</span>
                          <span className="kpi-value">
                            ${Number(summary.avg_amount || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                          </span>
                        </div>
                        <div className="kpi-card">
                          <span className="kpi-label">Distinct Statuses</span>
                          <span className="kpi-value">{summary.distinct_statuses ?? Object.keys(summary.status_counts || {}).length}</span>
                        </div>
                      </div>

                      {/* ── Status Breakdown Pills ────────────── */}
                      {summary.status_counts && (
                        <div className="status-breakdown-row">
                          <span className="breakdown-title">Breakdown:</span>
                          {Object.entries(summary.status_counts).map(([st, count]) => {
                            const amt = summary.status_amounts?.[st] || 0;
                            return (
                              <div key={st} className={`breakdown-pill breakdown-${st}`}>
                                <strong className="breakdown-status-name">{st.toUpperCase()}:</strong> {count.toLocaleString()} rows (${Number(amt).toLocaleString('en-US', { maximumFractionDigits: 0 })})
                              </div>
                            );
                          })}
                        </div>
                      )}

                      {/* ── Post-Processing Action Bar ────────── */}
                      <div className="post-actions-bar">
                        {sampleRows.length > 0 && (
                          <button
                            className="action-btn btn-secondary"
                            onClick={() => toggleTable(job.id)}
                          >
                            {expandedTables[job.id] ? '▲ Hide Transactions Table' : '📊 View Ingested Records (Preview)'}
                          </button>
                        )}

                        <button
                          className="action-btn btn-primary"
                          onClick={() => triggerReportForJob(job)}
                          disabled={generatingReportFor === job.id}
                        >
                          {generatingReportFor === job.id ? '⚡ Triggering Report…' : '📥 Generate Export Report'}
                        </button>
                      </div>

                      {/* ── Interactive Transactions Table ──────── */}
                      {expandedTables[job.id] && sampleRows.length > 0 && (
                        <div className="table-wrapper">
                          <table className="transactions-table">
                            <thead>
                              <tr>
                                <th>#</th>
                                <th>User ID</th>
                                <th>Amount</th>
                                <th>Status</th>
                                <th>Date & Time</th>
                              </tr>
                            </thead>
                            <tbody>
                              {sampleRows.map((r, idx) => (
                                <tr key={idx}>
                                  <td><code>{r.id}</code></td>
                                  <td><code className="user-id-snippet" title={r.user_id}>{r.user_id.slice(0, 8)}…</code></td>
                                  <td className="amount-col">${Number(r.amount).toFixed(2)}</td>
                                  <td>
                                    <span className={`table-status table-status-${r.status}`}>
                                      {r.status}
                                    </span>
                                  </td>
                                  <td className="date-col">{new Date(r.created_at).toLocaleString()}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                          <p className="table-caption">Displaying first 10 sample transactions committed to the database.</p>
                        </div>
                      )}
                    </div>
                  )}

                  {/* ── Raw JSON Toggle ──────────────────────── */}
                  <div className="result-raw-toggle-wrapper">
                    <button
                      className="raw-toggle-btn"
                      onClick={() => toggleRaw(job.id)}
                    >
                      {expandedRaw[job.id] ? 'Hide Raw JSON ▴' : 'View Raw JSON ▾'}
                    </button>
                    {expandedRaw[job.id] && (
                      <pre className="job-result">{JSON.stringify(job.result, null, 2)}</pre>
                    )}
                  </div>
                </div>
              )}

              {/* ── FAILED: Error Card ─────────────────────────── */}
              {isFailed && job.result && (
                <div className="result-card result-failure">
                  <div className="result-card-header">
                    <span className="result-card-title">❌ Ingestion Failed</span>
                  </div>
                  <pre className="job-result job-result-error">{JSON.stringify(job.result, null, 2)}</pre>
                </div>
              )}

              {/* ── CANCELLED: Notice ──────────────────────────── */}
              {isCancelled && (
                <div className="result-card result-cancelled">
                  <div className="result-card-header">
                    <span className="result-card-title">⊘ Cancelled</span>
                  </div>
                  <p className="cancelled-note">
                    {job.result?.message || 'This job was cancelled before it started processing.'}
                  </p>
                </div>
              )}

              {/* ── Event Timeline ──────────────────────────────── */}
              <div className="timeline">
                {job.events.map((evt, i) => (
                  <div key={i} className="timeline-entry">
                    <span className="timeline-time">{evt.time}</span>
                    <span className="timeline-arrow">→</span>
                    <StatusBadge status={evt.status} />
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </section>
    </div>
  );
}
