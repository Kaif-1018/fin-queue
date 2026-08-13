import { useState, useRef, useCallback } from 'react';
import './App.css';

// ── API configuration ─────────────────────────────────────────────
// In production (behind nginx), API_BASE is empty → relative URLs hit the proxy.
// In development (Vite), default to the backend's direct address.
const API_BASE = import.meta.env.VITE_API_URL ?? (import.meta.env.DEV ? 'http://localhost:8000' : '');
const WS_BASE  = API_BASE
  ? API_BASE.replace(/^http/, 'ws')
  : `ws://${window.location.host}`;

// ── Status badge colours ──────────────────────────────────────────
const STATUS_STYLES = {
  PENDING:    { bg: '#fef3c7', color: '#92400e', label: 'PENDING' },
  QUEUED:     { bg: '#e0e7ff', color: '#3730a3', label: 'QUEUED' },
  PROCESSING: { bg: '#dbeafe', color: '#1e40af', label: 'PROCESSING' },
  COMPLETED:  { bg: '#d1fae5', color: '#065f46', label: 'COMPLETED' },
  FAILED:     { bg: '#fee2e2', color: '#991b1b', label: 'FAILED' },
};

function StatusBadge({ status }) {
  const style = STATUS_STYLES[status] || { bg: '#f3f4f6', color: '#374151', label: status };
  return (
    <span className="status-badge" style={{ backgroundColor: style.bg, color: style.color }}>
      {style.label}
    </span>
  );
}

export default function App() {
  const [jobType, setJobType] = useState('document_extraction');
  const [submitting, setSubmitting] = useState(false);
  const [jobs, setJobs] = useState([]);         // { id, status, events[], result }
  const wsRefs = useRef({});                     // jobId → WebSocket

  // ── Submit a new job ──────────────────────────────────────────
  const submitJob = useCallback(async () => {
    setSubmitting(true);
    try {
      const res = await fetch(`${API_BASE}/api/v1/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_type: jobType, payload: { source: 'frontend' } }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      const job = {
        id: data.id,
        status: data.status,
        events: [{ time: new Date().toLocaleTimeString(), status: data.status }],
        result: null,
      };

      setJobs(prev => [job, ...prev]);
      openWebSocket(data.id);
    } catch (err) {
      console.error('Failed to submit job:', err);
      alert(`Error submitting job: ${err.message}`);
    } finally {
      setSubmitting(false);
    }
  }, [jobType]);

  // ── Open WebSocket for a job ──────────────────────────────────
  const openWebSocket = useCallback((jobId) => {
    const ws = new WebSocket(`${WS_BASE}/ws/jobs/${jobId}`);
    wsRefs.current[jobId] = ws;

    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      setJobs(prev =>
        prev.map(j => {
          if (j.id !== jobId) return j;
          return {
            ...j,
            status: msg.status || j.status,
            result: msg.result || j.result,
            events: [
              ...j.events,
              { time: new Date().toLocaleTimeString(), status: msg.status },
            ],
          };
        })
      );
    };

    ws.onclose = () => {
      delete wsRefs.current[jobId];
    };

    ws.onerror = (err) => {
      console.error(`WebSocket error for job ${jobId}:`, err);
    };
  }, []);

  // ── Render ────────────────────────────────────────────────────
  return (
    <div className="app">
      <header className="app-header">
        <h1>⚡ Job Processing Platform</h1>
        <p className="subtitle">Submit asynchronous jobs and watch them process in real-time</p>
      </header>

      <section className="submit-section">
        <div className="submit-card">
          <label htmlFor="job-type">Job Type</label>
          <div className="submit-row">
            <input
              id="job-type"
              type="text"
              value={jobType}
              onChange={e => setJobType(e.target.value)}
              placeholder="e.g. document_extraction"
            />
            <button
              id="submit-btn"
              onClick={submitJob}
              disabled={submitting || !jobType.trim()}
            >
              {submitting ? 'Submitting…' : 'Submit Job'}
            </button>
          </div>
        </div>
      </section>

      <section className="jobs-section">
        {jobs.length === 0 && (
          <p className="empty-state">No jobs yet — submit one above to get started.</p>
        )}

        {jobs.map(job => (
          <div key={job.id} className="job-card">
            <div className="job-header">
              <code className="job-id">{job.id}</code>
              <StatusBadge status={job.status} />
            </div>

            <div className="timeline">
              {job.events.map((evt, i) => (
                <div key={i} className="timeline-entry">
                  <span className="timeline-time">{evt.time}</span>
                  <span className="timeline-arrow">→</span>
                  <StatusBadge status={evt.status} />
                </div>
              ))}
            </div>

            {job.result && (
              <pre className="job-result">{JSON.stringify(job.result, null, 2)}</pre>
            )}
          </div>
        ))}
      </section>
    </div>
  );
}
