import { useState, useRef, useEffect, useCallback } from 'react';
import { motion } from 'framer-motion';
import {
  Upload, FileSpreadsheet, Loader2, ArrowRight,
  Check, AlertTriangle, Table2,
  Download, GitMerge, CheckCircle2, XCircle,
} from 'lucide-react';
import { uploadUnify, getUnifyStatus, downloadUnified, getUnifyJobs } from '../api/client';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

/* -- Unify pipeline stages ------------------------------ */
const STAGES = [
  { key: 'queued',         label: 'Queued' },
  { key: 'building_graph', label: 'Building graph' },
  { key: 'clustering',     label: 'Clustering' },
  { key: 'scoring',        label: 'Scoring network' },
  { key: 'done',           label: 'Complete' },
];

function UnifyPanel() {
  const [file, setFile] = useState(null);
  const [jobId, setJobId] = useState(() => localStorage.getItem('unify_job_id') || null);
  const [job, setJob] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [err, setErr] = useState(null);
  const [previewData, setPreviewData] = useState(null);
  const fileRef = useRef();

  const MAX_FILE_SIZE = 20 * 1024 * 1024; // 20 MB

  const handleFile = (e) => {
    const f = e.target.files?.[0];
    if (!f) return;
    if (f.size > MAX_FILE_SIZE) {
      setErr(`File too large (${(f.size / 1024 / 1024).toFixed(1)} MB). Maximum is 20 MB.`);
      e.target.value = '';
      return;
    }
    if (!f.name.endsWith('.csv')) {
      setErr('Only CSV files are accepted.');
      e.target.value = '';
      return;
    }
    setFile(f); setErr(null); setJob(null); setJobId(null); setPreviewData(null); localStorage.removeItem('unify_job_id');
  };

  const uploadInFlight = useRef(false);

  const handleUpload = async () => {
    if (!file) return;
    if (uploadInFlight.current) return;
    uploadInFlight.current = true;
    setUploading(true); setErr(null);
    try {
      const { data } = await uploadUnify(file);
      setJobId(data.job_id);
      localStorage.setItem('unify_job_id', data.job_id);
    } catch (e) {
      setErr(e.response?.data?.detail || 'Upload failed');
    } finally {
      setUploading(false);
      // Keep lock for 3 seconds after completion to prevent rapid re-click
      setTimeout(() => { uploadInFlight.current = false; }, 3000);
    }
  };

  // On mount: if no local jobId, check for any active job on backend
  // (useful when someone else started the pipeline from another laptop/tab)
  useEffect(() => {
    if (jobId) return; // already tracking our own job
    let cancelled = false;
    (async () => {
      try {
        const { data } = await getUnifyJobs();
        const active = (data.jobs || []).find(j => j.status === 'queued' || j.status === 'running');
        if (active && !cancelled) {
          setJobId(active.job_id);
          localStorage.setItem('unify_job_id', active.job_id);
        }
      } catch {
        // silent — backend may not be reachable yet
      }
    })();
    return () => { cancelled = true; };
  }, []); // run once on mount

  // Poll for status
  useEffect(() => {
    if (!jobId) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const { data } = await getUnifyStatus(jobId);
        if (!cancelled) setJob(data);
        if (data.status === 'complete' || data.status === 'failed') return;
        if (!cancelled) setTimeout(poll, 2000);
      } catch (e) {
        // 404 means the job was lost (instance scaled down) — clear stale state
        if (e.response?.status === 404) {
          if (!cancelled) {
            setJobId(null); setJob(null);
            localStorage.removeItem('unify_job_id');
          }
          return;
        }
        if (!cancelled) setTimeout(poll, 3000);
      }
    };
    poll();
    return () => { cancelled = true; };
  }, [jobId]);

  const handleDownload = useCallback(async () => {
    if (!jobId) return;
    try {
      const { data } = await downloadUnified(jobId);
      const url = URL.createObjectURL(data);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${job?.job_suffix || 'unified'}_results.csv`;
      a.click();
      URL.revokeObjectURL(url);

      // Also parse for preview
      const text = await data.text();
      const lines = text.split('\n').filter(l => l.trim());
      if (lines.length > 1) {
        const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
        const rows = lines.slice(1, 21).map(l => {
          const vals = l.split(',').map(v => v.trim().replace(/"/g, ''));
          const obj = {};
          headers.forEach((h, i) => { obj[h] = vals[i] || ''; });
          return obj;
        });
        setPreviewData({ headers, rows, total: lines.length - 1 });
      }
    } catch (e) {
      setErr('Download failed');
    }
  }, [jobId, job]);

  const stageIdx = job ? STAGES.findIndex(s => s.key === job.stage) : -1;
  const stats = job?.stats || {};

  return (
    <motion.div
      key="unify"
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -12 }}
      transition={spring}
      className="space-y-6"
    >
      {/* Description */}
      <div className="glass-card">
        <div className="flex items-start gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-[var(--accent-dim)] shrink-0">
            <GitMerge size={20} className="text-[var(--accent)]" aria-hidden="true" />
          </div>
          <div>
            <h3 className="text-sm font-semibold text-[var(--text-primary)]">Entity unification pipeline</h3>
            <p className="mt-1 text-xs text-[var(--text-muted)] leading-relaxed max-w-lg">
              Upload a CSV of individual records (not pairs). The pipeline blocks candidates, scores them with DeBERTa,
              enriches with field rules, applies veto logic, and clusters via transitive closure.
              Returns a unified CSV with cluster_id assigned to each record.
            </p>
          </div>
        </div>
      </div>

      {/* Upload zone */}
      {!jobId && (
        <>
          {!file ? (
            <div
              role="button"
              tabIndex={0}
              aria-label="Upload records CSV"
              onClick={() => fileRef.current?.click()}
              onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fileRef.current?.click(); } }}
              className="glass-card flex cursor-pointer flex-col items-center justify-center border-dashed py-16 transition-colors hover:border-[var(--border-accent)] hover:bg-[var(--bg-hover)]"
            >
              <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-[var(--accent-dim)] mb-4">
                <Upload size={24} className="text-[var(--accent)]" aria-hidden="true" />
              </div>
              <p className="text-sm font-medium text-[var(--text-secondary)]">Upload records CSV</p>
              <p className="mt-1.5 text-xs text-[var(--text-muted)]">
                Columns: id, name, address (+ optional: dob, email, phone, company)
              </p>
              <input ref={fileRef} type="file" accept=".csv" onChange={handleFile} className="hidden" />
            </div>
          ) : (
            <div className="glass-card flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-emerald-50 border border-emerald-100">
                  <FileSpreadsheet size={20} className="text-emerald-600" aria-hidden="true" />
                </div>
                <div>
                  <p className="text-sm font-medium text-[var(--text-primary)]">{file.name}</p>
                  <p className="text-xs text-[var(--text-muted)]">{(file.size / 1024).toFixed(1)} KB</p>
                </div>
              </div>
              <div className="flex gap-2">
                <button onClick={() => { setFile(null); }} className="btn-secondary text-xs">Clear</button>
                <button
                  onClick={handleUpload}
                  disabled={uploading}
                  className="btn-primary text-xs flex items-center gap-2"
                >
                  {uploading ? <Loader2 size={14} className="animate-spin" /> : <ArrowRight size={14} />}
                  {uploading ? 'Uploading...' : 'Run pipeline'}
                </button>
              </div>
            </div>
          )}
        </>
      )}

      {/* Error */}
      {err && (
        <motion.div
          role="alert"
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          transition={spring}
          className="flex items-center gap-3 rounded-xl border border-red-200 bg-red-50 p-4"
        >
          <AlertTriangle size={16} className="text-red-600 shrink-0" />
          <p className="text-sm text-red-700">{err}</p>
        </motion.div>
      )}

      {/* Pipeline progress */}
      {job && job.status !== 'failed' && (
        <motion.div
          className="glass-card"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={spring}
        >
          <h3 className="section-label mb-4">Pipeline progress</h3>
          <div className="flex items-center gap-2">
            {STAGES.map((s, i) => {
              const isComplete = job.status === 'complete';
              const active = !isComplete && s.key === job.stage;
              const done = isComplete || i < stageIdx;
              return (
                <div key={s.key} className="flex items-center gap-2 flex-1">
                  <div className={`flex h-7 w-7 items-center justify-center rounded-full text-xs font-medium shrink-0 transition-colors ${
                    done ? 'bg-emerald-100 text-emerald-700' :
                    active ? 'bg-[var(--accent-dim)] text-[var(--accent)] ring-2 ring-[var(--accent)]/30' :
                    'bg-black/[0.03] text-[var(--text-faint)]'
                  }`}>
                    {done ? <Check size={12} /> : active ? <Loader2 size={12} className="animate-spin" /> : i + 1}
                  </div>
                  <span className={`text-xs truncate ${active ? 'text-[var(--text-primary)] font-medium' : 'text-[var(--text-muted)]'}`}>
                    {s.label}
                  </span>
                  {i < STAGES.length - 1 && (
                    <div className={`flex-1 h-px ${done ? 'bg-emerald-300' : 'bg-black/[0.06]'}`} />
                  )}
                </div>
              );
            })}
          </div>
        </motion.div>
      )}

      {/* Failed state */}
      {job?.status === 'failed' && (
        <motion.div
          className="glass-card border-red-200"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={spring}
        >
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <XCircle size={20} className="text-red-600 shrink-0" />
              <div>
                <p className="text-sm font-medium text-red-700">Pipeline failed</p>
                <p className="text-xs text-red-600 mt-1">{job.error}</p>
              </div>
            </div>
            <button
              onClick={() => { setJobId(null); setJob(null); setFile(null); localStorage.removeItem('unify_job_id'); }}
              className="btn-secondary text-xs shrink-0"
            >
              Try again
            </button>
          </div>
        </motion.div>
      )}

      {/* Complete state */}
      {job?.status === 'complete' && (
        <motion.div
          className="space-y-6"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={spring}
        >
          {/* Success header + download */}
          <div className="glass-card flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-emerald-100">
                <CheckCircle2 size={20} className="text-emerald-600" />
              </div>
              <div>
                <p className="text-sm font-semibold text-emerald-700">Pipeline complete</p>
                <p className="text-xs text-[var(--text-muted)]">
                  {stats.n_records || '—'} records clustered into {stats.n_clusters || '—'} entities
                </p>
              </div>
            </div>
            <div className="flex gap-2">
              <button onClick={handleDownload} className="btn-primary text-xs flex items-center gap-2">
                <Download size={14} />
                Download CSV
              </button>
              <button
                onClick={() => { setJobId(null); setJob(null); setFile(null); setPreviewData(null); localStorage.removeItem('unify_job_id'); }}
                className="btn-secondary text-xs"
              >
                New run
              </button>
            </div>
          </div>

          {/* Stats */}
          <div>
            <h3 className="section-label mb-4">Cluster metrics</h3>
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
              {[
                { label: 'Records', value: stats.n_records },
                { label: 'Clusters', value: stats.n_clusters },
                { label: 'Singletons', value: `${((stats.singleton_rate || 0) * 100).toFixed(1)}%` },
                { label: 'Avg size', value: stats.cluster_size_mean },
                { label: 'Max cluster', value: stats.cluster_size_max },
                { label: 'Edges', value: stats.n_cluster_edges },
                { label: 'Vetoed edges', value: stats.n_vetoed_edges },
                { label: 'Avg DeBERTa', value: stats.avg_deberta_score },
              ].map((m, i) => (
                <motion.div
                  key={m.label}
                  className="metric-card"
                  initial={{ opacity: 0, y: 12 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: i * STAGGER_MS, ...spring }}
                >
                  <span className="metric-value text-[var(--text-primary)]">{m.value ?? '—'}</span>
                  <span className="metric-label">{m.label}</span>
                </motion.div>
              ))}
            </div>
          </div>

          {/* Preview table */}
          {previewData && (
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1, ...spring }}
            >
              <h3 className="section-label mb-4 flex items-center gap-2">
                <Table2 size={14} className="text-[var(--text-muted)]" />
                Preview (first {previewData.rows.length} of {previewData.total})
              </h3>
              <div className="glass-card overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left">
                      {previewData.headers.slice(0, 8).map(h => (
                        <th key={h} className="table-header text-xs">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {previewData.rows.map((row, i) => (
                      <tr key={i} className="table-row">
                        {previewData.headers.slice(0, 8).map(h => (
                          <td key={h} className="py-2.5 pr-4 text-xs text-[var(--text-secondary)] truncate max-w-[180px]">
                            {row[h] || '—'}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </motion.div>
          )}
        </motion.div>
      )}
    </motion.div>
  );
}

export default function Batch() {
  return (
    <div className="page-container">
      <motion.div
        className="mb-10"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal, ease: easeOut }}
      >
        <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
          Batch unification
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-[var(--text-muted)] max-w-xl">
          Upload a CSV of individual records. The pipeline blocks candidates, scores with DeBERTa,
          enriches with field rules, and clusters via transitive closure. Returns a unified CSV with cluster assignments.
        </p>
      </motion.div>

      <UnifyPanel />
    </div>
  );
}
