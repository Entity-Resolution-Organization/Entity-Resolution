import { useState, useRef, useEffect, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import Plot from '../components/Plot';
import {
  Upload, FileSpreadsheet, Loader2, ArrowRight, GripVertical,
  Check, AlertTriangle, X as XIcon, BarChart3, Table2, Layers,
  Download, GitMerge, Clock, CheckCircle2, XCircle,
} from 'lucide-react';
import { resolveEntities, uploadUnify, getUnifyStatus, downloadUnified } from '../api/client';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

/* -- Sample data ----------------------------------------- */
const SAMPLE = [
  { name1: 'Robert Smith', address1: '123 Main Street', name2: 'Bob Smith', address2: '123 Main St' },
  { name1: 'John Doe', address1: '456 Elm Avenue', name2: 'John Doe', address2: '456 Elm Ave' },
  { name1: 'Maria Garcia', address1: '789 Oak Blvd', name2: 'Maria Garcia', address2: '100 Pine Road' },
  { name1: 'Mohammad Al-Rashid', address1: '45 Desert Rd', name2: 'Mohammed Al Rashid', address2: '45 Desert Road' },
  { name1: 'Alice Johnson', address1: '200 Park Ave', name2: 'Robert Smith', address2: '200 Park Ave' },
];

const TARGET_FIELDS = ['name1', 'address1', 'name2', 'address2', 'email', 'dob', 'phone'];
const FIELD_LABELS = {
  name1: 'Name (Entity A)', address1: 'Address (Entity A)',
  name2: 'Name (Entity B)', address2: 'Address (Entity B)',
  email: 'Email', dob: 'Date of Birth', phone: 'Phone',
};

/* -- Row background by decision -------------------------- */
const rowBg = (decision) => {
  if (decision === 'MATCH') return 'rgba(5, 150, 105, 0.04)';
  if (decision === 'REVIEW') return 'rgba(180, 83, 9, 0.04)';
  if (decision === 'NO-MATCH') return 'rgba(220, 38, 38, 0.04)';
  return 'transparent';
};

/* -- Plotly shared config -------------------------------- */
const plotlyConfig = { displayModeBar: false, responsive: true };

const plotlyFont = { color: '#64748b', family: 'Outfit, system-ui' };

const axisDefaults = {
  color: '#64748b',
  gridcolor: 'rgba(0,0,0,0.06)',
  zerolinecolor: 'rgba(0,0,0,0.06)',
};

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

  const handleFile = (e) => {
    const f = e.target.files?.[0];
    if (f) { setFile(f); setErr(null); setJob(null); setJobId(null); setPreviewData(null); localStorage.removeItem('unify_job_id'); }
  };

  const handleUpload = async () => {
    if (!file) return;
    setUploading(true); setErr(null);
    try {
      const { data } = await uploadUnify(file);
      setJobId(data.job_id);
      localStorage.setItem('unify_job_id', data.job_id);
    } catch (e) {
      setErr(e.response?.data?.detail || 'Upload failed');
    } finally {
      setUploading(false);
    }
  };

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
      } catch {
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
      {job && job.status !== 'complete' && job.status !== 'failed' && (
        <motion.div
          className="glass-card"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={spring}
        >
          <h3 className="section-label mb-4">Pipeline progress</h3>
          <div className="flex items-center gap-2">
            {STAGES.map((s, i) => {
              const active = s.key === job.stage;
              const done = i < stageIdx;
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
          <div className="flex items-center gap-3">
            <XCircle size={20} className="text-red-600 shrink-0" />
            <div>
              <p className="text-sm font-medium text-red-700">Pipeline failed</p>
              <p className="text-xs text-red-600 mt-1">{job.error}</p>
            </div>
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
            <button onClick={handleDownload} className="btn-primary text-xs flex items-center gap-2">
              <Download size={14} />
              Download unified CSV
            </button>
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
  const [mode, setMode] = useState('sample');
  const [csvData, setCsvData] = useState(null);
  const [csvHeaders, setCsvHeaders] = useState([]);
  const [columnMap, setColumnMap] = useState({});
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState(null);
  const [fileName, setFileName] = useState('');
  const fileRef = useRef();

  /* -- CSV parsing ---------------------------------------- */
  const handleFile = (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setError(null);
    setFileName(file.name);
    const reader = new FileReader();
    reader.onload = (ev) => {
      try {
        const text = ev.target.result;
        const lines = text.split('\n').filter(l => l.trim());
        if (lines.length < 2) { setError('CSV must have a header row and at least one data row.'); return; }
        const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
        const rows = lines.slice(1).map(l => {
          const vals = l.split(',').map(v => v.trim().replace(/"/g, ''));
          const obj = {};
          headers.forEach((h, i) => { obj[h] = vals[i] || ''; });
          return obj;
        });
        setCsvHeaders(headers);
        setCsvData(rows);
        // Auto-map obvious columns
        const autoMap = {};
        headers.forEach(h => {
          const lower = h.toLowerCase();
          if (lower.includes('name') && lower.includes('1')) autoMap.name1 = h;
          else if (lower.includes('name') && lower.includes('2')) autoMap.name2 = h;
          else if (lower.includes('addr') && lower.includes('1')) autoMap.address1 = h;
          else if (lower.includes('addr') && lower.includes('2')) autoMap.address2 = h;
          else if (lower.includes('email')) autoMap.email = h;
          else if (lower.includes('dob') || lower.includes('birth')) autoMap.dob = h;
          else if (lower.includes('phone')) autoMap.phone = h;
        });
        setColumnMap(autoMap);
      } catch { setError('Failed to parse CSV file.'); }
    };
    reader.readAsText(file);
  };

  /* -- Column mapping ------------------------------------- */
  const handleMapColumn = (target, csvCol) => {
    setColumnMap(prev => {
      const next = { ...prev };
      Object.keys(next).forEach(k => { if (next[k] === csvCol) delete next[k]; });
      if (csvCol) next[target] = csvCol;
      else delete next[target];
      return next;
    });
  };

  /* -- Run batch ------------------------------------------ */
  const run = async () => {
    setLoading(true); setResults(null); setProgress(0); setError(null);
    const pairs = mode === 'sample' ? SAMPLE : csvData.map(row => ({
      name1: row[columnMap.name1] || '',
      address1: row[columnMap.address1] || '',
      name2: row[columnMap.name2] || '',
      address2: row[columnMap.address2] || '',
    }));

    const out = [];
    for (let i = 0; i < pairs.length; i++) {
      try {
        const { data } = await resolveEntities(pairs[i]);
        out.push({ ...pairs[i], ...data });
      } catch { out.push({ ...pairs[i], decision: 'ERROR', probability: 0 }); }
      setProgress(((i + 1) / pairs.length) * 100);
    }
    setResults(out);
    setLoading(false);
  };

  const counts = results ? {
    MATCH: results.filter(r => r.decision === 'MATCH').length,
    REVIEW: results.filter(r => r.decision === 'REVIEW').length,
    'NO-MATCH': results.filter(r => r.decision === 'NO-MATCH').length,
  } : {};

  const canRun = mode === 'sample' || (csvData && columnMap.name1 && columnMap.name2);
  const mappedCount = Object.keys(columnMap).length;

  return (
    <div className="page-container">
      {/* Header */}
      <motion.div
        className="mb-10"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal, ease: easeOut }}
      >
        <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
          Batch resolution
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-[var(--text-muted)] max-w-xl">
          Process multiple entity pairs at once. Use the built-in sample data or upload your own CSV with column mapping.
        </p>
      </motion.div>

      {/* Mode toggle */}
      <motion.div
        className="mb-8 flex gap-2"
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.05, duration: dur.normal, ease: easeOut }}
      >
        <button
          onClick={() => { setMode('sample'); setResults(null); }}
          className={`flex items-center gap-2 rounded-lg px-5 py-2.5 text-sm font-medium transition-colors ${
            mode === 'sample'
              ? 'bg-[var(--accent-dim)] text-[var(--accent)] border border-[var(--border-accent)]'
              : 'bg-black/[0.02] text-[var(--text-muted)] border border-black/[0.06] hover:bg-[var(--bg-hover)]'
          }`}
        >
          <FileSpreadsheet size={16} aria-hidden="true" />
          Sample data
        </button>
        <button
          onClick={() => { setMode('csv'); setResults(null); }}
          className={`flex items-center gap-2 rounded-lg px-5 py-2.5 text-sm font-medium transition-colors ${
            mode === 'csv'
              ? 'bg-[var(--accent-dim)] text-[var(--accent)] border border-[var(--border-accent)]'
              : 'bg-black/[0.02] text-[var(--text-muted)] border border-black/[0.06] hover:bg-[var(--bg-hover)]'
          }`}
        >
          <Upload size={16} aria-hidden="true" />
          Upload CSV
        </button>
        <button
          onClick={() => { setMode('unify'); setResults(null); }}
          className={`flex items-center gap-2 rounded-lg px-5 py-2.5 text-sm font-medium transition-colors ${
            mode === 'unify'
              ? 'bg-[var(--accent-dim)] text-[var(--accent)] border border-[var(--border-accent)]'
              : 'bg-black/[0.02] text-[var(--text-muted)] border border-black/[0.06] hover:bg-[var(--bg-hover)]'
          }`}
        >
          <GitMerge size={16} aria-hidden="true" />
          Unify records
        </button>
      </motion.div>

      {/* CSV upload + column mapping */}
      <AnimatePresence mode="wait">
        {mode === 'csv' && (
          <motion.div
            key="csv"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -12 }}
            transition={spring}
            className="space-y-5"
          >
            {/* Upload zone */}
            {!csvData ? (
              <div
                role="button"
                tabIndex={0}
                aria-label="Upload CSV file"
                onClick={() => fileRef.current?.click()}
                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fileRef.current?.click(); } }}
                className="glass-card flex cursor-pointer flex-col items-center justify-center border-dashed py-20 transition-colors hover:border-[var(--border-accent)] hover:bg-[var(--bg-hover)]"
              >
                <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-[var(--accent-dim)] mb-4">
                  <Upload size={24} className="text-[var(--accent)]" aria-hidden="true" />
                </div>
                <p className="text-sm font-medium text-[var(--text-secondary)]">Click to upload CSV</p>
                <p className="mt-1.5 text-xs text-[var(--text-muted)]">
                  Columns will be auto-detected when possible
                </p>
                <input ref={fileRef} type="file" accept=".csv" onChange={handleFile} className="hidden" />
              </div>
            ) : (
              <>
                {/* File info */}
                <div className="glass-card flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-emerald-50 border border-emerald-100">
                      <FileSpreadsheet size={20} className="text-emerald-600" aria-hidden="true" />
                    </div>
                    <div>
                      <p className="text-sm font-medium text-[var(--text-primary)]">
                        {fileName || `${csvData.length} rows`}
                      </p>
                      <p className="text-xs text-[var(--text-muted)]">
                        {csvData.length} rows, {csvHeaders.length} columns detected
                      </p>
                    </div>
                  </div>
                  <button
                    onClick={() => { setCsvData(null); setCsvHeaders([]); setColumnMap({}); setFileName(''); }}
                    className="btn-secondary text-xs"
                  >
                    Clear
                  </button>
                </div>

                {/* Column mapping */}
                <div className="glass-card">
                  <div className="flex items-center justify-between mb-4">
                    <h3 className="section-label">Column mapping</h3>
                    <span className="text-xs text-[var(--text-faint)]">
                      {mappedCount} of {TARGET_FIELDS.length} mapped
                    </span>
                  </div>
                  <p className="mb-5 text-xs text-[var(--text-muted)]">
                    Map your CSV columns to the required entity fields. Name columns are required.
                  </p>
                  <div className="space-y-2.5">
                    {TARGET_FIELDS.map((field, idx) => {
                      const required = field === 'name1' || field === 'name2';
                      const mapped = columnMap[field];
                      return (
                        <motion.div
                          key={field}
                          initial={{ opacity: 0, x: -8 }}
                          animate={{ opacity: 1, x: 0 }}
                          transition={{ delay: idx * STAGGER_MS, ...spring }}
                          className={`flex items-center gap-3 rounded-lg border px-3 py-2.5 transition-colors ${
                            mapped
                              ? 'border-emerald-200/60 bg-emerald-50/30'
                              : 'border-black/[0.04] bg-white/50'
                          }`}
                        >
                          <GripVertical size={14} className="text-[var(--text-faint)] shrink-0" aria-hidden="true" />
                          <div className="w-40 shrink-0">
                            <p className="text-xs font-medium text-[var(--text-secondary)]">
                              {FIELD_LABELS[field]}
                              {required && <span className="text-red-600 ml-1">*</span>}
                            </p>
                          </div>
                          <ArrowRight size={14} className="text-[var(--text-faint)] shrink-0" aria-hidden="true" />
                          <select
                            value={mapped || ''}
                            onChange={e => handleMapColumn(field, e.target.value)}
                            className="input-field flex-1 text-xs py-1.5"
                            {...(required ? { 'aria-required': 'true' } : {})}
                          >
                            <option value="">-- not mapped --</option>
                            {csvHeaders.map(h => (
                              <option key={h} value={h}>{h}</option>
                            ))}
                          </select>
                          {mapped && (
                            <motion.div
                              initial={{ scale: 0 }}
                              animate={{ scale: 1 }}
                              transition={{ type: 'spring', stiffness: 300, damping: 20 }}
                              className="flex h-5 w-5 items-center justify-center rounded-full bg-emerald-100"
                            >
                              <Check size={11} className="text-emerald-600" aria-hidden="true" />
                            </motion.div>
                          )}
                        </motion.div>
                      );
                    })}
                  </div>
                </div>
              </>
            )}
          </motion.div>
        )}
      </AnimatePresence>

      {/* Unify mode */}
      <AnimatePresence mode="wait">
        {mode === 'unify' && (
          <UnifyPanel />
        )}
      </AnimatePresence>

      {/* Error */}
      <AnimatePresence>
        {error && (
          <motion.div
            role="alert"
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            transition={spring}
            className="mt-4 flex items-center gap-3 rounded-xl border border-red-200 bg-red-50 p-4"
          >
            <AlertTriangle size={16} className="text-red-600 shrink-0" aria-hidden="true" />
            <p className="text-sm text-red-700">{error}</p>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Run button (pairwise modes only) */}
      {mode !== 'unify' && <motion.button
        onClick={run}
        disabled={loading || !canRun}
        aria-busy={loading}
        className="btn-primary mt-8 flex w-full items-center justify-center gap-2 py-3"
        whileTap={{ scale: 0.985 }}
      >
        {loading ? (
          <>
            <Loader2 size={16} className="animate-spin" aria-hidden="true" />
            <span>Processing... {progress.toFixed(0)}%</span>
          </>
        ) : (
          <span>
            {mode === 'sample'
              ? `Run sample batch (${SAMPLE.length} pairs)`
              : `Process ${csvData?.length || 0} pairs`}
          </span>
        )}
      </motion.button>}

      {/* Progress bar + results (pairwise modes only) */}
      {mode !== 'unify' && <div aria-live="polite">
      <AnimatePresence>
        {loading && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="mt-4 h-1.5 rounded-full bg-black/[0.04] overflow-hidden"
          >
            <motion.div
              className="h-full rounded-full"
              style={{ background: 'linear-gradient(90deg, #c2410c, #ea580c)' }}
              initial={{ width: 0 }}
              animate={{ width: `${progress}%` }}
              transition={{ duration: dur.fast, ease: easeOut }}
            />
          </motion.div>
        )}
      </AnimatePresence>

      {/* Results */}
      <AnimatePresence>
        {results && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }}
            transition={spring}
            className="mt-10 space-y-8"
          >
            {/* Summary metrics */}
            <div>
              <h2 className="section-label mb-4">Summary</h2>
              <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                {[
                  { label: 'Total', value: results.length, color: 'text-[var(--text-primary)]' },
                  { label: 'Match', value: counts.MATCH, color: 'text-emerald-600' },
                  { label: 'Review', value: counts.REVIEW, color: 'text-amber-700' },
                  { label: 'No match', value: counts['NO-MATCH'], color: 'text-red-600' },
                ].map((m, i) => (
                  <motion.div
                    key={m.label}
                    className="metric-card"
                    initial={{ opacity: 0, y: 12 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: i * STAGGER_MS, ...spring }}
                  >
                    <span className={`metric-value ${m.color}`}>{m.value}</span>
                    <span className="metric-label">{m.label}</span>
                  </motion.div>
                ))}
              </div>
            </div>

            {/* Charts */}
            <div>
              <h2 className="section-label mb-4 flex items-center gap-2">
                <BarChart3 size={14} className="text-[var(--text-muted)]" aria-hidden="true" />
                Analytics
              </h2>
              <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                {/* Donut chart */}
                <motion.div
                  className="glass-card"
                  initial={{ opacity: 0, y: 12 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.1, ...spring }}
                >
                  <h3 className="section-label mb-3">Decision distribution</h3>

                  <Plot
                    data={[{
                      type: 'pie',
                      hole: 0.55,
                      values: [counts.MATCH, counts.REVIEW, counts['NO-MATCH']],
                      labels: ['Match', 'Review', 'No match'],
                      marker: { colors: ['#059669', '#b45309', '#dc2626'] },
                      textinfo: 'label+percent',
                      textfont: { color: '#1e293b', size: 12, family: 'Outfit, system-ui' },
                      hoverinfo: 'label+value+percent',
                      sort: false,
                    }]}
                    layout={{
                      height: 260,
                      margin: { t: 16, b: 16, l: 16, r: 16 },
                      paper_bgcolor: 'rgba(0,0,0,0)',
                      plot_bgcolor: 'rgba(0,0,0,0)',
                      showlegend: false,
                      font: plotlyFont,
                    }}
                    config={plotlyConfig}
                    style={{ width: '100%' }}
                  />
                </motion.div>

                {/* Histogram */}
                <motion.div
                  className="glass-card"
                  initial={{ opacity: 0, y: 12 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.15, ...spring }}
                >
                  <h3 className="section-label mb-3">Probability distribution</h3>

                  <Plot
                    data={[{
                      type: 'histogram',
                      x: results.map(r => r.probability),
                      marker: {
                        color: 'rgba(194, 65, 12, 0.5)',
                        line: { color: 'rgba(194, 65, 12, 0.7)', width: 1 },
                      },
                      nbinsx: 20,
                      hoverinfo: 'x+y',
                    }]}
                    layout={{
                      height: 260,
                      margin: { t: 16, b: 44, l: 44, r: 16 },
                      paper_bgcolor: 'rgba(0,0,0,0)',
                      plot_bgcolor: 'rgba(0,0,0,0)',
                      xaxis: {
                        title: { text: 'Probability', font: { size: 11 } },
                        ...axisDefaults,
                      },
                      yaxis: {
                        title: { text: 'Count', font: { size: 11 } },
                        ...axisDefaults,
                      },
                      shapes: [
                        {
                          type: 'line', x0: 0.45, x1: 0.45, y0: 0, y1: 1, yref: 'paper',
                          line: { color: '#059669', dash: 'dash', width: 1.5 },
                        },
                        {
                          type: 'line', x0: 0.20, x1: 0.20, y0: 0, y1: 1, yref: 'paper',
                          line: { color: '#dc2626', dash: 'dash', width: 1.5 },
                        },
                      ],
                      font: plotlyFont,
                      bargap: 0.05,
                    }}
                    config={plotlyConfig}
                    style={{ width: '100%' }}
                  />
                </motion.div>
              </div>
            </div>

            {/* Results table */}
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2, ...spring }}
            >
              <h2 className="section-label mb-4 flex items-center gap-2">
                <Table2 size={14} className="text-[var(--text-muted)]" aria-hidden="true" />
                Results ({results.length})
              </h2>
              <div className="glass-card overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left">
                      <th className="table-header">#</th>
                      <th className="table-header">Name (A)</th>
                      <th className="table-header">Name (B)</th>
                      <th className="table-header text-right">Score</th>
                      <th className="table-header text-right">Decision</th>
                    </tr>
                  </thead>
                  <tbody>
                    {results.map((r, i) => (
                      <motion.tr
                        key={i}
                        className="table-row"
                        style={{ backgroundColor: rowBg(r.decision) }}
                        initial={{ opacity: 0, x: -6 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ delay: i * 0.03, duration: dur.fast, ease: easeOut }}
                      >
                        <td className="py-3 pr-3 font-mono text-xs tabular-nums text-[var(--text-faint)]">
                          {String(i + 1).padStart(2, '0')}
                        </td>
                        <td className="py-3 text-[var(--text-secondary)]">{r.name1}</td>
                        <td className="py-3 text-[var(--text-secondary)]">{r.name2}</td>
                        <td className="py-3 text-right font-mono text-sm tabular-nums text-[var(--text-primary)]">
                          {(r.probability * 100).toFixed(1)}%
                        </td>
                        <td className="py-3 text-right">
                          <span className={
                            r.decision === 'MATCH' ? 'badge-match' :
                            r.decision === 'REVIEW' ? 'badge-review' : 'badge-nomatch'
                          }>
                            {r.decision === 'MATCH' && <Check size={12} aria-hidden="true" />}
                            {r.decision === 'NO-MATCH' && <XIcon size={12} aria-hidden="true" />}
                            {r.decision === 'REVIEW' && <AlertTriangle size={12} aria-hidden="true" />}
                            {r.decision}
                          </span>
                        </td>
                      </motion.tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
      </div>}

      {/* Empty state (pairwise modes only) */}
      {mode !== 'unify' && !results && !loading && (
        <motion.div
          className="mt-16 flex flex-col items-center justify-center py-20 text-center"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.2, duration: dur.normal, ease: easeOut }}
        >
          <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-black/[0.02] border border-black/[0.06]">
            <Layers size={24} className="text-[var(--text-faint)]" aria-hidden="true" />
          </div>
          <p className="mt-5 text-sm text-[var(--text-muted)]">
            {mode === 'sample'
              ? 'Click the button above to run the sample batch'
              : 'Upload a CSV to begin'}
          </p>
        </motion.div>
      )}
    </div>
  );
}
