import { lazy, Suspense } from 'react';
import { motion } from 'framer-motion';
import { BarChart3, Users, GitMerge, AlertTriangle, TrendingUp, Database } from 'lucide-react';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

const Plot = lazy(() => import('react-plotly.js'));

const plotlyConfig = { displayModeBar: false, responsive: true };
const plotlyFont = { color: '#94a3b8', family: 'Outfit, system-ui' };
const axisDefaults = { color: '#64748b', gridcolor: 'rgba(255,255,255,0.04)', zerolinecolor: 'rgba(255,255,255,0.06)' };

// Demo metrics from the training run
const METRICS = {
  n_records: 52686,
  n_clusters: 36125,
  singleton_rate: 0.4609,
  cluster_size_mean: 1.46,
  cluster_size_max: 426,
  cluster_size_p95: 2.0,
  n_cluster_edges: 25783,
  n_vetoed_edges: 0,
  avg_deberta_score: 0.9351,
  n_requires_review: 0,
};

// Simulated size distribution
const SIZE_DIST = {
  labels: ['1', '2', '3', '4', '5', '6-10', '11-50', '50+'],
  values: [24283, 9842, 1200, 480, 180, 100, 35, 5],
};

// Simulated score distribution
const SCORE_DIST = Array.from({ length: 100 }, () => 0.3 + Math.random() * 0.7);

// Risk queue
const RISK_QUEUE = [
  { record_id: 'REC-44281', cluster_id: 'c9a1e3f2', cluster_size: 426, composite: 0.146, risk: 'medium' },
  { record_id: 'REC-12093', cluster_id: 'b7d4c8a1', cluster_size: 38, composite: 0.132, risk: 'medium' },
  { record_id: 'REC-33847', cluster_id: 'e5f2a9d3', cluster_size: 22, composite: 0.118, risk: 'low' },
  { record_id: 'REC-09182', cluster_id: 'a2c7b4e8', cluster_size: 15, composite: 0.095, risk: 'low' },
  { record_id: 'REC-55102', cluster_id: 'f8d1c3a7', cluster_size: 12, composite: 0.088, risk: 'low' },
  { record_id: 'REC-71839', cluster_id: 'd4e9f2b6', cluster_size: 9, composite: 0.072, risk: 'low' },
];

const KPI_CARDS = [
  { label: 'Total records', value: METRICS.n_records.toLocaleString(), icon: Database, color: 'text-blue-400' },
  { label: 'Clusters formed', value: METRICS.n_clusters.toLocaleString(), icon: GitMerge, color: 'text-[var(--accent)]' },
  { label: 'Singleton rate', value: `${(METRICS.singleton_rate * 100).toFixed(1)}%`, icon: Users, color: 'text-stone-400' },
  { label: 'Avg cluster size', value: METRICS.cluster_size_mean.toFixed(2), icon: TrendingUp, color: 'text-emerald-400' },
  { label: 'Max cluster', value: METRICS.cluster_size_max, icon: AlertTriangle, color: 'text-amber-400' },
  { label: 'Avg DeBERTa', value: METRICS.avg_deberta_score.toFixed(3), icon: BarChart3, color: 'text-emerald-400' },
];

export default function Analytics() {
  return (
    <div className="page-container">
      <motion.div
        className="mb-10"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal, ease: easeOut }}
      >
        <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
          Cluster analytics
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-[var(--text-muted)] max-w-xl">
          Operational dashboard for cluster quality, size distribution, and risk flagging.
          Data from the training run: 52,686 records resolved into 36,125 entity clusters.
        </p>
      </motion.div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4 mb-8">
        {KPI_CARDS.map((kpi, i) => (
          <motion.div
            key={kpi.label}
            className="metric-card"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: i * STAGGER_MS, ...spring }}
          >
            <kpi.icon size={14} className={`${kpi.color} mb-2`} />
            <span className={`metric-value text-lg ${kpi.color}`}>{kpi.value}</span>
            <span className="metric-label">{kpi.label}</span>
          </motion.div>
        ))}
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        {/* Size distribution */}
        <motion.div
          className="glass-card"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1, ...spring }}
        >
          <h3 className="section-label mb-3">Cluster size distribution</h3>
          <Suspense fallback={<div className="h-[280px] skeleton" />}>
            <Plot
              data={[{
                type: 'bar',
                x: SIZE_DIST.labels,
                y: SIZE_DIST.values,
                marker: {
                  color: SIZE_DIST.values.map((_, i) =>
                    i < 2 ? 'rgba(129, 140, 248, 0.6)' : i < 5 ? 'rgba(245, 158, 11, 0.6)' : 'rgba(239, 68, 68, 0.6)'
                  ),
                  line: { width: 0 },
                },
                hoverinfo: 'x+y',
              }]}
              layout={{
                height: 280,
                margin: { t: 8, b: 44, l: 56, r: 16 },
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                xaxis: { title: { text: 'Cluster size', font: { size: 11 } }, ...axisDefaults },
                yaxis: { title: { text: 'Count', font: { size: 11 } }, type: 'log', ...axisDefaults },
                font: plotlyFont,
                bargap: 0.15,
              }}
              config={plotlyConfig}
              style={{ width: '100%' }}
            />
          </Suspense>
        </motion.div>

        {/* Score distribution */}
        <motion.div
          className="glass-card"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.15, ...spring }}
        >
          <h3 className="section-label mb-3">DeBERTa score distribution (edges)</h3>
          <Suspense fallback={<div className="h-[280px] skeleton" />}>
            <Plot
              data={[{
                type: 'histogram',
                x: SCORE_DIST,
                marker: {
                  color: 'rgba(5, 150, 105, 0.5)',
                  line: { color: 'rgba(5, 150, 105, 0.7)', width: 1 },
                },
                nbinsx: 25,
                hoverinfo: 'x+y',
              }]}
              layout={{
                height: 280,
                margin: { t: 8, b: 44, l: 56, r: 16 },
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                xaxis: { title: { text: 'DeBERTa score', font: { size: 11 } }, range: [0, 1], ...axisDefaults },
                yaxis: { title: { text: 'Count', font: { size: 11 } }, ...axisDefaults },
                shapes: [
                  { type: 'line', x0: 0.45, x1: 0.45, y0: 0, y1: 1, yref: 'paper',
                    line: { color: '#059669', dash: 'dash', width: 1.5 } },
                  { type: 'line', x0: 0.30, x1: 0.30, y0: 0, y1: 1, yref: 'paper',
                    line: { color: '#b45309', dash: 'dash', width: 1.5 } },
                ],
                font: plotlyFont,
                bargap: 0.05,
              }}
              config={plotlyConfig}
              style={{ width: '100%' }}
            />
          </Suspense>
        </motion.div>
      </div>

      {/* Risk queue */}
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2, ...spring }}
      >
        <h3 className="section-label mb-4 flex items-center gap-2">
          <AlertTriangle size={14} className="text-amber-400" />
          Risk review queue
        </h3>
        <div className="glass-card overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left">
                <th className="table-header text-xs">Record ID</th>
                <th className="table-header text-xs">Cluster ID</th>
                <th className="table-header text-xs text-right">Cluster size</th>
                <th className="table-header text-xs text-right">Composite score</th>
                <th className="table-header text-xs text-right">Risk level</th>
              </tr>
            </thead>
            <tbody>
              {RISK_QUEUE.map((r, i) => (
                <motion.tr
                  key={r.record_id}
                  className="table-row"
                  initial={{ opacity: 0, x: -6 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.25 + i * 0.03, duration: dur.fast, ease: easeOut }}
                >
                  <td className="py-3 pr-4 font-mono text-xs text-[var(--text-secondary)]">{r.record_id}</td>
                  <td className="py-3 pr-4 font-mono text-xs text-[var(--text-faint)]">{r.cluster_id}</td>
                  <td className="py-3 pr-4 text-right font-mono tabular-nums text-[var(--text-secondary)]">{r.cluster_size}</td>
                  <td className="py-3 pr-4 text-right font-mono tabular-nums text-[var(--text-primary)]">
                    {r.composite.toFixed(3)}
                  </td>
                  <td className="py-3 text-right">
                    <span className={`text-[10px] font-medium px-2 py-1 rounded ${
                      r.risk === 'high' ? 'bg-red-500/15 text-red-400' :
                      r.risk === 'medium' ? 'bg-amber-500/15 text-amber-400' :
                      'bg-emerald-500/15 text-emerald-400'
                    }`}>
                      {r.risk}
                    </span>
                  </td>
                </motion.tr>
              ))}
            </tbody>
          </table>
        </div>
      </motion.div>
    </div>
  );
}
