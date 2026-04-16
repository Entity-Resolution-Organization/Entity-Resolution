import { motion } from 'framer-motion';
import {
  BarChart3, Users, GitMerge, AlertTriangle, TrendingUp, Database,
} from 'lucide-react';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

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

const RISK_QUEUE = [
  { record_id: 'REC-44281', cluster_id: 'c9a1e3f2', cluster_size: 426, composite: 0.146, risk: 'medium' },
  { record_id: 'REC-12093', cluster_id: 'b7d4c8a1', cluster_size: 38, composite: 0.132, risk: 'medium' },
  { record_id: 'REC-33847', cluster_id: 'e5f2a9d3', cluster_size: 22, composite: 0.118, risk: 'low' },
  { record_id: 'REC-09182', cluster_id: 'a2c7b4e8', cluster_size: 15, composite: 0.095, risk: 'low' },
  { record_id: 'REC-55102', cluster_id: 'f8d1c3a7', cluster_size: 12, composite: 0.088, risk: 'low' },
  { record_id: 'REC-71839', cluster_id: 'd4e9f2b6', cluster_size: 9, composite: 0.072, risk: 'low' },
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
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4 mb-8">
        {[
          { label: 'Total records', value: METRICS.n_records.toLocaleString(), Icon: Database, color: 'text-blue-400' },
          { label: 'Clusters formed', value: METRICS.n_clusters.toLocaleString(), Icon: GitMerge, color: 'text-[var(--accent)]' },
          { label: 'Singleton rate', value: `${(METRICS.singleton_rate * 100).toFixed(1)}%`, Icon: Users, color: 'text-stone-400' },
          { label: 'Avg cluster size', value: METRICS.cluster_size_mean.toFixed(2), Icon: TrendingUp, color: 'text-emerald-400' },
          { label: 'Max cluster', value: METRICS.cluster_size_max, Icon: AlertTriangle, color: 'text-amber-400' },
        ].map((kpi, i) => (
          <motion.div
            key={kpi.label}
            className="metric-card"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: i * STAGGER_MS, ...spring }}
          >
            <kpi.Icon size={14} className={`${kpi.color} mb-2`} />
            <span className={`metric-value text-lg ${kpi.color}`}>{kpi.value}</span>
            <span className="metric-label">{kpi.label}</span>
          </motion.div>
        ))}
      </div>

      {/* Size distribution — simple bar visualization */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        <motion.div
          className="glass-card"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1, ...spring }}
        >
          <h3 className="section-label mb-4">Cluster size distribution</h3>
          <div className="space-y-2.5">
            {[
              { label: '1 (singleton)', value: 24283, pct: 67.2 },
              { label: '2', value: 9842, pct: 27.2 },
              { label: '3', value: 1200, pct: 3.3 },
              { label: '4', value: 480, pct: 1.3 },
              { label: '5', value: 180, pct: 0.5 },
              { label: '6-10', value: 100, pct: 0.3 },
              { label: '11-50', value: 35, pct: 0.1 },
              { label: '50+', value: 5, pct: 0.01 },
            ].map((row, i) => (
              <div key={row.label} className="flex items-center gap-3 text-xs">
                <span className="w-20 text-right font-mono text-[var(--text-muted)] tabular-nums">{row.label}</span>
                <div className="flex-1 h-5 rounded bg-white/[0.04] overflow-hidden">
                  <motion.div
                    className="h-full rounded"
                    style={{ background: i < 2 ? 'rgba(129,140,248,0.5)' : i < 5 ? 'rgba(245,158,11,0.4)' : 'rgba(239,68,68,0.4)' }}
                    initial={{ width: 0 }}
                    animate={{ width: `${Math.max(row.pct, 0.5)}%` }}
                    transition={{ delay: 0.15 + i * 0.05, duration: dur.slow, ease: easeOut }}
                  />
                </div>
                <span className="w-16 text-right font-mono tabular-nums text-[var(--text-secondary)]">{row.value.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </motion.div>

        {/* Key metrics card */}
        <motion.div
          className="glass-card"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.15, ...spring }}
        >
          <h3 className="section-label mb-4">Edge quality</h3>
          <div className="space-y-4">
            {[
              { label: 'Cluster edge threshold', value: '0.45', bar: 45, color: 'bg-amber-500' },
              { label: 'Keep threshold', value: '0.30', bar: 30, color: 'bg-red-500' },
            ].map((m, i) => (
              <div key={m.label}>
                <div className="flex justify-between mb-1.5">
                  <span className="text-xs text-[var(--text-muted)]">{m.label}</span>
                  <span className="text-xs font-mono font-semibold tabular-nums text-[var(--text-primary)]">{m.value}</span>
                </div>
                <div className="h-2 rounded-full bg-white/[0.06] overflow-hidden">
                  <motion.div
                    className={`h-full rounded-full ${m.color}`}
                    initial={{ width: 0 }}
                    animate={{ width: `${m.bar}%` }}
                    transition={{ delay: 0.2 + i * 0.08, duration: dur.slow, ease: easeOut }}
                  />
                </div>
              </div>
            ))}
            <div className="pt-4 border-t border-white/[0.06] grid grid-cols-2 gap-4">
              <div>
                <p className="text-[10px] uppercase tracking-wider text-[var(--text-faint)]">Total edges</p>
                <p className="text-lg font-bold font-mono tabular-nums text-[var(--text-primary)]">{METRICS.n_cluster_edges.toLocaleString()}</p>
              </div>
              <div>
                <p className="text-[10px] uppercase tracking-wider text-[var(--text-faint)]">Vetoed edges</p>
                <p className="text-lg font-bold font-mono tabular-nums text-[var(--text-primary)]">{METRICS.n_vetoed_edges}</p>
              </div>
            </div>
          </div>
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
