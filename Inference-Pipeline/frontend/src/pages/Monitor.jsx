import { useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import {
  Activity, Clock, Check, X, Loader2, AlertTriangle, RefreshCw,
  BarChart3, Zap,
} from 'lucide-react';
import { getInferenceMetrics } from '../api/client';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

export default function Monitor() {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [refreshing, setRefreshing] = useState(false);

  const fetchStats = async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    try {
      const { data } = await getInferenceMetrics();
      setStats(data);
      setError(null);
    } catch {
      setError('Unable to load inference metrics.');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => { fetchStats(); }, []);

  if (loading) {
    return (
      <div className="page-container flex items-center justify-center py-32">
        <Loader2 size={24} className="animate-spin text-[var(--accent)]" />
      </div>
    );
  }

  return (
    <div className="page-container">
      {/* Header */}
      <div className="flex items-start justify-between mb-8">
        <div>
          <h1 className="text-2xl md:text-3xl font-bold tracking-tight text-[var(--text-primary)]">Inference monitor</h1>
          <p className="mt-2 text-sm text-[var(--text-muted)]">
            Real-time inference statistics, latency tracking, and decision distribution.
          </p>
        </div>
        <button
          onClick={() => fetchStats(true)}
          disabled={refreshing}
          className="btn-secondary flex items-center gap-2"
        >
          <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} aria-hidden="true" />
          Refresh
        </button>
      </div>

      {/* Error */}
      {error && (
        <div className="mb-6 flex items-center gap-3 rounded-xl border border-red-200 bg-red-50 p-4">
          <AlertTriangle size={16} className="text-red-600 shrink-0" aria-hidden="true" />
          <p className="text-sm text-red-700">{error}</p>
        </div>
      )}

      {stats && (
        <>
          {/* KPI cards */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
            {[
              { label: 'Total Requests', value: stats.request_count, icon: Zap, color: 'text-[var(--accent)]' },
              { label: 'Avg Latency', value: `${stats.avg_latency_ms?.toFixed(0) || '--'}ms`, icon: Clock, color: 'text-[var(--accent)]' },
              { label: 'Matches', value: stats.decision_distribution?.MATCH ?? stats.match_count ?? 0, icon: Check, color: 'text-emerald-600' },
              { label: 'No Match', value: stats.decision_distribution?.['NO-MATCH'] ?? stats.no_match_count ?? 0, icon: X, color: 'text-red-600' },
            ].map((kpi, i) => {
              const Icon = kpi.icon;
              return (
                <motion.div
                  key={kpi.label}
                  className="metric-card"
                  initial={{ opacity: 0, y: 12 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: i * STAGGER_MS, ...spring }}
                >
                  <div className={`flex h-9 w-9 items-center justify-center rounded-lg bg-black/[0.02] mb-3`}>
                    <Icon size={18} className={kpi.color} aria-hidden="true" />
                  </div>
                  <span className={`metric-value ${kpi.color}`}>
                    {typeof kpi.value === 'number' ? kpi.value.toLocaleString() : kpi.value}
                  </span>
                  <span className="metric-label">{kpi.label}</span>
                </motion.div>
              );
            })}
          </div>

          {/* Decision Distribution */}
          {stats.decision_distribution && (
            <motion.div
              className="glass-card mb-6"
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.25, ...spring }}
            >
              <h2 className="mb-4 text-[11px] font-semibold uppercase tracking-[0.12em] text-[var(--text-muted)]">
                Decision Distribution
              </h2>
              <div className="space-y-3">
                {[
                  { key: 'MATCH', color: 'bg-emerald-500', textColor: 'text-emerald-600' },
                  { key: 'REVIEW', color: 'bg-amber-500', textColor: 'text-amber-700' },
                  { key: 'NO-MATCH', color: 'bg-red-500', textColor: 'text-red-600' },
                ].map(({ key, color, textColor }) => {
                  const count = stats.decision_distribution[key] || 0;
                  const total = stats.request_count || 1;
                  const pct = (count / total) * 100;
                  return (
                    <div key={key}>
                      <div className="flex items-center justify-between text-xs mb-1.5">
                        <span className="text-[var(--text-muted)]">{key}</span>
                        <div className="flex items-center gap-2">
                          <span className="text-[var(--text-muted)]">{count}</span>
                          <span className={`font-semibold tabular-nums ${textColor}`}>{pct.toFixed(1)}%</span>
                        </div>
                      </div>
                      <div className="h-2 rounded-full bg-black/[0.02] overflow-hidden">
                        <motion.div
                          className={`h-full rounded-full ${color}`}
                          initial={{ width: 0 }}
                          animate={{ width: `${pct}%` }}
                          transition={{ duration: dur.slow, ease: easeOut }}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </motion.div>
          )}

          {/* Match Rate */}
          {stats.match_rate != null && (
            <motion.div
              className="glass-card"
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3, ...spring }}
            >
              <div className="flex items-center gap-2 mb-3">
                <BarChart3 size={16} className="text-[var(--text-muted)]" aria-hidden="true" />
                <h2 className="text-[11px] font-semibold uppercase tracking-[0.12em] text-[var(--text-muted)]">
                  Match Rate
                </h2>
              </div>
              <div className="flex items-baseline gap-2">
                <span className="text-3xl font-bold text-[var(--text-primary)] tabular-nums">
                  {(stats.match_rate * 100).toFixed(1)}%
                </span>
                <span className="text-sm text-[var(--text-muted)]">of resolved pairs</span>
              </div>
            </motion.div>
          )}
        </>
      )}

      {/* Bias / Quality Gate placeholder */}
      <motion.div
        className="glass-card mt-6"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.35, ...spring }}
      >
        <div className="flex items-center gap-2 mb-3">
          <Activity size={16} className="text-[var(--text-muted)]" aria-hidden="true" />
          <h2 className="text-[11px] font-semibold uppercase tracking-[0.12em] text-[var(--text-muted)]">
            Bias Detection & Quality Gate
          </h2>
        </div>
        <p className="text-sm text-[var(--text-muted)]">
          Bias report and quality gate results will be loaded from GCS metrics when available.
          Quality gates monitor F1, precision, and recall thresholds to prevent model degradation.
        </p>
      </motion.div>
    </div>
  );
}
