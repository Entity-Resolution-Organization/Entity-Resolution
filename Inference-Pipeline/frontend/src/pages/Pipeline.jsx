import { useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import {
  BarChart3, Cpu, Database, Layers, Settings, Loader2,
  AlertTriangle, Target, TrendingUp, Award, Activity,
} from 'lucide-react';
import { getPipelineMetrics } from '../api/client';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

function MetricRing({ value, label, icon: Icon, color, delay = 0 }) {
  const pct = value != null ? (value * 100) : null;
  const radius = 36;
  const circumference = 2 * Math.PI * radius;
  const offset = pct != null ? circumference * (1 - value) : circumference;

  return (
    <motion.div
      className="metric-card py-6"
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay, ...spring }}
    >
      <div className="relative flex h-20 w-20 items-center justify-center mb-3">
        <svg className="absolute inset-0" viewBox="0 0 80 80">
          <circle cx="40" cy="40" r={radius} fill="none" stroke="rgba(0,0,0,0.06)" strokeWidth="4" />
          {pct != null && (
            <motion.circle
              cx="40" cy="40" r={radius}
              fill="none"
              stroke={color}
              strokeWidth="4"
              strokeLinecap="round"
              strokeDasharray={circumference}
              initial={{ strokeDashoffset: circumference }}
              animate={{ strokeDashoffset: offset }}
              transition={{ duration: dur.slow + 0.4, delay: delay + 0.2, ease: easeOut }}
              transform="rotate(-90 40 40)"
            />
          )}
        </svg>
        <span className="text-lg font-bold text-[var(--text-primary)] tabular-nums">
          {pct != null ? `${pct.toFixed(1)}%` : '--'}
        </span>
      </div>
      <div className="flex items-center gap-1.5">
        <Icon size={13} className="text-[var(--text-muted)]" aria-hidden="true" />
        <span className="metric-label">{label}</span>
      </div>
    </motion.div>
  );
}

export default function Pipeline() {
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    getPipelineMetrics()
      .then(r => setMetrics(r.data))
      .catch(() => setError('Unable to load pipeline metrics.'))
      .finally(() => setLoading(false));
  }, []);

  const m = metrics?.model_metrics || {};
  const qg = metrics?.quality_gate || {};
  const bias = metrics?.bias_report || {};

  if (loading) {
    return (
      <div className="page-container flex items-center justify-center py-32">
        <Loader2 size={24} className="animate-spin text-[var(--accent)]" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="page-container">
        <div className="mt-12 flex items-center gap-3 rounded-xl border border-red-200 bg-red-50 p-5">
          <AlertTriangle size={18} className="text-red-600 shrink-0" aria-hidden="true" />
          <p className="text-sm text-red-700">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="page-container">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-2xl md:text-3xl font-bold tracking-tight text-[var(--text-primary)]">Pipeline metrics</h1>
        <p className="mt-2 text-sm text-[var(--text-muted)]">
          Model performance, quality gates, and training pipeline configuration.
        </p>
      </div>

      {/* KPI rings */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <MetricRing value={m.test_f1} label="F1 Score" icon={Target} color="#c2410c" delay={0} />
        <MetricRing value={m.test_precision} label="Precision" icon={TrendingUp} color="#c2410c" delay={0.05} />
        <MetricRing value={m.test_recall} label="Recall" icon={Award} color="#c2410c" delay={0.1} />
        <MetricRing value={m.test_auc} label="AUC" icon={Activity} color="#c2410c" delay={0.15} />
      </div>

      {/* Quality Gate */}
      {qg && Object.keys(qg).length > 0 && (
        <motion.div
          className="glass-card mb-6"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2, ...spring }}
        >
          <h2 className="mb-4 text-[11px] font-semibold uppercase tracking-[0.12em] text-[var(--text-muted)]">
            Quality Gate
          </h2>
          <div className="grid grid-cols-2 gap-x-8 gap-y-2 text-sm">
            {Object.entries(qg).map(([k, v]) => (
              <div key={k} className="flex justify-between border-b border-black/[0.04] py-2">
                <span className="text-[var(--text-muted)]">{k.replace(/_/g, ' ')}</span>
                <span className="font-medium text-[var(--text-primary)]">{typeof v === 'number' ? v.toFixed(4) : String(v)}</span>
              </div>
            ))}
          </div>
        </motion.div>
      )}

      {/* Model details */}
      <motion.div
        className="glass-card"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.25, ...spring }}
      >
        <div className="flex items-center gap-2 mb-5">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-[var(--accent-dim)]">
            <Settings size={16} className="text-[var(--accent)]" aria-hidden="true" />
          </div>
          <h2 className="text-sm font-semibold text-[var(--text-primary)]">Model configuration</h2>
        </div>
        <div className="grid grid-cols-2 gap-x-10 gap-y-1 text-sm">
          {[
            ['Base Model', 'microsoft/deberta-v3-base', Cpu],
            ['Fine-tuning', 'LoRA (r=8, alpha=16)', Layers],
            ['Target Modules', 'query_proj, value_proj', Settings],
            ['Training Data', '130K pairs (3 sources)', Database],
            ['Threshold', '0.45', BarChart3],
            ['Serving', 'Vertex AI (n1-standard-4)', Cpu],
          ].map(([k, v, Icon]) => (
            <div key={k} className="flex items-center justify-between border-b border-black/[0.04] py-2.5">
              <div className="flex items-center gap-2">
                <Icon size={13} className="text-[var(--text-muted)]" aria-hidden="true" />
                <span className="text-[var(--text-muted)]">{k}</span>
              </div>
              <span className="font-medium text-[var(--text-primary)] font-mono text-xs">{v}</span>
            </div>
          ))}
        </div>
      </motion.div>

      {/* Bias Report */}
      {bias && Object.keys(bias).length > 0 && (
        <motion.div
          className="glass-card mt-6"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3, ...spring }}
        >
          <h2 className="mb-4 text-[11px] font-semibold uppercase tracking-[0.12em] text-[var(--text-muted)]">
            Bias Report
          </h2>
          <div className="grid grid-cols-2 gap-x-8 gap-y-2 text-sm">
            {Object.entries(bias).map(([k, v]) => (
              <div key={k} className="flex justify-between border-b border-black/[0.04] py-2">
                <span className="text-[var(--text-muted)]">{k.replace(/_/g, ' ')}</span>
                <span className="font-medium text-[var(--text-primary)]">{typeof v === 'number' ? v.toFixed(4) : String(v)}</span>
              </div>
            ))}
          </div>
        </motion.div>
      )}
    </div>
  );
}
