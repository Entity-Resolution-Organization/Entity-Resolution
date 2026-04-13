import { motion } from 'framer-motion';
import {
  ScanSearch, Share2, BarChart3, AlertTriangle, Network, Cpu, Database,
} from 'lucide-react';
import { spring, STAGGER_MS } from '../motion';

const FEATURES = [
  {
    title: 'Transaction Graph Analysis',
    desc: 'GraphSAGE processes multi-hop relationships between entities and transactions to identify suspicious patterns.',
    icon: Network,
  },
  {
    title: 'Risk Scoring',
    desc: 'Each entity receives a fraud probability score based on graph embeddings and transaction features.',
    icon: BarChart3,
  },
  {
    title: 'Cluster Detection',
    desc: 'Automated detection of suspicious entity clusters exhibiting coordinated fraudulent behavior.',
    icon: Share2,
  },
];

export default function Fraud() {
  return (
    <div className="page-container">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-3xl font-bold tracking-tight text-[var(--text-primary)]">Fraud detection (GNN)</h1>
        <p className="mt-2 text-sm text-[var(--text-muted)]">
          GraphSAGE-based fraud detection on transaction networks.
        </p>
      </div>

      {/* Status banner */}
      <motion.div
        className="glass-card flex flex-col items-center justify-center py-14 text-center mb-8"
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={spring}
      >
        <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-[var(--accent-dim)] border border-[var(--border-accent)] mb-5">
          <ScanSearch size={28} className="text-[var(--accent)]" aria-hidden="true" />
        </div>
        <h2 className="text-xl font-semibold text-[var(--text-primary)] mb-2">GraphSAGE integration in progress</h2>
        <p className="max-w-lg text-sm text-[var(--text-muted)] leading-relaxed">
          The GNN model is being trained on transaction graph data. Once integrated,
          this page will display real-time fraud detection results, risk scores,
          and suspicious entity clusters.
        </p>

        {/* Placeholder metrics */}
        <div className="mt-8 flex flex-wrap gap-4">
          {[
            { label: 'Fraud Rate', icon: AlertTriangle },
            { label: 'Suspicious Entities', icon: Database },
            { label: 'Flagged Txns', icon: Cpu },
          ].map((m, i) => (
            <motion.div
              key={m.label}
              className="metric-card px-8 py-5"
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 + i * STAGGER_MS, ...spring }}
            >
              <m.icon size={16} className="text-[var(--text-muted)] mb-2" aria-hidden="true" />
              <span className="metric-value text-xl text-[var(--text-muted)]">--</span>
              <span className="metric-label">{m.label}</span>
            </motion.div>
          ))}
        </div>
      </motion.div>

      {/* Feature cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {FEATURES.map((f, i) => (
          <motion.div
            key={f.title}
            className="glass-card"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 + i * STAGGER_MS, ...spring }}
          >
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-black/[0.02] mb-3">
              <f.icon size={18} className="text-[var(--text-muted)]" aria-hidden="true" />
            </div>
            <h3 className="text-sm font-semibold text-[var(--text-primary)] mb-1.5">{f.title}</h3>
            <p className="text-xs text-[var(--text-muted)] leading-relaxed">{f.desc}</p>
          </motion.div>
        ))}
      </div>
    </div>
  );
}
