import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import {
  GitCompare, LayoutGrid, Share2, Upload, BarChart3, Activity,
  CheckCircle2, XCircle, ArrowRight, Cpu, Database, Globe,
} from 'lucide-react';
import { getHealth } from '../api/client';
import SpotlightCard from '../components/SpotlightCard';
import { spring, stagger, fadeUp, easeOut, dur } from '../motion';

const FEATURES = [
  { to: '/resolve',   icon: GitCompare, title: 'Pairwise resolution',   desc: 'Compare two entity records using DeBERTa semantic scoring and deterministic rules with field-level attribution.' },
  { to: '/scenarios', icon: LayoutGrid, title: 'Scenario gallery',      desc: 'Pre-built test cases across typos, nicknames, abbreviations, non-ASCII names, and address variants.' },
  { to: '/network',   icon: Share2,     title: 'Entity network',        desc: 'Force-directed graph of entity clusters resolved across heterogeneous data sources.' },
  { to: '/batch',     icon: Upload,     title: 'Batch processing',      desc: 'Upload CSV files, map columns, and resolve thousands of pairs with real-time progress.' },
  { to: '/pipeline',  icon: BarChart3,  title: 'Pipeline metrics',      desc: 'F1, precision, recall, AUC from model training plus quality gates and bias reports.' },
  { to: '/monitor',   icon: Activity,   title: 'Inference monitor',     desc: 'Live request counts, latency percentiles, and decision distribution across match/review/no-match.' },
];

const STAGES = [
  { label: 'Data Pipeline',      sub: 'Airflow + DVC + GCS',   icon: Database },
  { label: 'Model Pipeline',     sub: 'Vertex AI + MLflow',     icon: Cpu },
  { label: 'Inference Pipeline',  sub: 'FastAPI + React',        icon: Globe },
];

const STATS = [
  { value: '130K+',  label: 'Training pairs' },
  { value: '3',      label: 'Data sources' },
  { value: '<200ms', label: 'Avg latency' },
  { value: '0.45',   label: 'Match threshold' },
];

export default function Home() {
  const [health, setHealth] = useState(null);
  const [healthLoading, setHealthLoading] = useState(true);

  useEffect(() => {
    getHealth()
      .then(r => setHealth(r.data))
      .catch(() => setHealth(null))
      .finally(() => setHealthLoading(false));
  }, []);

  return (
    <div className="page-container">

      {/* ── Hero ──────────────────────────────────────── */}
      <motion.section
        className="relative pt-6 pb-14"
        initial={{ opacity: 0, y: 24 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.slow, ease: easeOut }}
      >
        {/* Ambient glow */}
        <div
          className="pointer-events-none absolute -top-20 left-1/4 h-64 w-[28rem] rounded-full opacity-[0.06]"
          style={{ background: 'radial-gradient(circle, #c2410c, transparent 70%)' }}
        />

        <div className="relative max-w-3xl">
          {/* Chip */}
          <div className="inline-flex items-center gap-2 rounded-full border border-black/[0.08] bg-white/60 px-4 py-1.5 text-xs text-[var(--text-muted)]">
            <span className="h-1.5 w-1.5 rounded-full bg-[var(--accent)]" />
            DeBERTa-v3 + LoRA on Google Cloud Vertex AI
          </div>

          {/* Display heading — serif */}
          <h1 className="mt-7 font-display text-4xl md:text-5xl lg:text-6xl font-medium leading-[1.08] tracking-tight text-[var(--text-primary)]">
            <span className="italic text-[var(--accent)]">Entity resolution</span>
            <br />
            for identity matching
          </h1>

          <p className="mt-6 max-w-2xl text-lg leading-relaxed text-[var(--text-secondary)]" style={{ textWrap: 'pretty' }}>
            Semantic identity matching that resolves duplicates, unifies records,
            and detects fraud across heterogeneous data sources.
          </p>

          {/* Status + CTA */}
          <div className="mt-8 flex items-center gap-4">
            <Link to="/resolve" className="btn-primary inline-flex items-center gap-2">
              <span>Try it now</span>
              <ArrowRight size={15} aria-hidden="true" />
            </Link>

            {healthLoading ? (
              <div className="inline-flex items-center gap-2 rounded-full border border-black/[0.06] bg-white/50 px-4 py-2">
                <div className="h-2 w-2 rounded-full bg-[var(--text-faint)] animate-pulse" />
                <span className="text-sm text-[var(--text-muted)]">Checking API</span>
              </div>
            ) : health ? (
              <motion.div
                className="inline-flex items-center gap-2 rounded-full border border-emerald-600/15 bg-emerald-50 px-4 py-2"
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ ...spring, delay: 0.3 }}
              >
                <CheckCircle2 size={15} className="text-emerald-600" aria-hidden="true" />
                <span className="text-sm font-medium text-emerald-700">API online</span>
                <span className="text-sm text-emerald-600/50">·</span>
                <span className="text-sm text-emerald-600/50">{health.model_client || health.client_type}</span>
              </motion.div>
            ) : (
              <div className="inline-flex items-center gap-2 rounded-full border border-red-600/15 bg-red-50 px-4 py-2">
                <XCircle size={15} className="text-red-600" aria-hidden="true" />
                <span className="text-sm font-medium text-red-700">API offline</span>
              </div>
            )}
          </div>
        </div>
      </motion.section>

      {/* ── Stats row ─────────────────────────────────── */}
      <motion.section
        className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-14"
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.15, ...spring }}
      >
        {STATS.map((s, i) => (
          <motion.div
            key={s.label}
            className="metric-card py-6"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 + i * 0.05, ...spring }}
          >
            <span className="metric-value text-2xl">{s.value}</span>
            <span className="metric-label">{s.label}</span>
          </motion.div>
        ))}
      </motion.section>

      {/* ── Architecture ──────────────────────────────── */}
      <motion.section
        className="glass-card-hero mb-14"
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.25, ...spring }}
      >
        <h2 className="section-label mb-6">System architecture</h2>
        <div className="flex flex-col md:flex-row items-stretch md:items-center justify-between gap-3">
          {STAGES.map((stage, i) => (
            <div key={stage.label} className="flex flex-1 items-center gap-3">
              <div className="flex flex-1 items-center gap-4 rounded-xl border border-black/[0.06] bg-white/50 p-5 transition-colors hover:bg-white/80 hover:border-black/[0.1]">
                <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-[var(--accent-dim)]">
                  <stage.icon size={20} className="text-[var(--accent)]" aria-hidden="true" />
                </div>
                <div>
                  <p className="text-sm font-semibold text-[var(--text-primary)]">{stage.label}</p>
                  <p className="mt-0.5 text-xs text-[var(--text-muted)]">{stage.sub}</p>
                </div>
              </div>
              {i < 2 && (
                <ArrowRight size={16} className="shrink-0 text-[var(--text-faint)] hidden md:block" aria-hidden="true" />
              )}
            </div>
          ))}
        </div>
      </motion.section>

      {/* ── Feature cards — uniform 3x2 grid ────────────── */}
      <section className="mb-14">
        <h2 className="font-display text-3xl font-medium tracking-tight text-[var(--text-primary)] mb-8">
          Explore the pipeline
        </h2>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 items-stretch"
          variants={stagger}
          initial="hidden"
          animate="visible"
        >
          {FEATURES.map(({ to, icon: Icon, title, desc }) => (
            <motion.div key={to} variants={fadeUp} className="flex">
              <SpotlightCard
                as={Link}
                to={to}
                className="group glass-card-hover flex h-full w-full flex-col no-underline"
              >
                <div className="relative z-10 flex flex-1 flex-col">
                  <div className="mb-4 flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-[var(--accent-dim)]">
                    <Icon size={20} className="text-[var(--accent)]" aria-hidden="true" />
                  </div>
                  <h3 className="text-[15px] font-semibold text-[var(--text-primary)]">{title}</h3>
                  <p className="mt-1.5 text-sm leading-relaxed text-[var(--text-muted)] line-clamp-2">{desc}</p>
                  <div className="mt-auto pt-4 flex items-center gap-1.5 text-xs font-medium text-[var(--text-faint)] transition-colors group-hover:text-[var(--accent)]">
                    <span>Explore</span>
                    <ArrowRight size={12} className="transition-transform group-hover:translate-x-0.5" aria-hidden="true" />
                  </div>
                </div>
              </SpotlightCard>
            </motion.div>
          ))}
        </motion.div>
      </section>

      {/* ── Team ──────────────────────────────────────── */}
      <motion.footer
        className="pt-8 pb-10 border-t border-[var(--border-subtle)] text-center"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.6 }}
      >
        <p className="text-[11px] font-medium uppercase tracking-[0.1em] text-[var(--text-faint)]">
          Group 13 · Northeastern University · MLOps Spring 2026
        </p>
        <p className="mt-2 text-sm text-[var(--text-muted)]">
          Aparup · Sushritha · Sai Pranav · Nishi · Gouri · Fatima
        </p>
      </motion.footer>
    </div>
  );
}
