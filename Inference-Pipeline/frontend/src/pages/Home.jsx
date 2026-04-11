import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import {
  GitCompare, Share2, Upload, BarChart3,
  ArrowRight, Cpu, Database, Globe, GitMerge, User, Eye, Shield, TrendingUp,
} from 'lucide-react';
import SpotlightCard from '../components/SpotlightCard';
import { spring, stagger, fadeUp, easeOut, dur } from '../motion';

const FEATURES = [
  { to: '/match',       icon: GitCompare, title: 'Pairwise matching',      desc: 'Compare two entity records using semantic scoring and deterministic rules with field-level attribution.' },
  { to: '/batch',       icon: Upload,     title: 'Batch processing',       desc: 'Upload CSV files and resolve thousands of records with real-time progress and cluster assignment.' },
  { to: '/clusters',    icon: GitMerge,   title: 'Cluster viewer',         desc: 'Visualize transitive closure — see how records connect through intermediary matches.' },
  { to: '/customer360', icon: User,       title: 'Customer 360',           desc: 'Unified entity view across CRM, billing, and fraud databases with golden record merge.' },
  { to: '/kyc',         icon: Eye,        title: 'KYC 2-hop',              desc: '2-hop network traversal to surface hidden connections to flagged entities.' },
  { to: '/fraud',       icon: Shield,     title: 'Fraud ring detection',   desc: 'Detect shared fields across different identities — synthetic identity and account takeover signals.' },
  { to: '/analytics',   icon: TrendingUp, title: 'Cluster analytics',      desc: 'Size distribution, singleton rates, risk scoring, and review queue for cluster quality.' },
  { to: '/pipeline',    icon: BarChart3,  title: 'Application metrics',    desc: 'F1, precision, recall, AUC from model training plus quality gates and bias reports.' },
];

const STAGES = [
  { label: 'Data Pipeline',      sub: 'Airflow + DVC + GCS',   icon: Database },
  { label: 'Model Pipeline',     sub: 'Vertex AI + MLflow',     icon: Cpu },
  { label: 'Inference Pipeline',  sub: 'FastAPI + React',        icon: Globe },
];

export default function Home() {
  return (
    <div className="page-container">

      {/* -- Hero -------------------------------------------- */}
      <motion.section
        className="relative pt-6 pb-14"
        initial={{ opacity: 0, y: 24 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.slow, ease: easeOut }}
      >
        <div
          className="pointer-events-none absolute -top-20 left-1/4 h-64 w-[28rem] rounded-full opacity-[0.06]"
          style={{ background: 'radial-gradient(circle, #c2410c, transparent 70%)' }}
        />

        <div className="relative max-w-3xl">
          <h1 className="mt-7 font-display text-4xl md:text-5xl lg:text-6xl font-medium leading-[1.08] tracking-tight text-[var(--text-primary)]">
            <span className="italic text-[var(--accent)]">Entity resolution</span>
            <br />
            for identity matching
          </h1>

          <p className="mt-5 text-xl font-semibold text-[var(--text-primary)]">
            Entity Matching + Context Engine
          </p>

          <p className="mt-4 max-w-2xl text-lg leading-relaxed text-[var(--text-secondary)]" style={{ textWrap: 'pretty' }}>
            Semantic identity matching that resolves duplicates, unifies records,
            and detects fraud across heterogeneous data sources.
          </p>
        </div>
      </motion.section>

      {/* -- Feature cards ----------------------------------- */}
      <section className="mb-14">
        <h2 className="font-display text-3xl font-medium tracking-tight text-[var(--text-primary)] mb-8">
          Explore the application
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

      {/* -- Architecture (moved down) ----------------------- */}
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

      {/* -- Team -------------------------------------------- */}
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
