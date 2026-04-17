import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';
import {
  GitCompare, Upload,
  ArrowRight, GitMerge, User, Eye, Shield, TrendingUp,
} from 'lucide-react';
import SpotlightCard from '../components/SpotlightCard';
import HeroAnimation from '../components/HeroAnimation';
import { stagger, fadeUp, easeOut, dur } from '../motion';

const FEATURES = [
  { to: '/match',       icon: GitCompare, title: 'Pairwise matching',      desc: 'Compare two entity records using semantic scoring and deterministic rules with field-level attribution.' },
  { to: '/batch',       icon: Upload,     title: 'Batch processing',       desc: 'Upload CSV files and resolve thousands of records with real-time progress and cluster assignment.' },
  { to: '/clusters',    icon: GitMerge,   title: 'Cluster viewer',         desc: 'Visualize transitive closure — see how records connect through intermediary matches.' },
  { to: '/customer360', icon: User,       title: 'Customer 360',           desc: 'Unified entity view across CRM, billing, and fraud databases with golden record merge.' },
  { to: '/kyc',         icon: Eye,        title: 'KYC 2-hop',              desc: '2-hop network traversal to surface hidden connections to flagged entities.' },
  { to: '/fraud',       icon: Shield,     title: 'Fraud ring detection',   desc: 'Detect shared fields across different identities — synthetic identity and account takeover signals.' },
  { to: '/analytics',   icon: TrendingUp, title: 'Cluster analytics',      desc: 'Size distribution, singleton rates, risk scoring, and review queue for cluster quality.' },
];

export default function Home() {
  return (
    <div className="page-container">

      {/* -- Hero -------------------------------------------- */}
      <motion.section
        className="relative pt-0 pb-10"
        initial={{ opacity: 0, y: 24 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.slow, ease: easeOut }}
      >
        <div
          className="pointer-events-none absolute -top-20 left-1/4 h-64 w-[28rem] rounded-full opacity-[0.06]"
          style={{ background: 'radial-gradient(circle, #c2410c, transparent 70%)' }}
        />

        <div className="relative flex flex-col lg:flex-row lg:items-center gap-10 lg:gap-10">
          <div className="lg:flex-1 min-w-0">
            <h1 className="mt-0 font-display text-4xl md:text-5xl lg:text-6xl font-medium leading-[1.08] tracking-tight text-[var(--text-primary)]">
              <span className="italic text-[var(--accent)]">Resolv</span>
              <br />
              Entity Resolution
            </h1>

            <p className="mt-5 text-xl font-semibold text-[var(--text-primary)]">
              Entity Matching + Context Engine
            </p>

            <p className="mt-4 max-w-xl text-lg leading-relaxed text-[var(--text-secondary)]" style={{ textWrap: 'pretty' }}>
              Semantic identity matching that resolves duplicates, unifies records,
              and detects fraud across heterogeneous data sources.
            </p>
          </div>

          <div className="hidden lg:flex lg:w-[560px] lg:shrink-0 justify-end overflow-hidden lg:-translate-x-16">
            <HeroAnimation />
          </div>
        </div>
      </motion.section>

      {/* -- Feature cards ----------------------------------- */}
      <section className="mb-14">
        <h2 className="font-display text-3xl font-medium tracking-tight text-[var(--text-primary)] mb-8 text-center">
          Explore the application
        </h2>

        <motion.div
          className="flex flex-wrap justify-center gap-4"
          variants={stagger}
          initial="hidden"
          animate="visible"
        >
          {FEATURES.map(({ to, icon: Icon, title, desc }) => (
            <motion.div key={to} variants={fadeUp} className="flex w-full md:w-[calc(50%-8px)] lg:w-[calc(33.333%-11px)]">
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

    </div>
  );
}
