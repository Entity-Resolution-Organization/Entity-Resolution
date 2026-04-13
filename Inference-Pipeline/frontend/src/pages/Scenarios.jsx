import { useState, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Play, Loader2, Check, X, AlertTriangle, RotateCcw,
  ArrowRight, User, MapPin,
} from 'lucide-react';
import { resolveEntities } from '../api/client';
import {
  spring, springSnappy, stagger, fadeUp, STAGGER_MS, dur,
} from '../motion';
import SpotlightCard from '../components/SpotlightCard';

/* ── Scenario Data ─────────────────────────────────────── */

const SCENARIOS = [
  { title: 'Exact Match', name1: 'Robert Smith', addr1: '123 Main Street', name2: 'Robert Smith', addr2: '123 Main Street', desc: 'Identical records' },
  { title: 'Nickname', name1: 'Robert Smith', addr1: '123 Main Street', name2: 'Bob Smith', addr2: '123 Main Street', desc: 'Robert vs Bob' },
  { title: 'Typo', name1: 'Robert Smith', addr1: '123 Main Street', name2: 'Robrt Smith', addr2: '123 Main Street', desc: 'Missing letter' },
  { title: 'First Initial', name1: 'Robert Smith', addr1: '123 Main Street', name2: 'R. Smith', addr2: '123 Main Street', desc: 'Abbreviated first name' },
  { title: 'Name Reorder', name1: 'Robert Smith', addr1: '123 Main Street', name2: 'Smith Robert', addr2: '123 Main Street', desc: 'Last, First format' },
  { title: 'Address Abbrev', name1: 'John Doe', addr1: '123 Main Street', name2: 'John Doe', addr2: '123 Main St', desc: 'Street vs St' },
  { title: 'Person Moved', name1: 'Robert Smith', addr1: '123 Main St, NY', name2: 'Robert Smith', addr2: '789 Oak Ave, LA', desc: 'Different address' },
  { title: 'Non-ASCII', name1: 'Mohammad Al-Rashid', addr1: '45 Desert Rd', name2: 'Mohammed Al Rashid', addr2: '45 Desert Road', desc: 'Unicode variants' },
  { title: 'Middle Name', name1: 'Robert Smith', addr1: '123 Main Street', name2: 'Robert James Smith', addr2: '123 Main Street', desc: 'Extra middle name' },
  { title: 'Same Addr Diff Person', name1: 'Robert Smith', addr1: '123 Main Street', name2: 'James Wilson', addr2: '123 Main Street', desc: 'Different people' },
  { title: 'Unicode Accents', name1: 'Jose Garcia', addr1: '100 Calle Principal', name2: 'Jose Garcia', addr2: '100 Calle Principal', desc: 'Accent marks' },
  { title: 'Completely Different', name1: 'Robert Smith', addr1: '123 Main Street', name2: 'Maria Garcia', addr2: '456 Oak Avenue', desc: 'No similarity' },
];

/* ── Helpers ───────────────────────────────────────────── */

function StatusIcon({ decision }) {
  if (decision === 'MATCH') return <Check size={13} className="text-emerald-600" aria-hidden="true" />;
  if (decision === 'NO-MATCH') return <X size={13} className="text-red-600" aria-hidden="true" />;
  return <AlertTriangle size={13} className="text-amber-700" aria-hidden="true" />;
}

function badgeClass(decision) {
  if (decision === 'MATCH') return 'badge-match';
  if (decision === 'NO-MATCH') return 'badge-nomatch';
  return 'badge-review';
}

function borderColor(decision) {
  if (decision === 'MATCH') return 'border-emerald-200';
  if (decision === 'NO-MATCH') return 'border-red-200';
  return 'border-amber-200';
}

/* ── Mini Record Preview ───────────────────────────────── */

function RecordPreview({ name, address, side }) {
  const isLeft = side === 'left';
  return (
    <div className={`flex-1 min-w-0 rounded-lg px-3 py-2.5 ${
      isLeft ? 'bg-black/[0.025]' : 'bg-black/[0.025]'
    }`}>
      <div className="flex items-center gap-1.5 mb-1">
        <User size={11} className="text-[var(--text-faint)] shrink-0" aria-hidden="true" />
        <span className="text-[12.5px] font-semibold text-[var(--text-primary)] truncate">
          {name}
        </span>
      </div>
      <div className="flex items-center gap-1.5">
        <MapPin size={11} className="text-[var(--text-faint)] shrink-0" aria-hidden="true" />
        <span className="text-[11px] text-[var(--text-muted)] truncate">
          {address}
        </span>
      </div>
    </div>
  );
}

/* ── Scenario Card ─────────────────────────────────────── */

function ScenarioCard({ scenario, index, result, isRunning, isGlobalRunning, onRun }) {
  const hasResult = !!result;
  const canClick = !hasResult && !isRunning && !isGlobalRunning;

  return (
    <motion.div
      variants={fadeUp}
    >
      <SpotlightCard
        className={`glass-card border transition-colors duration-300 ${
          hasResult
            ? `${borderColor(result.decision)} cursor-default`
            : canClick
              ? 'border-black/[0.06] cursor-pointer hover:border-black/[0.12]'
              : 'border-black/[0.06] cursor-default'
        }`}
        onClick={canClick ? () => onRun(index) : undefined}
        role="button"
        tabIndex={0}
        aria-disabled={!canClick}
        onKeyDown={canClick ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onRun(index); } } : undefined}
      >
        {/* Header */}
        <div className="flex items-start justify-between mb-1">
          <div className="min-w-0 flex-1">
            <h3 className="text-sm font-semibold text-[var(--text-primary)] leading-snug">
              {scenario.title}
            </h3>
            <p className="text-[11px] text-[var(--text-muted)] mt-0.5">
              {scenario.desc}
            </p>
          </div>
          {isRunning && (
            <Loader2
              size={15}
              className="animate-spin text-[var(--accent)] shrink-0 ml-2 mt-0.5"
              aria-hidden="true"
            />
          )}
        </div>

        {/* Record Comparison */}
        <div className="flex items-center gap-2 mt-3">
          <RecordPreview name={scenario.name1} address={scenario.addr1} side="left" />
          <ArrowRight size={14} className="text-[var(--text-faint)] shrink-0" aria-hidden="true" />
          <RecordPreview name={scenario.name2} address={scenario.addr2} side="right" />
        </div>

        {/* Result Area */}
        <AnimatePresence>
          {hasResult && (
            <motion.div
              className="flex items-center justify-between mt-4 pt-3 border-t border-black/[0.06]"
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -4 }}
              transition={springSnappy}
            >
              <span className={`${badgeClass(result.decision)} text-xs`}>
                <StatusIcon decision={result.decision} />
                {result.decision}
              </span>
              <span className="text-xl font-bold tabular-nums text-[var(--text-primary)] tracking-tight font-mono">
                {(result.probability * 100).toFixed(1)}%
              </span>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Idle Prompt */}
        {!hasResult && !isRunning && !isGlobalRunning && (
          <div className="mt-4 pt-3 border-t border-black/[0.04]">
            <p className="text-[10.5px] text-[var(--text-faint)] tracking-wide uppercase">
              Click to run
            </p>
          </div>
        )}
      </SpotlightCard>
    </motion.div>
  );
}

/* ── Main Component ────────────────────────────────────── */

export default function Scenarios() {
  const [results, setResults] = useState({});
  const [running, setRunning] = useState(false);
  const [runningIdx, setRunningIdx] = useState(null);

  const completedCount = Object.keys(results).length;
  const progressPct = (completedCount / SCENARIOS.length) * 100;

  /* Summary stats for the results strip */
  const summary = useMemo(() => {
    const vals = Object.values(results).filter(Boolean);
    return {
      matches: vals.filter(r => r.decision === 'MATCH').length,
      reviews: vals.filter(r => r.decision === 'REVIEW').length,
      noMatches: vals.filter(r => r.decision === 'NO-MATCH').length,
    };
  }, [results]);

  /* ── Business Logic ──────────────────────────────────── */

  const runAll = async () => {
    setRunning(true);
    setResults({});
    const promises = SCENARIOS.map(async (s, i) => {
      try {
        const { data } = await resolveEntities({
          name1: s.name1, address1: s.addr1, name2: s.name2, address2: s.addr2,
        });
        setResults(prev => ({ ...prev, [i]: data }));
      } catch {
        setResults(prev => ({ ...prev, [i]: null }));
      }
    });
    await Promise.all(promises);
    setRunning(false);
  };

  const runSingle = async (i) => {
    setRunningIdx(i);
    const s = SCENARIOS[i];
    try {
      const { data } = await resolveEntities({
        name1: s.name1, address1: s.addr1, name2: s.name2, address2: s.addr2,
      });
      setResults(prev => ({ ...prev, [i]: data }));
    } catch {
      setResults(prev => ({ ...prev, [i]: null }));
    }
    setRunningIdx(null);
  };

  const reset = () => setResults({});

  /* ── Render ──────────────────────────────────────────── */

  return (
    <div className="page-container">
      {/* ── Page Header ──────────────────────────────────── */}
      <motion.div
        className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-4 mb-8"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal }}
      >
        <div>
          <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
            Test Scenarios
          </h1>
          <p className="mt-2 text-sm text-[var(--text-muted)] max-w-lg leading-relaxed">
            Pre-built test cases demonstrating model behavior across
            name variations, typos, address changes, and edge cases.
          </p>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          <AnimatePresence>
            {completedCount > 0 && (
              <motion.button
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.9 }}
                transition={springSnappy}
                onClick={reset}
                className="btn-secondary flex items-center gap-2"
              >
                <RotateCcw size={14} aria-hidden="true" />
                Reset
              </motion.button>
            )}
          </AnimatePresence>

          <motion.button
            onClick={runAll}
            disabled={running}
            className="btn-primary flex items-center gap-2"
            whileTap={{ scale: 0.97 }}
          >
            {running ? (
              <>
                <Loader2 size={14} className="animate-spin" aria-hidden="true" />
                <span>Running {completedCount}/{SCENARIOS.length}</span>
              </>
            ) : (
              <>
                <Play size={14} aria-hidden="true" />
                <span>Run all {SCENARIOS.length}</span>
              </>
            )}
          </motion.button>
        </div>
      </motion.div>

      {/* ── Progress Bar ─────────────────────────────────── */}
      <div aria-live="polite">
      <AnimatePresence>
        {running && (
          <motion.div
            className="mb-6"
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: dur.fast }}
          >
            <div className="flex items-center justify-between text-xs text-[var(--text-muted)] mb-2">
              <span className="uppercase tracking-wider text-[11px] font-medium">
                Progress
              </span>
              <span className="tabular-nums font-mono text-[var(--text-secondary)]">
                {completedCount} / {SCENARIOS.length}
              </span>
            </div>
            <div className="h-1.5 rounded-full bg-black/[0.04] overflow-hidden">
              <motion.div
                className="h-full rounded-full bg-[var(--accent)]"
                initial={{ width: 0 }}
                animate={{ width: `${progressPct}%` }}
                transition={{ duration: dur.fast }}
              />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
      </div>

      {/* ── Results Summary Strip ────────────────────────── */}
      <AnimatePresence>
        {completedCount > 0 && !running && (
          <motion.div
            className="glass-card border border-black/[0.06] mb-6 py-3 px-5 flex items-center gap-6"
            initial={{ opacity: 0, y: -8 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            transition={springSnappy}
          >
            <span className="text-xs font-medium text-[var(--text-muted)] uppercase tracking-wider">
              Results
            </span>
            <div className="flex items-center gap-5 ml-auto">
              {summary.matches > 0 && (
                <div className="flex items-center gap-1.5">
                  <div className="w-2 h-2 rounded-full bg-emerald-500" />
                  <span className="text-sm font-semibold tabular-nums text-[var(--text-primary)]">
                    {summary.matches}
                  </span>
                  <span className="text-xs text-[var(--text-muted)]">match</span>
                </div>
              )}
              {summary.reviews > 0 && (
                <div className="flex items-center gap-1.5">
                  <div className="w-2 h-2 rounded-full bg-amber-500" />
                  <span className="text-sm font-semibold tabular-nums text-[var(--text-primary)]">
                    {summary.reviews}
                  </span>
                  <span className="text-xs text-[var(--text-muted)]">review</span>
                </div>
              )}
              {summary.noMatches > 0 && (
                <div className="flex items-center gap-1.5">
                  <div className="w-2 h-2 rounded-full bg-red-500" />
                  <span className="text-sm font-semibold tabular-nums text-[var(--text-primary)]">
                    {summary.noMatches}
                  </span>
                  <span className="text-xs text-[var(--text-muted)]">no-match</span>
                </div>
              )}
              <span className="text-xs text-[var(--text-faint)] pl-3 border-l border-black/[0.06]">
                {completedCount} of {SCENARIOS.length} completed
              </span>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* ── Scenario Grid ────────────────────────────────── */}
      <motion.div
        className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4"
        variants={stagger}
        initial="hidden"
        animate="visible"
      >
        {SCENARIOS.map((s, i) => (
          <ScenarioCard
            key={i}
            scenario={s}
            index={i}
            result={results[i]}
            isRunning={runningIdx === i}
            isGlobalRunning={running}
            onRun={runSingle}
          />
        ))}
      </motion.div>
    </div>
  );
}
