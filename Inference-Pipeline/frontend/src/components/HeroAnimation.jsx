import { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence, useMotionValue, animate } from 'framer-motion';
import { User, ShieldAlert, ShieldCheck, AlertTriangle } from 'lucide-react';

/**
 * Entity resolution animation — organic graph growth + typographic consolidation.
 *
 * Narrative:
 *   0 SEED    — a single luminous core pulses (a nascent identity)
 *   1 SEARCH  — dendrites grow outward along curved paths, reaching for sources.
 *              Each tip, upon arrival, materializes a source record chip.
 *   2 PULSE   — light travels back along each dendrite from record → core.
 *              The core grows brighter as each pulse is absorbed.
 *   3 BLOOM   — the core blooms into a golden record card. Inside, five source
 *              spellings of the name fade in overlapping, ghosted — then
 *              collapse into the canonical form.
 *
 * KYC variant: one dendrite leads to an OFAC-flagged record. Its pulse is red,
 * the golden card becomes a Risk Alert with a red shield.
 */

const SCENARIOS = [
  {
    type: 'clean',
    sources: [
      { source: 'CRM',            name: 'John Doe',       flagged: false },
      { source: 'Billing',        name: 'J. Doe',         flagged: false },
      { source: 'Voter Registry', name: 'John A. Doe',    flagged: false },
      { source: 'Fraud DB',       name: 'Johnny Doe',     flagged: false },
      { source: 'HR',             name: 'John A. Doe Jr.', flagged: false },
    ],
    golden: {
      name: 'John A. Doe',
      confidence: 94,
      cluster: 'e65b83c2',
      badge: 'Verified',
      label: 'Unified across 5 sources',
    },
    edges: [0.91, 0.85, 0.79, 0.73, 0.68],
  },
  {
    type: 'risk',
    sources: [
      { source: 'OFAC SDN',       name: 'John Q. Doe',  flagged: true  },
      { source: 'Bank Records',   name: 'J Doe',        flagged: false },
      { source: 'Trade License',  name: 'J. Q. Doe',    flagged: false },
      { source: 'CRM',            name: 'John Doe',     flagged: false },
      { source: 'Billing',        name: 'Jon Doe',      flagged: false },
    ],
    golden: {
      name: 'John Q. Doe',
      confidence: 89,
      cluster: 'a51dd171',
      badge: 'OFAC Risk',
      label: 'Sanctions match — 2-hop link',
    },
    edges: [0.93, 0.85, 0.71, 0.52, 0.48],
  },
  {
    type: 'clean',
    sources: [
      { source: 'CRM',            name: 'Jane Doe',        flagged: false },
      { source: 'HR',             name: 'Jane A. Doe',     flagged: false },
      { source: 'Billing',        name: 'J. Doe',          flagged: false },
      { source: 'Voter Registry', name: 'Jane Alice Doe',  flagged: false },
      { source: 'Fraud DB',       name: 'Jane Doe',        flagged: false },
    ],
    golden: {
      name: 'Jane Alice Doe',
      confidence: 97,
      cluster: 'be4c8cb1',
      badge: 'Verified',
      label: 'Unified across 5 sources',
    },
    edges: [0.93, 0.88, 0.81, 0.76, 0.70],
  },
];

const SOURCE_COLORS = {
  CRM:              '#3b82f6',
  Billing:          '#059669',
  'Voter Registry': '#b45309',
  'Fraud DB':       '#dc2626',
  HR:               '#7c3aed',
  'OFAC SDN':       '#dc2626',
  'Bank Records':   '#0d9488',
  'Trade License':  '#ca8a04',
};

// ---------------------------------------------------------------------------
// Geometry — 560×460 canvas, core on the right, dendrites reaching left.
// ---------------------------------------------------------------------------
const WIDTH  = 560;
const HEIGHT = 460;
const CORE   = { x: 240, y: 230 };

// Each dendrite: endpoint (where record materializes), control point for
// quadratic bezier, sprout delay, growth duration.
const DENDRITES = [
  { end: { x: 85,  y: 75  }, ctrl: { x: 290, y: 115 }, sprout: 0.00, grow: 0.90 },
  { end: { x: 140, y: 150 }, ctrl: { x: 325, y: 175 }, sprout: 0.18, grow: 0.85 },
  { end: { x: 70,  y: 235 }, ctrl: { x: 240, y: 255 }, sprout: 0.32, grow: 0.95 },
  { end: { x: 135, y: 320 }, ctrl: { x: 315, y: 295 }, sprout: 0.48, grow: 0.90 },
  { end: { x: 85,  y: 390 }, ctrl: { x: 270, y: 365 }, sprout: 0.62, grow: 0.85 },
];

const makePath = (d) =>
  `M ${CORE.x} ${CORE.y} Q ${d.ctrl.x} ${d.ctrl.y} ${d.end.x} ${d.end.y}`;

// Count-up hook
function useCountUp(target, duration, active) {
  const mv = useMotionValue(0);
  const [display, setDisplay] = useState(0);
  useEffect(() => {
    if (!active) { mv.set(0); setDisplay(0); return; }
    const controls = animate(mv, target, {
      duration,
      ease: [0.2, 0.8, 0.2, 1],
      onUpdate: (v) => setDisplay(Math.round(v)),
    });
    return () => controls.stop();
  }, [target, active]);
  return display;
}

export default function HeroAnimation() {
  const [scenarioIdx, setScenarioIdx] = useState(0);
  const [phase, setPhase] = useState(0);
  const reduceMotion = useRef(false);

  useEffect(() => {
    reduceMotion.current = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches || false;
  }, []);

  useEffect(() => {
    if (reduceMotion.current) { setPhase(3); return; }
    const timings = [1000, 2000, 1500, 4200]; // ms per phase
    const t = setTimeout(() => {
      if (phase < 3) setPhase(phase + 1);
      else { setPhase(0); setScenarioIdx((scenarioIdx + 1) % SCENARIOS.length); }
    }, timings[phase]);
    return () => clearTimeout(t);
  }, [phase, scenarioIdx]);

  const scenario = SCENARIOS[scenarioIdx];
  const isRisk = scenario.type === 'risk';
  const accentColor = isRisk ? '#dc2626' : '#c2410c';
  const confidenceValue = useCountUp(scenario.golden.confidence, 1.2, phase >= 3);

  return (
    <div
      className="relative pointer-events-none select-none overflow-hidden"
      style={{ width: WIDTH, height: HEIGHT }}
      aria-hidden="true"
    >
      {/* ── Atmospheric wash — radial from core ────────── */}
      <div
        className="absolute inset-0"
        style={{
          background: `radial-gradient(ellipse 65% 70% at ${CORE.x}px ${CORE.y}px, ${accentColor}12, transparent 70%)`,
        }}
      />

      {/* ── Dendrites (SVG curved paths) ────────────────── */}
      <svg className="absolute inset-0" width={WIDTH} height={HEIGHT} viewBox={`0 0 ${WIDTH} ${HEIGHT}`}>
        {DENDRITES.map((d, i) => {
          const isFlagged = scenario.sources[i]?.flagged;
          return (
            <motion.path
              key={`dendrite-${scenarioIdx}-${i}`}
              d={makePath(d)}
              stroke={isFlagged ? '#dc2626' : accentColor}
              strokeWidth={isFlagged ? 1.6 : 1.2}
              strokeLinecap="round"
              fill="none"
              initial={{ pathLength: 0, opacity: 0 }}
              animate={{
                pathLength: phase >= 1 ? 1 : 0,
                opacity:
                  phase === 0 ? 0 :
                  phase === 3 ? 0.14 :
                  (isFlagged ? 0.55 : 0.38),
              }}
              transition={{
                pathLength: {
                  duration: d.grow,
                  delay: phase === 1 ? d.sprout : 0,
                  ease: [0.15, 0.7, 0.3, 1],
                },
                opacity: { duration: 0.5 },
              }}
            />
          );
        })}
      </svg>

      {/* ── Pulses traveling from record → core (phase 2) ── */}
      <AnimatePresence>
        {phase === 2 && !reduceMotion.current && DENDRITES.map((d, i) => {
          const score = scenario.edges[i];
          const size = 7 + score * 7;  // 0.68→11.8, 0.93→13.5
          const color = scenario.sources[i].flagged ? '#dc2626' : accentColor;
          return (
            <motion.div
              key={`pulse-${scenarioIdx}-${i}`}
              className="absolute rounded-full"
              style={{
                left: -size / 2,
                top: -size / 2,
                width: size,
                height: size,
                background: color,
                boxShadow: `0 0 ${size * 1.8}px ${color}, 0 0 ${size * 3}px ${color}50`,
                offsetPath: `path("${makePath(d)}")`,
                offsetRotate: '0deg',
              }}
              initial={{ offsetDistance: '100%', opacity: 0, scale: 0.4 }}
              animate={{
                offsetDistance: '0%',
                opacity: [0, 1, 1, 0.9],
                scale: [0.4, 1, 1.1, 1.4],
              }}
              transition={{
                offsetDistance: { duration: 0.95, delay: i * 0.08, ease: [0.45, 0, 0.25, 1] },
                opacity:        { duration: 0.95, delay: i * 0.08, times: [0, 0.1, 0.85, 1] },
                scale:          { duration: 0.95, delay: i * 0.08, times: [0, 0.25, 0.85, 1] },
              }}
            />
          );
        })}
      </AnimatePresence>

      {/* ── Core — the luminous seed/receiver ────────────── */}
      <motion.div
        className="absolute rounded-full"
        style={{
          left: CORE.x,
          top: CORE.y,
          x: '-50%',
          y: '-50%',
          width: 14,
          height: 14,
          background: `radial-gradient(circle, white 0%, ${accentColor} 55%, ${accentColor}00 100%)`,
          boxShadow: `0 0 30px ${accentColor}, 0 0 60px ${accentColor}88`,
        }}
        animate={
          phase === 0 ? { scale: [1, 1.35, 1], opacity: [0.85, 1, 0.85] } :
          phase === 1 ? { scale: [1, 1.2, 1],  opacity: [0.85, 1, 0.85] } :
          phase === 2 ? { scale: [1, 1.3, 1.7, 2.2], opacity: [1, 1, 1, 0.9] } :
                        { scale: [2.2, 3.5, 0], opacity: [0.9, 0.5, 0] }
        }
        transition={
          phase === 2 ? { duration: 1.5, times: [0, 0.3, 0.7, 1], ease: 'easeInOut' } :
          phase === 3 ? { duration: 0.55, times: [0, 0.5, 1], ease: 'easeOut' } :
                        { duration: 1.8, repeat: Infinity, ease: 'easeInOut' }
        }
      />

      {/* ── Core charge halo — expands during phase 2 ───── */}
      {phase === 2 && (
        <motion.div
          className="absolute rounded-full pointer-events-none"
          style={{
            left: CORE.x,
            top: CORE.y,
            x: '-50%',
            y: '-50%',
            width: 50,
            height: 50,
            background: `radial-gradient(circle, ${accentColor}30, transparent 70%)`,
          }}
          initial={{ scale: 0.5, opacity: 0 }}
          animate={{ scale: [0.5, 1.2, 1.8, 2.4], opacity: [0, 0.8, 1, 0.6] }}
          transition={{ duration: 1.5, times: [0, 0.3, 0.7, 1], ease: 'easeOut' }}
        />
      )}

      {/* ── Bloom flash at phase 3 start ──────────────────── */}
      {phase === 3 && (
        <motion.div
          className="absolute rounded-full pointer-events-none"
          style={{
            left: CORE.x,
            top: CORE.y,
            x: '-50%',
            y: '-50%',
            width: 100,
            height: 100,
            background: 'radial-gradient(circle, white, transparent 60%)',
          }}
          initial={{ scale: 0, opacity: 0 }}
          animate={{ scale: [0, 1.5, 3], opacity: [0, 1, 0] }}
          transition={{ duration: 0.7, times: [0, 0.3, 1], ease: 'easeOut' }}
        />
      )}

      {/* ── Source record chips at dendrite tips ──────────── */}
      {scenario.sources.map((src, i) => {
        const d = DENDRITES[i];
        const sourceColor = SOURCE_COLORS[src.source] || '#c2410c';
        const appearTime = d.sprout + d.grow + 0.05; // appear just AFTER dendrite arrives
        return (
          <motion.div
            key={`${scenarioIdx}-chip-${i}`}
            className="absolute z-[5]"
            style={{
              left: d.end.x,
              top: d.end.y,
              transform: 'translate(-50%, -50%)',
              width: 160,
            }}
            initial={{ opacity: 0, scale: 0.3 }}
            animate={{
              opacity: phase === 0 ? 0 : phase === 1 || phase === 2 ? 1 : 0,
              scale:   phase === 0 ? 0.3 : phase === 3 ? 0.82 : 1,
            }}
            transition={{
              opacity: { duration: 0.4, delay: phase === 1 ? appearTime : 0 },
              scale:   { duration: 0.55, delay: phase === 1 ? appearTime : 0, ease: [0.2, 1.2, 0.3, 1] },
            }}
          >
            <div className="flex items-center gap-2 rounded-xl border border-black/[0.06] bg-white px-2.5 py-2 shadow-md">
              <div
                className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md"
                style={{ backgroundColor: sourceColor + '1a' }}
              >
                <User size={11} style={{ color: sourceColor }} strokeWidth={2.5} />
              </div>
              <div className="min-w-0 flex-1">
                <p
                  className="text-[8.5px] font-semibold uppercase tracking-wider leading-none"
                  style={{ color: sourceColor }}
                >
                  {src.source}
                </p>
                <p className="text-[11.5px] font-medium text-[var(--text-primary)] truncate mt-0.5">
                  {src.name}
                </p>
              </div>
              {src.flagged && phase >= 1 && phase < 3 && (
                <motion.span
                  initial={{ scale: 0, rotate: -15 }}
                  animate={{ scale: 1, rotate: 0 }}
                  transition={{ delay: phase === 1 ? appearTime + 0.1 : 0, type: 'spring', stiffness: 400, damping: 14 }}
                  className="shrink-0"
                >
                  <AlertTriangle size={13} className="text-red-600" fill="#fee2e2" strokeWidth={2.4} />
                </motion.span>
              )}
            </div>
          </motion.div>
        );
      })}

      {/* ── Golden record bloom ──────────────────────────── */}
      <AnimatePresence>
        {phase === 3 && (
          <motion.div
            key={`golden-${scenarioIdx}`}
            className="absolute z-10"
            style={{
              left: CORE.x,
              top: CORE.y,
              transform: 'translate(-50%, -50%)',
              width: 260,
            }}
            initial={{ opacity: 0, scale: 0.3, filter: 'blur(8px)' }}
            animate={{ opacity: 1, scale: 1, filter: 'blur(0px)' }}
            exit={{ opacity: 0, scale: 0.9 }}
            transition={{
              scale:   { delay: 0.2, duration: 0.6, ease: [0.2, 0.8, 0.2, 1] },
              opacity: { delay: 0.2, duration: 0.4 },
              filter:  { delay: 0.2, duration: 0.5 },
            }}
          >
            {/* Orbital rings behind card */}
            {[0, 1, 2].map((i) => (
              <motion.div
                key={`ring-${i}`}
                className="absolute inset-0 rounded-2xl"
                style={{ border: `1.5px solid ${accentColor}` }}
                initial={{ scale: 1, opacity: 0.45 }}
                animate={{ scale: [1, 1.55], opacity: [0.45, 0] }}
                transition={{ duration: 1.6, delay: 0.4 + i * 0.4, repeat: Infinity, ease: 'easeOut' }}
              />
            ))}

            <div
              className={`relative overflow-hidden rounded-2xl p-4 shadow-2xl ${
                isRisk
                  ? 'border border-red-500/30 bg-gradient-to-br from-white to-red-500/10'
                  : 'border border-[var(--accent)]/30 bg-gradient-to-br from-white to-[var(--accent)]/10'
              }`}
            >
              {/* Shine sweep */}
              <motion.div
                className="absolute inset-0 pointer-events-none"
                style={{
                  background: 'linear-gradient(105deg, transparent 35%, rgba(255,255,255,0.95) 50%, transparent 65%)',
                }}
                initial={{ x: '-130%' }}
                animate={{ x: '130%' }}
                transition={{ delay: 1.2, duration: 1.0, ease: 'easeInOut' }}
              />

              <div className="relative flex items-center gap-3">
                <motion.div
                  className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-xl border ${
                    isRisk ? 'bg-red-500/15 border-red-500/30' : 'bg-[var(--accent)]/15 border-[var(--accent)]/30'
                  }`}
                  initial={{ scale: 0, rotate: -90 }}
                  animate={{ scale: 1, rotate: 0 }}
                  transition={{ delay: 0.4, type: 'spring', stiffness: 280, damping: 16 }}
                >
                  {isRisk
                    ? <ShieldAlert size={20} className="text-red-600" strokeWidth={2.3} />
                    : <ShieldCheck size={20} className="text-[var(--accent)]" strokeWidth={2.3} />
                  }
                </motion.div>

                <div className="min-w-0 flex-1">
                  {/* Label + confidence pill */}
                  <div className="flex items-center gap-1.5 flex-nowrap">
                    <span
                      className="text-[9px] font-semibold uppercase tracking-wider whitespace-nowrap"
                      style={{ color: isRisk ? '#dc2626' : 'var(--accent)' }}
                    >
                      {isRisk ? 'Risk Alert' : 'Golden Record'}
                    </span>
                    <motion.span
                      className={`text-[9px] font-mono px-1 py-0.5 rounded font-semibold tabular-nums whitespace-nowrap ${
                        isRisk ? 'text-red-700 bg-red-500/10' : 'text-emerald-700 bg-emerald-500/10'
                      }`}
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      transition={{ delay: 1.4, duration: 0.3 }}
                    >
                      {confidenceValue}% · {scenario.golden.badge}
                    </motion.span>
                  </div>

                  {/* Consolidating name — ghost spellings fade in/out, canonical takes natural flow */}
                  <div className="relative mt-1">
                    {/* Ghost source spellings — absolute so they don't affect layout */}
                    {scenario.sources.map((src, i) => (
                      <motion.span
                        key={`ghost-${i}`}
                        className="absolute left-0 top-0 text-sm font-semibold whitespace-nowrap pointer-events-none"
                        style={{
                          color: SOURCE_COLORS[src.source] || accentColor,
                          letterSpacing: '0.02em',
                        }}
                        initial={{ opacity: 0, x: (i - 2) * 2.5, filter: 'blur(1px)' }}
                        animate={{
                          opacity: [0, 0.4, 0.4, 0],
                          x: [(i - 2) * 2.5, (i - 2) * 2.5, 0, 0],
                          filter: ['blur(1px)', 'blur(1px)', 'blur(0px)', 'blur(0px)'],
                        }}
                        transition={{
                          duration: 1.4,
                          delay: 0.4,
                          times: [0, 0.22, 0.62, 1],
                          ease: 'easeInOut',
                        }}
                      >
                        {src.name}
                      </motion.span>
                    ))}
                    {/* Canonical name — block level, defines height so meta row flows below */}
                    <motion.span
                      className="block text-sm font-semibold text-[var(--text-primary)] whitespace-nowrap"
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      transition={{ delay: 1.3, duration: 0.35 }}
                    >
                      {scenario.golden.name}
                    </motion.span>
                  </div>

                  {/* Meta row */}
                  <motion.div
                    className="mt-1.5 flex items-center gap-1.5 text-[10px] text-[var(--text-faint)]"
                    initial={{ opacity: 0, y: 4 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 1.55, duration: 0.3 }}
                  >
                    <motion.span
                      className={`inline-flex h-1.5 w-1.5 rounded-full shrink-0 ${isRisk ? 'bg-red-500' : 'bg-emerald-500'}`}
                      animate={{ opacity: [0.5, 1, 0.5] }}
                      transition={{ duration: 1.6, repeat: Infinity, ease: 'easeInOut' }}
                    />
                    <span className="truncate">{scenario.golden.label}</span>
                    <span className="ml-auto font-mono whitespace-nowrap">{scenario.golden.cluster}</span>
                  </motion.div>
                </div>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
