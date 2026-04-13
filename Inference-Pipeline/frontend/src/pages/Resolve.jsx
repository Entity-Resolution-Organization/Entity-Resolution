import { useState, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  ArrowRight, Loader2, Check, X, Minus, AlertTriangle,
  Mail, Calendar, Phone, User, MapPin, Clock, Zap,
} from 'lucide-react';
import { resolveEntities } from '../api/client';
import { spring, springSnappy, easeOut, dur, STAGGER_MS, resultReveal, resultItem } from '../motion';
import SpotlightCard from '../components/SpotlightCard';
import DecisionBadge from '../components/DecisionBadge';

/* ── Presets ──────────────────────────────────────────── */
const PRESETS = {
  '-- Select a scenario --': {},
  'Exact Match': {
    firstName1: 'Robert', lastName1: 'Smith', street1: '123 Main Street', city1: 'Raleigh', state1: 'NC',
    firstName2: 'Robert', lastName2: 'Smith', street2: '123 Main Street', city2: 'Raleigh', state2: 'NC',
    email1: 'rsmith@gmail.com', email2: 'rsmith@gmail.com', dob1: '1985-03-15', dob2: '1985-03-15',
  },
  'Nickname (Bob/Robert)': {
    firstName1: 'Robert', lastName1: 'Smith', street1: '123 Main Street', city1: 'Raleigh', state1: 'NC',
    firstName2: 'Bob', lastName2: 'Smith', street2: '123 Main Street', city2: 'Raleigh', state2: 'NC',
    email1: 'rsmith@gmail.com', email2: 'bsmith@gmail.com',
  },
  'Typo in Name': {
    firstName1: 'Robert', lastName1: 'Smith', street1: '123 Main Street', city1: 'Raleigh', state1: 'NC',
    firstName2: 'Robrt', lastName2: 'Smith', street2: '123 Main Street', city2: 'Raleigh', state2: 'NC',
  },
  'Person Moved': {
    firstName1: 'Robert', lastName1: 'Smith', street1: '123 Main Street', city1: 'New York', state1: 'NY',
    firstName2: 'Robert', lastName2: 'Smith', street2: '789 Oak Ave', city2: 'Los Angeles', state2: 'CA',
    phone1: '555-0101', phone2: '555-0101',
  },
  'Name Reordered': {
    firstName1: 'Robert', lastName1: 'Smith', street1: '123 Main Street', city1: 'Raleigh', state1: 'NC',
    firstName2: 'Smith', lastName2: 'Robert', street2: '123 Main Street', city2: 'Raleigh', state2: 'NC',
  },
  'Address Abbreviation': {
    firstName1: 'John', lastName1: 'Doe', street1: '123 Main Street', city1: 'Raleigh', state1: 'NC',
    firstName2: 'John', lastName2: 'Doe', street2: '123 Main St', city2: 'Raleigh', state2: 'NC',
    email1: 'jdoe@work.com', email2: 'jdoe@work.com', dob1: '1990-07-22', dob2: '1990-07-22',
  },
  'Non-ASCII Name': {
    firstName1: 'Mohammad', lastName1: 'Al-Rashid', street1: '45 Desert Rd', city1: 'Dubai', state1: '',
    firstName2: 'Mohammed', lastName2: 'Al Rashid', street2: '45 Desert Road', city2: 'Dubai', state2: '',
  },
  'Diff People Same Addr': {
    firstName1: 'Robert', lastName1: 'Smith', street1: '123 Main Street', city1: 'Raleigh', state1: 'NC',
    firstName2: 'James', lastName2: 'Wilson', street2: '123 Main Street', city2: 'Raleigh', state2: 'NC',
  },
  'Completely Different': {
    firstName1: 'Robert', lastName1: 'Smith', street1: '123 Main Street', city1: 'Raleigh', state1: 'NC',
    firstName2: 'Maria', lastName2: 'Garcia', street2: '456 Oak Avenue', city2: 'Durham', state2: 'NC',
  },
  'DOB Confirmed Match': {
    firstName1: 'Robert', lastName1: 'Smith', street1: '123 Main Street', city1: 'Raleigh', state1: 'NC',
    firstName2: 'Bob', lastName2: 'Smith', street2: '123 Main St', city2: 'Raleigh', state2: 'NC',
    email1: 'rsmith@gmail.com', email2: 'bobsmith@yahoo.com', dob1: '1985-03-15', dob2: '1985-03-15',
    phone1: '555-0101', phone2: '555-0102',
  },
  'Email Match Override': {
    firstName1: 'R.', lastName1: 'Smith', street1: '123 Main Street', city1: 'Raleigh', state1: 'NC',
    firstName2: 'Robert', lastName2: 'Smith', street2: '200 Park Ave', city2: 'Boston', state2: 'MA',
    email1: 'rsmith@gmail.com', email2: 'rsmith@gmail.com',
  },
};

const EMPTY_FORM = {
  firstName1: '', lastName1: '', street1: '', city1: '', state1: '',
  firstName2: '', lastName2: '', street2: '', city2: '', state2: '',
  email1: '', email2: '', dob1: '', dob2: '', phone1: '', phone2: '',
};

/* ── Helpers ──────────────────────────────────────────── */
function buildName(first, last) { return [first, last].filter(Boolean).join(' '); }
function buildAddress(street, city, state) { return [street, city, state].filter(Boolean).join(', '); }

function computeRules(form, baseProb) {
  const rules = [];
  const e1 = (form.email1 || '').trim().toLowerCase();
  const e2 = (form.email2 || '').trim().toLowerCase();
  if (e1 && e2) {
    rules.push({ field: 'Email', icon: Mail, match: e1 === e2, label: e1 === e2 ? 'Exact match' : 'Different', type: e1 === e2 ? 'match' : 'nomatch' });
  } else {
    rules.push({ field: 'Email', icon: Mail, match: null, label: 'Not provided', type: 'missing' });
  }
  const d1 = (form.dob1 || '').trim();
  const d2 = (form.dob2 || '').trim();
  if (d1 && d2) {
    const dobMatch = d1 === d2;
    rules.push({ field: 'DOB', icon: Calendar, match: dobMatch, label: dobMatch ? (baseProb > 0.5 ? 'Confirmed (override)' : 'Match') : 'Different', type: dobMatch ? 'match' : 'nomatch', override: dobMatch && baseProb > 0.5 });
  } else {
    rules.push({ field: 'DOB', icon: Calendar, match: null, label: 'Not provided', type: 'missing' });
  }
  const p1 = (form.phone1 || '').replace(/\D/g, '');
  const p2 = (form.phone2 || '').replace(/\D/g, '');
  if (p1 && p2) {
    const phoneMatch = p1 === p2;
    const partial = !phoneMatch && p1.length >= 4 && p2.length >= 4 && (p1.endsWith(p2.slice(-4)) || p2.endsWith(p1.slice(-4)));
    rules.push({ field: 'Phone', icon: Phone, match: phoneMatch, label: phoneMatch ? 'Match' : partial ? 'Partial (last 4)' : 'Different', type: phoneMatch ? 'match' : partial ? 'partial' : 'nomatch' });
  } else {
    rules.push({ field: 'Phone', icon: Phone, match: null, label: 'Not provided', type: 'missing' });
  }
  return rules;
}

async function computeAttribution(form, baseProb) {
  const name1 = buildName(form.firstName1, form.lastName1);
  const addr1 = buildAddress(form.street1, form.city1, form.state1);
  const name2 = buildName(form.firstName2, form.lastName2);
  const addr2 = buildAddress(form.street2, form.city2, form.state2);
  const masks = [
    { field: 'Name', payload: { name1: '***', address1: addr1, name2: '***', address2: addr2 } },
    { field: 'Address', payload: { name1, address1: '***', name2, address2: '***' } },
  ];
  const results = [];
  for (const mask of masks) {
    try {
      const { data } = await resolveEntities(mask.payload);
      results.push({ field: mask.field, maskedProb: data.probability, delta: baseProb - data.probability });
    } catch {
      results.push({ field: mask.field, maskedProb: null, delta: 0 });
    }
  }
  return results.sort((a, b) => b.delta - a.delta);
}

/* ── Reusable input field ─────────────────────────────── */
function Field({ label, id, ...props }) {
  return (
    <div>
      <label htmlFor={id} className="block text-[11px] font-medium text-[var(--text-muted)] mb-1">{label}</label>
      <input id={id} className="input-field" {...props} />
    </div>
  );
}

/* ── Rule row in results ──────────────────────────────── */
function RuleRow({ rule, delay }) {
  const Icon = rule.icon;
  const bg = rule.type === 'match' ? 'bg-emerald-50' : rule.type === 'partial' ? 'bg-amber-50' : rule.type === 'nomatch' ? 'bg-red-50' : 'bg-black/[0.02]';
  const iconColor = rule.type === 'match' ? 'text-emerald-600' : rule.type === 'partial' ? 'text-amber-700' : rule.type === 'nomatch' ? 'text-red-600' : 'text-[var(--text-faint)]';
  const badge = rule.type === 'match' ? ['bg-emerald-100', 'text-emerald-600', Check]
    : rule.type === 'partial' ? ['bg-amber-100', 'text-amber-700', Minus]
    : rule.type === 'nomatch' ? ['bg-red-100', 'text-red-600', X]
    : ['bg-black/[0.03]', 'text-[var(--text-faint)]', Minus];
  const BadgeIcon = badge[2];

  return (
    <motion.div
      className="flex items-center gap-3 rounded-lg border border-black/[0.04] bg-white/60 px-3 py-2.5"
      initial={{ opacity: 0, x: 8 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay, ...spring }}
    >
      <div className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-lg ${bg}`}>
        <Icon size={15} className={iconColor} aria-hidden="true" />
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-xs font-medium text-[var(--text-primary)]">{rule.field}</p>
        <p className="text-[11px] text-[var(--text-muted)]">{rule.label}</p>
      </div>
      <div className={`flex h-6 w-6 shrink-0 items-center justify-center rounded-full ${badge[0]}`}>
        <BadgeIcon size={13} className={badge[1]} aria-hidden="true" />
      </div>
    </motion.div>
  );
}

/* ── Component ────────────────────────────────────────── */
export default function Resolve() {
  const [form, setForm] = useState({ ...EMPTY_FORM });
  const [result, setResult] = useState(null);
  const [attribution, setAttribution] = useState(null);
  const [rules, setRules] = useState(null);
  const [finalProb, setFinalProb] = useState(null);
  const [loading, setLoading] = useState(false);
  const [latency, setLatency] = useState(0);
  const [error, setError] = useState(null);

  const set = useCallback((field, value) => {
    setForm(prev => ({ ...prev, [field]: value }));
  }, []);

  const handlePreset = (e) => {
    const p = PRESETS[e.target.value];
    if (p && Object.keys(p).length) setForm({ ...EMPTY_FORM, ...p });
  };

  const handleResolve = async () => {
    setLoading(true); setResult(null); setAttribution(null); setRules(null); setFinalProb(null); setError(null);
    const name1 = buildName(form.firstName1, form.lastName1);
    const addr1 = buildAddress(form.street1, form.city1, form.state1);
    const name2 = buildName(form.firstName2, form.lastName2);
    const addr2 = buildAddress(form.street2, form.city2, form.state2);
    const t0 = performance.now();
    try {
      const { data } = await resolveEntities({ name1, address1: addr1, name2, address2: addr2 });
      setLatency(Math.round(performance.now() - t0));
      setResult(data);
      const ruleResults = computeRules(form, data.probability);
      setRules(ruleResults);
      const dobRule = ruleResults.find(r => r.field === 'DOB');
      const fp = dobRule?.override ? 1.0 : data.probability;
      setFinalProb(fp);
      const attr = await computeAttribution(form, data.probability);
      setAttribution(attr);
    } catch {
      setError('Failed to resolve. Check that the API is running.');
    } finally {
      setLoading(false);
    }
  };

  const hasInput = form.firstName1 || form.lastName1 || form.firstName2 || form.lastName2;
  const effectiveDecision = finalProb != null
    ? (finalProb >= 0.45 ? 'MATCH' : finalProb >= 0.20 ? 'REVIEW' : 'NO-MATCH')
    : result?.decision;
  const score = (finalProb ?? result?.probability ?? 0) * 100;
  const hasOverride = finalProb != null && finalProb !== result?.probability;

  return (
    <div className="page-container">

      {/* ── Header + Preset (compact row) ────────────── */}
      <div className="flex flex-col md:flex-row md:items-end md:justify-between gap-4 mb-8">
        <div>
          <h1 className="font-display text-3xl md:text-4xl font-medium tracking-tight text-[var(--text-primary)]">
            Pairwise match explorer
          </h1>
          <p className="mt-2 text-sm text-[var(--text-muted)] max-w-2xl" style={{ textWrap: 'pretty' }}>
            Compare two entity records using semantic matching (DeBERTa) and deterministic rules.
            Field attribution shows each field's contribution to the match score.
          </p>
        </div>
        <div className="shrink-0">
          <label htmlFor="preset" className="block text-[11px] font-semibold uppercase tracking-[0.12em] text-[var(--text-muted)] mb-1.5">
            Quick scenarios
          </label>
          <select id="preset" onChange={handlePreset} className="input-field w-full md:w-64">
            {Object.keys(PRESETS).map(k => <option key={k}>{k}</option>)}
          </select>
        </div>
      </div>

      {/* ── Entity input cards ────────────────────────── */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
        {/* Entity A */}
        <div className="glass-card space-y-3">
          <div className="flex items-center gap-2 mb-2">
            <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-[var(--accent-dim)]">
              <User size={14} className="text-[var(--accent)]" aria-hidden="true" />
            </div>
            <span className="text-xs font-semibold uppercase tracking-[0.1em] text-[var(--accent)]">Entity A</span>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <Field label="First name" id="fn1" placeholder="Robert" value={form.firstName1} onChange={e => set('firstName1', e.target.value)} />
            <Field label="Last name" id="ln1" placeholder="Smith" value={form.lastName1} onChange={e => set('lastName1', e.target.value)} />
          </div>
          <Field label="Street" id="st1" placeholder="123 Main Street" value={form.street1} onChange={e => set('street1', e.target.value)} />
          <div className="grid grid-cols-2 gap-3">
            <Field label="City" id="ci1" placeholder="Raleigh" value={form.city1} onChange={e => set('city1', e.target.value)} />
            <Field label="State" id="sa1" placeholder="NC" value={form.state1} onChange={e => set('state1', e.target.value)} />
          </div>
          <div className="pt-2 border-t border-[var(--border-subtle)]">
            <p className="text-[10px] font-medium uppercase tracking-[0.1em] text-[var(--text-faint)] mb-2">Structured fields</p>
            <div className="space-y-3">
              <Field label="Email" id="em1" placeholder="rsmith@gmail.com" value={form.email1} onChange={e => set('email1', e.target.value)} />
              <div className="grid grid-cols-2 gap-3">
                <Field label="Date of birth" id="dob1" type="date" value={form.dob1} onChange={e => set('dob1', e.target.value)} />
                <Field label="Phone" id="ph1" placeholder="555-0101" value={form.phone1} onChange={e => set('phone1', e.target.value)} />
              </div>
            </div>
          </div>
        </div>

        {/* Entity B */}
        <div className="glass-card space-y-3">
          <div className="flex items-center gap-2 mb-2">
            <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-[var(--accent-dim)]">
              <User size={14} className="text-[var(--accent)]" aria-hidden="true" />
            </div>
            <span className="text-xs font-semibold uppercase tracking-[0.1em] text-[var(--accent)]">Entity B</span>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <Field label="First name" id="fn2" placeholder="Bob" value={form.firstName2} onChange={e => set('firstName2', e.target.value)} />
            <Field label="Last name" id="ln2" placeholder="Smith" value={form.lastName2} onChange={e => set('lastName2', e.target.value)} />
          </div>
          <Field label="Street" id="st2" placeholder="123 Main St" value={form.street2} onChange={e => set('street2', e.target.value)} />
          <div className="grid grid-cols-2 gap-3">
            <Field label="City" id="ci2" placeholder="Raleigh" value={form.city2} onChange={e => set('city2', e.target.value)} />
            <Field label="State" id="sa2" placeholder="NC" value={form.state2} onChange={e => set('state2', e.target.value)} />
          </div>
          <div className="pt-2 border-t border-[var(--border-subtle)]">
            <p className="text-[10px] font-medium uppercase tracking-[0.1em] text-[var(--text-faint)] mb-2">Structured fields</p>
            <div className="space-y-3">
              <Field label="Email" id="em2" placeholder="bsmith@gmail.com" value={form.email2} onChange={e => set('email2', e.target.value)} />
              <div className="grid grid-cols-2 gap-3">
                <Field label="Date of birth" id="dob2" type="date" value={form.dob2} onChange={e => set('dob2', e.target.value)} />
                <Field label="Phone" id="ph2" placeholder="555-0102" value={form.phone2} onChange={e => set('phone2', e.target.value)} />
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* ── Resolve button ────────────────────────────── */}
      <button
        onClick={handleResolve}
        disabled={loading || !hasInput}
        aria-busy={loading}
        className="btn-primary mt-6 flex w-full items-center justify-center gap-2 py-3.5"
      >
        {loading ? (
          <><Loader2 size={16} className="animate-spin" aria-hidden="true" /><span>Resolving...</span></>
        ) : (
          <><Zap size={16} aria-hidden="true" /><span>Resolve entities</span></>
        )}
      </button>

      {/* ── Error ─────────────────────────────────────── */}
      <AnimatePresence>
        {error && (
          <motion.div
            role="alert"
            initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            className="mt-6 flex items-center gap-3 rounded-xl border border-red-200 bg-red-50 p-4"
          >
            <AlertTriangle size={18} className="text-red-600 shrink-0" aria-hidden="true" />
            <p className="text-sm text-red-700">{error}</p>
          </motion.div>
        )}
      </AnimatePresence>

      {/* ── Results ───────────────────────────────────── */}
      <div aria-live="polite">
      <AnimatePresence>
        {result && (
          <motion.div
            variants={resultReveal}
            initial="hidden"
            animate="visible"
            exit={{ opacity: 0, transition: { duration: 0.15 } }}
            className="mt-10 space-y-5"
          >
            {/* Hero result card */}
            <motion.div variants={resultItem}>
            <SpotlightCard className="glass-card-hero">
              <div className="relative z-10">
                <div className="flex items-start justify-between gap-6">
                  <div>
                    <h2 className="section-label mb-2">Match result</h2>
                    <div className="flex items-baseline gap-3">
                      <span className="font-display text-5xl md:text-6xl font-medium tracking-tight text-[var(--text-primary)] tabular-nums">
                        {score.toFixed(1)}%
                      </span>
                      {hasOverride && (
                        <span className="inline-flex items-center gap-1.5 rounded-full bg-emerald-100 border border-emerald-200 px-3 py-1 text-xs font-semibold text-emerald-700">
                          <Check size={12} aria-hidden="true" />
                          DOB override
                        </span>
                      )}
                    </div>
                    {hasOverride && (
                      <p className="mt-1 text-sm text-[var(--text-muted)]">
                        Base semantic score: <span className="font-mono tabular-nums">{(result.probability * 100).toFixed(1)}%</span>
                      </p>
                    )}
                  </div>
                  <DecisionBadge decision={effectiveDecision} confidence={result.confidence_level} />
                </div>

                <div className="mt-5 pt-4 border-t border-black/[0.06] flex items-center gap-4 text-xs text-[var(--text-muted)]">
                  <span className="flex items-center gap-1.5"><Clock size={12} aria-hidden="true" /> <span className="font-mono tabular-nums">{latency}ms</span> latency</span>
                  <span className="text-[var(--text-faint)]">·</span>
                  <span>Threshold: <span className="font-mono tabular-nums">0.45</span></span>
                  <span className="text-[var(--text-faint)]">·</span>
                  <span>Model: DeBERTa-v3 + LoRA</span>
                </div>
              </div>
            </SpotlightCard>
            </motion.div>

            {/* ── Two-layer detail columns ────────────── */}
            <motion.div variants={resultItem} className="grid grid-cols-1 lg:grid-cols-2 gap-5">

              {/* Left: Semantic layer */}
              <div className="space-y-5">
                {/* Field attribution */}
                {attribution && (
                  <motion.div
                    className="glass-card"
                    initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.15, ...spring }}
                  >
                    <h2 className="section-label mb-4">Semantic layer (DeBERTa)</h2>
                    <div className="space-y-3">
                      <div className="flex items-center justify-between rounded-lg bg-[var(--bg-hover)] px-3 py-2">
                        <span className="text-xs text-[var(--text-muted)]">Baseline score</span>
                        <span className="text-sm font-semibold text-[var(--text-primary)] font-mono tabular-nums">
                          {(result.probability * 100).toFixed(1)}%
                        </span>
                      </div>
                      {attribution.map((attr, i) => {
                        const absDelta = Math.abs(attr.delta);
                        const width = Math.min(absDelta / 0.5, 1) * 100;
                        const positive = attr.delta > 0;
                        return (
                          <motion.div
                            key={attr.field}
                            className="rounded-lg border border-black/[0.04] bg-white/60 px-3 py-2.5"
                            initial={{ opacity: 0, x: -8 }} animate={{ opacity: 1, x: 0 }}
                            transition={{ delay: 0.2 + i * STAGGER_MS, ...spring }}
                          >
                            <div className="flex items-center justify-between mb-1.5">
                              <span className="text-xs font-medium text-[var(--text-primary)]">{attr.field}</span>
                              <span className={`text-xs font-semibold font-mono tabular-nums ${positive ? 'text-emerald-600' : 'text-[var(--text-faint)]'}`}>
                                {positive ? '+' : ''}{(attr.delta * 100).toFixed(1)}%
                              </span>
                            </div>
                            <div className="h-1.5 rounded-full bg-black/[0.04] overflow-hidden">
                              <motion.div
                                className={`h-full rounded-full ${positive ? 'bg-emerald-500' : 'bg-slate-400'}`}
                                initial={{ width: 0 }}
                                animate={{ width: `${width}%` }}
                                transition={{ duration: dur.slow, delay: 0.35 + i * STAGGER_MS, ease: easeOut }}
                              />
                            </div>
                            <p className="mt-1 text-[10px] text-[var(--text-faint)]">
                              Masked: {attr.maskedProb != null ? `${(attr.maskedProb * 100).toFixed(1)}%` : 'N/A'}
                            </p>
                          </motion.div>
                        );
                      })}
                    </div>
                  </motion.div>
                )}

              </div>

              {/* Right: Structured rules layer */}
              <div className="space-y-5">
                {rules && (
                  <motion.div
                    className="glass-card"
                    initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.1, ...spring }}
                  >
                    <h2 className="section-label mb-4">Structured rules (deterministic)</h2>
                    <div className="space-y-2.5">
                      {rules.map((rule, i) => (
                        <RuleRow key={rule.field} rule={rule} delay={0.15 + i * STAGGER_MS} />
                      ))}
                    </div>
                    {hasOverride && (
                      <motion.div
                        className="mt-3 rounded-lg border border-emerald-200 bg-emerald-50 p-3"
                        initial={{ opacity: 0, y: 4 }} animate={{ opacity: 1, y: 0 }}
                        transition={{ delay: 0.4, ...spring }}
                      >
                        <p className="text-xs text-emerald-700">
                          DOB match + semantic score &gt; 0.5 triggered deterministic override to 100%
                        </p>
                      </motion.div>
                    )}
                  </motion.div>
                )}
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
      </div>

      {/* ── Empty state ───────────────────────────────── */}
      {!result && !loading && !error && (
        <motion.div
          className="mt-16 flex flex-col items-center justify-center py-20 text-center"
          initial={{ opacity: 0 }} animate={{ opacity: 1 }}
          transition={{ delay: 0.2 }}
        >
          <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-[var(--accent-surface)] border border-[var(--accent-dim)]">
            <Zap size={24} className="text-[var(--accent)]" aria-hidden="true" />
          </div>
          <p className="mt-5 text-sm font-medium text-[var(--text-secondary)]">
            Select a scenario or enter entity details above
          </p>
          <p className="mt-1 text-xs text-[var(--text-faint)]">
            Results will show semantic scores, field attribution, and deterministic rule checks
          </p>
        </motion.div>
      )}
    </div>
  );
}
