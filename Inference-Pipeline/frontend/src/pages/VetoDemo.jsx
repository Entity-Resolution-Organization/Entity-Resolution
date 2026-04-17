import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ShieldAlert, ShieldCheck, ToggleLeft, ToggleRight, User, Calendar, Mail, Phone } from 'lucide-react';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

const RECORD_A = {
  id: 'P2-CRM-001',
  name: 'Sarah Chen',
  address: '456 Market Street, San Francisco CA 94105',
  dob: '1988-11-22',
  email: 'sarah.chen@outlook.com',
  phone: '415-555-0198',
  company: 'DataBridge Solutions',
  source: 'CRM',
};

const RECORD_B = {
  id: 'P2-FRAUD-001',
  name: 'Sarah L Chen',
  address: '456 Market Street, San Francisco 94105',
  dob: '1990-06-10',
  email: 'sarah.chen@outlook.com',
  phone: '415-555-0198',
  company: 'DataBridge Solutions',
  source: 'Fraud DB',
};

const FIELDS = [
  { key: 'name', label: 'Name', icon: User },
  { key: 'dob', label: 'Date of birth', icon: Calendar, isVetoField: true },
  { key: 'email', label: 'Email', icon: Mail },
  { key: 'phone', label: 'Phone', icon: Phone },
];

export default function VetoDemo() {
  const [vetoEnabled, setVetoEnabled] = useState(false);
  const debertaScore = 0.84;

  return (
    <div className="page-container">
      <motion.div
        className="mb-10"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal, ease: easeOut }}
      >
        <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
          Veto rule demo
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-[var(--text-muted)] max-w-xl">
          Hard rules override ML confidence. These two records score
          <span className="font-mono text-emerald-400"> 0.84</span> on DeBERTa — a strong match.
          But their dates of birth are different. Toggle the veto rule to see the edge split.
        </p>
      </motion.div>

      {/* Toggle */}
      <motion.div
        className="glass-card mb-8 flex items-center justify-between"
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.05, ...spring }}
      >
        <div className="flex items-center gap-3">
          {vetoEnabled
            ? <ShieldAlert size={20} className="text-red-400" />
            : <ShieldCheck size={20} className="text-emerald-400" />
          }
          <div>
            <p className="text-sm font-semibold text-[var(--text-primary)]">
              DOB mismatch veto: {vetoEnabled ? 'enabled' : 'disabled'}
            </p>
            <p className="text-xs text-[var(--text-muted)]">
              {vetoEnabled
                ? 'Edge vetoed regardless of DeBERTa score — records split into separate entities'
                : 'No veto — DeBERTa score alone determines the match'
              }
            </p>
          </div>
        </div>
        <button
          onClick={() => setVetoEnabled(v => !v)}
          className="flex items-center gap-2 text-sm font-medium transition-colors"
        >
          {vetoEnabled
            ? <ToggleRight size={36} className="text-red-400" />
            : <ToggleLeft size={36} className="text-stone-500" />
          }
        </button>
      </motion.div>

      {/* Records side by side */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
        {[RECORD_A, RECORD_B].map((rec, idx) => (
          <motion.div
            key={rec.id}
            className="glass-card"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.1 + idx * 0.05, ...spring }}
          >
            <div className="flex items-center justify-between mb-4">
              <span className="text-xs font-mono text-[var(--text-faint)]">{rec.id}</span>
              <span className={`text-[10px] font-medium px-2 py-1 rounded ${
                rec.source === 'CRM' ? 'bg-blue-500/20 text-blue-400' : 'bg-red-500/20 text-red-400'
              }`}>{rec.source}</span>
            </div>
            <div className="space-y-3">
              {FIELDS.map(({ key, label, icon: Icon, isVetoField }) => {
                const mismatch = isVetoField && RECORD_A[key] !== RECORD_B[key];
                return (
                  <div
                    key={key}
                    className={`flex items-start gap-3 p-2.5 rounded-lg transition-colors ${
                      mismatch && vetoEnabled
                        ? 'bg-red-500/10 border border-red-500/20'
                        : mismatch
                        ? 'bg-amber-500/5 border border-amber-500/10'
                        : 'bg-white/[0.02] border border-white/[0.04]'
                    }`}
                  >
                    <Icon size={13} className={`mt-0.5 shrink-0 ${
                      mismatch && vetoEnabled ? 'text-red-400' : 'text-[var(--text-faint)]'
                    }`} />
                    <div className="flex-1">
                      <p className="text-[10px] uppercase tracking-wider text-[var(--text-faint)]">{label}</p>
                      <p className={`text-sm font-medium mt-0.5 ${
                        mismatch && vetoEnabled ? 'text-red-300' : 'text-[var(--text-primary)]'
                      }`}>
                        {rec[key] || '--'}
                      </p>
                    </div>
                    {mismatch && (
                      <span className={`text-[9px] font-medium px-1.5 py-0.5 rounded mt-1 ${
                        vetoEnabled ? 'bg-red-500/20 text-red-400' : 'bg-amber-500/15 text-amber-400'
                      }`}>
                        {vetoEnabled ? 'VETO' : 'mismatch'}
                      </span>
                    )}
                  </div>
                );
              })}
            </div>
          </motion.div>
        ))}
      </div>

      {/* Edge result */}
      <AnimatePresence mode="wait">
        <motion.div
          key={vetoEnabled ? 'vetoed' : 'matched'}
          className={`glass-card text-center py-10 ${
            vetoEnabled ? 'border-red-500/20' : 'border-emerald-500/20'
          }`}
          initial={{ opacity: 0, scale: 0.96 }}
          animate={{ opacity: 1, scale: 1 }}
          exit={{ opacity: 0, scale: 0.96 }}
          transition={spring}
        >
          {vetoEnabled ? (
            <>
              <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-red-500/10 border border-red-500/20 mx-auto mb-4">
                <ShieldAlert size={28} className="text-red-400" />
              </div>
              <h2 className="text-lg font-bold text-red-400 mb-2">Edge vetoed</h2>
              <p className="text-sm text-[var(--text-muted)] mb-4">
                DeBERTa score <span className="font-mono text-emerald-400">{debertaScore.toFixed(2)}</span> overridden by hard rule
              </p>
              <div className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-red-500/10 border border-red-500/20">
                <span className="text-xs font-mono text-red-400">veto_reason: "dob mismatch"</span>
              </div>
              <p className="mt-4 text-xs text-[var(--text-faint)]">
                Records assigned to separate clusters
              </p>
            </>
          ) : (
            <>
              <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-emerald-500/10 border border-emerald-500/20 mx-auto mb-4">
                <ShieldCheck size={28} className="text-emerald-400" />
              </div>
              <h2 className="text-lg font-bold text-emerald-400 mb-2">Match</h2>
              <p className="text-sm text-[var(--text-muted)] mb-4">
                DeBERTa score <span className="font-mono text-emerald-400">{debertaScore.toFixed(2)}</span> — above 0.45 threshold
              </p>
              <div className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-emerald-500/10 border border-emerald-500/20">
                <span className="text-xs font-mono text-emerald-400">Same cluster assigned</span>
              </div>
              <p className="mt-4 text-xs text-[var(--text-faint)]">
                DOB mismatch detected but no veto rule active
              </p>
            </>
          )}
        </motion.div>
      </AnimatePresence>

      {/* Config snippet */}
      <motion.div
        className="glass-card mt-8"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.3, ...spring }}
      >
        <h3 className="section-label mb-3">field_rules config</h3>
        <pre className="text-xs font-mono text-stone-400 leading-relaxed overflow-x-auto">
{`- field: dob
  scorer: date_exact
  weight: 1.5
  veto_on_mismatch: ${vetoEnabled ? 'true   # ← active' : 'false  # ← inactive'}
  has_flag: true`}
        </pre>
      </motion.div>
    </div>
  );
}
