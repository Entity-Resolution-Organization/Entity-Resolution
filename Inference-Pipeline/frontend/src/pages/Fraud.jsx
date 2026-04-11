import { useState } from 'react';
import { motion } from 'framer-motion';
import { Shield, Mail, Phone, AlertTriangle, Users, Eye, ChevronDown, ChevronUp } from 'lucide-react';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

const FRAUD_RINGS = [
  {
    cluster_id: 'ring-001',
    shared_field: 'email',
    shared_value: 'jwilson.trades@proton.me',
    shared_field_score: 0.85,
    records: [
      { id: 'P3-CRM-001', name: 'James Wilson', address: '789 Pine Ave, Chicago', source: 'CRM', dob: '1975-07-04' },
      { id: 'P3-BANK-001', name: 'J Wilson', address: '789 Pine Ave, Chicago', source: 'Bank Records', dob: '' },
      { id: 'P4-CRM-001', name: 'Maria Garcia', address: '321 Elm Dr, Miami', source: 'CRM', dob: '1982-09-18' },
    ],
    deberta_scores: { 'P3-P4': 0.32 },
    risk_signal: 'Same email address on records with clearly different names and locations — classic synthetic identity pattern',
  },
  {
    cluster_id: 'ring-002',
    shared_field: 'company',
    shared_value: 'Eurasian Import Export',
    shared_field_score: 0.72,
    records: [
      { id: 'P4-CRM-001', name: 'Maria Garcia', address: '321 Elm Dr, Miami', source: 'CRM', dob: '1982-09-18' },
      { id: 'P4-TRADE-001', name: 'Maria Garcia', address: '321 Elm Dr, Miami', source: 'Trade License', dob: '' },
      { id: 'P5-OFAC-001', name: 'Viktor Petrov', address: '15 Nevsky Prospect, SPb', source: 'OFAC SDN', dob: '1970-01-30' },
      { id: 'P5-TRADE-001', name: 'Viktor A Petrov', address: '15 Nevsky Prospect', source: 'Trade License', dob: '1970-01-30' },
    ],
    deberta_scores: { 'P4-P5': 0.18 },
    risk_signal: 'Same company links clean entity to OFAC-flagged individual — potential sanctions evasion through shared corporate structure',
  },
];

const SOURCE_COLORS = {
  CRM: '#3b82f6', Billing: '#059669', 'Fraud DB': '#dc2626', 'Bank Records': '#0891b2',
  'OFAC SDN': '#ef4444', 'Trade License': '#d97706',
};

export default function Fraud() {
  const [expanded, setExpanded] = useState(FRAUD_RINGS[0].cluster_id);

  return (
    <div className="page-container">
      <motion.div
        className="mb-10"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal, ease: easeOut }}
      >
        <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
          Fraud ring detection
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-[var(--text-muted)] max-w-xl">
          Detects clusters where the same linking field (email, phone, company) appears across
          records with clearly different identities. High rule score + low DeBERTa score = shared
          field across different people — a fraud ring signal.
        </p>
      </motion.div>

      {/* Summary metrics */}
      <motion.div
        className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-8"
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.05, ...spring }}
      >
        {[
          { label: 'Rings detected', value: FRAUD_RINGS.length, icon: Shield, color: 'text-red-400' },
          { label: 'Records involved', value: FRAUD_RINGS.reduce((s, r) => s + r.records.length, 0), icon: Users, color: 'text-amber-400' },
          { label: 'Shared emails', value: FRAUD_RINGS.filter(r => r.shared_field === 'email').length, icon: Mail, color: 'text-blue-400' },
          { label: 'Shared companies', value: FRAUD_RINGS.filter(r => r.shared_field === 'company').length, icon: AlertTriangle, color: 'text-amber-400' },
        ].map((m, i) => (
          <motion.div
            key={m.label}
            className="metric-card"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.1 + i * STAGGER_MS, ...spring }}
          >
            <m.icon size={14} className={`${m.color} mb-2`} />
            <span className={`metric-value text-xl ${m.color}`}>{m.value}</span>
            <span className="metric-label">{m.label}</span>
          </motion.div>
        ))}
      </motion.div>

      {/* Fraud ring cards */}
      <div className="space-y-6">
        {FRAUD_RINGS.map((ring, i) => {
          const isOpen = expanded === ring.cluster_id;
          return (
            <motion.div
              key={ring.cluster_id}
              className="glass-card"
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.15 + i * 0.08, ...spring }}
            >
              {/* Header */}
              <button
                onClick={() => setExpanded(isOpen ? null : ring.cluster_id)}
                className="w-full flex items-center justify-between text-left"
              >
                <div className="flex items-center gap-3">
                  <div className={`flex h-10 w-10 items-center justify-center rounded-lg shrink-0 ${
                    ring.shared_field === 'email' ? 'bg-red-500/10' : 'bg-amber-500/10'
                  }`}>
                    {ring.shared_field === 'email'
                      ? <Mail size={18} className="text-red-400" />
                      : <Shield size={18} className="text-amber-400" />
                    }
                  </div>
                  <div>
                    <div className="flex items-center gap-2">
                      <h3 className="text-sm font-semibold text-[var(--text-primary)]">
                        Shared {ring.shared_field}
                      </h3>
                      <span className="text-[10px] font-medium px-2 py-0.5 rounded bg-red-500/15 text-red-400">
                        anomaly: {ring.shared_field_score.toFixed(2)}
                      </span>
                    </div>
                    <p className="text-xs font-mono text-[var(--text-muted)] mt-0.5">{ring.shared_value}</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <span className="text-xs text-[var(--text-faint)]">{ring.records.length} records</span>
                  {isOpen ? <ChevronUp size={16} className="text-[var(--text-faint)]" /> : <ChevronDown size={16} className="text-[var(--text-faint)]" />}
                </div>
              </button>

              {/* Expanded content */}
              {isOpen && (
                <motion.div
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: 'auto' }}
                  transition={{ duration: dur.normal }}
                  className="mt-5 pt-5 border-t border-white/[0.06]"
                >
                  {/* Risk signal */}
                  <div className="flex items-start gap-2 mb-5 p-3 rounded-lg bg-red-500/5 border border-red-500/10">
                    <AlertTriangle size={14} className="text-red-400 mt-0.5 shrink-0" />
                    <p className="text-xs text-red-300 leading-relaxed">{ring.risk_signal}</p>
                  </div>

                  {/* Records table */}
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="text-left">
                          <th className="table-header text-xs">ID</th>
                          <th className="table-header text-xs">Name</th>
                          <th className="table-header text-xs">Address</th>
                          <th className="table-header text-xs">Source</th>
                          <th className="table-header text-xs">DOB</th>
                        </tr>
                      </thead>
                      <tbody>
                        {ring.records.map((r) => (
                          <tr key={r.id} className="table-row">
                            <td className="py-2.5 pr-4 font-mono text-[10px] text-[var(--text-faint)]">{r.id}</td>
                            <td className="py-2.5 pr-4 text-xs font-medium text-[var(--text-primary)]">{r.name}</td>
                            <td className="py-2.5 pr-4 text-xs text-[var(--text-muted)] max-w-[200px] truncate">{r.address}</td>
                            <td className="py-2.5 pr-4">
                              <span
                                className="text-[10px] font-medium px-1.5 py-0.5 rounded"
                                style={{
                                  backgroundColor: (SOURCE_COLORS[r.source] || '#c2410c') + '20',
                                  color: SOURCE_COLORS[r.source] || '#c2410c',
                                }}
                              >
                                {r.source}
                              </span>
                            </td>
                            <td className="py-2.5 text-xs font-mono text-[var(--text-muted)]">{r.dob || '--'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </motion.div>
              )}
            </motion.div>
          );
        })}
      </div>
    </div>
  );
}
