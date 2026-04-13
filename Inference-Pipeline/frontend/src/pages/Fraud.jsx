import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Shield, Mail, Building2, AlertTriangle, Users, ChevronDown, ChevronUp, Loader2 } from 'lucide-react';
import { getFraudRings } from '../api/client';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

const SOURCE_COLORS = {
  CRM: '#3b82f6', Billing: '#059669', 'Fraud DB': '#dc2626', 'Bank Records': '#0891b2',
  'OFAC SDN List': '#ef4444', 'Trade License': '#d97706', 'HR System': '#7c3aed',
  'Tax Records': '#0891b2', 'Voter Registry': '#b45309',
};

const RISK_SIGNALS = {
  shared_email: 'Same email address appears across records in different identity clusters. This pattern may indicate synthetic identity fraud or account takeover.',
  shared_company: 'Same company links entities from different clusters. This may indicate shell company usage, sanctions evasion, or organized fraud networks.',
};

export default function Fraud() {
  const [rings, setRings] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);
  const [expanded, setExpanded] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        const { data } = await getFraudRings();
        setRings(data.rings || []);
        if (data.rings?.length) setExpanded(data.rings[0].ring_id);
      } catch {
        setErr('Failed to load fraud rings.');
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  const emailRings = rings.filter(r => r.type === 'shared_email');
  const companyRings = rings.filter(r => r.type === 'shared_company');

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
          Cross-cluster anomaly detection. Surfaces records that share linking fields
          (email, phone, company) across different identity clusters — a signal of
          synthetic identity fraud or coordinated networks.
        </p>
      </motion.div>

      {loading && (
        <div className="flex items-center justify-center py-16">
          <Loader2 size={24} className="animate-spin text-[var(--accent)]" />
        </div>
      )}

      {err && (
        <div className="flex items-center gap-3 rounded-xl border border-red-200 bg-red-50 p-4 mb-6">
          <AlertTriangle size={16} className="text-red-600 shrink-0" />
          <p className="text-sm text-red-700">{err}</p>
        </div>
      )}

      {!loading && !err && (
        <>
          {/* Summary metrics */}
          <motion.div
            className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-8"
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.05, ...spring }}
          >
            {[
              { label: 'Rings detected', value: rings.length, icon: Shield, color: 'text-red-400' },
              { label: 'Records involved', value: rings.reduce((s, r) => s + r.record_count, 0), icon: Users, color: 'text-amber-400' },
              { label: 'Shared emails', value: emailRings.length, icon: Mail, color: 'text-blue-400' },
              { label: 'Shared companies', value: companyRings.length, icon: Building2, color: 'text-amber-400' },
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

          {/* Ring cards */}
          <div className="space-y-6">
            {rings.map((ring, i) => {
              const isEmail = ring.type === 'shared_email';
              const isOpen = expanded === ring.ring_id;
              return (
                <motion.div
                  key={ring.ring_id}
                  className="glass-card"
                  initial={{ opacity: 0, y: 12 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.15 + i * 0.08, ...spring }}
                >
                  <button
                    onClick={() => setExpanded(isOpen ? null : ring.ring_id)}
                    className="w-full flex items-center justify-between text-left"
                  >
                    <div className="flex items-center gap-3">
                      <div className={`flex h-10 w-10 items-center justify-center rounded-lg shrink-0 ${
                        isEmail ? 'bg-red-500/10' : 'bg-amber-500/10'
                      }`}>
                        {isEmail
                          ? <Mail size={18} className="text-red-400" />
                          : <Building2 size={18} className="text-amber-400" />
                        }
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <h3 className="text-sm font-semibold text-[var(--text-primary)]">
                            Shared {isEmail ? 'email' : 'company'}
                          </h3>
                          <span className="text-[10px] font-medium px-2 py-0.5 rounded bg-red-500/15 text-red-400">
                            {ring.cluster_count} clusters
                          </span>
                        </div>
                        <p className="text-xs font-mono text-[var(--text-muted)] mt-0.5">{ring.shared_value}</p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="text-xs text-[var(--text-faint)]">{ring.record_count} records</span>
                      {isOpen ? <ChevronUp size={16} className="text-[var(--text-faint)]" /> : <ChevronDown size={16} className="text-[var(--text-faint)]" />}
                    </div>
                  </button>

                  {isOpen && (
                    <motion.div
                      initial={{ opacity: 0, height: 0 }}
                      animate={{ opacity: 1, height: 'auto' }}
                      transition={{ duration: dur.normal }}
                      className="mt-5 pt-5 border-t border-white/[0.06]"
                    >
                      <div className="flex items-start gap-2 mb-5 p-3 rounded-lg bg-red-500/5 border border-red-500/10">
                        <AlertTriangle size={14} className="text-red-400 mt-0.5 shrink-0" />
                        <p className="text-xs text-red-300 leading-relaxed">{RISK_SIGNALS[ring.type]}</p>
                      </div>

                      <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="text-left">
                              <th className="table-header text-xs">Record ID</th>
                              <th className="table-header text-xs">Name</th>
                              <th className="table-header text-xs">Cluster</th>
                              <th className="table-header text-xs">Source</th>
                            </tr>
                          </thead>
                          <tbody>
                            {ring.records.map((r) => (
                              <tr key={r.record_id} className="table-row">
                                <td className="py-2.5 pr-4 font-mono text-[10px] text-[var(--text-faint)]">{r.record_id}</td>
                                <td className="py-2.5 pr-4 text-xs font-medium text-[var(--text-primary)]">{r.name}</td>
                                <td className="py-2.5 pr-4 font-mono text-[10px] text-[var(--text-faint)]">{r.cluster_id.slice(0, 8)}</td>
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
        </>
      )}
    </div>
  );
}
