import { useState } from 'react';
import { motion } from 'framer-motion';
import { Search, User, Building2, Mail, Phone, Calendar, MapPin, Shield, Layers } from 'lucide-react';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

const DEMO_ENTITY = {
  cluster_id: 'a3f8c1e2',
  golden_record: {
    name: 'Michael J Greene',
    address: '22 Oak Street, Apt 4B, Boston MA 02108',
    dob: '1985-03-15',
    email: 'mgreene85@gmail.com',
    phone: '617-555-0142',
    company: 'Vertex Analytics',
  },
  confidence: 0.935,
  sources: [
    {
      id: 'P1-CRM-001', source: 'CRM', name: 'Michael Greene',
      address: '22 Oak Street, Boston MA 02108', dob: '1985-03-15',
      email: 'mgreene85@gmail.com', phone: '617-555-0142', company: 'Vertex Analytics',
    },
    {
      id: 'P1-BILL-001', source: 'Billing', name: 'Mike J Green',
      address: '22 Oak St, Boston MA 02108', dob: '1985-03-15',
      email: 'mike.greene@vertexanalytics.com', phone: '6175550142', company: 'Vertex Analytics Inc',
    },
    {
      id: 'P1-FRAUD-001', source: 'Fraud DB', name: 'M Greene',
      address: '22 Oak Street Boston 02108', dob: '1985-03-15',
      email: 'mgreene85@gmail.com', phone: '', company: '',
    },
    {
      id: 'P1-VOTER-001', source: 'Voter Registry', name: 'Michael J Greene',
      address: '22 Oak St Apt 4B, Boston MA 02108', dob: '1985-03-15',
      email: '', phone: '617-555-0142', company: '',
    },
  ],
};

const SOURCE_COLORS = {
  CRM: '#3b82f6', Billing: '#059669', 'Fraud DB': '#dc2626', 'Voter Registry': '#b45309',
};

const FIELDS = [
  { key: 'name', label: 'Name', icon: User },
  { key: 'address', label: 'Address', icon: MapPin },
  { key: 'dob', label: 'Date of birth', icon: Calendar },
  { key: 'email', label: 'Email', icon: Mail },
  { key: 'phone', label: 'Phone', icon: Phone },
  { key: 'company', label: 'Company', icon: Building2 },
];

export default function Customer360() {
  const entity = DEMO_ENTITY;

  return (
    <div className="page-container">
      <motion.div
        className="mb-10"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal, ease: easeOut }}
      >
        <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
          Customer 360
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-[var(--text-muted)] max-w-xl">
          One entity, unified across every system. The golden record merges the best attributes
          from all source records in the cluster.
        </p>
      </motion.div>

      {/* Golden record card */}
      <motion.div
        className="glass-card mb-8 relative overflow-hidden"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.05, ...spring }}
      >
        <div className="absolute top-0 right-0 w-32 h-32 bg-gradient-to-bl from-[var(--accent)]/5 to-transparent rounded-bl-full" />
        <div className="flex items-center gap-4 mb-6">
          <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-[var(--accent-dim)] border border-[var(--border-accent)]">
            <User size={24} className="text-[var(--accent)]" />
          </div>
          <div>
            <h2 className="text-lg font-bold text-[var(--text-primary)]">{entity.golden_record.name}</h2>
            <div className="flex items-center gap-3 mt-1">
              <span className="text-xs font-mono text-[var(--text-faint)]">cluster: {entity.cluster_id}</span>
              <span className="text-xs px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 font-medium">
                {(entity.confidence * 100).toFixed(0)}% confidence
              </span>
              <span className="text-xs px-2 py-0.5 rounded-full bg-stone-500/10 text-stone-400">
                {entity.sources.length} sources
              </span>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {FIELDS.map(({ key, label, icon: Icon }, i) => (
            <motion.div
              key={key}
              className="flex items-start gap-3 p-3 rounded-lg bg-white/[0.02] border border-white/[0.04]"
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 + i * STAGGER_MS, ...spring }}
            >
              <Icon size={14} className="text-[var(--text-faint)] mt-0.5 shrink-0" />
              <div>
                <p className="text-[10px] uppercase tracking-wider text-[var(--text-faint)]">{label}</p>
                <p className="text-sm font-medium text-[var(--text-primary)] mt-0.5">
                  {entity.golden_record[key] || <span className="text-[var(--text-faint)]">--</span>}
                </p>
              </div>
            </motion.div>
          ))}
        </div>
      </motion.div>

      {/* Source records */}
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2, ...spring }}
      >
        <h3 className="section-label mb-4 flex items-center gap-2">
          <Layers size={14} className="text-[var(--text-muted)]" />
          Source records ({entity.sources.length})
        </h3>

        <div className="glass-card overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left">
                <th className="table-header text-xs">Source</th>
                {FIELDS.map(f => (
                  <th key={f.key} className="table-header text-xs">{f.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {entity.sources.map((src, i) => (
                <motion.tr
                  key={src.id}
                  className="table-row"
                  initial={{ opacity: 0, x: -6 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.25 + i * 0.04, duration: dur.fast, ease: easeOut }}
                >
                  <td className="py-2.5 pr-4">
                    <span
                      className="text-[10px] font-medium px-2 py-1 rounded"
                      style={{
                        backgroundColor: (SOURCE_COLORS[src.source] || '#c2410c') + '20',
                        color: SOURCE_COLORS[src.source] || '#c2410c',
                      }}
                    >
                      {src.source}
                    </span>
                  </td>
                  {FIELDS.map(f => (
                    <td key={f.key} className="py-2.5 pr-4 text-xs text-[var(--text-secondary)] max-w-[160px] truncate">
                      {src[f.key] || <span className="text-[var(--text-faint)]">--</span>}
                    </td>
                  ))}
                </motion.tr>
              ))}
            </tbody>
          </table>
        </div>
      </motion.div>
    </div>
  );
}
