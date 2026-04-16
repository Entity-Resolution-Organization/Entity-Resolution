import { useState, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Search, User, Building2, Mail, Phone, Calendar, MapPin,
  Layers, Loader2, AlertTriangle,
} from 'lucide-react';
import { search360, getClusterProfile } from '../api/client';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

const SOURCE_COLORS = {
  CRM: '#3b82f6', Billing: '#059669', 'Fraud DB': '#dc2626',
  'Voter Registry': '#b45309', 'HR System': '#7c3aed', 'Tax Records': '#0891b2',
  'Bank Records': '#0d9488', 'Trade License': '#ca8a04', 'OFAC SDN List': '#be123c',
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
  const [query, setQuery] = useState('');
  const [searchResults, setSearchResults] = useState(null);
  const [profile, setProfile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [err, setErr] = useState(null);

  const handleSearch = useCallback(async () => {
    if (!query.trim()) return;
    setLoading(true); setErr(null); setSearchResults(null); setProfile(null);
    try {
      const { data } = await search360(query.trim());
      setSearchResults(data.results);
      // Auto-load first result
      if (data.results.length === 1) {
        loadProfile(data.results[0].cluster_id);
      }
    } catch {
      setErr('Search failed. Ensure the backend is running.');
    } finally {
      setLoading(false);
    }
  }, [query]);

  const loadProfile = async (clusterId) => {
    setLoadingProfile(true); setErr(null);
    try {
      const { data } = await getClusterProfile(clusterId);
      setProfile(data);
    } catch {
      setErr('Failed to load cluster profile.');
    } finally {
      setLoadingProfile(false);
    }
  };

  return (
    <div className="page-container">
      <motion.div
        className="mb-8"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal, ease: easeOut }}
      >
        <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
          Customer 360
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-[var(--text-muted)] max-w-xl">
          Search for an entity to see their unified golden record — merged from
          every source system in the cluster.
        </p>
      </motion.div>

      {/* Search bar */}
      <motion.div
        className="glass-card mb-6"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.05, ...spring }}
      >
        <div className="flex gap-3">
          <div className="flex-1">
            <input
              className="input-field"
              placeholder="Search by name (e.g. Michael Greene, Sarah Chen, James Wilson)"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') handleSearch(); }}
            />
          </div>
          <button onClick={handleSearch} disabled={loading || !query.trim()} className="btn-primary text-sm flex items-center gap-2 px-5">
            {loading ? <Loader2 size={14} className="animate-spin" /> : <Search size={14} />}
            Search
          </button>
        </div>
      </motion.div>

      {/* Error */}
      <AnimatePresence>
        {err && (
          <motion.div
            role="alert"
            initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            className="flex items-center gap-3 rounded-xl border border-red-200 bg-red-50 p-4 mb-6"
          >
            <AlertTriangle size={16} className="text-red-600 shrink-0" />
            <p className="text-sm text-red-700">{err}</p>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Search results (multiple matches) */}
      {searchResults && searchResults.length > 1 && !profile && (
        <motion.div
          className="glass-card mb-6"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={spring}
        >
          <h3 className="section-label mb-4">{searchResults.length} entities found</h3>
          <div className="space-y-2">
            {searchResults.map((r) => (
              <button
                key={r.cluster_id}
                onClick={() => loadProfile(r.cluster_id)}
                className="w-full flex items-center justify-between p-3 rounded-lg border border-[var(--border-default)] hover:border-[var(--border-accent)] hover:bg-[var(--bg-hover)] transition-colors text-left"
              >
                <div className="flex items-center gap-3">
                  <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-[var(--accent-dim)]">
                    <User size={16} className="text-[var(--accent)]" />
                  </div>
                  <div>
                    <p className="text-sm font-medium text-[var(--text-primary)]">{r.name}</p>
                    <p className="text-xs text-[var(--text-muted)]">
                      {r.cluster_size} records from {r.sources.join(', ')}
                    </p>
                  </div>
                </div>
                <span className="text-xs font-mono text-[var(--text-faint)]">{r.avg_edge_score.toFixed(2)}</span>
              </button>
            ))}
          </div>
        </motion.div>
      )}

      {/* No results */}
      {searchResults && searchResults.length === 0 && (
        <motion.div
          className="glass-card text-center py-12"
          initial={{ opacity: 0 }} animate={{ opacity: 1 }}
        >
          <Search size={32} className="mx-auto text-[var(--text-faint)] mb-3" />
          <p className="text-sm text-[var(--text-muted)]">No entities found for "{query}"</p>
        </motion.div>
      )}

      {/* Loading profile */}
      {loadingProfile && (
        <div className="flex items-center justify-center py-16">
          <Loader2 size={24} className="animate-spin text-[var(--accent)]" />
        </div>
      )}

      {/* Profile view */}
      {profile && !loadingProfile && (
        <motion.div
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={spring}
          className="space-y-6"
        >
          {/* Back to results */}
          {searchResults && searchResults.length > 1 && (
            <button
              onClick={() => setProfile(null)}
              className="text-xs text-[var(--accent)] hover:underline"
            >
              ← Back to search results
            </button>
          )}

          {/* Golden record card */}
          <div className="glass-card relative overflow-hidden">
            <div className="absolute top-0 right-0 w-32 h-32 bg-gradient-to-bl from-[var(--accent)]/5 to-transparent rounded-bl-full" />
            <div className="flex items-center gap-4 mb-6">
              <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-[var(--accent-dim)] border border-[var(--border-accent)]">
                <User size={24} className="text-[var(--accent)]" />
              </div>
              <div>
                <h2 className="text-lg font-bold text-[var(--text-primary)]">{profile.golden_record.name}</h2>
                <div className="flex items-center gap-3 mt-1 flex-wrap">
                  <span className="text-xs font-mono text-[var(--text-faint)]">cluster: {profile.cluster_id.slice(0, 8)}</span>
                  <span className="text-xs px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 font-medium">
                    {(profile.meta.avg_edge_score * 100).toFixed(0)}% confidence
                  </span>
                  <span className="text-xs px-2 py-0.5 rounded-full bg-stone-500/10 text-stone-400">
                    {profile.golden_record.source_count} sources
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
                      {profile.golden_record[key] || <span className="text-[var(--text-faint)]">--</span>}
                    </p>
                  </div>
                </motion.div>
              ))}
            </div>
          </div>

          {/* Source records */}
          <div>
            <h3 className="section-label mb-4 flex items-center gap-2">
              <Layers size={14} className="text-[var(--text-muted)]" />
              Source records ({profile.records.length})
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
                  {profile.records.map((src, i) => (
                    <motion.tr
                      key={src.record_id}
                      className="table-row"
                      initial={{ opacity: 0, x: -6 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: 0.1 + i * 0.04, duration: dur.fast, ease: easeOut }}
                    >
                      <td className="py-2.5 pr-4">
                        <span
                          className="text-[10px] font-medium px-2 py-1 rounded whitespace-nowrap"
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
          </div>

          {/* Edge scores */}
          {profile.edges.length > 0 && (
            <div>
              <h3 className="section-label mb-4">Match edges ({profile.edges.length})</h3>
              <div className="glass-card overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left">
                      <th className="table-header text-xs">Record A</th>
                      <th className="table-header text-xs">Record B</th>
                      <th className="table-header text-xs">DeBERTa</th>
                      <th className="table-header text-xs">Fields matched</th>
                    </tr>
                  </thead>
                  <tbody>
                    {profile.edges.map((e, i) => (
                      <tr key={i} className="table-row">
                        <td className="py-2 pr-4 text-xs font-mono text-[var(--text-secondary)]">{e.record_id_1}</td>
                        <td className="py-2 pr-4 text-xs font-mono text-[var(--text-secondary)]">{e.record_id_2}</td>
                        <td className="py-2 pr-4">
                          <span className={`text-xs font-mono font-medium ${e.deberta_score >= 0.8 ? 'text-emerald-600' : e.deberta_score >= 0.5 ? 'text-amber-600' : 'text-red-600'}`}>
                            {e.deberta_score.toFixed(4)}
                          </span>
                        </td>
                        <td className="py-2 pr-4 text-xs text-[var(--text-muted)]">
                          {[
                            e.has_email && 'email',
                            e.has_phone && 'phone',
                            e.has_dob && 'dob',
                            e.has_company && 'company',
                          ].filter(Boolean).join(', ') || 'name + address only'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </motion.div>
      )}
    </div>
  );
}
