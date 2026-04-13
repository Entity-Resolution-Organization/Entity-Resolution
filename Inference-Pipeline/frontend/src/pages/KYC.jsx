import { useState, useEffect, useCallback, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  ShieldAlert, Eye, AlertTriangle, Loader2, ChevronRight, ArrowLeft,
} from 'lucide-react';
import ForceGraph2D from 'react-force-graph-2d';
import { getKycAlerts, getKycInvestigation } from '../api/client';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

const SEVERITY_STYLES = {
  critical: { bg: 'bg-red-500/10', text: 'text-red-400', border: 'border-red-500/20', label: 'Critical' },
  high:     { bg: 'bg-amber-500/10', text: 'text-amber-400', border: 'border-amber-500/20', label: 'High' },
  medium:   { bg: 'bg-yellow-500/10', text: 'text-yellow-400', border: 'border-yellow-500/20', label: 'Medium' },
};

export default function KYC() {
  const [alerts, setAlerts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);
  const [investigation, setInvestigation] = useState(null);
  const [loadingInv, setLoadingInv] = useState(false);
  const graphRef = useRef();

  useEffect(() => {
    (async () => {
      try {
        const { data } = await getKycAlerts();
        setAlerts(data.alerts || []);
      } catch {
        setErr('Failed to load KYC alerts.');
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  const investigate = useCallback(async (recordId) => {
    setLoadingInv(true); setErr(null);
    try {
      const { data } = await getKycInvestigation(recordId);
      setInvestigation(data);
    } catch {
      setErr('Failed to load investigation.');
    } finally {
      setLoadingInv(false);
    }
  }, []);

  const graphData = investigation ? {
    nodes: investigation.nodes.map(n => ({
      id: n.id,
      name: n.name,
      source: n.source,
      isFlagged: n.is_flagged,
      isTarget: n.is_target,
    })),
    links: investigation.edges.map(e => ({
      source: e.source,
      target: e.target,
      score: e.score,
      type: e.type,
      label: e.label || '',
    })),
  } : null;

  const criticalCount = alerts.filter(a => a.severity === 'critical').length;
  const highCount = alerts.filter(a => a.severity === 'high').length;
  const mediumCount = alerts.filter(a => a.severity === 'medium').length;

  return (
    <div className="page-container">
      <motion.div
        className="mb-8"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal, ease: easeOut }}
      >
        <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
          KYC risk screening
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-[var(--text-muted)] max-w-xl">
          Identifies entities linked to sanctioned or flagged individuals through shared
          corporate structures, emails, or cluster proximity. Click an alert to investigate
          the network path.
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

      {!loading && !err && !investigation && (
        <>
          {/* Summary */}
          <motion.div
            className="grid grid-cols-3 gap-4 mb-8"
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.05, ...spring }}
          >
            {[
              { label: 'Critical', value: criticalCount, color: 'text-red-400' },
              { label: 'High', value: highCount, color: 'text-amber-400' },
              { label: 'Medium', value: mediumCount, color: 'text-yellow-400' },
            ].map((m, i) => (
              <motion.div
                key={m.label}
                className="metric-card"
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.1 + i * STAGGER_MS, ...spring }}
              >
                <span className={`metric-value text-xl ${m.color}`}>{m.value}</span>
                <span className="metric-label">{m.label}</span>
              </motion.div>
            ))}
          </motion.div>

          {/* Alert list */}
          <div className="space-y-3">
            {alerts.map((alert, i) => {
              const s = SEVERITY_STYLES[alert.severity] || SEVERITY_STYLES.medium;
              return (
                <motion.button
                  key={`${alert.record_id}-${alert.risk_type}`}
                  onClick={() => investigate(alert.record_id)}
                  className={`w-full glass-card flex items-center justify-between text-left hover:border-[var(--border-accent)] transition-colors ${s.border}`}
                  initial={{ opacity: 0, x: -8 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.1 + i * 0.04, duration: dur.fast, ease: easeOut }}
                >
                  <div className="flex items-center gap-3">
                    <div className={`flex h-10 w-10 items-center justify-center rounded-lg shrink-0 ${s.bg}`}>
                      <ShieldAlert size={18} className={s.text} />
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-semibold text-[var(--text-primary)]">{alert.name}</span>
                        <span className={`text-[10px] font-medium px-2 py-0.5 rounded ${s.bg} ${s.text}`}>
                          {s.label}
                        </span>
                      </div>
                      <p className="text-xs text-[var(--text-muted)] mt-0.5 max-w-lg">{alert.risk_label}</p>
                    </div>
                  </div>
                  <ChevronRight size={16} className="text-[var(--text-faint)] shrink-0" />
                </motion.button>
              );
            })}
          </div>
        </>
      )}

      {/* Investigation view */}
      {loadingInv && (
        <div className="flex items-center justify-center py-16">
          <Loader2 size={24} className="animate-spin text-[var(--accent)]" />
        </div>
      )}

      <AnimatePresence>
        {investigation && !loadingInv && (
          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }}
            transition={spring}
            className="space-y-6"
          >
            <button
              onClick={() => setInvestigation(null)}
              className="flex items-center gap-1 text-xs text-[var(--accent)] hover:underline"
            >
              <ArrowLeft size={12} /> Back to alerts
            </button>

            {/* Investigation header */}
            <div className="glass-card">
              <div className="flex items-center gap-3">
                <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-red-500/10">
                  <Eye size={20} className="text-red-400" />
                </div>
                <div>
                  <h2 className="text-lg font-bold text-[var(--text-primary)]">{investigation.alert.name}</h2>
                  <p className="text-xs text-[var(--text-muted)] mt-0.5">{investigation.alert.risk_label}</p>
                </div>
              </div>
            </div>

            {/* Graph */}
            {graphData && graphData.nodes.length > 0 && (
              <div className="glass-card" style={{ height: 380 }}>
                <h3 className="section-label mb-3">Network graph</h3>
                <ForceGraph2D
                  ref={graphRef}
                  graphData={graphData}
                  width={700}
                  height={320}
                  nodeLabel="name"
                  nodeRelSize={6}
                  nodeColor={(n) =>
                    n.isFlagged ? '#ef4444' :
                    n.isTarget ? '#f59e0b' :
                    '#818cf8'
                  }
                  nodeCanvasObjectMode={() => 'after'}
                  nodeCanvasObject={(node, ctx, globalScale) => {
                    const label = node.name?.split(' ')[0] || '';
                    const fontSize = 10 / globalScale;
                    ctx.font = `${fontSize}px Outfit, sans-serif`;
                    ctx.textAlign = 'center';
                    ctx.fillStyle = node.isFlagged ? '#fca5a5' : '#d1d5db';
                    ctx.fillText(label, node.x, node.y + 10 / globalScale);
                  }}
                  linkColor={(l) => l.type === 'cross_cluster' ? '#f59e0b' : 'rgba(129,140,248,0.3)'}
                  linkWidth={(l) => l.type === 'cross_cluster' ? 2 : 1}
                  linkLineDash={(l) => l.type === 'cross_cluster' ? [4, 4] : []}
                  linkDirectionalParticles={(l) => l.type === 'cross_cluster' ? 3 : 0}
                  linkDirectionalParticleWidth={3}
                  linkDirectionalParticleColor={() => '#f59e0b'}
                  cooldownTicks={60}
                  onEngineStop={() => graphRef.current?.zoomToFit(300, 40)}
                />
              </div>
            )}

            {/* Node list */}
            <div className="glass-card">
              <h3 className="section-label mb-4">Entities in network ({investigation.nodes.length})</h3>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left">
                      <th className="table-header text-xs">Name</th>
                      <th className="table-header text-xs">Record ID</th>
                      <th className="table-header text-xs">Source</th>
                      <th className="table-header text-xs">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {investigation.nodes.map((n) => (
                      <tr key={n.id} className="table-row">
                        <td className="py-2.5 pr-4 text-xs font-medium text-[var(--text-primary)]">{n.name}</td>
                        <td className="py-2.5 pr-4 font-mono text-[10px] text-[var(--text-faint)]">{n.id}</td>
                        <td className="py-2.5 pr-4 text-xs text-[var(--text-muted)]">{n.source}</td>
                        <td className="py-2.5 pr-4">
                          {n.is_flagged ? (
                            <span className="text-[10px] font-medium px-2 py-0.5 rounded bg-red-500/15 text-red-400">OFAC flagged</span>
                          ) : n.is_target ? (
                            <span className="text-[10px] font-medium px-2 py-0.5 rounded bg-amber-500/15 text-amber-400">under investigation</span>
                          ) : (
                            <span className="text-[10px] text-[var(--text-faint)]">linked</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
