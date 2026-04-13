import { useState, useRef, useEffect } from 'react';
import { motion } from 'framer-motion';
import { GitMerge, Users, Loader2, Info } from 'lucide-react';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';
import ForceGraph2D from 'react-force-graph-2d';
import { getClusters, getClusterProfile } from '../api/client';

const SOURCE_COLORS = {
  CRM: '#3b82f6', Billing: '#059669', 'Fraud DB': '#dc2626',
  'Voter Registry': '#b45309', 'HR System': '#7c3aed', 'Tax Records': '#0891b2',
  'Bank Records': '#0d9488', 'Trade License': '#ca8a04', 'OFAC SDN List': '#be123c',
};

export default function Clusters() {
  const [clusterList, setClusterList] = useState([]);
  const [selected, setSelected] = useState(null);
  const [profile, setProfile] = useState(null);
  const [loading, setLoading] = useState(true);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const graphRef = useRef();

  useEffect(() => {
    (async () => {
      try {
        const { data } = await getClusters();
        setClusterList(data.clusters || []);
        // Auto-select first cluster
        if (data.clusters?.length) {
          selectCluster(data.clusters[0].cluster_id);
        }
      } catch {
        // silent
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  const selectCluster = async (clusterId) => {
    setSelected(clusterId); setLoadingProfile(true);
    try {
      const { data } = await getClusterProfile(clusterId);
      setProfile(data);
    } catch {
      setProfile(null);
    } finally {
      setLoadingProfile(false);
    }
  };

  // Build graph data from profile
  const graphData = profile ? {
    nodes: profile.records.map(r => ({
      id: r.record_id,
      name: r.name,
      source: r.source,
    })),
    links: profile.edges.map(e => ({
      source: e.record_id_1,
      target: e.record_id_2,
      score: e.deberta_score,
    })),
  } : null;

  const threshold = 0.45;

  return (
    <div className="page-container">
      <motion.div
        className="mb-8"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal, ease: easeOut }}
      >
        <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
          Cluster explorer
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-[var(--text-muted)] max-w-xl">
          Visualize how transitive closure connects records. If A matches B and B matches C,
          all three are unified — even if A and C score below threshold individually.
        </p>
      </motion.div>

      {loading && (
        <div className="flex items-center justify-center py-16">
          <Loader2 size={24} className="animate-spin text-[var(--accent)]" />
        </div>
      )}

      {!loading && (
        <div className="flex flex-col lg:flex-row gap-6">
          {/* Cluster selector sidebar */}
          <div className="lg:w-72 shrink-0 space-y-2">
            <h3 className="section-label mb-3">Clusters ({clusterList.length})</h3>
            {clusterList.map((c) => (
              <button
                key={c.cluster_id}
                onClick={() => selectCluster(c.cluster_id)}
                className={`w-full text-left p-3 rounded-lg border transition-colors ${
                  selected === c.cluster_id
                    ? 'border-[var(--border-accent)] bg-[var(--accent-dim)]'
                    : 'border-[var(--border-default)] hover:border-[var(--border-accent)] hover:bg-[var(--bg-hover)]'
                }`}
              >
                <div className="flex items-center justify-between">
                  <span className="text-xs font-medium text-[var(--text-primary)]">
                    {c.names.slice(0, 2).join(', ')}
                  </span>
                  <span className="text-[10px] font-mono text-[var(--text-faint)]">{c.cluster_size}</span>
                </div>
                <div className="flex items-center gap-2 mt-1">
                  <span className="text-[10px] text-[var(--text-faint)]">{c.edge_count} edges</span>
                  <span className="text-[10px] text-[var(--text-faint)]">avg {c.avg_edge_score.toFixed(2)}</span>
                </div>
              </button>
            ))}
          </div>

          {/* Main content */}
          <div className="flex-1 space-y-6">
            {loadingProfile && (
              <div className="flex items-center justify-center py-16">
                <Loader2 size={24} className="animate-spin text-[var(--accent)]" />
              </div>
            )}

            {profile && !loadingProfile && (
              <>
                {/* Graph */}
                <motion.div
                  className="glass-card"
                  style={{ height: 380 }}
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={spring}
                >
                  <div className="flex items-center justify-between mb-3">
                    <h3 className="section-label">Cluster graph</h3>
                    <div className="flex items-center gap-4">
                      <div className="flex items-center gap-1.5">
                        <div className="w-6 h-0.5 bg-emerald-400 rounded" />
                        <span className="text-[10px] text-[var(--text-faint)]">direct match</span>
                      </div>
                      <div className="flex items-center gap-1.5">
                        <div className="w-6 h-0.5 bg-amber-400 rounded" style={{ borderTop: '2px dashed #f59e0b' }} />
                        <span className="text-[10px] text-[var(--text-faint)]">transitive link</span>
                      </div>
                    </div>
                  </div>
                  <ForceGraph2D
                    ref={graphRef}
                    graphData={graphData}
                    width={600}
                    height={320}
                    nodeLabel="name"
                    nodeRelSize={7}
                    nodeColor={() => '#818cf8'}
                    nodeCanvasObjectMode={() => 'after'}
                    nodeCanvasObject={(node, ctx, globalScale) => {
                      const label = node.name?.split(' ')[0] || node.id;
                      const fontSize = 10 / globalScale;
                      ctx.font = `${fontSize}px Outfit, sans-serif`;
                      ctx.textAlign = 'center';
                      ctx.fillStyle = '#d1d5db';
                      ctx.fillText(label, node.x, node.y + 12 / globalScale);
                    }}
                    linkColor={(l) => l.score >= threshold ? '#34d399' : '#f59e0b'}
                    linkWidth={(l) => l.score >= threshold ? 2 : 1.5}
                    linkLineDash={(l) => l.score >= threshold ? [] : [4, 4]}
                    linkLabel={(l) => l.score.toFixed(4)}
                    cooldownTicks={60}
                    onEngineStop={() => graphRef.current?.zoomToFit(300, 40)}
                  />
                </motion.div>

                {/* Transitive closure explainer */}
                {profile.edges.some(e => e.deberta_score < threshold) && (
                  <div className="flex items-start gap-2 p-3 rounded-lg bg-amber-500/5 border border-amber-500/10">
                    <Info size={14} className="text-amber-400 mt-0.5 shrink-0" />
                    <p className="text-xs text-amber-300 leading-relaxed">
                      Dashed amber edges show transitive links — pairs below the {threshold} match threshold
                      that are unified through shared connections. The DeBERTa score alone would not match them,
                      but connected component analysis brings them into the same cluster.
                    </p>
                  </div>
                )}

                {/* Edge table */}
                <div className="glass-card overflow-x-auto">
                  <h3 className="section-label mb-4">Edges ({profile.edges.length})</h3>
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left">
                        <th className="table-header text-xs">Record A</th>
                        <th className="table-header text-xs">Record B</th>
                        <th className="table-header text-xs">DeBERTa score</th>
                        <th className="table-header text-xs">Type</th>
                      </tr>
                    </thead>
                    <tbody>
                      {profile.edges.sort((a, b) => b.deberta_score - a.deberta_score).map((e, i) => (
                        <tr key={i} className="table-row">
                          <td className="py-2 pr-4 text-xs font-mono text-[var(--text-secondary)]">{e.record_id_1}</td>
                          <td className="py-2 pr-4 text-xs font-mono text-[var(--text-secondary)]">{e.record_id_2}</td>
                          <td className="py-2 pr-4">
                            <span className={`text-xs font-mono font-medium ${
                              e.deberta_score >= threshold ? 'text-emerald-500' : 'text-amber-500'
                            }`}>
                              {e.deberta_score.toFixed(4)}
                            </span>
                          </td>
                          <td className="py-2 pr-4 text-xs">
                            {e.deberta_score >= threshold ? (
                              <span className="text-emerald-500">direct</span>
                            ) : (
                              <span className="text-amber-500">transitive</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                {/* Records */}
                <div className="glass-card overflow-x-auto">
                  <h3 className="section-label mb-4 flex items-center gap-2">
                    <Users size={14} className="text-[var(--text-muted)]" />
                    Records ({profile.records.length})
                  </h3>
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left">
                        <th className="table-header text-xs">ID</th>
                        <th className="table-header text-xs">Name</th>
                        <th className="table-header text-xs">Address</th>
                        <th className="table-header text-xs">Source</th>
                      </tr>
                    </thead>
                    <tbody>
                      {profile.records.map((r) => (
                        <tr key={r.record_id} className="table-row">
                          <td className="py-2 pr-4 font-mono text-[10px] text-[var(--text-faint)]">{r.record_id}</td>
                          <td className="py-2 pr-4 text-xs font-medium text-[var(--text-primary)]">{r.name}</td>
                          <td className="py-2 pr-4 text-xs text-[var(--text-muted)] max-w-[200px] truncate">{r.address}</td>
                          <td className="py-2 pr-4">
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
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
