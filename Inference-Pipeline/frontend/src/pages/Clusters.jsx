import { useState, useRef, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Search, GitMerge, Users, ArrowRight, Loader2, Info } from 'lucide-react';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';
import ForceGraph2D from 'react-force-graph-2d';

const DEMO_CLUSTER = {
  cluster_id: 'demo-transitive',
  records: [
    { id: 'R1', name: 'Mike Green', address: '22 Oak St', source: 'CRM' },
    { id: 'R2', name: 'Michael Greene', address: '48 2nd Ave', source: 'Billing' },
    { id: 'R3', name: 'Mike J Green', address: '22 Oak St', source: 'Fraud DB' },
  ],
  edges: [
    { source: 'R1', target: 'R3', score: 0.91, direct: true },
    { source: 'R3', target: 'R2', score: 0.87, direct: true },
    { source: 'R1', target: 'R2', score: 0.41, direct: false },
  ],
};

const SOURCE_COLORS = {
  CRM: '#3b82f6',
  Billing: '#059669',
  'Fraud DB': '#dc2626',
  'Voter Registry': '#b45309',
  'HR System': '#8b5cf6',
  'OFAC SDN List': '#ef4444',
  'Bank Records': '#0891b2',
  'Tax Records': '#65a30d',
  'Trade License': '#d97706',
};

export default function Clusters() {
  const [selected, setSelected] = useState(DEMO_CLUSTER);
  const [hovered, setHovered] = useState(null);
  const containerRef = useRef();
  const [dimensions, setDimensions] = useState({ width: 800, height: 500 });

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(entries => {
      for (const entry of entries) {
        setDimensions({
          width: entry.contentRect.width - 48,
          height: Math.max(entry.contentRect.height - 48, 450),
        });
      }
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const graphData = selected ? {
    nodes: selected.records.map(r => ({
      id: r.id,
      name: r.name,
      source: r.source,
      color: SOURCE_COLORS[r.source] || '#c2410c',
    })),
    links: selected.edges.map(e => ({
      source: e.source,
      target: e.target,
      score: e.score,
      direct: e.direct,
    })),
  } : null;

  return (
    <div className="page-container">
      <motion.div
        className="mb-10"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal, ease: easeOut }}
      >
        <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
          Transitive cluster viewer
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-[var(--text-muted)] max-w-xl">
          Visualize how transitive closure connects records that DeBERTa alone would miss.
          Record 1 and Record 2 score below threshold pairwise (0.41) but both match Record 3 — so all three unify.
        </p>
      </motion.div>

      {/* Key insight callout */}
      <motion.div
        className="glass-card mb-8 flex items-start gap-3"
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.05, ...spring }}
      >
        <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-amber-500/10 shrink-0 mt-0.5">
          <Info size={16} className="text-amber-400" />
        </div>
        <div>
          <p className="text-sm font-medium text-[var(--text-primary)]">Why this matters</p>
          <p className="mt-1 text-xs text-[var(--text-muted)] leading-relaxed">
            DeBERTa scores R1↔R2 at <span className="font-mono text-red-400">0.41</span> (below 0.45 threshold).
            But R1↔R3 scores <span className="font-mono text-emerald-400">0.91</span> and R3↔R2 scores <span className="font-mono text-emerald-400">0.87</span>.
            Connected components collapses all three into one entity. The
            <span className="text-amber-400"> amber dashed edge</span> shows the transitive-only link.
          </p>
        </div>
      </motion.div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Graph */}
        <motion.div
          ref={containerRef}
          className="glass-card lg:col-span-2 min-h-[500px] relative"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1, ...spring }}
        >
          {graphData && (
              <ForceGraph2D
                graphData={graphData}
                width={dimensions.width}
                height={dimensions.height}
                backgroundColor="transparent"
                nodeRelSize={8}
                nodeCanvasObject={(node, ctx, globalScale) => {
                  const r = 8;
                  ctx.beginPath();
                  ctx.arc(node.x, node.y, r, 0, 2 * Math.PI);
                  ctx.fillStyle = node.color;
                  ctx.fill();
                  ctx.strokeStyle = hovered === node.id ? '#fff' : 'rgba(255,255,255,0.2)';
                  ctx.lineWidth = hovered === node.id ? 2.5 : 1;
                  ctx.stroke();

                  const label = node.name;
                  const fontSize = Math.max(11 / globalScale, 3);
                  ctx.font = `600 ${fontSize}px Outfit, system-ui`;
                  ctx.textAlign = 'center';
                  ctx.fillStyle = 'rgba(255,255,255,0.9)';
                  ctx.fillText(label, node.x, node.y + r + fontSize + 2);
                }}
                linkCanvasObject={(link, ctx) => {
                  const start = link.source;
                  const end = link.target;
                  if (!start.x || !end.x) return;

                  ctx.beginPath();
                  if (link.direct) {
                    ctx.strokeStyle = `rgba(5, 150, 105, ${Math.max(0.3, link.score)})`;
                    ctx.lineWidth = 2;
                    ctx.setLineDash([]);
                  } else {
                    ctx.strokeStyle = 'rgba(245, 158, 11, 0.7)';
                    ctx.lineWidth = 2;
                    ctx.setLineDash([6, 4]);
                  }
                  ctx.moveTo(start.x, start.y);
                  ctx.lineTo(end.x, end.y);
                  ctx.stroke();
                  ctx.setLineDash([]);

                  const midX = (start.x + end.x) / 2;
                  const midY = (start.y + end.y) / 2;
                  ctx.font = '600 10px Outfit, monospace';
                  ctx.textAlign = 'center';
                  ctx.fillStyle = link.direct ? 'rgba(5, 150, 105, 0.9)' : 'rgba(245, 158, 11, 0.9)';
                  ctx.fillText(link.score.toFixed(2), midX, midY - 6);
                }}
                onNodeHover={(node) => setHovered(node?.id || null)}
                d3VelocityDecay={0.3}
                cooldownTicks={100}
              />
          )}

          {/* Legend */}
          <div className="absolute bottom-4 left-4 flex gap-4 text-[10px] text-stone-400">
            <span className="flex items-center gap-1.5">
              <span className="h-2 w-6 rounded-full bg-emerald-500" /> Direct match
            </span>
            <span className="flex items-center gap-1.5">
              <span className="h-0.5 w-6 border-t-2 border-dashed border-amber-500" /> Transitive only
            </span>
          </div>
        </motion.div>

        {/* Record details panel */}
        <motion.div
          className="space-y-4"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.15, ...spring }}
        >
          <h3 className="section-label">Cluster records</h3>
          {selected?.records.map((r, i) => (
            <motion.div
              key={r.id}
              className={`glass-card transition-colors ${hovered === r.id ? 'border-[var(--border-accent)]' : ''}`}
              initial={{ opacity: 0, x: 8 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.2 + i * STAGGER_MS, ...spring }}
              onMouseEnter={() => setHovered(r.id)}
              onMouseLeave={() => setHovered(null)}
            >
              <div className="flex items-center gap-2 mb-2">
                <span className="text-[10px] font-mono text-[var(--text-faint)]">{r.id}</span>
                <span
                  className="text-[10px] font-medium px-1.5 py-0.5 rounded"
                  style={{
                    backgroundColor: (SOURCE_COLORS[r.source] || '#c2410c') + '20',
                    color: SOURCE_COLORS[r.source] || '#c2410c',
                  }}
                >
                  {r.source}
                </span>
              </div>
              <p className="text-sm font-semibold text-[var(--text-primary)]">{r.name}</p>
              <p className="text-xs text-[var(--text-muted)] mt-0.5">{r.address}</p>
            </motion.div>
          ))}

          {/* Edge table */}
          <h3 className="section-label mt-6">Edge scores</h3>
          <div className="glass-card">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-left text-[var(--text-faint)]">
                  <th className="pb-2">Pair</th>
                  <th className="pb-2 text-right">Score</th>
                  <th className="pb-2 text-right">Type</th>
                </tr>
              </thead>
              <tbody>
                {selected?.edges.map((e, i) => (
                  <tr key={i} className="border-t border-white/[0.04]">
                    <td className="py-2 font-mono text-[var(--text-secondary)]">
                      {e.source}↔{e.target}
                    </td>
                    <td className={`py-2 text-right font-mono tabular-nums ${
                      e.score >= 0.45 ? 'text-emerald-400' : 'text-red-400'
                    }`}>
                      {e.score.toFixed(2)}
                    </td>
                    <td className="py-2 text-right">
                      {e.direct
                        ? <span className="text-emerald-400">direct</span>
                        : <span className="text-amber-400">transitive</span>
                      }
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </motion.div>
      </div>
    </div>
  );
}
