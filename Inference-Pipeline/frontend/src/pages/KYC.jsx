import { useState, useRef, useEffect, lazy, Suspense } from 'react';
import { motion } from 'framer-motion';
import { AlertTriangle, Shield, Eye, Loader2 } from 'lucide-react';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

const ForceGraph2D = lazy(() => import('react-force-graph-2d'));

const NODES = [
  { id: 'P4-CRM-001', name: 'Maria Garcia', source: 'CRM', hop: 0, flagged: false },
  { id: 'P4-BILL-001', name: 'M Garcia', source: 'Billing', hop: 0, flagged: false },
  { id: 'P5-OFAC-001', name: 'Viktor Petrov', source: 'OFAC SDN', hop: 1, flagged: true },
  { id: 'P5-BANK-001', name: 'V Petrov', source: 'Bank Records', hop: 1, flagged: true },
  { id: 'P5-TRADE-001', name: 'Viktor A Petrov', source: 'Trade License', hop: 1, flagged: true },
];

const EDGES = [
  { source: 'P4-CRM-001', target: 'P4-BILL-001', score: 0.89, type: 'intra-cluster' },
  { source: 'P4-CRM-001', target: 'P5-OFAC-001', score: 0.72, type: 'cross-cluster' },
  { source: 'P5-OFAC-001', target: 'P5-BANK-001', score: 0.91, type: 'intra-cluster' },
  { source: 'P5-OFAC-001', target: 'P5-TRADE-001', score: 0.88, type: 'intra-cluster' },
];

const RISK_SCORES = {
  'P4-CRM-001': { cluster_risk: 0.12, bad_neighbour: 0.50, shared_field: 0.30, centrality: 0.25, composite: 0.31 },
  'P5-OFAC-001': { cluster_risk: 0.35, bad_neighbour: 1.00, shared_field: 0.00, centrality: 0.40, composite: 0.62 },
};

export default function KYC() {
  const containerRef = useRef();
  const [dimensions, setDimensions] = useState({ width: 800, height: 500 });
  const [selectedNode, setSelectedNode] = useState('P4-CRM-001');

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

  const graphData = {
    nodes: NODES.map(n => ({
      ...n,
      color: n.flagged ? '#ef4444' : n.id === selectedNode ? '#818cf8' : '#3b82f6',
    })),
    links: EDGES.map(e => ({ ...e })),
  };

  const scores = RISK_SCORES[selectedNode];

  return (
    <div className="page-container">
      <motion.div
        className="mb-10"
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: dur.normal, ease: easeOut }}
      >
        <h1 className="font-display text-2xl md:text-3xl lg:text-4xl font-bold tracking-tight text-[var(--text-primary)]">
          KYC 2-hop analysis
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-[var(--text-muted)] max-w-xl">
          Know Your Customer network traversal. Maria Garcia has no direct link to the OFAC-flagged
          entity — but she shares a company ("Eurasian Import Export") with Viktor Petrov, who is on the sanctions list.
          Invisible in siloed systems, visible in the unified graph.
        </p>
      </motion.div>

      {/* Path callout */}
      <motion.div
        className="glass-card mb-8 flex items-center gap-4 border-red-500/10"
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.05, ...spring }}
      >
        <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-red-500/10 shrink-0">
          <AlertTriangle size={18} className="text-red-400" />
        </div>
        <div className="flex-1">
          <p className="text-sm font-medium text-[var(--text-primary)]">2-hop risk path detected</p>
          <div className="flex items-center gap-2 mt-1.5 text-xs">
            <span className="px-2 py-0.5 rounded bg-blue-500/15 text-blue-400 font-medium">Maria Garcia</span>
            <span className="text-[var(--text-faint)]">→ shared company →</span>
            <span className="px-2 py-0.5 rounded bg-red-500/15 text-red-400 font-medium">Viktor Petrov (OFAC)</span>
          </div>
        </div>
        <span className="text-xs font-mono px-3 py-1.5 rounded-lg bg-amber-500/10 text-amber-400 font-medium shrink-0">
          bad_neighbour: 0.50
        </span>
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
          <Suspense fallback={<div className="flex items-center justify-center h-full"><Loader2 size={24} className="animate-spin text-[var(--text-muted)]" /></div>}>
            <ForceGraph2D
              graphData={graphData}
              width={dimensions.width}
              height={dimensions.height}
              backgroundColor="transparent"
              nodeRelSize={8}
              nodeCanvasObject={(node, ctx, globalScale) => {
                const r = node.flagged ? 10 : 8;

                // Flagged glow
                if (node.flagged) {
                  ctx.beginPath();
                  ctx.arc(node.x, node.y, r + 4, 0, 2 * Math.PI);
                  ctx.fillStyle = 'rgba(239, 68, 68, 0.15)';
                  ctx.fill();
                }

                ctx.beginPath();
                ctx.arc(node.x, node.y, r, 0, 2 * Math.PI);
                ctx.fillStyle = node.color;
                ctx.fill();
                ctx.strokeStyle = node.flagged ? '#ef4444' : 'rgba(255,255,255,0.2)';
                ctx.lineWidth = node.flagged ? 2 : 1;
                ctx.stroke();

                // Label
                const fontSize = Math.max(11 / globalScale, 3);
                ctx.font = `600 ${fontSize}px Outfit, system-ui`;
                ctx.textAlign = 'center';
                ctx.fillStyle = node.flagged ? 'rgba(239, 68, 68, 0.9)' : 'rgba(255,255,255,0.9)';
                ctx.fillText(node.name, node.x, node.y + r + fontSize + 2);

                // Flagged badge
                if (node.flagged) {
                  ctx.font = `700 ${Math.max(8 / globalScale, 2)}px Outfit`;
                  ctx.fillStyle = '#ef4444';
                  ctx.fillText('OFAC', node.x, node.y - r - 4);
                }
              }}
              linkCanvasObject={(link, ctx) => {
                const start = link.source;
                const end = link.target;
                if (!start.x || !end.x) return;

                ctx.beginPath();
                if (link.type === 'cross-cluster') {
                  ctx.strokeStyle = 'rgba(239, 68, 68, 0.5)';
                  ctx.lineWidth = 2.5;
                  ctx.setLineDash([8, 4]);
                } else {
                  ctx.strokeStyle = 'rgba(100, 116, 139, 0.3)';
                  ctx.lineWidth = 1.5;
                  ctx.setLineDash([]);
                }
                ctx.moveTo(start.x, start.y);
                ctx.lineTo(end.x, end.y);
                ctx.stroke();
                ctx.setLineDash([]);
              }}
              onNodeClick={(node) => setSelectedNode(node.id)}
              d3VelocityDecay={0.3}
              cooldownTicks={100}
            />
          </Suspense>

          {/* Legend */}
          <div className="absolute bottom-4 left-4 flex gap-4 text-[10px] text-stone-400">
            <span className="flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-blue-500" /> Clean entity
            </span>
            <span className="flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-red-500 ring-2 ring-red-500/30" /> Flagged (OFAC)
            </span>
            <span className="flex items-center gap-1.5">
              <span className="h-0.5 w-5 border-t-2 border-dashed border-red-400" /> Cross-cluster link
            </span>
          </div>
        </motion.div>

        {/* Score breakdown */}
        <motion.div
          className="space-y-4"
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.15, ...spring }}
        >
          <h3 className="section-label">Network scores</h3>
          <p className="text-xs text-[var(--text-muted)]">
            Click a node to view its risk breakdown
          </p>

          {scores ? (
            <div className="space-y-3">
              {[
                { label: 'Cluster risk', value: scores.cluster_risk, weight: 0.30, color: 'bg-blue-500' },
                { label: 'Bad neighbour', value: scores.bad_neighbour, weight: 0.40, color: 'bg-red-500' },
                { label: 'Shared field', value: scores.shared_field, weight: 0.20, color: 'bg-amber-500' },
                { label: 'Centrality', value: scores.centrality, weight: 0.10, color: 'bg-purple-500' },
              ].map((s, i) => (
                <motion.div
                  key={s.label}
                  className="glass-card py-3"
                  initial={{ opacity: 0, x: 8 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.2 + i * STAGGER_MS, ...spring }}
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-xs text-[var(--text-secondary)]">{s.label}</span>
                    <div className="flex items-center gap-2">
                      <span className="text-[10px] text-[var(--text-faint)]">w={s.weight}</span>
                      <span className="text-sm font-mono font-semibold tabular-nums text-[var(--text-primary)]">
                        {s.value.toFixed(2)}
                      </span>
                    </div>
                  </div>
                  <div className="h-1.5 rounded-full bg-white/[0.06] overflow-hidden">
                    <motion.div
                      className={`h-full rounded-full ${s.color}`}
                      initial={{ width: 0 }}
                      animate={{ width: `${s.value * 100}%` }}
                      transition={{ duration: dur.slow, ease: easeOut }}
                    />
                  </div>
                </motion.div>
              ))}

              {/* Composite */}
              <div className="glass-card py-4 text-center border-[var(--border-accent)]">
                <p className="text-[10px] uppercase tracking-wider text-[var(--text-faint)] mb-1">Composite context score</p>
                <p className={`text-2xl font-bold font-mono tabular-nums ${
                  scores.composite >= 0.7 ? 'text-red-400' :
                  scores.composite >= 0.4 ? 'text-amber-400' : 'text-emerald-400'
                }`}>
                  {scores.composite.toFixed(2)}
                </p>
                <p className={`text-xs mt-1 ${
                  scores.composite >= 0.7 ? 'text-red-400' :
                  scores.composite >= 0.4 ? 'text-amber-400' : 'text-emerald-400'
                }`}>
                  {scores.composite >= 0.7 ? 'High risk' :
                   scores.composite >= 0.4 ? 'Medium risk' : 'Low risk'}
                </p>
              </div>
            </div>
          ) : (
            <div className="glass-card py-8 text-center">
              <Eye size={20} className="text-[var(--text-faint)] mx-auto mb-2" />
              <p className="text-xs text-[var(--text-muted)]">Click a node to view scores</p>
            </div>
          )}
        </motion.div>
      </div>
    </div>
  );
}
