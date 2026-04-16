import { useState, useCallback, useRef, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Loader2, Share2, Database } from 'lucide-react';
import ForceGraph2D from 'react-force-graph-2d';
import { resolveEntities } from '../api/client';
import { spring, dur, STAGGER_MS } from '../motion';

/* ── Static data ──────────────────────────────────────────── */

const RECORDS = [
  { id: 1, name: 'Robert Smith', address: '123 Main St, Raleigh NC', source: 'NC Voters' },
  { id: 2, name: 'Bob Smith', address: '123 Main Street', source: 'Pseudopeople' },
  { id: 3, name: 'SMITH, ROBERT J', address: '123 Main St, Raleigh', source: 'OFAC SDN' },
  { id: 4, name: 'Maria Garcia', address: '789 Elm Ave, Durham NC', source: 'NC Voters' },
  { id: 5, name: 'Maria L. Garcia', address: '789 Elm Avenue', source: 'Pseudopeople' },
  { id: 6, name: 'James Wilson', address: '456 Oak Blvd, Charlotte', source: 'NC Voters' },
  { id: 7, name: 'Mohammad Al-Rashid', address: '45 Desert Rd', source: 'OFAC SDN' },
  { id: 8, name: 'Mohammed Al Rashid', address: '45 Desert Road', source: 'Pseudopeople' },
  { id: 9, name: 'Mohamad Alrashid', address: '45 Desert Rd, Dubai', source: 'Bank CRM' },
  { id: 10, name: 'Alice Johnson', address: '200 Park Ave, Boston', source: 'NC Voters' },
];

const SOURCE_COLORS = {
  'NC Voters': '#3b82f6',
  'Pseudopeople': '#059669',
  'OFAC SDN': '#dc2626',
  'Bank CRM': '#b45309',
};

/* ── Component ────────────────────────────────────────────── */

export default function Network() {
  const [graphData, setGraphData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [stats, setStats] = useState(null);
  const graphRef = useRef();
  const containerRef = useRef();
  const [dimensions, setDimensions] = useState({ width: 900, height: 600 });

  /* Resize observer keeps graph dimensions in sync with container */
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(entries => {
      for (const entry of entries) {
        setDimensions({
          width: entry.contentRect.width - 48,
          height: Math.max(entry.contentRect.height - 48, 600),
        });
      }
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  /* Pairwise resolution */
  const resolveNetwork = async () => {
    setLoading(true);
    setProgress(0);
    setStats(null);
    const nodes = RECORDS.map(r => ({
      id: r.id,
      name: r.name,
      source: r.source,
      color: SOURCE_COLORS[r.source] || '#c2410c',
    }));
    const links = [];
    const totalPairs = (RECORDS.length * (RECORDS.length - 1)) / 2;
    // Build all pairs first
    const pairs = [];
    for (let i = 0; i < RECORDS.length; i++) {
      for (let j = i + 1; j < RECORDS.length; j++) {
        pairs.push([i, j]);
      }
    }

    // Process in batches of 5
    const BATCH_SIZE = 5;
    let done = 0;
    for (let b = 0; b < pairs.length; b += BATCH_SIZE) {
      const batch = pairs.slice(b, b + BATCH_SIZE);
      const batchResults = await Promise.all(
        batch.map(async ([i, j]) => {
          try {
            const { data } = await resolveEntities({
              name1: RECORDS[i].name, address1: RECORDS[i].address,
              name2: RECORDS[j].name, address2: RECORDS[j].address,
            });
            return { i, j, data };
          } catch {
            return null;
          }
        })
      );
      for (const result of batchResults) {
        if (result && result.data.probability > 0.3) {
          links.push({
            source: RECORDS[result.i].id,
            target: RECORDS[result.j].id,
            probability: result.data.probability,
            decision: result.data.decision,
            color: result.data.decision === 'MATCH'
              ? 'rgba(5, 150, 105, 0.6)'
              : result.data.decision === 'REVIEW'
                ? 'rgba(180, 83, 9, 0.45)'
                : 'rgba(220, 38, 38, 0.3)',
            width: result.data.probability * 3.5,
          });
        }
      }
      done += batch.length;
      setProgress((done / totalPairs) * 100);
    }

    setGraphData({ nodes, links });
    setStats({
      totalPairs,
      edges: links.length,
      clusters: new Set(links.flatMap(l => [l.source, l.target])).size,
      matches: links.filter(l => l.decision === 'MATCH').length,
    });
    setLoading(false);
  };

  /* Canvas node painter */
  const paintNode = useCallback((node, ctx) => {
    const r = 7;

    /* glow */
    ctx.shadowBlur = 12;
    ctx.shadowColor = node.color;
    ctx.beginPath();
    ctx.arc(node.x, node.y, r, 0, 2 * Math.PI);
    ctx.fillStyle = node.color;
    ctx.fill();
    ctx.shadowBlur = 0;

    /* border ring */
    ctx.beginPath();
    ctx.arc(node.x, node.y, r + 1.5, 0, 2 * Math.PI);
    ctx.strokeStyle = 'rgba(0,0,0,0.12)';
    ctx.lineWidth = 0.5;
    ctx.stroke();

    /* label */
    ctx.font = '500 3.5px Outfit, system-ui, sans-serif';
    ctx.fillStyle = '#1e293b';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    ctx.fillText(node.name, node.x, node.y + r + 4);
  }, []);

  /* ── Render ─────────────────────────────────────────────── */

  return (
    <div className="page-container">
      {/* Header */}
      <div className="flex items-start justify-between mb-8">
        <div>
          <h1 className="font-display text-2xl md:text-3xl font-bold tracking-tight text-[var(--text-primary)]">
            Entity network
          </h1>
          <p className="mt-2 text-sm text-[var(--text-muted)]">
            Resolve all records pairwise and visualize entity clusters with force-directed layout.
          </p>
        </div>
        <motion.button
          onClick={resolveNetwork}
          disabled={loading}
          className="btn-primary flex items-center gap-2"
          whileTap={{ scale: 0.97 }}
        >
          {loading ? (
            <>
              <Loader2 size={16} className="animate-spin" aria-hidden="true" />
              <span>{progress.toFixed(0)}%</span>
            </>
          ) : (
            <>
              <Share2 size={16} aria-hidden="true" />
              <span>Resolve network</span>
            </>
          )}
        </motion.button>
      </div>

      {/* Legend */}
      <div className="mb-6 flex items-center gap-5">
        {Object.entries(SOURCE_COLORS).map(([src, clr]) => (
          <div
            key={src}
            className="flex items-center gap-2 text-xs text-[var(--text-muted)]"
          >
            <span
              className="h-2.5 w-2.5 rounded-full ring-2 ring-black/10"
              style={{ background: clr }}
            />
            <span>{src}</span>
          </div>
        ))}
      </div>

      {/* Progress bar */}
      {loading && (
        <div className="mb-4 h-1.5 rounded-full bg-black/[0.04] overflow-hidden">
          <motion.div
            className="h-full rounded-full"
            style={{ background: 'linear-gradient(90deg, #c2410c, #ea580c)' }}
            animate={{ width: `${progress}%` }}
            transition={{ duration: dur.fast }}
          />
        </div>
      )}

      {/* Stats */}
      <AnimatePresence>
        {stats && (
          <motion.div
            className="mb-5 grid grid-cols-2 md:grid-cols-4 gap-3"
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={spring}
          >
            {[
              { label: 'Pairs Tested', value: stats.totalPairs },
              { label: 'Edges (>0.3)', value: stats.edges, color: 'text-[var(--accent)]' },
              { label: 'Connected Nodes', value: stats.clusters, color: 'text-[var(--accent)]' },
              { label: 'Strong Matches', value: stats.matches, color: 'text-emerald-600' },
            ].map((s, i) => (
              <motion.div
                key={s.label}
                className="metric-card"
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: i * STAGGER_MS, ...spring }}
              >
                <span className={`metric-value text-xl ${s.color || 'text-[var(--text-primary)]'}`}>
                  {s.value}
                </span>
                <span className="metric-label">{s.label}</span>
              </motion.div>
            ))}
          </motion.div>
        )}
      </AnimatePresence>

      {/* Graph */}
      <div
        ref={containerRef}
        className="glass-card relative"
        aria-label="Entity relationship network graph"
        style={{ minHeight: 640 }}
      >
        {graphData ? (
            <ForceGraph2D
              ref={graphRef}
              graphData={graphData}
              nodeCanvasObject={paintNode}
              nodePointerAreaPaint={(node, color, ctx) => {
                ctx.beginPath();
                ctx.arc(node.x, node.y, 10, 0, 2 * Math.PI);
                ctx.fillStyle = color;
                ctx.fill();
              }}
              linkColor={l => l.color}
              linkWidth={l => l.width}
              linkDirectionalParticles={2}
              linkDirectionalParticleWidth={l => l.probability * 2.5}
              linkDirectionalParticleColor={l => l.color}
              backgroundColor="rgba(0,0,0,0)"
              width={dimensions.width}
              height={dimensions.height}
              d3AlphaDecay={0.02}
              d3VelocityDecay={0.3}
              warmupTicks={50}
              cooldownTime={3000}
            />
        ) : (
          <div className="flex flex-col items-center justify-center gap-3" style={{ minHeight: 600 }}>
            <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-black/[0.02] border border-black/[0.06]">
              <Database size={22} className="text-[var(--text-muted)]" aria-hidden="true" />
            </div>
            <p className="text-sm text-[var(--text-muted)]">
              {loading
                ? 'Building entity graph...'
                : 'Click "Resolve network" to build the entity graph'}
            </p>
          </div>
        )}
      </div>

      {stats && (
        <p className="sr-only">
          Entity network: {stats.totalPairs} pairs tested, {stats.edges} connections found, {stats.matches} strong matches across {stats.clusters} connected entities.
        </p>
      )}

      {/* Input Records */}
      <div className="mt-6 glass-card">
        <h2 className="section-label mb-4">
          Input Records ({RECORDS.length})
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
          {RECORDS.map(r => (
            <motion.div
              key={r.id}
              className="flex items-center gap-3 rounded-lg border border-black/[0.04] bg-white/50 px-3 py-2.5 text-sm transition-colors hover:bg-black/[0.02]"
              initial={{ opacity: 0, y: 6 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: r.id * STAGGER_MS, ...spring }}
            >
              <span
                className="h-2.5 w-2.5 shrink-0 rounded-full ring-2 ring-black/10"
                style={{ background: SOURCE_COLORS[r.source] }}
              />
              <span className="font-medium text-[var(--text-primary)] truncate">
                {r.name}
              </span>
              <span className="text-[var(--text-muted)] truncate flex-1">
                {r.address}
              </span>
              <span className="text-[10px] text-[var(--text-faint)] shrink-0">
                {r.source}
              </span>
            </motion.div>
          ))}
        </div>
      </div>
    </div>
  );
}
