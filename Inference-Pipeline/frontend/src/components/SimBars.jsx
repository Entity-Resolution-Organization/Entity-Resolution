import { memo } from 'react';
import { motion } from 'framer-motion';
import { spring, easeOut, dur, STAGGER_MS } from '../motion';

const LABELS = {
  name_jaro_winkler: 'Name (Jaro-Winkler)',
  name_levenshtein_ratio: 'Name (Levenshtein)',
  address_token_overlap: 'Address (Token overlap)',
  address_jaro_winkler: 'Address (Jaro-Winkler)',
};

function barColor(pct) {
  if (pct >= 80) return 'bg-emerald-500';
  if (pct >= 50) return 'bg-amber-500';
  return 'bg-red-500';
}

function SimBars({ similarities }) {
  if (!similarities) return null;
  const entries = Object.entries(similarities);

  return (
    <div className="space-y-4">
      {entries.map(([key, val], i) => {
        const pct = Math.round(val * 100);
        return (
          <motion.div
            key={key}
            initial={{ opacity: 0, x: -8 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ ...spring, delay: i * STAGGER_MS }}
          >
            <div className="flex justify-between text-xs mb-1.5">
              <span className="text-[var(--text-muted)]">{LABELS[key] || key}</span>
              <span className="font-semibold text-[var(--text-primary)] tabular-nums">{pct}%</span>
            </div>
            <div className="h-1.5 rounded-full bg-black/[0.06] overflow-hidden">
              <motion.div
                className={`h-full rounded-full ${barColor(pct)}`}
                initial={{ width: 0 }}
                animate={{ width: `${Math.min(pct, 100)}%` }}
                transition={{ duration: dur.slow, delay: i * STAGGER_MS + 0.1, ease: easeOut }}
              />
            </div>
          </motion.div>
        );
      })}
    </div>
  );
}

export default memo(SimBars);
