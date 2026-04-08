import { motion } from 'framer-motion';
import { Check, AlertTriangle, X } from 'lucide-react';
import { springSnappy } from '../motion';

const CONFIG = {
  MATCH: {
    class: 'badge-match',
    icon: Check,
    label: 'MATCH',
  },
  REVIEW: {
    class: 'badge-review',
    icon: AlertTriangle,
    label: 'REVIEW',
  },
  'NO-MATCH': {
    class: 'badge-nomatch',
    icon: X,
    label: 'NO MATCH',
  },
};

export default function DecisionBadge({ decision, confidence }) {
  const cfg = CONFIG[decision] || CONFIG.REVIEW;
  const Icon = cfg.icon;

  return (
    <motion.div
      className="flex flex-col items-center gap-2"
      initial={{ scale: 0.85, opacity: 0 }}
      animate={{ scale: 1, opacity: 1 }}
      transition={springSnappy}
    >
      <span className={`${cfg.class} text-base px-6 py-2.5`}>
        <Icon size={15} strokeWidth={2.5} aria-hidden="true" />
        {cfg.label}
      </span>
      {confidence && (
        <motion.span
          className="text-xs text-[var(--text-muted)]"
          initial={{ opacity: 0, y: 4 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.15, duration: 0.25 }}
        >
          {confidence} confidence
        </motion.span>
      )}
    </motion.div>
  );
}
