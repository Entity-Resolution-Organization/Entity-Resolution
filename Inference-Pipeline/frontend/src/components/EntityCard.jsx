import { motion } from 'framer-motion';
import { User, MapPin } from 'lucide-react';
import { spring } from '../motion';

export default function EntityCard({ label, name, address }) {
  return (
    <motion.div
      className="glass-card border border-black/[0.06]"
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={spring}
    >
      <p className="section-label mb-4 text-[var(--accent)]">
        {label}
      </p>

      <div className="space-y-3">
        <div className="flex items-start gap-3">
          <div className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-[var(--accent-dim)]">
            <User size={14} className="text-[var(--accent)]" />
          </div>
          <div>
            <p className="text-[11px] text-[var(--text-muted)] mb-0.5">Name</p>
            <p className="text-base font-semibold text-[var(--text-primary)] leading-tight">
              {name || <span className="text-[var(--text-muted)] font-normal italic">empty</span>}
            </p>
          </div>
        </div>

        <div className="flex items-start gap-3">
          <div className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-[var(--accent-dim)]">
            <MapPin size={14} className="text-[var(--accent)]" />
          </div>
          <div>
            <p className="text-[11px] text-[var(--text-muted)] mb-0.5">Address</p>
            <p className="text-sm text-[var(--text-secondary)] leading-tight">
              {address || <span className="text-[var(--text-muted)] italic">empty</span>}
            </p>
          </div>
        </div>
      </div>
    </motion.div>
  );
}
