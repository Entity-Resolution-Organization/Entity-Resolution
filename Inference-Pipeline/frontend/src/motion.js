/**
 * Shared motion constants.
 * Standardizes all animation timing across the app.
 *
 * Springs: Use physics-based springs for interactive state changes.
 * Ease:    Use cubic-bezier for duration-based animations (bars, progress).
 * Stagger: 50ms between sibling items in lists.
 */

// ── Springs ────────────────────────────────────────────
// Standard spring for most transitions (cards, containers, results)
export const spring = { type: 'spring', stiffness: 170, damping: 22 };

// Snappy spring for small elements (badges, icons, status pills)
export const springSnappy = { type: 'spring', stiffness: 300, damping: 26 };

// ── Easing ─────────────────────────────────────────────
// Use for duration-based animations (bar fills, progress, SVG strokes)
export const easeOut = [0.25, 1, 0.5, 1];        // quart — smooth, refined
export const easeOutExpo = [0.16, 1, 0.3, 1];    // expo — confident, decisive

// ── Durations ──────────────────────────────────────────
export const dur = {
  instant: 0.15,   // button press feedback
  fast: 0.25,      // state changes (hover, toggle)
  normal: 0.35,    // layout changes (accordion, card reveal)
  slow: 0.6,       // bar fills, progress strokes
};

// ── Stagger ────────────────────────────────────────────
export const STAGGER_MS = 0.05; // 50ms between list items

// Variant helpers for staggered lists
export const stagger = {
  hidden: {},
  visible: { transition: { staggerChildren: STAGGER_MS } },
};

export const fadeUp = {
  hidden: { opacity: 0, y: 16 },
  visible: { opacity: 1, y: 0, transition: { ...spring } },
};

export const fadeIn = {
  hidden: { opacity: 0 },
  visible: { opacity: 1, transition: { duration: dur.normal } },
};

// ── Result reveal choreography ─────────────────────────
// Container staggers children 80ms apart after a short delay
export const resultReveal = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: { staggerChildren: 0.08, delayChildren: 0.04 },
  },
};

export const resultItem = {
  hidden: { opacity: 0, y: 14 },
  visible: { opacity: 1, y: 0, transition: { ...spring } },
};

// ── Page-level entrance ────────────────────────────────
// Non-hero pages: single fade, no stagger
export const pageEnter = {
  initial: { opacity: 0, y: 12 },
  animate: { opacity: 1, y: 0 },
  transition: { duration: dur.normal, ease: easeOut },
};
