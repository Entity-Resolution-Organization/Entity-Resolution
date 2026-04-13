import { useRef, useCallback } from 'react';

/**
 * SpotlightCard — wraps any element with mouse-tracking border glow.
 *
 * The border illuminates at the cursor position via CSS radial-gradient
 * positioned with --spot-x / --spot-y custom properties.
 * Pure CSS rendering (GPU-accelerated, zero layout cost).
 *
 * Usage:
 *   <SpotlightCard className="glass-card">content</SpotlightCard>
 *   <SpotlightCard as={Link} to="/foo" className="glass-card-hover">link</SpotlightCard>
 */
export default function SpotlightCard({
  children,
  className = '',
  as: Tag = 'div',
  ...props
}) {
  const ref = useRef(null);

  const handleMouseMove = useCallback((e) => {
    const el = ref.current;
    if (!el) return;
    const rect = el.getBoundingClientRect();
    el.style.setProperty('--spot-x', `${e.clientX - rect.left}px`);
    el.style.setProperty('--spot-y', `${e.clientY - rect.top}px`);
  }, []);

  return (
    <Tag
      ref={ref}
      onMouseMove={handleMouseMove}
      className={`spotlight-card ${className}`}
      {...props}
    >
      {children}
    </Tag>
  );
}
