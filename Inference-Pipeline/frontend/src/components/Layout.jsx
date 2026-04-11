import { useState, useEffect, useRef } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Home, GitCompare, LayoutGrid, Share2, Upload, BarChart3, Activity, Shield,
  Fingerprint, Menu, X, GitMerge, User, ShieldAlert, Eye, TrendingUp,
} from 'lucide-react';
import { springSnappy } from '../motion';

const NAV_ITEMS = [
  { to: '/',             icon: Home,        label: 'Home' },
  { to: '/match',        icon: GitCompare,  label: 'Match' },
  { to: '/batch',        icon: Upload,      label: 'Batch' },
  { to: '/clusters',     icon: GitMerge,    label: 'Clusters' },
  { to: '/customer360',  icon: User,        label: '360' },
  { to: '/kyc',          icon: Eye,         label: 'KYC' },
  { to: '/fraud',        icon: Shield,      label: 'Fraud' },
  { to: '/analytics',    icon: TrendingUp,  label: 'Analytics' },
  { to: '/pipeline',     icon: BarChart3,   label: 'Pipeline' },
];

export default function Layout() {
  const [mobileOpen, setMobileOpen] = useState(false);
  const drawerRef = useRef(null);

  /* Close on Escape */
  useEffect(() => {
    if (!mobileOpen) return;
    const handle = (e) => { if (e.key === 'Escape') setMobileOpen(false); };
    document.addEventListener('keydown', handle);
    return () => document.removeEventListener('keydown', handle);
  }, [mobileOpen]);

  /* Focus trap for mobile drawer */
  useEffect(() => {
    if (!mobileOpen || !drawerRef.current) return;
    const el = drawerRef.current;
    const focusable = el.querySelectorAll('a, button, [tabindex="0"]');
    if (!focusable.length) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    first.focus();
    const trap = (e) => {
      if (e.key !== 'Tab') return;
      if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
      if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    };
    el.addEventListener('keydown', trap);
    return () => el.removeEventListener('keydown', trap);
  }, [mobileOpen]);

  return (
    <div className="flex flex-col min-h-screen bg-[var(--bg-deep)]">
      <a href="#main-content" className="skip-link">Skip to content</a>

      {/* ── Top navigation bar ─────────────────────────── */}
      <header className="sticky top-0 z-30 border-b border-white/[0.06] bg-[#0a0a0a]">
        <div className="flex items-center h-14 px-5 md:px-8">

          {/* Brand */}
          <NavLink to="/" className="flex items-center gap-2.5 shrink-0 no-underline">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-white/10 border border-white/15">
              <Fingerprint size={16} className="text-white" aria-hidden="true" />
            </div>
            <span className="text-[15px] font-bold tracking-tight text-white">
              Resolv
            </span>
          </NavLink>

          {/* Desktop nav */}
          <nav className="hidden md:flex items-center gap-1 ml-8" aria-label="Main navigation">
            {NAV_ITEMS.map(({ to, icon: Icon, label }) => (
              <NavLink
                key={to}
                to={to}
                end={to === '/'}
                className={({ isActive }) =>
                  `relative flex items-center gap-1.5 px-3 py-1.5 rounded-md text-[13px] font-medium transition-colors ${
                    isActive
                      ? 'text-orange-400 bg-orange-500/10'
                      : 'text-stone-400 hover:text-stone-200 hover:bg-white/[0.05]'
                  }`
                }
              >
                {({ isActive }) => (
                  <>
                    <Icon size={14} strokeWidth={isActive ? 2 : 1.75} aria-hidden="true" />
                    <span>{label}</span>
                    {isActive && (
                      <motion.div
                        className="absolute bottom-0 left-3 right-3 h-[2px] rounded-full bg-orange-400"
                        layoutId="nav-indicator"
                        transition={springSnappy}
                      />
                    )}
                  </>
                )}
              </NavLink>
            ))}
          </nav>

          <div className="ml-auto" />

          {/* Mobile hamburger */}
          <button
            className="flex md:hidden items-center justify-center h-9 w-9 rounded-lg ml-auto text-stone-400 hover:text-stone-200 hover:bg-white/[0.05] transition-colors"
            onClick={() => setMobileOpen(true)}
            aria-label="Open navigation"
          >
            <Menu size={20} aria-hidden="true" />
          </button>
        </div>
      </header>

      {/* ── Mobile drawer ──────────────────────────────── */}
      <AnimatePresence>
        {mobileOpen && (
          <>
            <motion.div
              key="backdrop"
              className="fixed inset-0 z-40 bg-black/50 md:hidden"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setMobileOpen(false)}
              aria-hidden="true"
            />
            <motion.div
              key="drawer"
              ref={drawerRef}
              role="dialog"
              aria-label="Navigation"
              className="fixed inset-y-0 left-0 z-50 w-64 flex flex-col bg-[var(--bg-base)] px-4 py-5 md:hidden"
              initial={{ x: '-100%' }}
              animate={{ x: 0 }}
              exit={{ x: '-100%' }}
              transition={{ type: 'spring', stiffness: 400, damping: 40 }}
            >
              <div className="flex items-center justify-between mb-6">
                <span className="text-sm font-bold text-white">
                  Resolv
                </span>
                <button
                  onClick={() => setMobileOpen(false)}
                  className="flex h-8 w-8 items-center justify-center rounded-lg text-stone-400 hover:bg-white/[0.06]"
                  aria-label="Close navigation"
                >
                  <X size={16} aria-hidden="true" />
                </button>
              </div>
              <nav className="flex flex-col gap-1">
                {NAV_ITEMS.map(({ to, icon: Icon, label }) => (
                  <NavLink
                    key={to}
                    to={to}
                    end={to === '/'}
                    onClick={() => setMobileOpen(false)}
                    className={({ isActive }) =>
                      `flex items-center gap-3 px-3 py-2.5 rounded-md text-sm font-medium transition-colors ${
                        isActive
                          ? 'text-orange-400 bg-orange-500/10'
                          : 'text-stone-400 hover:text-stone-200 hover:bg-white/[0.06]'
                      }`
                    }
                  >
                    <Icon size={16} aria-hidden="true" />
                    <span>{label}</span>
                  </NavLink>
                ))}
              </nav>
              <div className="mt-auto pt-4 border-t border-white/[0.06] text-[11px] text-stone-500">
                <p className="text-[10px] text-stone-600">Northeastern University · Spring 2026</p>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* ── Main content (full width) ──────────────────── */}
      <main id="main-content" className="flex-1 w-full overflow-x-hidden bg-[var(--bg-deep)] py-6 md:py-8">
        <Outlet />
      </main>
    </div>
  );
}
