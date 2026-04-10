import { lazy, Suspense } from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { MotionConfig } from 'framer-motion';
import ErrorBoundary from './components/ErrorBoundary';
import Layout from './components/Layout';

const Home = lazy(() => import('./pages/Home'));
const Resolve = lazy(() => import('./pages/Resolve'));
const Scenarios = lazy(() => import('./pages/Scenarios'));
const Network = lazy(() => import('./pages/Network'));
const Batch = lazy(() => import('./pages/Batch'));
const Pipeline = lazy(() => import('./pages/Pipeline'));
const Monitor = lazy(() => import('./pages/Monitor'));
const Fraud = lazy(() => import('./pages/Fraud'));
const NotFound = lazy(() => import('./pages/NotFound'));

export default function App() {
  return (
    <ErrorBoundary>
      <MotionConfig reducedMotion="user">
        <BrowserRouter>
          <Suspense fallback={<div className="flex-1 flex items-center justify-center"><div className="h-6 w-6 rounded-full border-2 border-[var(--accent)] border-t-transparent animate-spin" /></div>}>
            <Routes>
              <Route element={<Layout />}>
                <Route index element={<Home />} />
                <Route path="resolve" element={<Resolve />} />
                <Route path="scenarios" element={<Scenarios />} />
                <Route path="network" element={<Network />} />
                <Route path="batch" element={<Batch />} />
                <Route path="pipeline" element={<Pipeline />} />
                <Route path="monitor" element={<Monitor />} />
                <Route path="fraud" element={<Fraud />} />
                <Route path="*" element={<NotFound />} />
              </Route>
            </Routes>
          </Suspense>
        </BrowserRouter>
      </MotionConfig>
    </ErrorBoundary>
  );
}
