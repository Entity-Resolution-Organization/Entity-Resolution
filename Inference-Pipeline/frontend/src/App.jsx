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
const Clusters = lazy(() => import('./pages/Clusters'));
const Customer360 = lazy(() => import('./pages/Customer360'));
const KYC = lazy(() => import('./pages/KYC'));
const Fraud = lazy(() => import('./pages/Fraud'));
const Analytics = lazy(() => import('./pages/Analytics'));
const Monitor = lazy(() => import('./pages/Monitor'));
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
                <Route path="match" element={<Resolve />} />
                <Route path="resolve" element={<Resolve />} />
                <Route path="scenarios" element={<Scenarios />} />
                <Route path="network" element={<Network />} />
                <Route path="batch" element={<Batch />} />
                <Route path="clusters" element={<Clusters />} />
                <Route path="customer360" element={<Customer360 />} />
                <Route path="kyc" element={<KYC />} />
                <Route path="fraud" element={<Fraud />} />
                <Route path="analytics" element={<Analytics />} />
                <Route path="monitor" element={<Monitor />} />
                <Route path="*" element={<NotFound />} />
              </Route>
            </Routes>
          </Suspense>
        </BrowserRouter>
      </MotionConfig>
    </ErrorBoundary>
  );
}
