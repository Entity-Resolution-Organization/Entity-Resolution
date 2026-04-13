import { Link } from 'react-router-dom';
import { ArrowLeft } from 'lucide-react';

export default function NotFound() {
  return (
    <div className="page-container flex flex-col items-center justify-center py-32 text-center">
      <p className="font-display text-7xl font-medium text-[var(--text-faint)]">404</p>
      <p className="mt-4 text-lg text-[var(--text-secondary)]">Page not found</p>
      <Link to="/" className="btn-primary mt-8 inline-flex items-center gap-2">
        <ArrowLeft size={16} aria-hidden="true" />
        Back to home
      </Link>
    </div>
  );
}
