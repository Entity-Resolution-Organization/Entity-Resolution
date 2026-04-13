import { Component } from 'react';
import { AlertTriangle } from 'lucide-react';

export default class ErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false };
  }
  static getDerivedStateFromError() {
    return { hasError: true };
  }
  render() {
    if (this.state.hasError) {
      return (
        <div className="flex min-h-screen items-center justify-center bg-[var(--bg-deep)] p-8">
          <div className="text-center">
            <AlertTriangle size={40} className="mx-auto text-[var(--accent)]" aria-hidden="true" />
            <p className="mt-4 text-lg font-medium text-[var(--text-primary)]">Something went wrong</p>
            <p className="mt-2 text-sm text-[var(--text-muted)]">Try refreshing the page.</p>
            <button onClick={() => window.location.reload()} className="btn-primary mt-6">
              Refresh
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
