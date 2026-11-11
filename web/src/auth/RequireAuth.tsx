import { Navigate, useLocation } from 'react-router';
import type { ReactNode } from 'react';
import { useToken } from './session';

// RequireAuth sends a signed-out visitor to /login, and back here afterwards.
export function RequireAuth({ children }: { children: ReactNode }) {
  const token = useToken();
  const { pathname, search } = useLocation();
  if (!token) return <Navigate to={`/login?next=${encodeURIComponent(pathname + search)}`} replace />;
  return <>{children}</>;
}
