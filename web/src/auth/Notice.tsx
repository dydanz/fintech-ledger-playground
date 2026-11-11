import { useEffect, useState } from 'react';
import { useLocation } from 'react-router';
import { session } from './session';

// Notice shows a one-line message: from navigation state ("Account created"),
// or the flash left when a session ended ("Password changed").
export function Notice() {
  const fromState = (useLocation().state as { notice?: string } | null)?.notice;
  const [flash] = useState(() => session.peekFlash());
  useEffect(() => session.dropFlash(), []);
  const notice = fromState ?? flash;
  return notice ? (
    <p className="notice" role="status">
      {notice}
    </p>
  ) : null;
}

export function ErrorLine({ error }: { error: unknown }) {
  if (!error) return null;
  return (
    <p className="error" role="alert">
      {error instanceof Error ? error.message : String(error)}
    </p>
  );
}
