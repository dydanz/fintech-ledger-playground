import { useSyncExternalStore } from 'react';

// The user's access token lives in sessionStorage: it survives a reload but
// not closing the tab, and it's only ever sent to this console's own
// /console/api (spec/2026-11-10 Web Console Design D§5.4). Service tokens
// never reach the browser.
const KEY = 'ledger-console.token';
// FLASH holds the message for the login page when a session ends ("Password
// changed", "Your session ended"). It's read once. Kept apart from navigation
// state because clearing the token re-renders at once, and a page's
// RequireAuth may redirect to /login before the caller's own navigate lands.
const FLASH = 'ledger-console.flash';
const listeners = new Set<() => void>();
const emit = () => listeners.forEach((fn) => fn());

export const session = {
  token(): string | null {
    return sessionStorage.getItem(KEY);
  },
  set(token: string) {
    sessionStorage.setItem(KEY, token);
    emit();
  },
  clear() {
    sessionStorage.removeItem(KEY);
    emit();
  },
  // end clears the session and leaves notice for the login page.
  end(notice: string) {
    sessionStorage.setItem(FLASH, notice);
    this.clear();
  },
  // peekFlash returns the pending notice; dropFlash forgets it once shown.
  // (Two calls, so React StrictMode's double render can't consume it.)
  peekFlash(): string | null {
    return sessionStorage.getItem(FLASH);
  },
  dropFlash() {
    sessionStorage.removeItem(FLASH);
  },
  subscribe(fn: () => void) {
    listeners.add(fn);
    return () => listeners.delete(fn);
  },
};

export function useToken(): string | null {
  return useSyncExternalStore(session.subscribe, session.token);
}

// safeNext keeps a post-login redirect on this site: a path, never a
// protocol-relative or absolute URL.
export function safeNext(next: string | null): string {
  return next && next.startsWith('/') && !next.startsWith('//') && !next.startsWith('/\\') ? next : '/me';
}
