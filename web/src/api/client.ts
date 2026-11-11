import { session } from '../auth/session';

// ApiError is an error envelope from the console API ({"error":{code,message}}),
// or a network failure (status 0).
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

type Options = { method?: string; body?: unknown; auth?: boolean; headers?: Record<string, string> };

let onUnauthorized: (() => void) | null = null;

// setUnauthorizedHandler is called once by the app: it runs when an
// authenticated call gets 401, after the session has ended.
export function setUnauthorizedHandler(fn: (() => void) | null) {
  onUnauthorized = fn;
}

// api calls the console's own API. With auth, it sends the user's token, and
// a 401 means the session ended: the token is cleared and the handler runs.
// Any other error (a wrong password is 403, a refused login is an
// unauthenticated 401) is thrown for the page to show.
export async function api<T>(path: string, opts: Options = {}): Promise<T> {
  const headers: Record<string, string> = { Accept: 'application/json', ...opts.headers };
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json';
  const token = session.token();
  if (opts.auth && token) headers.Authorization = `Bearer ${token}`;

  let res: Response;
  try {
    res = await fetch(`/console/api${path}`, {
      method: opts.method ?? 'GET',
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
    });
  } catch {
    throw new ApiError(0, 'network_error', 'The console could not be reached. Check your connection and try again.');
  }
  if (res.status === 204) return undefined as T;
  let data: unknown = null;
  try {
    data = await res.json();
  } catch {
    /* not JSON */
  }
  if (!res.ok) {
    if (res.status === 401 && opts.auth) {
      session.end('Your session ended. Log in again.');
      onUnauthorized?.();
    }
    const err = (data as { error?: { code?: string; message?: string } } | null)?.error;
    throw new ApiError(res.status, err?.code ?? 'error', err?.message ?? `Request failed (${res.status}).`);
  }
  return data as T;
}
