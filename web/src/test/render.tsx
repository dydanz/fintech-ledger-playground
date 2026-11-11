import { render } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { App } from '../App';

// renderApp renders the whole app at path, as main.tsx does, with retries
// off so a failed request fails the test at once.
export function renderApp(path: string) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

type Reply = { status: number; body?: unknown };

// mockFetch answers console API calls by "METHOD /path" and records them.
export function mockFetch(routes: Record<string, Reply | (() => Reply) | Error>) {
  const calls: { method: string; path: string; headers: Record<string, string>; body: unknown }[] = [];
  const fn = vi.fn(async (url: string, init: RequestInit = {}) => {
    const method = init.method ?? 'GET';
    const path = url.replace(/^\/console\/api/, '');
    calls.push({ method, path, headers: (init.headers ?? {}) as Record<string, string>, body: init.body ? JSON.parse(String(init.body)) : undefined });
    const r = routes[`${method} ${path}`];
    if (r instanceof Error) throw r;
    const reply = typeof r === 'function' ? r() : (r ?? { status: 404, body: { error: { code: 'not_found', message: 'route not found' } } });
    return new Response(reply.status === 204 ? null : JSON.stringify(reply.body ?? {}), {
      status: reply.status,
      headers: { 'Content-Type': 'application/json' },
    });
  });
  vi.stubGlobal('fetch', fn);
  return calls;
}
