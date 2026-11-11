import { QueryClient } from '@tanstack/react-query';
import { ApiError } from './client';

// shouldRetry retries what might succeed on a second try: no answer at all,
// or a 5xx. A 4xx (no account yet, a refusal, an ended session) is an answer:
// retrying it only delays the page.
export function shouldRetry(failureCount: number, error: unknown): boolean {
  if (failureCount >= 2) return false;
  return !(error instanceof ApiError) || error.status === 0 || error.status >= 500;
}

export function makeQueryClient() {
  return new QueryClient({ defaultOptions: { queries: { retry: shouldRetry }, mutations: { retry: false } } });
}
