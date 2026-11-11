import { ApiError } from './client';
import { newKey } from './key';
import { shouldRetry } from './queryClient';
import { outcome } from '../pages/MyAccount';

describe('newKey', () => {
  it('makes distinct UUID v4 keys without crypto.randomUUID (plain http from a hostname)', () => {
    vi.stubGlobal('crypto', { getRandomValues: globalThis.crypto.getRandomValues.bind(globalThis.crypto) });
    const a = newKey(), b = newKey();
    expect(a).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(a).not.toBe(b);
  });
});

describe('shouldRetry', () => {
  it.each([
    [0, new ApiError(404, 'account_not_found', 'x'), false],
    [0, new ApiError(401, 'invalid_or_expired', 'x'), false],
    [0, new ApiError(422, 'insufficient_funds', 'x'), false],
    [0, new ApiError(503, 'upstream_unavailable', 'x'), true],
    [0, new ApiError(0, 'network_error', 'x'), true],
    [1, new ApiError(503, 'upstream_unavailable', 'x'), true],
    [2, new ApiError(503, 'upstream_unavailable', 'x'), false],
  ])('after %i failures, %o → %s', (n, err, want) => {
    expect(shouldRetry(n, err)).toBe(want);
  });
});

describe('outcome', () => {
  it('shows the message of an unexpected error instead of nothing', () => {
    expect(outcome(undefined, new TypeError('crypto.randomUUID is not a function'))).toBe('crypto.randomUUID is not a function');
  });
});
