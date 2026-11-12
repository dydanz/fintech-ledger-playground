import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { resetDefaults } from './render';

afterEach(() => {
  cleanup();
  sessionStorage.clear();
  vi.unstubAllGlobals();
  resetDefaults();
});
