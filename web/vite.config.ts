import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

// `npm run dev` proxies the console API to a running ledger-console, so the
// app can be developed with live reload against `make up`.
export default defineConfig({
  plugins: [react()],
  server: { proxy: { '/console/api': 'http://localhost:8090' } },
  // TZ pins clock times in tests; the app shows the viewer's local time.
  test: { environment: 'jsdom', globals: true, setupFiles: ['./src/test/setup.ts'], env: { TZ: 'UTC' } },
});
