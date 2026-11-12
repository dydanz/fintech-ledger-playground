// Formatting for the ops screens: plain numbers and durations, local times.

export const num = (n: number) => n.toLocaleString('en-US');

export const money = (n: number, currency: string) => `${currency} ${num(n)}`;

// clock is a timestamp's local time of day, 24-hour: 07:12:05.
export const clock = (iso: string) => new Date(iso).toLocaleTimeString('en-GB', { hour12: false });

// shortId is the end of an id, enough to tell rows apart: …7f5a.
export const shortId = (id: string) => `…${id.slice(-4)}`;

// duration renders 850 ms, 45 s, 4 min 50 s or 2 h 5 min.
export function duration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s} s`;
  if (s < 3600) return s % 60 ? `${Math.floor(s / 60)} min ${s % 60} s` : `${s / 60} min`;
  const m = Math.floor(s / 60);
  return m % 60 ? `${Math.floor(m / 60)} h ${m % 60} min` : `${m / 60} h`;
}

export const between = (from: string, to: string) => duration(new Date(to).getTime() - new Date(from).getTime());

// percent rounds down, so a single failure never shows as 100%.
export const percent = (r: number) => `${(Math.floor(r * 1000 + 1e-9) / 10).toFixed(1)}%`;

// when is a timestamp's local date and time: 12 Nov, 07:11:18.
export const when = (iso: string) =>
  new Date(iso).toLocaleString('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
