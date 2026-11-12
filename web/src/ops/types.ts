// The ops API's shapes (internal/console/ops.go), as the screens read them.

export type Label = 'normal' | 'recovered' | 'rejected' | 'reversed' | 'timed_out' | 'in_progress';

export type OpsTx = {
  id: string;
  type: 'CREDIT' | 'DEBIT' | 'VOID';
  status: 'PENDING' | 'PROCESSING' | 'SETTLED' | 'FAILED' | 'VOIDED';
  failure_reason?: string;
  account_id: string;
  amount: number;
  currency: string;
  original_transaction_id?: string;
  requested_by: string;
  trace_id: string;
  created_at: string;
  updated_at: string;
  settled_at?: string;
  balance_after?: number;
};

export type FeedItem = { transaction: OpsTx; label: Label; what: string; attention: boolean };

export type Incident = {
  kind: 'kafka' | 'database' | 'redis';
  active: boolean;
  started_at: string;
  ended_at?: string;
  title: string;
  text: string;
  count: number;
  recovered?: number;
  peak_lag_ms?: number;
};

export type Reconcile = { status: 'clean' | 'mismatches' | 'failed' | 'unknown'; mismatches: number; mode?: string; at?: string };

export type Monitor = {
  generated_at: string;
  window: { since: string; until: string };
  signals: { available: boolean };
  tiles: {
    transactions: { count: number; per_second: number };
    handled_correctly: { ratio: number; system_failures: number; valid: number };
    needed_recovery: { recovered: number; refused: number; still_pending: number; oldest_pending_ms: number };
    reconciler: Reconcile;
    outbox: { pending: number; oldest_age_ms: number };
  };
  incidents: Incident[] | null;
  feed: FeedItem[] | null;
  handled: { label: Label | 'refused_try_again'; count: number }[];
  self_healing: { at: string; text: string }[] | null;
};
