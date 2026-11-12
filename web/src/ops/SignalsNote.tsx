// SignalsNote is shown on the transaction lists when Loki can't be read:
// recoveries are known only from the logs, so the labels can't say which
// transactions the recovery job completed (D§6.3).
export function SignalsNote() {
  return (
    <p className="signals-note" role="note">
      <b>Activity signals unavailable.</b> The logs can't be read right now, so recovered transactions show as Normal. Each transaction's
      outcome (settled, refused, reversed) comes from the ledger and is correct.
    </p>
  );
}
