import type { Label } from './types';

export const labelName: Record<Label, string> = {
  normal: 'Normal',
  recovered: 'Recovered',
  rejected: 'Rejected',
  reversed: 'Reversed',
  timed_out: 'Timed out',
  in_progress: 'In progress',
};

const tone: Record<Label, string> = {
  normal: 'c-ok',
  recovered: 'c-rec',
  rejected: 'c-rej',
  reversed: 'c-warn',
  timed_out: 'c-bad',
  in_progress: 'c-muted',
};

// LabelChip is how a transaction was handled (D§6.2). A label that needs a
// person to look (a timeout, a payment stuck past 60 s) is red.
export function LabelChip({ label, attention = false }: { label: Label; attention?: boolean }) {
  return (
    <span className={`chip ${attention ? 'c-bad' : tone[label]}`} data-testid="chip">
      {labelName[label]}
    </span>
  );
}
