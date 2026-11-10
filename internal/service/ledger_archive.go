package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"fintech-ledger-playground/internal/archive"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/repository"
)

// ArchiveStore is what the archiver reads and changes in Postgres (repository.ArchiveRepo).
type ArchiveStore interface {
	Partitions(ctx context.Context, cutoff time.Time) ([]repository.Partition, error)
	HeldAccounts(ctx context.Context, p repository.Partition, accounts []string) ([]string, error)
	WriteCheckpoints(ctx context.Context, p repository.Partition, manifestURI string) ([]archive.Checkpoint, error)
	CountRows(ctx context.Context, p repository.Partition) (int64, error)
	JournalPage(ctx context.Context, p repository.Partition, after string, limit int) ([]archive.JournalRow, error)
	EventPage(ctx context.Context, p repository.Partition, after string, limit int) ([]archive.EventRow, error)
	MarkVerified(ctx context.Context, p repository.Partition, manifestURI string, rows int64, checkpoints int) error
	Detach(ctx context.Context, p repository.Partition) error
	Drop(ctx context.Context, p repository.Partition) error
}

type ArchiveConfig struct {
	After    time.Duration    // LEDGER_ARCHIVE_AFTER: a month is archived once it ended this long ago
	HoldAll  bool             // LEDGER_LEGAL_HOLD=*: archive nothing
	Hold     []string         // LEDGER_LEGAL_HOLD: account ids; a month with any of their rows is skipped
	FileRows int              // rows per Parquet file
	Now      func() time.Time // nil means time.Now; tests inject a clock
}

// Archiver moves monthly partitions older than LEDGER_ARCHIVE_AFTER out of
// Postgres into write-once storage (TRD §7.8). For each month, oldest first:
// checkpoints, Parquet export with a manifest, read-back verification, then
// DETACH ... CONCURRENTLY and DROP. That drop is the only path that removes
// ledger rows. Every step logs ledger.archive. Each step is resumable: a
// month recorded as verified is detached and dropped on the next run.
//
// A partition is never dropped unless its bucket is in Object Lock
// compliance mode and the export read back byte for byte.
type Archiver struct {
	store   ArchiveStore
	objects archive.Objects
	cfg     ArchiveConfig
	now     func() time.Time
}

func NewArchiver(store ArchiveStore, objects archive.Objects, cfg ArchiveConfig) (*Archiver, error) {
	if cfg.After <= 0 || cfg.FileRows < 1 {
		return nil, errors.New("archiver: need a positive LEDGER_ARCHIVE_AFTER and rows per file")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Archiver{store: store, objects: objects, cfg: cfg, now: now}, nil
}

// errArchive tags an error with the ledger.archive reason it is logged with.
type errArchive struct {
	reason, step string
	err          error
}

func (e *errArchive) Error() string { return e.step + ": " + e.err.Error() }
func (e *errArchive) Unwrap() error { return e.err }

func fail(reason, step string, err error) error {
	return &errArchive{reason: reason, step: step, err: err}
}

// Run is the ledger-worker job.
func (a *Archiver) Run(ctx context.Context) error {
	log := logger.FromContext(ctx)
	start := a.now()
	if err := a.objects.CheckWORM(ctx); err != nil {
		reason := "bucket_unavailable"
		if errors.Is(err, archive.ErrNotWORM) {
			reason = "bucket_not_worm"
		}
		log.ErrorContext(ctx, "archive bucket check failed; nothing archived", "event", "ledger.archive",
			"outcome", "failure", "reason", reason, "step", "bucket", "error", err)
		return err
	}
	cutoff := start.Add(-a.cfg.After).UTC()
	parts, err := a.store.Partitions(ctx, cutoff)
	if err != nil {
		log.ErrorContext(ctx, "archive could not list partitions", "event", "ledger.archive", "outcome", "failure",
			"reason", "pg_error", "step", "list", "error", err)
		return err
	}
	var archived, held int
	for _, month := range groupByMonth(parts) {
		todo := slices.DeleteFunc(slices.Clone(month), func(p repository.Partition) bool { return p.Status == "dropped" })
		if len(todo) == 0 {
			continue
		}
		label := todo[0].Month.Format("2006-01")
		if a.cfg.HoldAll {
			log.WarnContext(ctx, "legal hold on everything; archive skipped", "event", "ledger.archive", "outcome", "denied",
				"reason", "legal_hold", "step", "hold", "month", label, "hold_all", true)
			held++
			break
		}
		holders, err := a.holders(ctx, todo)
		if err != nil {
			log.ErrorContext(ctx, "archive legal-hold check failed", "event", "ledger.archive", "outcome", "failure",
				"reason", "pg_error", "step", "hold", "month", label, "error", err)
			return err
		}
		if len(holders) > 0 {
			log.WarnContext(ctx, "month has rows of an account on legal hold; skipped", "event", "ledger.archive",
				"outcome", "denied", "reason", "legal_hold", "step", "hold", "month", label, "held_accounts", holders)
			held++
			continue
		}
		for _, p := range todo {
			if err := a.partition(ctx, p); err != nil {
				var e *errArchive
				reason, step := "pg_error", "unknown"
				if errors.As(err, &e) {
					reason, step = e.reason, e.step
				}
				log.ErrorContext(ctx, "archive step failed; partition kept", "event", "ledger.archive", "outcome", "failure",
					"reason", reason, "step", step, "table", p.Table, "month", label, "partition", p.Name, "error", err)
				return err // later months wait: archive oldest first
			}
			archived++
		}
	}
	log.InfoContext(ctx, "archive run finished", "event", "ledger.archive", "outcome", "success", "step", "run",
		"archived", archived, "held_months", held, "cutoff", cutoff.Format(time.RFC3339),
		"duration_ms", float64(a.now().Sub(start).Microseconds())/1000)
	return nil
}

func (a *Archiver) holders(ctx context.Context, parts []repository.Partition) ([]string, error) {
	var out []string
	for _, p := range parts {
		if p.Name == "" || len(a.cfg.Hold) == 0 {
			continue
		}
		ids, err := a.store.HeldAccounts(ctx, p, a.cfg.Hold)
		if err != nil {
			return nil, err
		}
		out = append(out, ids...)
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

// partition takes one partition the rest of the way: export and verify
// unless already verified, then detach and drop.
func (a *Archiver) partition(ctx context.Context, p repository.Partition) error {
	log := logger.FromContext(ctx)
	label := p.Month.Format("2006-01")
	attrs := []any{"event", "ledger.archive", "outcome", "success", "table", p.Table, "month", label, "partition", p.Name}
	if p.Status == "" {
		prefix := "ledger/" + p.Table + "/" + label + "/"
		manifestURI := a.objects.URI(prefix + "manifest.json")
		m := archive.Manifest{Table: p.Table, Month: label, Partition: p.Name, CreatedAt: a.now().UTC()}
		if p.Table == "journal_entries" {
			cps, err := a.store.WriteCheckpoints(ctx, p, manifestURI)
			if err != nil {
				return fail("pg_error", "checkpoint", err)
			}
			m.Checkpoints = cps
			log.InfoContext(ctx, "checkpoints written", append(attrs, "step", "checkpoint", "checkpoints", len(cps))...)
		}
		start := time.Now()
		var err error
		if p.Table == "journal_entries" {
			err = export(ctx, a, &m, prefix, func(after string) ([]archive.JournalRow, error) {
				return a.store.JournalPage(ctx, p, after, a.cfg.FileRows)
			}, func(r archive.JournalRow) string { return r.ID })
		} else {
			err = export(ctx, a, &m, prefix, func(after string) ([]archive.EventRow, error) {
				return a.store.EventPage(ctx, p, after, a.cfg.FileRows)
			}, func(r archive.EventRow) string { return r.EventID })
		}
		if err != nil {
			return fail("export_failed", "export", err)
		}
		log.InfoContext(ctx, "partition exported", append(attrs, "step", "export", "rows", m.Rows, "files", len(m.Files),
			"manifest", manifestURI, "duration_ms", msSince(start))...)

		start = time.Now()
		if err := a.verify(ctx, p, m, prefix); err != nil {
			return fail("verify_failed", "verify", err)
		}
		if err := a.store.MarkVerified(ctx, p, manifestURI, m.Rows, len(m.Checkpoints)); err != nil {
			return fail("pg_error", "verify", err)
		}
		log.InfoContext(ctx, "export read back and verified", append(attrs, "step", "verify", "rows", m.Rows,
			"files", len(m.Files), "duration_ms", msSince(start))...)
	}
	if err := a.store.Detach(ctx, p); err != nil {
		return fail("detach_failed", "detach", err)
	}
	log.InfoContext(ctx, "partition detached", append(attrs, "step", "detach")...)
	if err := a.store.Drop(ctx, p); err != nil {
		return fail("detach_failed", "drop", err)
	}
	log.InfoContext(ctx, "partition dropped", append(attrs, "step", "drop")...)
	return nil
}

// export writes the partition as Parquet files of up to FileRows rows, then
// the manifest, which is written last.
func export[T archive.JournalRow | archive.EventRow](ctx context.Context, a *Archiver, m *archive.Manifest, prefix string,
	page func(after string) ([]T, error), id func(T) string) error {
	after := ""
	for i := 0; ; i++ {
		rows, err := page(after)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		data, err := archive.Encode(rows)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%spart-%04d.parquet", prefix, i)
		uri, sha, err := a.objects.Put(ctx, key, data)
		if err != nil {
			return err
		}
		first, last := id(rows[0]), id(rows[len(rows)-1])
		m.Files = append(m.Files, archive.ManifestFile{Key: key, URI: uri, Rows: int64(len(rows)), Bytes: int64(len(data)),
			SHA256: sha, MinID: first, MaxID: last})
		if m.MinID == "" {
			m.MinID = first
		}
		m.MaxID, m.Rows, after = last, m.Rows+int64(len(rows)), last
	}
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	_, _, err = a.objects.Put(ctx, prefix+"manifest.json", body)
	return err
}

// verify reads the manifest and every file back and checks them against the
// partition (§7.8 step 3): checksums, row counts, id ranges, and the total
// against Postgres.
func (a *Archiver) verify(ctx context.Context, p repository.Partition, want archive.Manifest, prefix string) error {
	raw, err := a.objects.Get(ctx, prefix+"manifest.json")
	if err != nil {
		return err
	}
	var got archive.Manifest
	if err := json.Unmarshal(raw, &got); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if got.Rows != want.Rows || len(got.Files) != len(want.Files) || got.MinID != want.MinID || got.MaxID != want.MaxID {
		return fmt.Errorf("manifest read back differs: %d rows/%d files, want %d/%d", got.Rows, len(got.Files), want.Rows, len(want.Files))
	}
	var total int64
	for _, f := range got.Files {
		data, err := a.objects.Get(ctx, f.Key)
		if err != nil {
			return err
		}
		if sha := archive.SHA256(data); sha != f.SHA256 {
			return fmt.Errorf("%s: sha256 %s, manifest says %s", f.Key, sha, f.SHA256)
		}
		n, first, last, err := decodeIDs(p.Table, data)
		if err != nil {
			return fmt.Errorf("%s: %w", f.Key, err)
		}
		if n != f.Rows || first != f.MinID || last != f.MaxID {
			return fmt.Errorf("%s: %d rows %s..%s, manifest says %d %s..%s", f.Key, n, first, last, f.Rows, f.MinID, f.MaxID)
		}
		total += n
	}
	inPG, err := a.store.CountRows(ctx, p)
	if err != nil {
		return err
	}
	if total != got.Rows || total != inPG {
		return fmt.Errorf("rows: files %d, manifest %d, partition %d", total, got.Rows, inPG)
	}
	return nil
}

func decodeIDs(table string, data []byte) (n int64, first, last string, err error) {
	ids := func(rows []string) (int64, string, string) {
		if len(rows) == 0 {
			return 0, "", ""
		}
		return int64(len(rows)), rows[0], rows[len(rows)-1]
	}
	if table == "journal_entries" {
		rows, err := archive.Decode[archive.JournalRow](data)
		if err != nil {
			return 0, "", "", err
		}
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = r.ID
		}
		n, first, last = ids(out)
		return n, first, last, nil
	}
	rows, err := archive.Decode[archive.EventRow](data)
	if err != nil {
		return 0, "", "", err
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.EventID
	}
	n, first, last = ids(out)
	return n, first, last, nil
}

func groupByMonth(parts []repository.Partition) [][]repository.Partition {
	var out [][]repository.Partition
	for _, p := range parts {
		if n := len(out); n > 0 && out[n-1][0].Month.Equal(p.Month) {
			out[n-1] = append(out[n-1], p)
			continue
		}
		out = append(out, []repository.Partition{p})
	}
	return out
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }
