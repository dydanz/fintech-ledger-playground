//go:build integration

package service_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"fintech-ledger-playground/internal/archive"
	"fintech-ledger-playground/internal/repository"
	"fintech-ledger-playground/internal/service"
)

// Integration tests reach the compose S3 gateway (archive-store) here.
func s3Env(t *testing.T) (endpoint, access, secret string) {
	t.Helper()
	endpoint = os.Getenv("TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("TEST_S3_ENDPOINT not set; skipping archive test")
	}
	return endpoint, os.Getenv("TEST_S3_ACCESS_KEY"), os.Getenv("TEST_S3_SECRET_KEY")
}

// testBucket returns an Objects store on a new bucket (Object Lock, compliance,
// 1 day) and a raw client for checking it.
func testBucket(t *testing.T, worm bool) (*archive.S3, *minio.Client, string) {
	t.Helper()
	endpoint, access, secret := s3Env(t)
	b := make([]byte, 4)
	rand.Read(b)
	bucket := "test-" + hex.EncodeToString(b)
	raw, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, secret, ""), Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !worm { // a plain bucket the archiver must refuse
		if err := raw.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
			t.Fatal(err)
		}
	}
	s, err := archive.NewS3(archive.S3Config{BucketURI: "s3://" + bucket, Endpoint: endpoint, AccessKey: access, SecretKey: secret,
		Region: "us-east-1", RetentionDays: 1, CreateBucket: true})
	if err != nil {
		t.Fatal(err)
	}
	return s, raw, bucket
}

// archiveClock is a time when this month ended more than LEDGER_ARCHIVE_AFTER
// ago, so the test's own data is eligible.
const testArchiveAfter = 61320 * time.Hour

func archiveClock() time.Time { return time.Now().Add(testArchiveAfter + 62*24*time.Hour) }

func (e *svcEnv) archiver(store service.ArchiveStore, objects archive.Objects, hold []string, holdAll bool) *service.Archiver {
	e.t.Helper()
	a, err := service.NewArchiver(store, objects, service.ArchiveConfig{After: testArchiveAfter, Hold: hold, HoldAll: holdAll,
		FileRows: 50, Now: archiveClock})
	if err != nil {
		e.t.Fatal(err)
	}
	return a
}

func thisMonth() (time.Time, string) {
	n := time.Now().UTC()
	m := time.Date(n.Year(), n.Month(), 1, 0, 0, 0, 0, time.UTC)
	return m, "y" + m.Format("2006") + "m" + m.Format("01")
}

func (e *svcEnv) archiveEvents(reason string) []map[string]any {
	var out []map[string]any
	for _, r := range records(e.t, e.logs) {
		if r["event"] == "ledger.archive" && (reason == "" || r["reason"] == reason) {
			out = append(out, r)
		}
	}
	return out
}

// TRD §7.8 archive drill: checkpoints, Parquet export with a manifest,
// read-back verification, detach and drop; afterwards the balances and
// projections are unchanged and the reconciler is clean through the
// checkpoints.
func TestArchiveDrill(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	e.workload(20)
	month, suffix := thisMonth()
	journal, events := "journal_entries_"+suffix, "events_"+suffix
	want := e.projections()
	journalRows := e.count(`SELECT count(*) FROM ledger.` + journal)
	eventRows := e.count(`SELECT count(*) FROM ledger.` + events)
	sequenced := e.count(`SELECT count(DISTINCT ledger_id) FROM ledger.` + journal + ` WHERE ledger_sequence IS NOT NULL`)
	objects, raw, bucket := testBucket(t, true)

	e.resetLogs()
	if err := e.archiver(repository.NewArchiveRepo(e.pool), objects, nil, false).Run(e.ctx); err != nil {
		t.Fatalf("archive run: %v\n%s", err, e.logs)
	}

	// Every step logged, for both tables of this month.
	var steps []string
	for _, r := range e.archiveEvents("") {
		if r["month"] == month.Format("2006-01") {
			steps = append(steps, r["table"].(string)+":"+r["step"].(string))
		}
	}
	if want := []string{"journal_entries:checkpoint", "journal_entries:export", "journal_entries:verify", "journal_entries:detach",
		"journal_entries:drop", "events:export", "events:verify", "events:detach", "events:drop"}; !slices.Equal(steps, want) {
		t.Fatalf("steps = %v, want %v", steps, want)
	}

	// Postgres: partitions gone, months recorded, checkpoints written.
	if n := e.count(`SELECT count(*) FROM pg_class WHERE relname IN ($1, $2)`, journal, events); n != 0 {
		t.Fatalf("%d partition(s) still exist", n)
	}
	if n := e.count(`SELECT count(*) FROM ledger.archives WHERE month = $1 AND status = 'dropped'`, month); n != 2 {
		t.Fatalf("archives rows dropped for this month: %d, want 2", n)
	}
	if n := e.count(`SELECT count(*) FROM ledger.checkpoints`); n != sequenced {
		t.Fatalf("%d checkpoints, want %d (one per ledger with sequenced lines)", n, sequenced)
	}

	// Storage: the manifest and files read back, with every row.
	for table, rows := range map[string]int{"journal_entries": journalRows, "events": eventRows} {
		prefix := "ledger/" + table + "/" + month.Format("2006-01") + "/"
		data, err := objects.Get(e.ctx, prefix+"manifest.json")
		if err != nil {
			t.Fatal(err)
		}
		var m archive.Manifest
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, f := range m.Files {
			b, err := objects.Get(e.ctx, f.Key)
			if err != nil || archive.SHA256(b) != f.SHA256 {
				t.Fatalf("%s: %v / checksum mismatch", f.Key, err)
			}
			total += f.Rows
		}
		if total != int64(rows) || m.Rows != int64(rows) || len(m.Files) != (rows+49)/50 {
			t.Fatalf("%s: manifest %d rows in %d files, files %d; partition had %d", table, m.Rows, len(m.Files), total, rows)
		}
		if table == "journal_entries" && len(m.Checkpoints) != sequenced {
			t.Fatalf("manifest lists %d checkpoints, want %d", len(m.Checkpoints), sequenced)
		}
		// Write-once: a locked version can't be deleted.
		info, err := raw.StatObject(e.ctx, bucket, prefix+"manifest.json", minio.StatObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := raw.RemoveObject(e.ctx, bucket, prefix+"manifest.json", minio.RemoveObjectOptions{VersionID: info.VersionID,
			GovernanceBypass: true}); err == nil {
			t.Fatal("deleted a locked manifest version")
		}
	}

	// The projections are untouched, and still the fold of what's left.
	if got := e.projections(); !slices.Equal(got, want) {
		t.Fatal("archiving changed the projections")
	}
	if rep := e.reconcile(e.reconciler(time.Minute), true); len(rep.Mismatches) != 0 {
		t.Fatalf("reconcile after archive: %v", reasons(rep))
	}
	if rep := e.rebuild(service.RebuildOptions{}); rep.Changed != 0 || rep.Failed != 0 || rep.Skipped == 0 {
		t.Fatalf("rebuild after archive: %+v", rep)
	}

	// A second run finds nothing left to do.
	e.resetLogs()
	if err := e.archiver(repository.NewArchiveRepo(e.pool), objects, nil, false).Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	if s := e.archiveEvents(""); len(s) != 1 || s[0]["step"] != "run" || s[0]["archived"] != 0.0 {
		t.Fatalf("second run: %v", s)
	}
}

func TestArchiveLegalHold(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accts := e.workload(21)
	month, suffix := thisMonth()
	objects, _, _ := testBucket(t, true)
	for name, tc := range map[string]struct {
		hold    []string
		holdAll bool
	}{"one account": {hold: []string{accts[2]}}, "everything": {holdAll: true}} {
		t.Run(name, func(t *testing.T) {
			e.resetLogs()
			if err := e.archiver(repository.NewArchiveRepo(e.pool), objects, tc.hold, tc.holdAll).Run(e.ctx); err != nil {
				t.Fatal(err)
			}
			held := e.archiveEvents("legal_hold")
			if len(held) == 0 || held[0]["outcome"] != "denied" || held[0]["level"] != "warn" {
				t.Fatalf("no legal_hold skip logged:\n%s", e.logs)
			}
			if tc.hold != nil && !strings.Contains(e.logs.String(), accts[2]) {
				t.Fatalf("held account not named:\n%s", e.logs)
			}
			if n := e.count(`SELECT count(*) FROM pg_class WHERE relname = $1`, "journal_entries_"+suffix); n != 1 {
				t.Fatal("a held month was dropped")
			}
			// Later months without held rows may still be archived; this one must not be.
			if n := e.count(`SELECT count(*) FROM ledger.archives WHERE month = $1`, month); n != 0 {
				t.Fatalf("the held month was archived (%d rows)", n)
			}
		})
	}
}

// failingObjects reads back a journal file that is still valid Parquet with
// the same row count and ids, but one amount changed: only the checksum can
// tell (a flipped byte would already fail to decode).
type failingObjects struct{ archive.Objects }

func (f failingObjects) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := f.Objects.Get(ctx, key)
	if err != nil || !strings.Contains(key, "journal_entries") || !strings.HasSuffix(key, ".parquet") {
		return b, err
	}
	rows, err := archive.Decode[archive.JournalRow](b)
	if err != nil || len(rows) == 0 {
		return b, err
	}
	rows[0].Amount++
	return archive.Encode(rows)
}

// flakyDetach fails the first Detach, like a worker killed mid-run.
type flakyDetach struct {
	*repository.ArchiveRepo
	failed bool
}

func (f *flakyDetach) Detach(ctx context.Context, p repository.Partition) error {
	if !f.failed {
		f.failed = true
		return errors.New("connection reset")
	}
	return f.ArchiveRepo.Detach(ctx, p)
}

func TestArchiveNeverDropsWithoutAVerifiedWORMExport(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	e.workload(22)
	month, suffix := thisMonth()
	partitionLeft := func() bool {
		return e.count(`SELECT count(*) FROM pg_class WHERE relname = $1`, "journal_entries_"+suffix) == 1
	}

	t.Run("bucket without object lock", func(t *testing.T) {
		plain, _, _ := testBucket(t, false)
		e.resetLogs()
		if err := e.archiver(repository.NewArchiveRepo(e.pool), plain, nil, false).Run(e.ctx); !errors.Is(err, archive.ErrNotWORM) {
			t.Fatalf("err = %v, want ErrNotWORM", err)
		}
		if f := e.archiveEvents("bucket_not_worm"); len(f) != 1 || f[0]["level"] != "error" || !partitionLeft() {
			t.Fatalf("not refused: %v", f)
		}
	})
	objects, _, _ := testBucket(t, true)
	t.Run("export does not read back", func(t *testing.T) {
		e.resetLogs()
		if err := e.archiver(repository.NewArchiveRepo(e.pool), failingObjects{objects}, nil, false).Run(e.ctx); err == nil {
			t.Fatal("corrupt read-back accepted")
		}
		if f := e.archiveEvents("verify_failed"); len(f) != 1 || !partitionLeft() ||
			e.count(`SELECT count(*) FROM ledger.archives`) != 0 {
			t.Fatalf("verify failure: %v", f)
		}
	})
	t.Run("detach fails, next run resumes", func(t *testing.T) {
		store := &flakyDetach{ArchiveRepo: repository.NewArchiveRepo(e.pool)}
		e.resetLogs()
		if err := e.archiver(store, objects, nil, false).Run(e.ctx); err == nil {
			t.Fatal("detach failure not returned")
		}
		if f := e.archiveEvents("detach_failed"); len(f) != 1 || !partitionLeft() ||
			e.count(`SELECT count(*) FROM ledger.archives WHERE status = 'verified' AND month = $1`, month) != 1 {
			t.Fatalf("after failed detach: %v", f)
		}
		e.resetLogs()
		if err := e.archiver(store, objects, nil, false).Run(e.ctx); err != nil {
			t.Fatal(err)
		}
		var steps []string
		for _, r := range e.archiveEvents("") {
			if r["table"] == "journal_entries" && r["month"] == month.Format("2006-01") {
				steps = append(steps, r["step"].(string))
			}
		}
		if !slices.Equal(steps, []string{"detach", "drop"}) || partitionLeft() {
			t.Fatalf("resume steps = %v (export must not repeat)", steps)
		}
		if rep := e.reconcile(e.reconciler(time.Minute), true); len(rep.Mismatches) != 0 {
			t.Fatalf("reconcile after resumed archive: %v", reasons(rep))
		}
	})
}
