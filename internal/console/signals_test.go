package console_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"fintech-ledger-playground/internal/console"
)

// kafkaDrill is the relay's own log during the Kafka drill after #39 (Kafka
// stopped 10:41:57, started 10:47:00), as printed in that PR's evidence:
// time, level, event, reason, then pending/lag_ms or failing_ms/retry_in.
const kafkaDrill = `10:41:15 info outbox.lag  pending=0 lag_ms=0
10:41:45 info outbox.lag  pending=3 lag_ms=33.873
10:42:07 warn outbox.publish kafka_unavailable failing_ms=0 retry_in=200ms
10:42:15 warn outbox.lag  pending=2641 lag_ms=17604.021
10:42:18 warn outbox.publish kafka_unavailable failing_ms=10204 retry_in=400ms
10:42:28 warn outbox.publish kafka_unavailable failing_ms=20607 retry_in=800ms
10:42:39 warn outbox.publish kafka_unavailable failing_ms=31411 retry_in=1.6s
10:42:45 warn outbox.lag  pending=7140 lag_ms=47603.775
10:42:50 warn outbox.publish kafka_unavailable failing_ms=43016 retry_in=3.2s
10:43:04 warn outbox.publish kafka_unavailable failing_ms=56220 retry_in=5s
10:43:15 warn outbox.lag  pending=11643 lag_ms=77604.541
10:43:19 error outbox.publish kafka_unavailable failing_ms=71224 retry_in=5s
10:43:34 error outbox.publish kafka_unavailable failing_ms=86228 retry_in=5s
10:43:45 warn outbox.lag  pending=16141 lag_ms=107605.073
10:43:49 error outbox.publish kafka_unavailable failing_ms=101233 retry_in=5s
10:44:04 error outbox.publish kafka_unavailable failing_ms=116238 retry_in=5s
10:44:15 warn outbox.lag  pending=20641 lag_ms=137604.762
10:44:19 error outbox.publish kafka_unavailable failing_ms=131243 retry_in=5s
10:44:34 error outbox.publish kafka_unavailable failing_ms=146249 retry_in=5s
10:44:45 warn outbox.lag  pending=25140 lag_ms=167604.257
10:44:49 error outbox.publish kafka_unavailable failing_ms=161257 retry_in=5s
10:45:04 error outbox.publish kafka_unavailable failing_ms=176262 retry_in=5s
10:45:15 warn outbox.lag  pending=29641 lag_ms=197605.724
10:45:19 error outbox.publish kafka_unavailable failing_ms=191268 retry_in=5s
10:45:34 error outbox.publish kafka_unavailable failing_ms=206273 retry_in=5s
10:45:45 warn outbox.lag  pending=34141 lag_ms=227605.602
10:45:49 error outbox.publish kafka_unavailable failing_ms=221279 retry_in=5s
10:46:04 error outbox.publish kafka_unavailable failing_ms=236283 retry_in=5s
10:46:15 warn outbox.lag  pending=38641 lag_ms=257605.346
10:46:19 error outbox.publish kafka_unavailable failing_ms=251288 retry_in=5s
10:46:34 error outbox.publish kafka_unavailable failing_ms=266292 retry_in=5s
10:46:45 warn outbox.lag  pending=43141 lag_ms=287606.15
10:46:49 error outbox.publish kafka_unavailable failing_ms=281298 retry_in=5s
10:47:04 error outbox.publish kafka_unavailable failing_ms=296310 retry_in=5s
10:47:15 info outbox.lag  pending=2 lag_ms=18.351`

var day = time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)

func at(hms string) time.Time {
	t, err := time.Parse("15:04:05", hms)
	if err != nil {
		panic(err)
	}
	return day.Add(time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second)
}

// buckets builds 15 s buckets from..to, as the Loki metric queries do.
func buckets(from, to time.Time) []console.Bucket {
	var b []console.Bucket
	for t := from; !t.After(to); t = t.Add(console.BucketWidth) {
		b = append(b, console.Bucket{At: t})
	}
	return b
}

func bucketOf(b []console.Bucket, t time.Time) *console.Bucket {
	for i := len(b) - 1; i >= 0; i-- {
		if !b[i].At.After(t) {
			return &b[i]
		}
	}
	return nil
}

// kafkaBuckets turns the drill's lines into buckets: kafka_unavailable
// failures counted, outbox.lag's maximum kept.
func kafkaBuckets(t *testing.T) []console.Bucket {
	b := buckets(at("10:41:00"), at("10:48:00"))
	for _, line := range strings.Split(kafkaDrill, "\n") {
		f := strings.Fields(line)
		bk := bucketOf(b, at(f[0]))
		switch f[2] {
		case "outbox.publish":
			bk.KafkaFailures++
		case "outbox.lag":
			v, err := strconv.ParseFloat(strings.TrimPrefix(f[len(f)-1], "lag_ms="), 64)
			if err != nil {
				t.Fatal(err)
			}
			if bk.LagMs == nil || v > *bk.LagMs {
				bk.LagMs = &v
			}
		}
	}
	return b
}

func TestIncidentKafkaFromTheDrill(t *testing.T) {
	inc := console.Incidents(kafkaBuckets(t), at("10:48:00"))
	if len(inc) != 1 {
		t.Fatalf("%d incidents, want 1: %+v", len(inc), inc)
	}
	k := inc[0]
	if k.Kind != "kafka" || k.Active || !k.Start.Equal(at("10:42:00")) || k.End == nil || !k.End.Equal(at("10:47:15")) {
		t.Fatalf("incident = %+v, want kafka 10:42:00 to 10:47:15, resolved", k)
	}
	if k.Count != 22 || k.PeakLagMs < 287000 {
		t.Fatalf("count %d, peak lag %v; want 22 failed publishes and the 287 s peak", k.Count, k.PeakLagMs)
	}
	// outbox.lag is sampled every 30 s, so the peak is a lower bound and the
	// end an upper one: the text says "at least" and "within", never "for".
	if !strings.Contains(k.Text, "delivered within 5 min 15 s of the first failure") || !strings.Contains(k.Text, "oldest waited at least") ||
		strings.Contains(k.Text, "down for") || !strings.Contains(k.Title, "Kafka") {
		t.Fatalf("resolved text = %q / %q", k.Title, k.Text)
	}
}

// Mid-outage (cut the drill at 10:44:30): the incident is active.
func TestIncidentKafkaWhileActive(t *testing.T) {
	var cut []console.Bucket
	for _, b := range kafkaBuckets(t) {
		if b.At.Before(at("10:44:30")) {
			cut = append(cut, b)
		}
	}
	inc := console.Incidents(cut, at("10:44:30"))
	if len(inc) != 1 || !inc[0].Active || inc[0].End != nil || !strings.Contains(inc[0].Text, "Payments keep working") {
		t.Fatalf("incidents = %+v, want one active Kafka incident", inc)
	}
}

// databaseBuckets is the Postgres drill after #39 (killed 07:11:18, writes
// back 07:12:02): 503s while Postgres was down, the recovery job completing
// one payment afterwards. Built from that run's timeline and counts (1,096
// refusals), not raw lines, which weren't kept.
func databaseBuckets() []console.Bucket {
	d := func(hms string) time.Time { return day.Add(7*time.Hour + mustDur(hms)) }
	b := make([]console.Bucket, 0)
	for t := d("10m30s"); !t.After(d("13m00s")); t = t.Add(console.BucketWidth) {
		b = append(b, console.Bucket{At: t, Writes2xx: 700})
	}
	// Buckets 07:11:15 to 07:12:00 hold the refusals (Postgres killed at
	// 07:11:18, the last refusal at 07:12:01); writes are back from 07:12:15.
	for i, n := range map[int]int{3: 260, 4: 300, 5: 300, 6: 236} {
		b[i].Refused, b[i].Writes2xx = n, 0
	}
	bucketOf(b, d("12m05s")).Recovered = 1
	return b
}

func mustDur(s string) time.Duration { d, _ := time.ParseDuration(s); return d }

func TestIncidentDatabaseFromTheDrill(t *testing.T) {
	inc := console.Incidents(databaseBuckets(), day.Add(7*time.Hour+13*time.Minute))
	if len(inc) != 1 || inc[0].Kind != "database" || inc[0].Active {
		t.Fatalf("incidents = %+v, want one resolved database incident", inc)
	}
	db := inc[0]
	if db.Count != 1096 || db.Recovered != 1 {
		t.Fatalf("refused %d, recovered %d; want 1096 and 1", db.Count, db.Recovered)
	}
	if !strings.Contains(db.Text, "1,096") || !strings.Contains(db.Text, "1 ") {
		t.Fatalf("text = %q", db.Text)
	}
	// The count is of 503 answers, and clients retry: it isn't a number of
	// payments, and the drill's report.txt counts requests (#55). Say so.
	if !strings.Contains(db.Text, "1,096 attempts were refused") || !strings.Contains(db.Text, "each retry counts") ||
		strings.Contains(db.Text, "requests were refused") {
		t.Fatalf("text = %q, want it to count attempts, retries included", db.Text)
	}
}

// One refusal reads as one, not "1 requests".
func TestIncidentDatabaseSingleRefusal(t *testing.T) {
	d := func(hms string) time.Time { return day.Add(7*time.Hour + mustDur(hms)) }
	var b []console.Bucket
	for at := d("10m30s"); !at.After(d("12m00s")); at = at.Add(console.BucketWidth) {
		b = append(b, console.Bucket{At: at, Writes2xx: 10})
	}
	b[3].Refused, b[3].Writes2xx = 1, 0
	inc := console.Incidents(b, d("12m30s"))
	if len(inc) != 1 || !strings.Contains(inc[0].Text, "1 attempt was refused") {
		t.Fatalf("incidents = %+v", inc)
	}
}

func TestIncidentRedis(t *testing.T) {
	b := buckets(at("09:00:00"), at("09:05:00"))
	for i := 4; i < 8; i++ { // 09:01:00 to 09:01:45
		b[i].RedisFailures = 100
	}
	inc := console.Incidents(b, at("09:05:00"))
	if len(inc) != 1 || inc[0].Kind != "redis" || inc[0].Active || !inc[0].Start.Equal(at("09:01:00")) {
		t.Fatalf("incidents = %+v", inc)
	}
	if !strings.Contains(inc[0].Text, "cache") {
		t.Fatalf("text = %q", inc[0].Text)
	}
	// Still failing at the last bucket: active.
	if inc := console.Incidents(b[:8], at("09:01:50")); len(inc) != 1 || !inc[0].Active {
		t.Fatalf("redis still failing: %+v", inc)
	}
}

func TestQuietBucketsHaveNoIncidents(t *testing.T) {
	b := buckets(at("09:00:00"), at("09:10:00"))
	for i := range b {
		b[i].Writes2xx = 500
		lag := 12.0
		b[i].LagMs = &lag
	}
	if inc := console.Incidents(b, at("09:10:00")); len(inc) != 0 {
		t.Fatalf("incidents on a quiet window: %+v", inc)
	}
}

// Failures can stop while the backlog is still old (the relay working
// through it): the incident lasts until a lag sample is healthy again.
func TestIncidentKafkaLastsUntilTheLagIsHealthy(t *testing.T) {
	b := buckets(at("09:00:00"), at("09:03:00"))
	lag := func(v float64) *float64 { return &v }
	b[1].KafkaFailures, b[2].KafkaFailures = 3, 2
	b[3].LagMs, b[4].LagMs, b[5].LagMs = lag(45000), lag(30000), lag(9000) // no failures, still behind
	b[6].LagMs = lag(40)
	inc := console.Incidents(b, at("09:03:00"))
	if len(inc) != 1 || inc[0].End == nil || !inc[0].End.Equal(b[6].At) {
		t.Fatalf("incident = %+v, want it to end at %s, the first healthy lag sample", inc, b[6].At)
	}
}

// With nothing left mid-way, the text says so instead of "0 stuck payments
// were completed".
func TestIncidentDatabaseWithoutRecoveries(t *testing.T) {
	b := databaseBuckets()
	for i := range b {
		b[i].Recovered = 0
	}
	inc := console.Incidents(b, day.Add(7*time.Hour+13*time.Minute))
	if len(inc) != 1 || strings.Contains(inc[0].Text, "0 stuck") || !strings.Contains(inc[0].Text, "no payment was left half-done") {
		t.Fatalf("text = %q", inc[0].Text)
	}
}

// The title names what failed, the same while active and once resolved: the
// screen writes "Incident in progress:" or "Resolved:" around it.
func TestIncidentTitleNamesTheFailure(t *testing.T) {
	redis := buckets(at("09:00:00"), at("09:05:00"))
	for i := 4; i < 8; i++ {
		redis[i].RedisFailures = 100
	}
	for _, c := range []struct {
		name string
		inc  []console.Incident
		want string
	}{
		{"kafka resolved", console.Incidents(kafkaBuckets(t), at("10:48:00")), "Kafka unavailable"},
		{"database resolved", console.Incidents(databaseBuckets(), day.Add(7*time.Hour+13*time.Minute)), "Database unavailable"},
		{"redis resolved", console.Incidents(redis, at("09:05:00")), "Redis unavailable"},
		{"redis active", console.Incidents(redis[:8], at("09:01:50")), "Redis unavailable"},
	} {
		if len(c.inc) != 1 || c.inc[0].Title != c.want {
			t.Errorf("%s: incidents %+v, want title %q", c.name, c.inc, c.want)
		}
	}
}
