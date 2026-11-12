package console

import (
	"fmt"
	"sort"
	"time"
)

// BucketWidth is the step of the Loki metric queries behind incidents.
const BucketWidth = 15 * time.Second

// Bucket is what happened in one BucketWidth slice, counted from the logs
// (spec/2026-11-10 Web Console Design D§6.4).
type Bucket struct {
	At            time.Time `json:"at"`
	KafkaFailures int       `json:"kafka_failures"` // outbox.publish reason=kafka_unavailable
	LagMs         *float64  `json:"lag_ms"`         // highest outbox.lag in the slice; nil without a sample
	Refused       int       `json:"refused"`        // ledger-api POSTs answered 503
	Writes2xx     int       `json:"writes_2xx"`     // ledger-api POSTs answered 2xx
	RedisFailures int       `json:"redis_failures"` // ledger-api dependency.call dependency=redis
	Recovered     int       `json:"recovered"`      // ledger.recover outcome=success
}

// Incident is an outage the Monitor explains in plain words.
type Incident struct {
	Kind      string     `json:"kind"` // kafka, database, redis
	Active    bool       `json:"active"`
	Start     time.Time  `json:"started_at"`
	End       *time.Time `json:"ended_at,omitempty"`
	Title     string     `json:"title"`
	Text      string     `json:"text"`
	Count     int        `json:"count"`                 // failed publishes, refused requests, or failed cache calls
	Recovered int        `json:"recovered,omitempty"`   // database: payments the recovery job completed
	PeakLagMs float64    `json:"peak_lag_ms,omitempty"` // kafka: the oldest event's age at worst
}

// Thresholds (starting values to tune against the chaos drills, D§6.4).
const (
	lagHealthyMs       = 5000             // the outbox is caught up again
	redisQuiet         = 30 * time.Second // no cache failures this long ends a Redis incident
	recoveryAfterwards = 2 * time.Minute  // recoveries this soon after a database incident belong to it
)

// Incidents finds the outages in buckets (oldest first), most recent first.
// An incident with no end yet is active.
func Incidents(b []Bucket, now time.Time) []Incident {
	var out []Incident
	out = append(out, kafkaIncidents(b)...)
	out = append(out, databaseIncidents(b)...)
	out = append(out, redisIncidents(b, now)...)
	sort.Slice(out, func(i, j int) bool { return out[i].Start.After(out[j].Start) })
	return out
}

// kafkaIncidents: from the first kafka_unavailable until a lag sample under
// 5 s with no failure in its slice.
func kafkaIncidents(b []Bucket) []Incident {
	var out []Incident
	var cur *Incident
	for _, k := range b {
		if cur == nil && k.KafkaFailures > 0 {
			cur = &Incident{Kind: "kafka", Start: k.At}
		}
		if cur == nil {
			continue
		}
		cur.Count += k.KafkaFailures
		if k.LagMs != nil && *k.LagMs > cur.PeakLagMs {
			cur.PeakLagMs = *k.LagMs
		}
		if k.KafkaFailures == 0 && k.LagMs != nil && *k.LagMs < lagHealthyMs {
			end := k.At
			cur.End = &end
			out = append(out, finishKafka(*cur))
			cur = nil
		}
	}
	if cur != nil {
		cur.Active = true
		out = append(out, finishKafka(*cur))
	}
	return out
}

func finishKafka(i Incident) Incident {
	i.Title = "Kafka unavailable"
	if i.Active {
		i.Text = "Payments keep working. Events are saved in the ledger's outbox and will be delivered when Kafka is back."
	} else {
		i.Text = fmt.Sprintf("Kafka was down for %s. Payments kept working, and every event queued meanwhile was delivered once it was back (oldest waited %s).",
			durationWords(i.End.Sub(i.Start)), durationWords(time.Duration(i.PeakLagMs)*time.Millisecond))
		i.Title = "Resolved: Kafka unavailable"
	}
	return i
}

// databaseIncidents: from the first 503 until a slice with writes
// succeeding and none refused.
func databaseIncidents(b []Bucket) []Incident {
	var out []Incident
	var cur *Incident
	for _, k := range b {
		if cur == nil && k.Refused > 0 {
			cur = &Incident{Kind: "database", Start: k.At}
		}
		if cur == nil {
			continue
		}
		cur.Count += k.Refused
		if k.Refused == 0 && k.Writes2xx > 0 {
			end := k.At
			cur.End = &end
			out = append(out, finishDatabase(*cur, b))
			cur = nil
		}
	}
	if cur != nil {
		cur.Active = true
		out = append(out, finishDatabase(*cur, b))
	}
	return out
}

func finishDatabase(i Incident, b []Bucket) Incident {
	if i.Active {
		i.Title = "Database unavailable"
		i.Text = "Payments are refused with \"try again\" (503) until the database is back. Nothing half-done is kept."
		return i
	}
	// The recovery job's completions from the incident's start until
	// recoveryAfterwards past its end belong to it.
	n := 0
	for _, k := range b {
		if !k.At.Before(i.Start) && !k.At.After(i.End.Add(recoveryAfterwards)) {
			n += k.Recovered
		}
	}
	i.Recovered = n
	i.Title = "Resolved: database unavailable"
	stuck := "no payment was left half-done"
	if n > 0 {
		stuck = fmt.Sprintf("%d %s caught mid-way and completed by the recovery job", n, plural(n, "payment was", "payments were"))
	}
	i.Text = fmt.Sprintf("The database was down for %s. %s requests were refused with \"try again\" (503); %s.",
		durationWords(i.End.Sub(i.Start)), thousands(i.Count), stuck)
	return i
}

// redisIncidents: cache failures, ended by redisQuiet without one.
func redisIncidents(b []Bucket, now time.Time) []Incident {
	var out []Incident
	var cur *Incident
	var last time.Time
	for _, k := range b {
		if k.RedisFailures > 0 {
			if cur == nil {
				cur = &Incident{Kind: "redis", Start: k.At}
			}
			cur.Count += k.RedisFailures
			last = k.At
			continue
		}
		if cur != nil && k.At.Sub(last) >= redisQuiet {
			end := last.Add(BucketWidth)
			cur.End = &end
			out = append(out, finishRedis(*cur))
			cur = nil
		}
	}
	if cur != nil {
		if now.Sub(last) >= redisQuiet+BucketWidth {
			end := last.Add(BucketWidth)
			cur.End = &end
		} else {
			cur.Active = true
		}
		out = append(out, finishRedis(*cur))
	}
	return out
}

func finishRedis(i Incident) Incident {
	if i.Active {
		i.Title = "Redis unavailable"
		i.Text = "Payments keep working without the cache, a little slower; duplicates are still caught by the database."
		return i
	}
	i.Title = "Resolved: Redis unavailable"
	i.Text = fmt.Sprintf("Redis was down for %s. Payments kept working without the cache.", durationWords(i.End.Sub(i.Start)))
	return i
}

func durationWords(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%d s", int(d.Seconds()))
	}
	m, s := int(d.Minutes()), int(d.Seconds())%60
	if s == 0 {
		return fmt.Sprintf("%d min", m)
	}
	return fmt.Sprintf("%d min %d s", m, s)
}

func thousands(n int) string {
	s := fmt.Sprint(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}
