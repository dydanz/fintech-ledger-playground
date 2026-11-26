package signing

import (
	"container/heap"
	"time"
)

// nonceCache holds each nonce until its expiry (its timestamp plus the skew).
// Expired entries are swept on every insert; capSize is a soft cap that's
// reported, never enforced by evicting a live nonce.
type nonceCache struct {
	expiry  map[string]time.Time
	order   expiryHeap
	capSize int
	over    bool
}

func newNonceCache(capSize int) *nonceCache {
	return &nonceCache{expiry: map[string]time.Time{}, capSize: capSize}
}

// add records nonce until expires, after sweeping what expired by now. fresh
// is false for a nonce already held; crossed is true when this insert first
// took the cache past its cap.
func (c *nonceCache) add(nonce string, expires, now time.Time) (fresh, crossed bool) {
	for c.order.Len() > 0 && !c.order[0].at.After(now) {
		e := heap.Pop(&c.order).(entry)
		if c.expiry[e.nonce].Equal(e.at) {
			delete(c.expiry, e.nonce)
		}
	}
	if len(c.expiry) <= c.capSize {
		c.over = false
	}
	if _, seen := c.expiry[nonce]; seen {
		return false, false
	}
	c.expiry[nonce] = expires
	heap.Push(&c.order, entry{nonce: nonce, at: expires})
	if len(c.expiry) > c.capSize && !c.over {
		c.over = true
		return true, true
	}
	return true, false
}

func (c *nonceCache) len() int { return len(c.expiry) }

type entry struct {
	nonce string
	at    time.Time
}

type expiryHeap []entry

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *expiryHeap) Push(x any)        { *h = append(*h, x.(entry)) }
func (h *expiryHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}
