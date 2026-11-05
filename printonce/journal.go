// Package printonce makes sure an order's labels are auto-printed exactly
// once, even when several triggers fire for the same order.
//
// In the kitchen the same order can ask for labels from different places:
// the oven station ("into the oven"), the handout event from the order
// source, the packer's own button, a print agent on the branch network.
// They race, and a tablet can lose Wi-Fi between "got the labels" and "the
// printer took them". Two rules came out of production incidents:
//
//   - a claim is taken before printing and confirmed after, so two triggers
//     never print the same order twice;
//   - an unconfirmed claim expires, so a tablet that died mid-print does not
//     block the order forever — the next trigger prints it.
//
// Manual reprints (torn label) bypass the journal on purpose.
package printonce

import (
	"sync"
	"time"
)

// DefaultClaimTTL is how long an unconfirmed claim blocks other triggers.
const DefaultClaimTTL = 2 * time.Minute

type entry struct {
	claimedAt time.Time
	confirmed bool
	by        string
}

// Journal is the in-memory version. In production the same rules live in
// PostgreSQL (see schema.sql) so all service replicas share them.
type Journal struct {
	mu  sync.Mutex
	ttl time.Duration
	now func() time.Time
	m   map[string]entry
}

// New creates a journal; ttl <= 0 means DefaultClaimTTL.
func New(ttl time.Duration) *Journal {
	if ttl <= 0 {
		ttl = DefaultClaimTTL
	}
	return &Journal{ttl: ttl, now: time.Now, m: map[string]entry{}}
}

// Claim reserves auto-printing of order for device. It returns false when
// the labels were already printed or another device holds a fresh claim.
func (j *Journal) Claim(order, device string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	if e, ok := j.m[order]; ok && (e.confirmed || j.now().Sub(e.claimedAt) < j.ttl) {
		return false
	}
	j.m[order] = entry{claimedAt: j.now(), by: device}
	return true
}

// Confirm records that the printer accepted the labels. Confirming after
// the claim expired is still the truth: the labels came out.
func (j *Journal) Confirm(order, device string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.m[order] = entry{claimedAt: j.now(), confirmed: true, by: device}
}

// Release gives the order back after a failed print, so the next trigger
// does not have to wait for the TTL. Only the claim holder can release.
func (j *Journal) Release(order, device string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if e, ok := j.m[order]; ok && !e.confirmed && e.by == device {
		delete(j.m, order)
	}
}

// Printed reports whether the order's labels are confirmed as printed.
func (j *Journal) Printed(order string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.m[order].confirmed
}
