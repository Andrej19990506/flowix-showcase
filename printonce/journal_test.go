package printonce

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestJournal() (*Journal, *time.Time) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	j := New(2 * time.Minute)
	j.now = func() time.Time { return now }
	return j, &now
}

func TestOnlyOneTriggerWins(t *testing.T) {
	j, _ := newTestJournal()
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if j.Claim("order-1", fmt.Sprintf("device-%d", i)) {
				winners.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("%d triggers won the claim, want 1", winners.Load())
	}
}

func TestConfirmedOrderIsNeverAutoPrintedAgain(t *testing.T) {
	j, now := newTestJournal()
	j.Claim("o", "oven")
	j.Confirm("o", "oven")
	*now = now.Add(24 * time.Hour)
	if j.Claim("o", "handout") {
		t.Fatal("handout printed labels that the oven already printed")
	}
	if !j.Printed("o") {
		t.Fatal("order must be reported as printed")
	}
}

func TestStaleClaimExpires(t *testing.T) {
	j, now := newTestJournal()
	j.Claim("o", "tablet-lost-wifi")
	if j.Claim("o", "packer") {
		t.Fatal("fresh claim must block")
	}
	*now = now.Add(3 * time.Minute)
	if !j.Claim("o", "packer") {
		t.Fatal("expired claim must not block forever")
	}
}

func TestReleaseOnlyByHolder(t *testing.T) {
	j, _ := newTestJournal()
	j.Claim("o", "a")
	j.Release("o", "b")
	if j.Claim("o", "c") {
		t.Fatal("someone else's release must not free the claim")
	}
	j.Release("o", "a")
	if !j.Claim("o", "c") {
		t.Fatal("holder's release must free the claim")
	}
}
