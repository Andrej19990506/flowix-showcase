package outbox

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

type memStore struct {
	events []Event
	sent   map[int64]bool
}

func (m *memStore) Fetch(_ context.Context, limit int) ([]Event, error) {
	var out []Event
	for _, e := range m.events {
		if !m.sent[e.ID] && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}

func (m *memStore) MarkSent(_ context.Context, ids []int64) error {
	for _, id := range ids {
		m.sent[id] = true
	}
	return nil
}

type memKafka struct {
	got    []int64
	failOn int64
}

func (k *memKafka) Publish(_ context.Context, _, _ string, body []byte) error {
	id := int64(body[0])
	if id == k.failOn {
		k.failOn = 0 // broker comes back on the next try
		return errors.New("broker unavailable")
	}
	k.got = append(k.got, id)
	return nil
}

func store(n int) *memStore {
	s := &memStore{sent: map[int64]bool{}}
	for i := 1; i <= n; i++ {
		s.events = append(s.events, Event{ID: int64(i), Key: "order-1", Topic: "orders", Body: []byte{byte(i)}})
	}
	return s
}

func TestRelayPublishesInOrder(t *testing.T) {
	s, k := store(5), &memKafka{}
	r := &Relay{Store: s, Publisher: k, Batch: 2}
	for {
		n, err := r.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	if !reflect.DeepEqual(k.got, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("published %v", k.got)
	}
}

func TestFailedEventIsNotOvertaken(t *testing.T) {
	s, k := store(4), &memKafka{failOn: 2}
	r := &Relay{Store: s, Publisher: k}
	if _, err := r.RunOnce(context.Background()); err == nil {
		t.Fatal("want publish error")
	}
	if !reflect.DeepEqual(k.got, []int64{1}) {
		t.Fatalf("events after the failed one went out: %v", k.got)
	}
	if _, err := r.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(k.got, []int64{1, 2, 3, 4}) {
		t.Fatalf("after retry: %v", k.got)
	}
}

type memDedup struct {
	mu   sync.Mutex
	seen map[int64]bool
}

func (d *memDedup) Seen(_ context.Context, id int64) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	was := d.seen[id]
	d.seen[id] = true
	return was, nil
}

func TestConsumerHandlesRedeliveryOnce(t *testing.T) {
	d := &memDedup{seen: map[int64]bool{}}
	calls := 0
	e := Event{ID: 7}
	for i := 0; i < 3; i++ { // relay crashed twice before marking sent
		if err := Handle(context.Background(), d, e, func(context.Context, Event) error { calls++; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("handled %d times, want 1", calls)
	}
}
