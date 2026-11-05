// Package outbox publishes domain events to Kafka without losing them and
// without publishing events of a rolled-back transaction.
//
// The order service writes the order and its "order.created" event in one
// database transaction. A relay then reads unsent events in order, publishes
// them and marks them sent. If the process dies between publish and mark,
// the event goes out again on restart — delivery is at-least-once, so
// consumers deduplicate by event id (see Dedup).
package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Event is one row of the outbox table.
type Event struct {
	ID    int64
	Key   string // Kafka key: events of one order stay ordered in one partition
	Topic string
	Body  []byte
}

// Store is the outbox table. Fetch must return unsent events in id order
// and lock them for this relay (FOR UPDATE SKIP LOCKED in Postgres), so
// several relay replicas do not publish the same batch at once.
type Store interface {
	Fetch(ctx context.Context, limit int) ([]Event, error)
	MarkSent(ctx context.Context, ids []int64) error
}

// Publisher is the Kafka producer.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, body []byte) error
}

// Relay moves events from Store to Publisher.
type Relay struct {
	Store     Store
	Publisher Publisher
	Batch     int
	Idle      time.Duration // pause when there is nothing to send
}

// RunOnce sends one batch and returns how many events were sent. It stops
// at the first failed publish: events after it must not overtake it, or a
// consumer would see "order.cancelled" before "order.created".
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	events, err := r.Store.Fetch(ctx, r.batch())
	if err != nil {
		return 0, fmt.Errorf("fetch outbox: %w", err)
	}
	sent := make([]int64, 0, len(events))
	var pubErr error
	for _, e := range events {
		if err := r.Publisher.Publish(ctx, e.Topic, e.Key, e.Body); err != nil {
			pubErr = fmt.Errorf("publish event %d: %w", e.ID, err)
			break
		}
		sent = append(sent, e.ID)
	}
	if len(sent) > 0 {
		if err := r.Store.MarkSent(ctx, sent); err != nil {
			return 0, errors.Join(pubErr, fmt.Errorf("mark sent: %w", err))
		}
	}
	return len(sent), pubErr
}

// Run loops until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	for {
		n, err := r.RunOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || n == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.idle()):
			}
		}
	}
}

func (r *Relay) batch() int {
	if r.Batch <= 0 {
		return 100
	}
	return r.Batch
}

func (r *Relay) idle() time.Duration {
	if r.Idle <= 0 {
		return time.Second
	}
	return r.Idle
}

// Dedup is the consumer side of at-least-once delivery: it remembers
// processed event ids. In production this is a table with a unique key,
// written in the same transaction as the consumer's own changes.
type Dedup interface {
	// Seen marks id as processed and reports whether it was processed before.
	Seen(ctx context.Context, id int64) (bool, error)
}

// Handle runs fn once per event id.
func Handle(ctx context.Context, d Dedup, e Event, fn func(context.Context, Event) error) error {
	seen, err := d.Seen(ctx, e.ID)
	if err != nil || seen {
		return err
	}
	return fn(ctx, e)
}
