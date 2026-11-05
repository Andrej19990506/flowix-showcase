package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Postgres is the Store over the outbox table:
//
//	CREATE TABLE outbox_events (
//	    id         bigserial   PRIMARY KEY,
//	    topic      text        NOT NULL,
//	    key        text        NOT NULL,
//	    body       bytea       NOT NULL,
//	    created_at timestamptz NOT NULL DEFAULT now(),
//	    sent_at    timestamptz
//	);
//	CREATE INDEX outbox_unsent ON outbox_events (id) WHERE sent_at IS NULL;
//
// Fetch locks rows until MarkSent commits, so it needs one transaction per
// batch; Tx below wraps that.
type Postgres struct {
	tx *sql.Tx
}

// Enqueue writes an event inside the caller's business transaction.
func Enqueue(ctx context.Context, tx *sql.Tx, topic, key string, body []byte) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO outbox_events (topic, key, body) VALUES ($1, $2, $3)`, topic, key, body)
	return err
}

// Tx runs one relay batch in a transaction.
func Tx(ctx context.Context, db *sql.DB, publisher Publisher, batch int) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	n, runErr := (&Relay{Store: &Postgres{tx: tx}, Publisher: publisher, Batch: batch}).RunOnce(ctx)
	if n > 0 {
		if err := tx.Commit(); err != nil {
			return 0, err
		}
	}
	return n, runErr
}

func (p *Postgres) Fetch(ctx context.Context, limit int) ([]Event, error) {
	rows, err := p.tx.QueryContext(ctx, `
		SELECT id, topic, key, body FROM outbox_events
		 WHERE sent_at IS NULL
		 ORDER BY id
		 LIMIT $1
		   FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.Topic, &e.Key, &e.Body); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (p *Postgres) MarkSent(ctx context.Context, ids []int64) error {
	args := make([]any, len(ids))
	marks := make([]string, len(ids))
	for i, id := range ids {
		args[i] = id
		marks[i] = fmt.Sprintf("$%d", i+1)
	}
	_, err := p.tx.ExecContext(ctx,
		`UPDATE outbox_events SET sent_at = now() WHERE id IN (`+strings.Join(marks, ",")+`)`, args...)
	return err
}
