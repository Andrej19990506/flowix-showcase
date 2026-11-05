// Package boarddelta streams a kitchen board to tablets as one snapshot
// followed by small deltas.
//
// A busy station board holds 30–60 orders and changes several times a
// second. Re-sending the whole board on every change made tablets redraw
// every card and dropped frames at peak. The server keeps a fingerprint of
// each order it last sent and emits only orders whose content changed, plus
// ids of orders that left the board. Every message carries a sequence
// number; a client that sees a gap asks for a fresh snapshot instead of
// guessing.
package boarddelta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// Order is one card on the board. Real orders carry lines, timers and
// handoff data; for the delta logic only the id and a stable encoding of
// the rest matter.
type Order struct {
	ID    string   `json:"id"`
	Code  string   `json:"code"`
	Lines []string `json:"lines"`
	Ready bool     `json:"ready"`
}

// Message is what goes down the stream.
type Message struct {
	Seq      uint64   `json:"seq"`
	Snapshot bool     `json:"snapshot"`
	Upserts  []Order  `json:"upserts,omitempty"`
	Removes  []string `json:"removes,omitempty"`
}

// Differ remembers what one stream has already seen. It is not safe for
// concurrent use: each subscriber owns its own Differ.
type Differ struct {
	seq  uint64
	sent map[string][]byte
}

// Next compares the current board with what was sent last time. The first
// call, or a call after Reset, returns a full snapshot. A call with no
// changes returns ok=false so the caller can skip sending.
func (d *Differ) Next(board []Order) (msg Message, ok bool, err error) {
	current := make(map[string][]byte, len(board))
	for _, o := range board {
		b, err := json.Marshal(o) // struct fields encode in a fixed order
		if err != nil {
			return Message{}, false, fmt.Errorf("encode order %s: %w", o.ID, err)
		}
		current[o.ID] = b
	}

	if d.sent == nil {
		d.seq++
		d.sent = current
		return Message{Seq: d.seq, Snapshot: true, Upserts: board}, true, nil
	}

	for _, o := range board {
		if prev, seen := d.sent[o.ID]; !seen || !bytes.Equal(prev, current[o.ID]) {
			msg.Upserts = append(msg.Upserts, o)
		}
	}
	for id := range d.sent {
		if _, still := current[id]; !still {
			msg.Removes = append(msg.Removes, id)
		}
	}
	if len(msg.Upserts) == 0 && len(msg.Removes) == 0 {
		return Message{}, false, nil
	}
	sort.Strings(msg.Removes)
	d.seq++
	d.sent = current
	msg.Seq = d.seq
	return msg, true, nil
}

// Reset forces the next message to be a snapshot (client reconnected or
// reported it is out of sync).
func (d *Differ) Reset() { d.sent = nil }

// ErrOutOfSync means the client missed a message and must resubscribe.
var ErrOutOfSync = fmt.Errorf("board stream out of sync")

// Assembler is the client side: it rebuilds the board from the stream and
// keeps untouched orders as the same values, so the UI can skip redrawing
// them.
type Assembler struct {
	seq    uint64
	orders map[string]Order
	order  []string // display order: first appearance
}

// Apply folds one message into the board and returns the board in display
// order.
func (a *Assembler) Apply(m Message) ([]Order, error) {
	switch {
	case m.Snapshot:
		a.orders = make(map[string]Order, len(m.Upserts))
		a.order = a.order[:0]
	case a.orders == nil || m.Seq != a.seq+1:
		return nil, ErrOutOfSync
	}
	a.seq = m.Seq
	for _, o := range m.Upserts {
		if _, known := a.orders[o.ID]; !known {
			a.order = append(a.order, o.ID)
		}
		a.orders[o.ID] = o
	}
	for _, id := range m.Removes {
		delete(a.orders, id)
	}
	out := make([]Order, 0, len(a.orders))
	kept := a.order[:0]
	for _, id := range a.order {
		if o, ok := a.orders[id]; ok {
			out = append(out, o)
			kept = append(kept, id)
		}
	}
	a.order = kept
	return out, nil
}
