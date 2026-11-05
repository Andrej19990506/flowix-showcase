package boarddelta

import (
	"errors"
	"reflect"
	"testing"
)

func TestFirstMessageIsSnapshot(t *testing.T) {
	var d Differ
	board := []Order{{ID: "a", Code: "0901"}, {ID: "b", Code: "0902"}}
	m, ok, err := d.Next(board)
	if err != nil || !ok || !m.Snapshot || len(m.Upserts) != 2 || m.Seq != 1 {
		t.Fatalf("got %+v ok=%v err=%v", m, ok, err)
	}
}

func TestOnlyChangedOrdersAreSent(t *testing.T) {
	var d Differ
	board := []Order{{ID: "a", Code: "0901"}, {ID: "b", Code: "0902"}}
	d.Next(board)

	board[1].Ready = true
	m, ok, _ := d.Next(board)
	if !ok || m.Snapshot || len(m.Upserts) != 1 || m.Upserts[0].ID != "b" || len(m.Removes) != 0 {
		t.Fatalf("want only b, got %+v", m)
	}
	if _, ok, _ := d.Next(board); ok {
		t.Fatal("no changes must produce no message")
	}
}

func TestRemovedOrders(t *testing.T) {
	var d Differ
	d.Next([]Order{{ID: "a"}, {ID: "b"}, {ID: "c"}})
	m, _, _ := d.Next([]Order{{ID: "b"}})
	if !reflect.DeepEqual(m.Removes, []string{"a", "c"}) {
		t.Fatalf("removes = %v", m.Removes)
	}
}

func TestAssemblerFollowsStream(t *testing.T) {
	var d Differ
	var a Assembler
	steps := [][]Order{
		{{ID: "a", Code: "1"}, {ID: "b", Code: "2"}},
		{{ID: "a", Code: "1"}, {ID: "b", Code: "2", Ready: true}, {ID: "c", Code: "3"}},
		{{ID: "b", Code: "2", Ready: true}, {ID: "c", Code: "3"}},
	}
	for i, board := range steps {
		m, ok, err := d.Next(board)
		if err != nil || !ok {
			t.Fatalf("step %d: ok=%v err=%v", i, ok, err)
		}
		got, err := a.Apply(m)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if !reflect.DeepEqual(got, board) {
			t.Fatalf("step %d: client board %+v, server %+v", i, got, board)
		}
	}
}

func TestGapMeansResubscribe(t *testing.T) {
	var d Differ
	var a Assembler
	m1, _, _ := d.Next([]Order{{ID: "a"}})
	a.Apply(m1)
	d.Next([]Order{{ID: "a", Ready: true}}) // lost on the way
	m3, _, _ := d.Next([]Order{{ID: "b"}})
	if _, err := a.Apply(m3); !errors.Is(err, ErrOutOfSync) {
		t.Fatalf("want ErrOutOfSync, got %v", err)
	}

	d.Reset()
	snap, _, _ := d.Next([]Order{{ID: "b"}})
	got, err := a.Apply(snap)
	if err != nil || len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("after resync: %+v %v", got, err)
	}
}
