package lifecycle

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestLedgerCursorConflictAndBound(t *testing.T) {
	root := t.TempDir()
	l, e := Open(root, "run", 1000)
	if e != nil {
		t.Fatal(e)
	}
	row := Entry{ID: "one", Kind: "event", Data: json.RawMessage(`{"a":1}`)}
	if e = l.Append(row, 0); e != nil {
		t.Fatal(e)
	}
	used := l.Used()
	if e = l.Append(row, 0); e != nil || l.Used() != used {
		t.Fatal("duplicate")
	}
	row.Data = json.RawMessage(`{"a":2}`)
	if !errors.Is(l.Append(row, 0), ErrConflict) {
		t.Fatal("conflict")
	}
	wrong := "other:0"
	if _, _, e = l.Page(&wrong, 10); !errors.Is(e, ErrCursor) {
		t.Fatal("foreign cursor")
	}
	wrong = "run:999"
	if _, _, e = l.Page(&wrong, 10); !errors.Is(e, ErrCursor) {
		t.Fatal("future cursor")
	}
	if _, _, e = l.Page(nil, 101); e == nil {
		t.Fatal("unbounded page")
	}
	row.ID = "two"
	if !errors.Is(l.Append(row, 1000), ErrCapacity) {
		t.Fatal("reservation ignored")
	}
	reopened, e := Open(root, "run", 1000)
	if e != nil {
		t.Fatal(e)
	}
	rows, _, e := reopened.Page(nil, 100)
	if e != nil || len(rows) != 1 {
		t.Fatal("reopen")
	}
}
