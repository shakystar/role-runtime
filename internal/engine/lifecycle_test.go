package engine

import (
	"encoding/json"
	"errors"
	"github.com/shakystar/role-runtime/internal/lifecycle"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLifecycleIntentRecovery(t *testing.T) {
	root := t.TempDir()
	s, e := OpenStore(root)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.EnableLifecycle("run", 0); e != nil {
		t.Fatal(e)
	}
	task := coopTask("producer", "producer")
	if e = s.SubmitCooperative(task); e != nil {
		t.Fatal(e)
	}
	r, e := s.LeaseCooperative(task.ID, time.Now(), time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	// Make projection fail after the execution receipt+event intent can commit.
	ledgerPath := filepath.Join(root, "lifecycle")
	if e = os.Rename(ledgerPath, ledgerPath+"-saved"); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(ledgerPath, []byte("not a directory"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = s.FinishCooperative(task.ID, r.Generation, Transition{Text: "done"}, time.Now()); e != nil {
		t.Fatal(e)
	}
	receipt, e := s.CooperativeReceipt(task.ID)
	if e != nil || receipt.Status != "completed" || len(receipt.Events) != 1 {
		t.Fatalf("%+v %v", receipt, e)
	}
	if s.LifecycleHealth()["reserved_bytes"].(int) <= 0 {
		t.Fatal("pending completion lost capacity reservation")
	}
	s.Close()
	os.Remove(ledgerPath)
	if e = os.Rename(ledgerPath+"-saved", ledgerPath); e != nil {
		t.Fatal(e)
	}
	s, e = OpenStore(root)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.EnableLifecycle("run", 0); e != nil {
		t.Fatal(e)
	}
	rows, _, e := s.LifecyclePage(nil, 100)
	if e != nil || len(rows) != 3 {
		t.Fatalf("rows=%d err=%v", len(rows), e)
	}
	ends := 0
	for _, row := range rows {
		var event lifecycle.Event
		if json.Unmarshal(row.Data, &event) != nil {
			t.Fatal("bad event")
		}
		if event.Kind == "attempt.finished" {
			ends++
		}
	}
	if ends != 1 {
		t.Fatal("duplicate or lost completion")
	}
	receipt, _ = s.CooperativeReceipt(task.ID)
	if receipt.Generation != 1 || len(receipt.Events) != 0 {
		t.Fatal("receipt replayed or intent retained")
	}
}
func TestLifecycleCapacityReservesCompletion(t *testing.T) {
	s := fixtureStore(t)
	if e := s.EnableLifecycle("run", terminalReserve+4096); e != nil {
		t.Fatal(e)
	}
	first := coopTask("one", "one")
	if e := s.SubmitCooperative(first); e != nil {
		t.Fatal(e)
	}
	if e := s.SubmitCooperative(coopTask("two", "two")); !errors.Is(e, lifecycle.ErrCapacity) {
		t.Fatalf("expected capacity: %v", e)
	}
	r, e := s.LeaseCooperative("one", time.Now(), time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.FinishCooperative("one", r.Generation, Transition{Text: "done"}, time.Now()); e != nil {
		t.Fatal(e)
	}
	if _, e = s.CooperativeReceipt("two"); !os.IsNotExist(e) {
		t.Fatal("rejected work persisted")
	}
}
func TestLifecycleProjectionIdempotence(t *testing.T) {
	s := fixtureStore(t)
	if e := s.EnableLifecycle("run", 0); e != nil {
		t.Fatal(e)
	}
	if e := s.SubmitCooperative(coopTask("one", "one")); e != nil {
		t.Fatal(e)
	}
	rows, cursor, e := s.LifecyclePage(nil, 100)
	if e != nil || len(rows) != 1 {
		t.Fatal(e)
	}
	r, e := s.CooperativeReceipt("one")
	if e != nil {
		t.Fatal(e)
	}
	var event lifecycle.Event
	json.Unmarshal(rows[0].Data, &event)
	r.Events = []lifecycle.Event{event}
	data, _ := encodeCooperative(r)
	if e = atomicWrite(s.coopPath("one"), data); e != nil {
		t.Fatal(e)
	}
	more, next, e := s.LifecyclePage(&cursor, 100)
	if e != nil || len(more) != 0 || next != cursor {
		t.Fatalf("duplicate %d %s %v", len(more), next, e)
	}
}

func TestLifecycleAuxiliaryEvents(t *testing.T) {
	s := fixtureStore(t)
	if e := s.EnableLifecycle("run", 0); e != nil {
		t.Fatal(e)
	}
	if e := s.SubmitCooperative(coopTask("one", "one")); e != nil {
		t.Fatal(e)
	}
	r, e := s.LeaseCooperative("one", time.Now(), time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if e = s.RecordStartupRetry("one", r.Generation, 1); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.FinishCooperative("one", r.Generation, Transition{Text: "done"}, time.Now()); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if e = s.RecordCacheHit("one", "delivery"); e != nil {
			t.Fatal(e)
		}
	}
	rows, _, e := s.LifecyclePage(nil, 100)
	if e != nil || len(rows) != 5 {
		t.Fatalf("rows=%d err=%v", len(rows), e)
	}
}
