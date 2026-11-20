package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/shakystar/role-runtime/internal/engine"
)

type counted struct{ calls int }

func (c *counted) Execute(_ context.Context, t engine.Task) (engine.Result, error) {
	c.calls++
	return engine.Result{Text: t.Input}, nil
}
func (*counted) Close() error { return nil }
func TestDurableDeduplicationAndUncertainWork(t *testing.T) {
	root := t.TempDir()
	store, err := engine.OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.RegisterProject(engine.Project{ID: "bridge", Workspace: root}); err != nil {
		t.Fatal(err)
	}
	e := &counted{}
	req := request{ID: "task", Role: "reviewer", Instruction: "review", Input: "evidence"}
	first, err := execute(context.Background(), store, e, req)
	if err != nil || first.Status != "succeeded" || first.Stats.MaxActive != 1 {
		t.Fatalf("%+v %v", first, err)
	}
	store.Close()
	store, err = engine.OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lazy := &lazyExecutor{factory: func() (engine.Executor, error) {
		t.Fatal("cached receipt started backend")
		return nil, errors.New("unexpected backend")
	}}
	second, err := execute(context.Background(), store, lazy, req)
	if err != nil || !second.Cached || e.calls != 1 {
		t.Fatalf("cached=%v calls=%d err=%v", second.Cached, e.calls, err)
	}
	changed := req
	changed.Input = "different"
	if _, err = execute(context.Background(), store, e, changed); err == nil {
		t.Fatal("changed input accepted")
	}
	changed = req
	changed.Instruction = "different"
	if _, err = execute(context.Background(), store, e, changed); !errors.Is(err, engine.ErrConflict) {
		t.Fatalf("changed role accepted: %v", err)
	}
	interrupted := engine.Task{ID: "interrupted", Project: "bridge", Role: req.Role, Version: "v1", Instance: "main", Input: req.Input}
	if err = store.Claim(interrupted); err != nil {
		t.Fatal(err)
	}
	req.ID = "interrupted"
	if _, err = execute(context.Background(), store, e, req); err == nil {
		t.Fatal("uncertain work replayed")
	}
	if e.calls != 1 {
		t.Fatal("unexpected execution")
	}
	// Receipt corruption must fail closed, not start a replacement execution.
	req.ID = "corrupt"
	if err = os.WriteFile(filepath.Join(root, "tasks", engine.Digest(req.ID)+".json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = execute(context.Background(), store, e, req); err == nil {
		t.Fatal("corrupt receipt accepted")
	}
}

func TestLiteralFixtureText(t *testing.T) {
	task := engine.Task{Input: `{"fixture_text":"{\"notice\":\"ready\"}"}`}
	got, err := (fixture{}).Execute(context.Background(), task)
	if err != nil || got.Text != `{"notice":"ready"}` {
		t.Fatalf("text %q, err %v", got.Text, err)
	}
	task.Input = `{"fixture":"raw-json"}`
	got, err = (fixture{}).Execute(context.Background(), task)
	if err != nil || got.Text != `"raw-json"` {
		t.Fatal("legacy fixture changed")
	}
}

func TestInspectAbsentDoesNotCreateState(t *testing.T) {
	state := filepath.Join(t.TempDir(), "absent")
	var out bytes.Buffer
	if err := inspectAttempt(state, "run", "attempt", &out); err != nil {
		t.Fatal(err)
	}
	var result inspection
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || result.Status != "absent" || result.Version != 1 {
		t.Fatal("invalid reply")
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatal("inspection created state")
	}
}

func TestInspectRequiresBoundRunAndPreservesBytes(t *testing.T) {
	store, err := engine.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	req := request{SchemaVersion: 1, RunID: "run-1", AttemptID: "attempt-1", ID: "work", Role: "writer", Instruction: "write", Input: "hello", Traceparent: "00-11111111111111111111111111111111-2222222222222222-01"}
	if err := bindProtocol(store, req); err != nil {
		t.Fatal(err)
	}
	if err := bindAttempt(store, req); err != nil {
		t.Fatal(err)
	}
	task := engine.Task{ID: req.ID, Project: "bridge", Role: req.Role, Version: "v1", Instance: "main", Input: req.Input}
	if err := store.Update(engine.Outcome{Task: task, Status: "succeeded", Result: engine.Result{Text: "done"}}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := inspectAttempt(store.Root, "other", req.AttemptID, &out); err == nil {
		t.Fatal("mismatched run accepted")
	}
	if err := inspectAttempt(store.Root, req.RunID, req.AttemptID, &out); err != nil {
		t.Fatal(err)
	}
	var result inspection
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join(store.Root, "tasks", engine.Digest(req.ID)+".json"))
	if err != nil || result.Status != "observed" || !bytes.Equal(result.Receipt, want) {
		t.Fatal("receipt bytes did not round trip")
	}
}
