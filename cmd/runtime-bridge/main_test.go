package main

import (
	"context"
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
	second, err := execute(context.Background(), store, e, req)
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
