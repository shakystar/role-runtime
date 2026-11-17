package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveDefinitionsDoesNotCreatePiSession(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.RegisterProject(Project{ID: "p", Workspace: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if err = s.RegisterRole(Role{ID: "r", Version: "v1", Instruction: "review", Tools: []string{"advisory"}}); err != nil {
		t.Fatal(err)
	}
	task := Task{ID: "one", Project: "p", Role: "r", Version: "v1", Instance: "main", Input: "test"}
	a, err := s.ResolveDefinitions(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(s.Root, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 || a.SessionID != "" || a.SessionPath != "" {
		t.Fatal("definition lookup created harness state")
	}
	if err = s.Quarantine(task, errors.New("uncertain")); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ResolveDefinitions(context.Background(), task); err == nil {
		t.Fatal("quarantine bypassed")
	}
}
