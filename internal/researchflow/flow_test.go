package researchflow

import (
	"context"
	"encoding/json"
	"github.com/shakystar/role-runtime/internal/engine"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fake struct {
	fn func(engine.Execution) (engine.Transition, error)
}

func (f *fake) Step(_ context.Context, x engine.Execution) (engine.Transition, error) { return f.fn(x) }
func (f *fake) Heartbeat(context.Context) error                                       { return nil }
func (f *fake) Close() error                                                          { return nil }
func setup(t *testing.T) (*Flow, map[string]engine.Task) {
	t.Helper()
	s, e := engine.OpenStore(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	s.RegisterProject(engine.Project{ID: "p", Workspace: t.TempDir(), Rules: "test"})
	specs := []RoleSpec{{ID: "a", Recipients: []string{"c"}}, {ID: "b", Recipients: []string{"c"}}, {ID: "c", Dependencies: []string{"a", "b"}, Recipients: []string{"d"}}, {ID: "d", Dependencies: []string{"c"}}}
	tasks := map[string]engine.Task{}
	for _, v := range specs {
		s.RegisterRole(engine.Role{ID: v.ID, Version: "v1", Instruction: "test", Tools: []string{"advisory"}})
		tasks[v.ID] = engine.Task{ID: v.ID, Project: "p", Role: v.ID, Version: "v1", Instance: "main", Input: "test"}
		if e := s.SubmitCooperative(tasks[v.ID]); e != nil {
			t.Fatal(e)
		}
	}
	f, e := New(s, specs, tasks)
	if e != nil {
		t.Fatal(e)
	}
	return f, tasks
}
func transition(f *Flow, role string, in Input) engine.Transition {
	d := Document{Role: role, Summary: role + " summary", Findings: "findings", Sources: []Source{{ID: role + "1", URL: "https://example.org/" + role, Title: "source", Access: "body-read", Observation: "observed"}}, Limitations: []string{"test"}, Readbacks: map[string]string{}}
	for _, dep := range f.specs[role].Dependencies {
		d.Readbacks[dep] = in.Documents[dep].Summary
	}
	b, _ := json.Marshal(d)
	tr := engine.Transition{Text: string(b)}
	for _, to := range f.specs[role].Recipients {
		tr.Send = append(tr.Send, engine.Message{ID: role + "-to-" + to, From: engine.AddressOf(f.tasks[role]), To: engine.AddressOf(f.tasks[to]), Correlation: role + "-v1", Body: "ready"})
	}
	return tr
}
func commit(t *testing.T, f *Flow, role string) {
	t.Helper()
	r, e := f.store.LeaseCooperative(f.tasks[role].ID, time.Now(), time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	tr, e := f.Wrap(&fake{func(x engine.Execution) (engine.Transition, error) {
		var in Input
		json.Unmarshal([]byte(x.Task.Input), &in)
		return transition(f, role, in), nil
	}}).Step(context.Background(), r.Execution)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.store.FinishCooperative(r.Task.ID, r.Generation, tr, time.Now()); e != nil {
		t.Fatal(e)
	}
}
func TestCommittedFanInAndTransitiveEvidence(t *testing.T) {
	f, tasks := setup(t)
	commit(t, f, "b")
	var captured Input
	w := f.Wrap(&fake{func(x engine.Execution) (engine.Transition, error) {
		json.Unmarshal([]byte(x.Task.Input), &captured)
		return engine.Transition{Checkpoint: "pending", Wait: &engine.WaitFor{From: engine.AddressOf(tasks["a"]), Correlation: "a-v1"}}, nil
	}})
	if _, e := w.Step(context.Background(), engine.Execution{Task: tasks["c"]}); e != nil {
		t.Fatal(e)
	}
	if len(captured.Missing) != 1 || captured.Missing[0] != "a" || captured.Documents["b"].Summary == "" {
		t.Fatal(captured)
	}
	commit(t, f, "a")
	commit(t, f, "c")
	w = f.Wrap(&fake{func(x engine.Execution) (engine.Transition, error) {
		json.Unmarshal([]byte(x.Task.Input), &captured)
		return transition(f, "d", captured), nil
	}})
	if _, e := w.Step(context.Background(), engine.Execution{Task: tasks["d"]}); e != nil {
		t.Fatal(e)
	}
	if len(captured.Documents) != 3 || captured.Documents["a"].Sources[0].URL != "https://example.org/a" {
		t.Fatal("lost ancestor sources")
	}
}
func TestUncommittedAndCorruptArtifact(t *testing.T) {
	f, tasks := setup(t)
	w := f.Wrap(&fake{func(x engine.Execution) (engine.Transition, error) { return transition(f, "a", Input{}), nil }})
	x := engine.Execution{Task: tasks["a"]}
	if _, e := w.Step(context.Background(), x); e != nil {
		t.Fatal(e)
	}
	if _, _, e := f.LoadDocument("a"); e == nil {
		t.Fatal("uncommitted artifact published")
	}
	if _, e := w.Step(context.Background(), x); e != nil {
		t.Fatal("idempotent prepare", e)
	}
	commit(t, f, "a")
	os.WriteFile(filepath.Join(f.store.Root, "research-documents/a.json"), []byte("corrupt"), 0600)
	if _, _, e := f.LoadDocument("a"); e == nil {
		t.Fatal("corruption accepted")
	}
}
func TestInvalidCompletionAndReadback(t *testing.T) {
	for _, mode := range []string{"missing", "readback", "oversize", "wait-send", "limitations-string"} {
		t.Run(mode, func(t *testing.T) {
			f, tasks := setup(t)
			if mode == "readback" || mode == "oversize" || mode == "limitations-string" {
				commit(t, f, "a")
				commit(t, f, "b")
			}
			w := f.Wrap(&fake{func(x engine.Execution) (engine.Transition, error) {
				var in Input
				json.Unmarshal([]byte(x.Task.Input), &in)
				tr := transition(f, "c", in)
				if mode == "readback" {
					var d Document
					json.Unmarshal([]byte(tr.Text), &d)
					d.Readbacks["a"] = "wrong"
					b, _ := json.Marshal(d)
					tr.Text = string(b)
				}
				if mode == "limitations-string" {
					var d map[string]any
					json.Unmarshal([]byte(tr.Text), &d)
					d["limitations"] = "invalid single string"
					b, _ := json.Marshal(d)
					tr.Text = string(b)
				}
				if mode == "oversize" {
					tr.Text = string(make([]byte, 8193))
				}
				if mode == "wait-send" {
					tr.Wait = &engine.WaitFor{From: engine.AddressOf(tasks["a"]), Correlation: "a-v1"}
					tr.Checkpoint = "wait"
				}
				return tr, nil
			}})
			if _, e := w.Step(context.Background(), engine.Execution{Task: tasks["c"]}); e == nil {
				t.Fatal("invalid accepted")
			}
		})
	}
}
