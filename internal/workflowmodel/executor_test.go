package workflowmodel

import (
	"context"
	"encoding/json"
	"errors"
	sdk "github.com/shakystar/codex-adapter"
	"github.com/shakystar/role-runtime/internal/control"
	"github.com/shakystar/role-runtime/internal/engine"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeModel struct {
	requests []sdk.Request
	bad      bool
	failure  error
	closed   bool
}

func (m *fakeModel) Execute(_ context.Context, r sdk.Request) (sdk.Result, error) {
	m.requests = append(m.requests, r)
	if m.failure != nil {
		return sdk.Result{}, m.failure
	}
	if m.bad {
		return sdk.Result{Text: `{"title":"wrong","body":"one","body":"two"}`}, nil
	}
	if r.Role == "producer" {
		return sdk.Result{Text: `{"title":"Plan","body":"Verify receipts and immutable IDs; never replay uncertain work."}`}, nil
	}
	var input struct {
		Hash     string          `json:"source_sha256"`
		Document json.RawMessage `json:"document"`
	}
	if json.Unmarshal([]byte(r.Input), &input) != nil || !strings.Contains(string(input.Document), "Verify receipts") {
		return sdk.Result{}, errors.New("document not handed off")
	}
	data, _ := json.Marshal(map[string]string{"verdict": "accept", "reason": "Requirements covered", "source_sha256": input.Hash})
	return sdk.Result{Text: string(data)}, nil
}
func (m *fakeModel) Heartbeat(context.Context) error { return nil }
func (m *fakeModel) Close() error                    { m.closed = true; return nil }
func setup(t *testing.T) (*engine.Store, *Executor, *fakeModel) {
	t.Helper()
	s, err := engine.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err = s.EnableLifecycle("run", 0); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(s.Root, "workspace"), 0700)
	m := &fakeModel{}
	e := &Executor{Store: s, Factory: func(context.Context, func(sdk.Status)) (Model, error) { return m, nil }}
	t.Cleanup(func() { e.Close() })
	return s, e, m
}
func lease(t *testing.T, s *engine.Store, role string) engine.CooperativeRecord {
	t.Helper()
	task := engine.Task{ID: role, Project: "pipeline", Role: role, Version: "v1", Instance: "main", Input: "fixed"}
	if err := s.SubmitCooperative(task); err != nil {
		t.Fatal(err)
	}
	r, err := s.LeaseCooperative(role, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func write(t *testing.T, s *engine.Store, e *Executor) {
	t.Helper()
	r := lease(t, s, "producer")
	transition, err := e.Step(context.Background(), r.Execution)
	if err != nil {
		t.Fatal(err)
	}
	if transition.ExecutionState != "completed" || len(transition.Send) != 1 {
		t.Fatal("missing runtime evidence")
	}
	if err = s.FinishCooperative("producer", r.Generation, transition, time.Now()); err != nil {
		t.Fatal(err)
	}
}
func TestWriterReviewerHandoff(t *testing.T) {
	s, e, m := setup(t)
	write(t, s, e)
	r := lease(t, s, "consumer")
	transition, err := e.Step(context.Background(), r.Execution)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.requests) != 2 || m.closed {
		t.Fatal("unexpected model lifecycle")
	}
	var ref control.Reference
	if json.Unmarshal([]byte(transition.Text), &ref) != nil || ref.Artifact.ID != "review.json" {
		t.Fatal("review reference")
	}
	if m.requests[0].SessionKey == m.requests[1].SessionKey {
		t.Fatal("roles shared session")
	}
}
func TestDependencyTamperBeforeModelCall(t *testing.T) {
	s, e, m := setup(t)
	write(t, s, e)
	os.WriteFile(filepath.Join(s.Root, "artifacts", "document.json"), []byte("tampered"), 0600)
	r := lease(t, s, "consumer")
	_, err := e.Step(context.Background(), r.Execution)
	f := engine.FailureMetadata(err)
	if f == nil || f.Execution != "not_started" || len(m.requests) != 1 {
		t.Fatalf("%+v", f)
	}
}
func TestMalformedOutputAfterCompletion(t *testing.T) {
	s, e, m := setup(t)
	m.bad = true
	r := lease(t, s, "producer")
	_, err := e.Step(context.Background(), r.Execution)
	f := engine.FailureMetadata(err)
	if f == nil || f.Execution != "completed" {
		t.Fatalf("%+v", f)
	}
	if _, err = os.Stat(filepath.Join(s.Root, "artifacts", "document.json")); !os.IsNotExist(err) {
		t.Fatal("invalid output published")
	}
}
func TestFailureEvidenceAndStartupRetry(t *testing.T) {
	for _, state := range []string{"not_started", "unknown", "started", "completed"} {
		t.Run(state, func(t *testing.T) {
			s, e, m := setup(t)
			r := lease(t, s, "producer")
			failure := &sdk.Failure{Version: 1, Stage: "account_read", Code: "workspace_routing_timeout", Execution: state, Retryable: state == "not_started", Attempts: 1}
			if state == "not_started" {
				e.Factory = func(_ context.Context, status func(sdk.Status)) (Model, error) {
					status(sdk.Status{Phase: "startup_retry", Failure: failure})
					return nil, failure
				}
			} else {
				m.failure = failure
			}
			_, err := e.Step(context.Background(), r.Execution)
			if engine.FailureMetadata(err) == nil {
				t.Fatal("lost structured failure")
			}
			if err = s.FenceCooperative("producer", r.Generation, err); err != nil {
				t.Fatal(err)
			}
			receipt, err := s.CooperativeReceipt("producer")
			if err != nil || receipt.Failure == nil || receipt.Failure.Execution != state {
				t.Fatalf("%+v %v", receipt, err)
			}
			_, q := os.Stat(filepath.Join(s.Root, "uncertain", r.Task.Key()+".json"))
			if (state == "not_started") != os.IsNotExist(q) {
				t.Fatal("quarantine policy")
			}
			if state == "not_started" && receipt.Status != "failed" {
				t.Fatal("preflight is not uncertain execution")
			}
		})
	}
}
