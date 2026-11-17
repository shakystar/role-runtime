package codexcooperation

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/shakystar/role-runtime/internal/engine"
)

func execution() engine.Execution {
	task := engine.Task{ID: "task-1", Project: "project", Role: "reviewer", Version: "v1", Instance: "main", Input: "Review the document"}
	return engine.Execution{Task: task, Generation: 2, Checkpoint: "await document", Inbox: &engine.Message{ID: "message-1", From: engine.Address{Project: "project", Role: "writer", Version: "v1", Instance: "main"}, To: engine.AddressOf(task), Correlation: "document-v1", Body: "artifact:doc-v1"}}
}

func TestStepPreservesResumeEnvelopeAndIdentity(t *testing.T) {
	e := execution()
	original := e
	called := false
	tr, err := step(context.Background(), e, func(ctx context.Context, task engine.Task) (engine.Result, error) {
		called = true
		if task.Key() != e.Task.Key() || task.ID != e.Task.ID {
			t.Fatal("task identity changed")
		}
		var got engine.Execution
		if err := json.Unmarshal([]byte(task.Input), &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, e) {
			t.Fatalf("resume state lost: %#v", got)
		}
		return engine.Result{Text: `{"checkpoint":"reviewed doc-v1","send":[],"wait":null,"text":"accepted"}`}, nil
	})
	if err != nil || !called || tr.Text != "accepted" || tr.Wait != nil {
		t.Fatalf("%+v %v", tr, err)
	}
	if !reflect.DeepEqual(e, original) {
		t.Fatal("input mutated")
	}
}

func TestWaitAndSMSRoundTrip(t *testing.T) {
	text := `{"checkpoint":"draft submitted","send":[{"id":"sent-1","from":{"project":"p","role":"writer","version":"v1","instance":"main"},"to":{"project":"p","role":"reviewer","version":"v1","instance":"main"},"correlation":"draft-v1","body":"artifact:doc-v1"}],"wait":{"from":{"project":"p","role":"reviewer","version":"v1","instance":"main"},"correlation":"review-v1"},"text":""}`
	tr, err := decode(text)
	if err != nil || tr.Wait == nil || tr.Wait.Correlation != "review-v1" || len(tr.Send) != 1 || tr.Send[0].Body != "artifact:doc-v1" {
		t.Fatalf("%+v %v", tr, err)
	}
}

func TestMalformedTransitionsRejected(t *testing.T) {
	for _, text := range []string{
		`null`, `{}`, `{"text":"done"}`,
		`{"checkpoint":"","send":[],"wait":null,"text":"done","extra":true}`,
		`{"checkpoint":"","send":[],"wait":null,"text":"done"} {}`,
		`{"checkpoint":"","send":[],"wait":null,"text":"done"} trailing`,
		`{"checkpoint":null,"send":[],"wait":null,"text":"done"}`,
		`{"checkpoint":"","send":null,"wait":null,"text":"done"}`,
		`{"checkpoint":"x","send":[],"wait":{"from":{"project":"p","role":"r","version":"v1","instance":"main","tool":"bad"},"correlation":"c"},"text":""}`,
		strings.Repeat("x", (256<<10)+1),
	} {
		if tr, err := decode(text); err == nil {
			t.Errorf("accepted invalid transition %+v", tr)
		}
	}
}

func TestInputLimitBeforeExecutor(t *testing.T) {
	e := execution()
	e.Checkpoint = strings.Repeat("x", 64<<10)
	_, err := step(context.Background(), e, func(context.Context, engine.Task) (engine.Result, error) {
		t.Fatal("called executor for oversized envelope")
		return engine.Result{}, nil
	})
	if err == nil {
		t.Fatal("missing bound")
	}
}

func TestCancellationAndExecutorFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := step(ctx, execution(), func(context.Context, engine.Task) (engine.Result, error) {
		t.Fatal("executed canceled step")
		return engine.Result{}, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	want := errors.New("executor failed")
	if _, err := step(context.Background(), execution(), func(context.Context, engine.Task) (engine.Result, error) { return engine.Result{}, want }); !errors.Is(err, want) {
		t.Fatal(err)
	}
}

func TestSchemaCopiesAreIndependent(t *testing.T) {
	a := TransitionSchema()
	a[0] = '!'
	if !json.Valid(TransitionSchema()) {
		t.Fatal("schema shared mutable storage")
	}
}
