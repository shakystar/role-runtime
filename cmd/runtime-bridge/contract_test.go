package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shakystar/role-runtime/internal/engine"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func v1Request() request {
	return request{SchemaVersion: 1, RunID: "run-1", AttemptID: "attempt-1", ID: "task-1", Role: "reviewer", Instruction: "Review", Input: `{"fixture":{"ok":true}}`, Traceparent: "00-" + strings.Repeat("1", 32) + "-" + strings.Repeat("2", 16) + "-03"}
}
func TestWireValidation(t *testing.T) {
	req := v1Request()
	b, _ := json.Marshal(req)
	if _, err := decodeRequest(b, false); err != nil {
		t.Fatal(err)
	}
	bad := []string{
		strings.Replace(string(b), `"schema_version":1`, `"schema_version":true`, 1),
		strings.Replace(string(b), `"schema_version":1`, `"schema_version":2`, 1),
		strings.Replace(string(b), `"id":"task-1"`, `"id":".invalid"`, 1),
		strings.Replace(string(b), `"run_id":"run-1"`, `"run_id":null`, 1),
		strings.Replace(string(b), strings.Repeat("1", 32), strings.Repeat("0", 32), 1),
		strings.Replace(string(b), `"id":"task-1"`, `"id":"task-1","id":"other"`, 1),
		string(b[:len(b)-1]) + `,"unknown":true}`,
		string(b) + ` {}`,
	}
	for _, s := range bad {
		if _, err := decodeRequest([]byte(s), false); err == nil {
			t.Fatalf("accepted invalid request: %s", s)
		}
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(b, &fields)
	for k := range fields {
		m := map[string]json.RawMessage{}
		for key, v := range fields {
			if key != k {
				m[key] = v
			}
		}
		data, _ := json.Marshal(m)
		if _, err := decodeRequest(data, false); err == nil {
			t.Fatalf("accepted missing %s", k)
		}
	}
	old := []byte(`{"id":"t","role":"r","instruction":"i","input":"x"}`)
	if _, err := decodeRequest(old, false); err == nil {
		t.Fatal("implicit legacy accepted")
	}
	if _, err := decodeRequest(old, true); err != nil {
		t.Fatal(err)
	}
}
func setupStore(t *testing.T) *engine.Store {
	t.Helper()
	root := t.TempDir()
	s, err := engine.OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RegisterProject(engine.Project{ID: "bridge", Workspace: root}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func TestV1TraceCacheAndIdentity(t *testing.T) {
	store := setupStore(t)
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	req := v1Request()
	e := &counted{}
	first := handle(context.Background(), store, e, tp.Tracer("test"), req)
	if first.Status != "succeeded" || first.Cached || first.AttemptID != req.AttemptID {
		t.Fatalf("%+v", first)
	}
	req.Traceparent = "00-" + strings.Repeat("1", 32) + "-" + strings.Repeat("3", 16) + "-01"
	second := handle(context.Background(), store, e, tp.Tracer("test"), req)
	if !second.Cached || second.Stats.Submitted != 0 || e.calls != 1 {
		t.Fatalf("%+v", second)
	}
	spans := recorder.Ended()
	if len(spans) != 3 {
		t.Fatalf("spans=%d", len(spans))
	}
	var requestID string
	executions := 0
	for _, s := range spans {
		if s.SpanContext().TraceID().String() != strings.Repeat("1", 32) {
			t.Fatal("trace changed")
		}
		if s.Name() == "runtime.request" && s.Parent().SpanID().String() == strings.Repeat("2", 16) {
			requestID = s.SpanContext().SpanID().String()
		}
		if s.Name() == "runtime.execute" {
			executions++
		}
	}
	if executions != 1 || requestID == "" {
		t.Fatal("missing request/execution")
	}
	for _, s := range spans {
		if s.Name() == "runtime.execute" && s.Parent().SpanID().String() != requestID {
			t.Fatal("execution parent mismatch")
		}
	}
	collision := v1Request()
	collision.ID = "another-task"
	if result := handle(context.Background(), store, e, tp.Tracer("test"), collision); result.Status != "error" {
		t.Fatal("attempt reused for another task")
	}
	collision = v1Request()
	collision.Traceparent = "00-" + strings.Repeat("4", 32) + "-" + strings.Repeat("2", 16) + "-01"
	if result := handle(context.Background(), store, e, tp.Tracer("test"), collision); result.Status != "error" {
		t.Fatal("run trace changed")
	}
	req.AttemptID = "different"
	if result := handle(context.Background(), store, e, tp.Tracer("test"), req); result.Status != "error" {
		t.Fatal("changed attempt accepted")
	}
	req = v1Request()
	req.RunID = "different"
	if result := handle(context.Background(), store, e, tp.Tracer("test"), req); result.Status != "error" {
		t.Fatal("changed run accepted")
	}
	if e.calls != 1 {
		t.Fatal("duplicate execution")
	}
}

type failedExecutor struct{}

func (failedExecutor) Execute(context.Context, engine.Task) (engine.Result, error) {
	return engine.Result{}, errors.New("private failure detail")
}
func (failedExecutor) Close() error { return nil }
func TestFailedAndUncertainAttemptNotReplayed(t *testing.T) {
	store := setupStore(t)
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	defer tp.Shutdown(context.Background())
	req := v1Request()
	result := handle(context.Background(), store, failedExecutor{}, tp.Tracer("test"), req)
	if result.ErrorCode != "execution_failed" || strings.Contains(result.Error, "private") {
		t.Fatalf("%+v", result)
	}
	e := &counted{}
	if result = handle(context.Background(), store, e, tp.Tracer("test"), req); result.Status != "error" || e.calls != 0 {
		t.Fatal("failed work replayed")
	}
	for _, span := range rec.Ended() {
		if span.Status().Code != codes.Error {
			t.Fatal("failure not observed")
		}
	}
	req.ID = "uncertain"
	req.AttemptID = "uncertain-attempt"
	if err := bindAttempt(store, req); err != nil {
		t.Fatal(err)
	}
	if result = handle(context.Background(), store, e, tp.Tracer("test"), req); result.Status != "error" || e.calls != 0 {
		t.Fatal("intent-only work replayed")
	}
}
func TestLocalTelemetryFailureDoesNotChangeResult(t *testing.T) {
	store := setupStore(t)
	// Make the local observation path unwritable as a file. Execution still succeeds.
	if err := os.Mkdir(filepath.Join(store.Root, "bridge-spans.jsonl"), 0700); err != nil {
		t.Fatal(err)
	}
	telemetry := newTelemetry(store.Root)
	result := handle(context.Background(), store, &counted{}, telemetry.provider.Tracer("test"), v1Request())
	telemetry.close()
	if result.Status != "succeeded" || telemetry.exporter.localFailed != 2 {
		t.Fatalf("result=%+v lost=%d", result, telemetry.exporter.localFailed)
	}
}

func TestSharedContractVectors(t *testing.T) {
	b, err := os.ReadFile("testdata/v1-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name  string `json:"name"`
		Wire  string `json:"wire"`
		Valid bool   `json:"valid"`
	}
	if err = json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			_, err := decodeRequest([]byte(c.Wire), false)
			if (err == nil) != c.Valid {
				t.Fatalf("valid=%v err=%v", c.Valid, err)
			}
		})
	}
}

func TestEmptyTextFieldAndLegacyIsolation(t *testing.T) {
	out := response{SchemaVersion: 1, Status: "succeeded", Text: ""}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"text":""`) {
		t.Fatal("required result text omitted")
	}
	store := setupStore(t)
	legacy := request{ID: "old", Role: "r", Instruction: "i", Input: "x"}
	if err = bindProtocol(store, legacy); err != nil {
		t.Fatal(err)
	}
	if err = bindProtocol(store, v1Request()); err == nil {
		t.Fatal("v1 mixed with legacy state")
	}
}

// A typed nil returned with an initialization error must never be retained.
func TestLazyInitializationFailureClose(t *testing.T) {
	l := &lazyExecutor{factory: func() (engine.Executor, error) {
		var e *counted
		return e, errors.New("initialization failed")
	}}
	if _, err := l.Execute(context.Background(), engine.Task{}); err == nil {
		t.Fatal("missing initialization error")
	}
	if l.inner != nil {
		t.Fatal("failed executor retained")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}
