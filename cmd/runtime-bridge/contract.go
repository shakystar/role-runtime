package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/shakystar/role-runtime/internal/engine"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)
var traceParent = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

func decodeRequest(data []byte, legacy bool) (request, error) {
	var req request
	// Reject duplicate top-level keys rather than silently taking the last value.
	dec := json.NewDecoder(bytes.NewReader(data))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return req, errors.New("JSON object required")
	}
	seen := map[string]bool{}
	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return req, err
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return req, errors.New("duplicate field")
		}
		switch key {
		case "schema_version", "run_id", "attempt_id", "id", "role", "instruction", "input", "traceparent":
		default:
			return req, errors.New("unknown field")
		}
		seen[key] = true
		var value json.RawMessage
		if err = dec.Decode(&value); err != nil {
			return req, err
		}
	}
	if _, err = dec.Token(); err != nil {
		return req, err
	}
	var extra any
	if err = dec.Decode(&extra); err != io.EOF {
		return req, errors.New("one JSON object required")
	}
	dec = json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&req); err != nil {
		return req, err
	}
	if req.SchemaVersion == 0 && legacy && !seen["schema_version"] {
		if len(seen) != 4 {
			return req, errors.New("legacy context fields forbidden")
		}
		return req, validateTask(req)
	}
	if !seen["schema_version"] || req.SchemaVersion != 1 {
		return req, errors.New("schema_version 1 required")
	}
	return req, validateV1(req)
}
func validateTask(req request) error {
	if !identifier.MatchString(req.ID) || !identifier.MatchString(req.Role) || req.Instruction == "" || len(req.Instruction) > 16<<10 {
		return errors.New("invalid task definition")
	}
	return (engine.Task{ID: req.ID, Project: "bridge", Role: req.Role, Version: "v1", Instance: "main", Input: req.Input}).Validate()
}
func validateV1(req request) error {
	if req.SchemaVersion != 1 || !identifier.MatchString(req.RunID) || !identifier.MatchString(req.AttemptID) {
		return errors.New("invalid v1 identity")
	}
	if err := validateTask(req); err != nil {
		return err
	}
	if !traceParent.MatchString(req.Traceparent) {
		return errors.New("invalid traceparent")
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": req.Traceparent})
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return errors.New("invalid trace context")
	}
	return nil
}
func durableWrite(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".bridge-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func bindProtocol(store *engine.Store, req request) error {
	mode := struct {
		Version int    `json:"version"`
		Run     string `json:"run_id"`
		TraceID string `json:"trace_id"`
	}{req.SchemaVersion, req.RunID, ""}
	if req.SchemaVersion == 1 {
		mode.TraceID = req.Traceparent[3:35]
	}
	path := filepath.Join(store.Root, "bridge-protocol.json")
	b, err := os.ReadFile(path)
	if err == nil {
		expected, _ := json.Marshal(mode)
		if !bytes.Equal(b, expected) {
			return errors.New("protocol/run binding conflict")
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	// Legacy state requires explicit legacy mode; never silently reinterpret receipts.
	if req.SchemaVersion == 1 {
		entries, e := os.ReadDir(filepath.Join(store.Root, "tasks"))
		if e != nil {
			return e
		}
		if len(entries) > 0 {
			return errors.New("legacy receipts require legacy mode or a new state directory")
		}
	}
	return durableWrite(path, mode)
}
func bindAttempt(store *engine.Store, req request) error {
	dir := filepath.Join(store.Root, "bridge-attempts")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	attemptPath := filepath.Join(dir, "attempt-"+engine.Digest(req.AttemptID)+".json")
	existing, attemptErr := os.ReadFile(attemptPath)
	encodedTask, _ := json.Marshal(req.ID)
	if attemptErr == nil && !bytes.Equal(existing, encodedTask) {
		return errors.New("attempt ID belongs to another task")
	}
	if attemptErr != nil && !os.IsNotExist(attemptErr) {
		return attemptErr
	}
	if os.IsNotExist(attemptErr) {
		if err := durableWrite(attemptPath, req.ID); err != nil {
			return err
		}
	}
	path := filepath.Join(dir, engine.Digest(req.ID)+".json")
	// Trace parent changes for each delivery, but logical execution identity does not.
	logical := req
	logical.Traceparent = ""
	expected, _ := json.Marshal(logical)
	b, err := os.ReadFile(path)
	if err == nil {
		if !bytes.Equal(b, expected) {
			return errors.New("task or attempt identity conflict")
		}
		if _, err = store.Receipt(req.ID); err != nil {
			return errors.New("attempt intent exists without a complete receipt; explicit recovery required")
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	if _, err = store.Receipt(req.ID); !os.IsNotExist(err) {
		return errors.New("receipt without matching attempt intent")
	}
	return durableWrite(path, logical)
}

type tracedExecutor struct {
	engine.Executor
	tracer trace.Tracer
	req    request
}

func (e tracedExecutor) Execute(ctx context.Context, task engine.Task) (engine.Result, error) {
	ctx, span := e.tracer.Start(ctx, "runtime.execute", trace.WithAttributes(attribute.String("run_id", e.req.RunID), attribute.String("task_id", task.ID), attribute.String("attempt_id", e.req.AttemptID)))
	defer span.End()
	result, err := e.Executor.Execute(ctx, task)
	if err != nil {
		span.SetStatus(codes.Error, "execution_failed")
	} else {
		span.SetStatus(codes.Ok, "")
	}
	return result, err
}
func handle(ctx context.Context, store *engine.Store, executor engine.Executor, tracer trace.Tracer, req request) (out response) {
	out = response{SchemaVersion: 1, ID: req.ID, RunID: req.RunID, AttemptID: req.AttemptID}
	if err := validateV1(req); err != nil {
		out.Status = "error"
		out.ErrorCode = "invalid_request"
		out.Error = "invalid v1 request"
		return
	}
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": req.Traceparent})
	ctx, span := tracer.Start(ctx, "runtime.request", trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attribute.String("run_id", req.RunID), attribute.String("task_id", req.ID), attribute.String("attempt_id", req.AttemptID)))
	defer span.End()
	fail := func(code string) {
		out.Status = "error"
		out.ErrorCode = code
		out.Error = code
		span.SetStatus(codes.Error, code)
	}
	if err := bindProtocol(store, req); err != nil {
		fail("conflict")
		return
	}
	if err := bindAttempt(store, req); err != nil {
		fail("conflict")
		return
	}
	result, err := execute(ctx, store, tracedExecutor{executor, tracer, req}, req)
	out.Status, out.Text, out.Cached, out.Stats = result.Status, result.Text, result.Cached, result.Stats
	if err != nil {
		fail("execution_blocked")
		return
	}
	if result.Status != "succeeded" {
		fail("execution_failed")
		return
	}
	span.SetAttributes(attribute.Bool("cached", out.Cached))
	if out.Cached {
		span.AddEvent("receipt_cache_hit")
	}
	span.SetStatus(codes.Ok, "")
	return
}

// Keep decode errors free of request bodies, which can contain private prompts.
func invalidResponse(req request) response {
	return response{SchemaVersion: 1, ID: req.ID, RunID: req.RunID, AttemptID: req.AttemptID, Status: "error", ErrorCode: "invalid_request", Error: fmt.Sprint("request rejected by bridge v1 contract")}
}
