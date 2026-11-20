// Package workflowmodel connects the existing Codex adapter to a bounded document
// handoff. The runtime, never the model, writes artifact paths and SMS addresses.
package workflowmodel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/shakystar/codex-adapter"
	"github.com/shakystar/role-runtime/internal/control"
	"github.com/shakystar/role-runtime/internal/engine"
	"github.com/shakystar/role-runtime/internal/lifecycle"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const Brief = "Write a short technical note for a local document handoff. Requirements: verify completion receipts before consuming a document; use immutable task identities; do not automatically replay uncertain work. No external research or tools."
const WriterInstruction = "Use only the supplied requirements. Return exactly JSON with title and body strings. Explain all requirements briefly. No markdown fences."
const ReviewerInstruction = "Review the supplied document against the requirements independently. Return exactly JSON with verdict (accept or revise), reason (string), and source_sha256 copied from the supplied document hash. Do not assume the document is correct. No tools or markdown fences."

type Model interface {
	Execute(context.Context, sdk.Request) (sdk.Result, error)
	Heartbeat(context.Context) error
	Close() error
}
type Factory func(context.Context, func(sdk.Status)) (Model, error)
type Executor struct {
	Contract *WorkContract
	Tracer   trace.Tracer
	RunID    string
	Store    *engine.Store
	Factory  Factory
	OnStatus func(sdk.Status)
	mu       sync.Mutex
	model    Model
	current  engine.Execution
}

func (e *Executor) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.model == nil {
		return nil
	}
	err := e.model.Close()
	e.model = nil
	return err
}
func (e *Executor) Heartbeat(ctx context.Context) error {
	e.mu.Lock()
	model := e.model
	e.mu.Unlock()
	if model == nil {
		return ctx.Err()
	}
	return model.Heartbeat(ctx)
}

type phaseError struct {
	cause        error
	stage, state string
}

func (e phaseError) Error() string { return "workflow " + e.stage + " failed (" + e.state + ")" }
func (e phaseError) Unwrap() error { return e.cause }
func (e phaseError) ExecutionFailure() (string, string, string, bool) {
	return e.stage, "workflow_failed", e.state, false
}
func (e *Executor) Step(ctx context.Context, x engine.Execution) (transition engine.Transition, err error) {
	tracer := e.Tracer
	if tracer == nil {
		tracer = trace.NewNoopTracerProvider().Tracer("workflow")
	}
	ctx, span := tracer.Start(ctx, "workflow.step", trace.WithAttributes(attribute.String("run_id", e.RunID), attribute.String("task_id", x.Task.ID), attribute.String("role", x.Task.Role)))
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, "step_failed")
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.End()
	}()
	state, stage := "not_started", "dependency"
	defer func() {
		if err != nil && engine.FailureMetadata(err) == nil {
			err = phaseError{err, stage, state}
		}
	}()
	if x.Task.Project != "pipeline" || x.Task.Version != "v1" || x.Task.Instance != "main" {
		return transition, errors.New("unsupported task scope")
	}
	req := sdk.Request{ID: x.Task.ID, Role: x.Task.Role, SessionKey: x.Task.Key(), Workspace: filepath.Join(e.Store.Root, "workspace"), Tools: []string{"advisory"}}
	file := "document.json"
	expectedHash := ""
	switch x.Task.Role {
	case "producer":
		req.Instructions = WriterInstruction
		req.Input = Brief
		if e.Contract != nil {
			b, _ := json.Marshal(e.Contract)
			req.Input = string(b)
		}
	case "consumer":
		record, readErr := e.Store.CooperativeReceipt("producer")
		if readErr != nil || record.Status != "completed" {
			return transition, errors.New("producer not completed")
		}
		var ref control.Reference
		if control.DecodeJSON([]byte(record.Text), &ref) != nil || ref.Producer != "producer" || ref.Artifact.ID != "document.json" || ref.Artifact.Version != "v1" {
			return transition, errors.New("invalid dependency reference")
		}
		root, openErr := os.OpenRoot(filepath.Join(e.Store.Root, "artifacts"))
		if openErr != nil {
			return transition, openErr
		}
		defer root.Close()
		info, readErr := root.Stat(ref.Artifact.ID)
		if readErr != nil || !info.Mode().IsRegular() || info.Size() > 16<<10 {
			return transition, errors.New("invalid dependency size")
		}
		f, readErr := root.Open(ref.Artifact.ID)
		if readErr != nil {
			return transition, readErr
		}
		data, readErr := io.ReadAll(io.LimitReader(f, (16<<10)+1))
		f.Close()
		if readErr != nil || len(data) > 16<<10 {
			return transition, errors.New("dependency exceeds bound")
		}
		hash := sha256.Sum256(data)
		expectedHash = hex.EncodeToString(hash[:])
		if expectedHash != ref.Artifact.SHA256 {
			return transition, errors.New("dependency hash mismatch")
		}
		var requirements any = Brief
		if e.Contract != nil {
			requirements = e.Contract
		}
		input, _ := json.Marshal(map[string]any{"requirements": requirements, "document": json.RawMessage(data), "source_sha256": expectedHash})
		req.Instructions = ReviewerInstruction
		if e.Contract != nil {
			req.Instructions = ProposalInstruction
		}
		req.Input = string(input)
		file = "review.json"
	default:
		return transition, errors.New("unsupported role")
	}
	e.mu.Lock()
	e.current = x
	e.mu.Unlock()

	stage = "initialize"
	e.mu.Lock()
	model := e.model
	e.mu.Unlock()
	if model == nil {
		if e.Factory == nil {
			return transition, errors.New("model factory required")
		}
		model, err = e.Factory(ctx, e.report)
		if err != nil {
			return transition, err
		}
		e.mu.Lock()
		e.model = model
		e.mu.Unlock()
	}

	state, stage = "unknown", "model"
	modelCtx, modelSpan := tracer.Start(ctx, "workflow.model", trace.WithAttributes(attribute.String("task_id", x.Task.ID)))
	result, err := model.Execute(modelCtx, req)
	if err != nil {
		modelSpan.SetStatus(codes.Error, "model_failed")
	} else {
		modelSpan.SetStatus(codes.Ok, "")
	}
	modelSpan.End()
	if err != nil {
		return transition, err
	}
	state, stage = "completed", "output_validation"
	var data []byte
	if x.Task.Role == "producer" {
		var doc struct {
			Title string `json:"title"`
			Body  string `json:"body"`
		}
		if control.DecodeJSON([]byte(result.Text), &doc) != nil || doc.Title == "" || doc.Body == "" {
			return transition, errors.New("invalid writer output")
		}
		data, _ = json.Marshal(doc)
		if e.Contract != nil {
			data, _ = json.Marshal(struct {
				Title    string        `json:"title"`
				Body     string        `json:"body"`
				Contract *WorkContract `json:"task_contract"`
			}{doc.Title, doc.Body, e.Contract})
		}
	} else if e.Contract != nil {
		data, err = e.Contract.recordProposal(result.Text, expectedHash)
		if err != nil {
			return transition, err
		}
	} else {
		var review struct {
			Verdict string `json:"verdict"`
			Reason  string `json:"reason"`
			Hash    string `json:"source_sha256"`
		}
		if control.DecodeJSON([]byte(result.Text), &review) != nil || (review.Verdict != "accept" && review.Verdict != "revise") || review.Reason == "" || review.Hash != expectedHash {
			return transition, errors.New("invalid reviewer output")
		}
		data, _ = json.Marshal(review)
	}
	if len(data) > 16<<10 {
		return transition, errors.New("artifact exceeds limit")
	}
	stage = "artifact_write"
	if err = os.MkdirAll(filepath.Join(e.Store.Root, "artifacts"), 0700); err != nil {
		return transition, err
	}
	path := filepath.Join(e.Store.Root, "artifacts", file)
	if _, statErr := os.Lstat(path); statErr == nil {
		return transition, errors.New("artifact already exists; explicit recovery required")
	} else if !os.IsNotExist(statErr) {
		return transition, statErr
	}
	if err = lifecycle.Write(path, data); err != nil {
		return transition, err
	}
	hash := sha256.Sum256(data)
	ref := control.Reference{Producer: x.Task.ID, Artifact: control.Artifact{ID: file, Version: "v1", SHA256: hex.EncodeToString(hash[:])}}
	body, _ := json.Marshal(ref)
	transition = engine.Transition{Text: string(body), ExecutionState: "completed"}
	if x.Task.Role == "producer" {
		transition.Send = []engine.Message{{ID: "document-ready", From: engine.AddressOf(x.Task), To: engine.Address{Project: "pipeline", Role: "consumer", Version: "v1", Instance: "main"}, Correlation: "document-v1", Body: string(body)}}
	}
	return transition, nil
}

var _ engine.StepExecutor = (*Executor)(nil)

func (e *Executor) report(s sdk.Status) {
	e.mu.Lock()
	x := e.current
	e.mu.Unlock()
	s.TaskID = x.Task.ID
	s.Role = x.Task.Role
	if s.Phase == "startup_retry" && s.Failure != nil {
		if err := e.Store.RecordStartupRetry(x.Task.ID, x.Generation, s.Failure.Attempts); err != nil {
			s.Phase = "retry_record_failed"
		}
	}
	if e.OnStatus != nil {
		e.OnStatus(s)
	}
}
