// runtime-bridge is an experimental sequential JSONL boundary for external planners.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/shakystar/codex-adapter/rpc"
	"github.com/shakystar/role-runtime/internal/codexadapter"
	"github.com/shakystar/role-runtime/internal/engine"
)

type request struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	AttemptID     string `json:"attempt_id"`
	Traceparent   string `json:"traceparent"`
	ID            string `json:"id"`
	Role          string `json:"role"`
	Instruction   string `json:"instruction"`
	Input         string `json:"input"`
}
type response struct {
	SchemaVersion int          `json:"schema_version"`
	RunID         string       `json:"run_id"`
	AttemptID     string       `json:"attempt_id"`
	ErrorCode     string       `json:"error_code,omitempty"`
	ID            string       `json:"id"`
	Status        string       `json:"status"`
	Text          string       `json:"text"`
	Cached        bool         `json:"cached"`
	Stats         engine.Stats `json:"stats"`
	Error         string       `json:"error,omitempty"`
}

// Pool owns scheduling, but the bridge owns the executor across sequential submissions.
type borrowed struct{ engine.Executor }

func (borrowed) Close() error { return nil }

// lazyExecutor avoids starting/authenticating Codex when only cached receipts are read.
// The sequential bridge never calls it concurrently.
type lazyExecutor struct {
	factory func() (engine.Executor, error)
	inner   engine.Executor
}

func (l *lazyExecutor) Execute(ctx context.Context, task engine.Task) (engine.Result, error) {
	if l.inner == nil {
		inner, err := l.factory()
		if err != nil {
			return engine.Result{}, err
		}
		l.inner = inner
	}
	return l.inner.Execute(ctx, task)
}
func (l *lazyExecutor) Close() error {
	if l.inner != nil {
		return l.inner.Close()
	}
	return nil
}

type fixture struct {
	store *engine.Store
}

func (fixture) Close() error { return nil }
func (f fixture) Execute(ctx context.Context, t engine.Task) (engine.Result, error) {
	var input struct {
		Fixture json.RawMessage `json:"fixture"`
		Text    *string         `json:"fixture_text"`
	}
	if err := json.Unmarshal([]byte(t.Input), &input); err != nil {
		return engine.Result{}, err
	}
	var output string
	switch {
	case input.Text != nil:
		if len(input.Fixture) != 0 {
			return engine.Result{}, errors.New("ambiguous fixture output")
		}
		output = *input.Text
	case len(input.Fixture) == 0:
		return engine.Result{}, errors.New("fixture field required in fixture backend")
	default:
		output = string(input.Fixture)
	}
	activation, err := f.store.ResolveDefinitions(ctx, t)
	if err != nil {
		return engine.Result{}, err
	}
	return engine.Result{
		Text:              output,
		ActualMemberID:    activation.Role.ID,
		ActualRoleVersion: activation.Role.Version,
	}, nil
}
func execute(ctx context.Context, store *engine.Store, executor engine.Executor, req request) (response, error) {
	out := response{ID: req.ID}
	role := engine.Role{ID: req.Role, Version: "v1", Instruction: req.Instruction, Tools: []string{"advisory"}}
	if err := store.RegisterRole(role); err != nil {
		return out, err
	}
	task := engine.Task{ID: req.ID, Project: "bridge", Role: req.Role, Version: "v1", Instance: "main", Input: req.Input}
	if err := task.Validate(); err != nil {
		return out, err
	}
	prior, err := store.Receipt(task.ID)
	if err == nil {
		if prior.Task != task {
			return out, errors.New("task ID reused with different input")
		}
		if prior.Status != "succeeded" {
			return out, errors.New("prior task not successful; explicit recovery required")
		}
		out.Status, out.Text, out.Cached = prior.Status, prior.Result.Text, true
		return out, nil
	}
	if !os.IsNotExist(err) {
		return out, err
	}
	ch := make(chan engine.Task, 1)
	ch <- task
	close(ch)
	pool := engine.Pool{Workers: 1, QueueSize: 1, Store: store, Factory: func(int) (engine.Executor, error) { return borrowed{executor}, nil }}
	out.Stats, err = pool.Run(ctx, ch, func(o engine.Outcome) error {
		out.Status, out.Text, out.Error = o.Status, o.Result.Text, o.Error
		return nil
	})
	return out, err
}
func run() error {
	state := flag.String("state", "", "persistent runtime directory")
	backend := flag.String("backend", "fixture", "fixture or codex")
	binary := flag.String("codex", "codex", "Codex executable")
	model := flag.String("model", "", "Codex model: gpt-6.2-sol, gpt-6.2-astra, or experimental gpt-6-luna (requires --reasoning-effort=high)")
	reasoningEffort := flag.String("reasoning-effort", "", "Codex reasoning effort: low, medium, or high")
	legacy := flag.Bool("allow-legacy", false, "explicitly accept unversioned requests in an isolated state")
	inspect := flag.String("inspect-attempt", "", "read one existing attempt without submitting work")
	runID := flag.String("run-id", "", "expected owner of the inspected attempt")
	capabilities := flag.Bool("capabilities", false, "print supported bridge interfaces")
	flag.Parse()
	if *capabilities {
		return json.NewEncoder(os.Stdout).Encode(map[string]int{"bridge": 1, "inspect_attempt": 1, "fixture_text": 1})
	}
	var configErr error
	flag.Visit(func(f *flag.Flag) {
		if configErr != nil {
			return
		}
		switch f.Name {
		case "model":
			if *model != "gpt-6.2-sol" && *model != "gpt-6.2-astra" && *model != "gpt-6-luna" {
				configErr = errors.New("--model must be gpt-6.2-sol or gpt-6.2-astra")
			}
		case "reasoning-effort":
			if *reasoningEffort != "low" && *reasoningEffort != "medium" && *reasoningEffort != "high" {
				configErr = errors.New("--reasoning-effort must be low, medium, or high")
			}
		}
	})
	if configErr != nil {
		return configErr
	}
	if *model == "gpt-6-luna" && *reasoningEffort != "high" {
		return errors.New("--model=gpt-6-luna requires --reasoning-effort=high")
	}
	if *inspect != "" || *runID != "" {
		return inspectAttempt(*state, *runID, *inspect, os.Stdout)
	}
	if *state == "" || (*backend != "fixture" && *backend != "codex") {
		return errors.New("--state and valid --backend required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	store, err := engine.OpenStore(*state)
	if err != nil {
		return err
	}
	defer store.Close()
	workspace := filepath.Join(store.Root, "workspace")
	if err = os.MkdirAll(workspace, 0700); err != nil {
		return err
	}
	if err = store.RegisterProject(engine.Project{ID: "bridge", Workspace: workspace, Rules: "Integration pilot. Backend=" + *backend + ". Use only supplied synthetic evidence. Do not call tools or modify files. Return only the requested JSON."}); err != nil {
		return err
	}
	var executor engine.Executor = fixture{store: store}
	if *backend == "codex" {
		executor = &lazyExecutor{factory: func() (engine.Executor, error) {
			return codexadapter.New(codexadapter.Config{Store: store, Model: *model, ReasoningEffort: *reasoningEffort, RPC: codexrpc.Config{Command: *binary, Dir: workspace}, StartupTimeout: 30 * time.Second, HeartbeatInterval: 15 * time.Second, HeartbeatTimeout: 10 * time.Second, OnStatus: func(s codexadapter.Status) { _ = json.NewEncoder(os.Stderr).Encode(s) }})
		}}
	}
	defer executor.Close()
	telemetry := newTelemetry(store.Root)
	defer telemetry.close()
	tracer := telemetry.provider.Tracer("role-runtime.bridge.v1")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 128<<10)
	enc := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		req, decodeErr := decodeRequest(scanner.Bytes(), *legacy)
		var out response
		if decodeErr != nil {
			out = invalidResponse(req)
		} else if req.SchemaVersion == 0 {
			if bindErr := bindProtocol(store, req); bindErr != nil {
				out = invalidResponse(req)
			} else {
				out, err = execute(ctx, store, executor, req)
				if err != nil {
					out.Status = "error"
					out.Error = "legacy execution blocked"
				}
			}
		} else {
			out = handle(ctx, store, executor, tracer, req)
		}
		if err = enc.Encode(out); err != nil {
			return err
		}
	}
	return scanner.Err()
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
