// runtime-bridge is an experimental sequential JSONL boundary for external planners.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/shakystar/codex-adapter/rpc"
	"github.com/shakystar/role-runtime/internal/codexadapter"
	"github.com/shakystar/role-runtime/internal/engine"
)

type request struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Instruction string `json:"instruction"`
	Input       string `json:"input"`
}
type response struct {
	ID     string       `json:"id"`
	Status string       `json:"status"`
	Text   string       `json:"text,omitempty"`
	Cached bool         `json:"cached"`
	Stats  engine.Stats `json:"stats"`
	Error  string       `json:"error,omitempty"`
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
		var err error
		l.inner, err = l.factory()
		if err != nil {
			return engine.Result{}, err
		}
	}
	return l.inner.Execute(ctx, task)
}
func (l *lazyExecutor) Close() error {
	if l.inner != nil {
		return l.inner.Close()
	}
	return nil
}

type fixture struct{}

func (fixture) Close() error { return nil }
func (fixture) Execute(_ context.Context, t engine.Task) (engine.Result, error) {
	var input struct {
		Fixture json.RawMessage `json:"fixture"`
	}
	if err := json.Unmarshal([]byte(t.Input), &input); err != nil {
		return engine.Result{}, err
	}
	if len(input.Fixture) == 0 {
		return engine.Result{}, errors.New("fixture field required in fixture backend")
	}
	return engine.Result{Text: string(input.Fixture)}, nil
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
	flag.Parse()
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
	var executor engine.Executor = fixture{}
	if *backend == "codex" {
		executor = &lazyExecutor{factory: func() (engine.Executor, error) {
			return codexadapter.New(codexadapter.Config{Store: store, RPC: codexrpc.Config{Command: *binary, Dir: workspace}, StartupTimeout: 30 * time.Second, HeartbeatInterval: 15 * time.Second, HeartbeatTimeout: 10 * time.Second, OnStatus: func(s codexadapter.Status) { _ = json.NewEncoder(os.Stderr).Encode(s) }})
		}}
	}
	defer executor.Close()
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 128<<10)
	enc := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var req request
		dec := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		dec.DisallowUnknownFields()
		if err = dec.Decode(&req); err != nil {
			return err
		}
		var extra any
		if err = dec.Decode(&extra); err != io.EOF {
			return errors.New("one JSON object per line required")
		}
		out, e := execute(ctx, store, executor, req)
		if e != nil {
			out.Status = "error"
			out.Error = e.Error()
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
