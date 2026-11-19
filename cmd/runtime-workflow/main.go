// runtime-workflow hosts the control API and a bounded cooperative fixture pipeline.
// The fixture backend is explicit; no model or business outcome is simulated as real.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/shakystar/role-runtime/internal/control"
	"github.com/shakystar/role-runtime/internal/engine"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

type fixture struct {
	root, mode string
	delay      time.Duration
}

func (f fixture) Close() error                        { return nil }
func (f fixture) Heartbeat(ctx context.Context) error { return ctx.Err() }
func (f fixture) Step(ctx context.Context, x engine.Execution) (engine.Transition, error) {
	if x.Task.Role != "producer" {
		return engine.Transition{Text: "synthetic consumer completed"}, nil
	}
	data := []byte(`{"synthetic":true,"document":"ready"}`)
	hash := sha256.Sum256(data)
	ref := control.Reference{Producer: x.Task.ID, Artifact: control.Artifact{ID: "document.json", Version: "v1", SHA256: hex.EncodeToString(hash[:])}}
	body, _ := json.Marshal(ref)
	to := engine.Address{Project: x.Task.Project, Role: "consumer", Version: "v1", Instance: "main"}
	m := engine.Message{ID: "document-ready", From: engine.AddressOf(x.Task), To: to, Correlation: "document-v1", Body: string(body)}
	select {
	case <-ctx.Done():
		return engine.Transition{}, ctx.Err()
	case <-time.After(f.delay):
	}
	if f.mode == "failed" {
		return engine.Transition{}, errors.New("synthetic producer failure")
	}
	if f.mode != "missing" {
		if e := os.MkdirAll(filepath.Join(f.root, "artifacts"), 0700); e != nil {
			return engine.Transition{}, e
		}
		if f.mode == "corrupt" {
			data = []byte("changed")
		}
		if e := os.WriteFile(filepath.Join(f.root, "artifacts", ref.Artifact.ID), data, 0600); e != nil {
			return engine.Transition{}, e
		}
	}
	return engine.Transition{Text: string(body), Send: []engine.Message{m}}, nil
}
func run() error {
	root := flag.String("state", "", "owned state directory")
	socket := flag.String("socket", "", "local Unix socket")
	runID := flag.String("run-id", "workflow-v1", "immutable run")
	delay := flag.Duration("fixture-delay", 200*time.Millisecond, "synthetic producer duration")
	mode := flag.String("fixture-mode", "normal", "normal, early, wrong, missing, corrupt, failed")
	workers := flag.Int("workers", 2, "bounded fixture workers")
	limit := flag.Int("ledger-limit", 0, "lifecycle bytes (zero defaults 64 MiB)")
	flag.Parse()
	if *root == "" || *delay < 0 || *delay > time.Minute {
		return errors.New("invalid fixture configuration")
	}
	switch *mode {
	case "normal", "early", "wrong", "missing", "corrupt", "failed":
	default:
		return errors.New("invalid fixture mode")
	}
	s, e := engine.OpenStore(*root)
	if e != nil {
		return e
	}
	defer s.Close()
	if e = s.EnableLifecycle(*runID, *limit); e != nil {
		return e
	}
	producer := engine.Task{ID: "producer", Project: "pipeline", Role: "producer", Version: "v1", Instance: "main", Input: "synthetic"}
	if e = s.SubmitCooperative(producer); e != nil && !errors.Is(e, engine.ErrDuplicate) {
		return e
	}
	consumer := engine.Address{Project: "pipeline", Role: "consumer", Version: "v1", Instance: "main"}
	if *mode == "early" || *mode == "wrong" || *mode == "failed" {
		data := []byte(`{"synthetic":true,"document":"ready"}`)
		hash := sha256.Sum256(data)
		body, _ := json.Marshal(control.Reference{Producer: "producer", Artifact: control.Artifact{ID: "document.json", Version: "v1", SHA256: hex.EncodeToString(hash[:])}})
		correlation := "document-v1"
		if *mode == "wrong" {
			correlation = "wrong-version"
		}
		if e = s.Deliver(engine.Message{ID: "document-ready", From: engine.AddressOf(producer), To: consumer, Correlation: correlation, Body: string(body)}); e != nil {
			return e
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	server := &control.Server{Store: s, Socket: *socket, Config: control.Config{RunID: *runID, Project: "pipeline", Consumers: map[string]engine.Address{"graph": consumer}}}
	if e = server.Start(ctx); e != nil {
		return e
	}
	defer server.Close()
	json.NewEncoder(os.Stdout).Encode(map[string]any{"socket": server.Socket, "run_id": *runID, "backend": "fixture"})
	done := make(chan error, 1)
	go func() {
		_, e := (engine.CooperativePool{Workers: *workers, Store: s, Factory: func(int) (engine.StepExecutor, error) { return fixture{s.Root, *mode, *delay}, nil }, PollInterval: 10 * time.Millisecond}).Run(ctx)
		done <- e
	}()
	select {
	case e = <-done:
		if e != nil && !errors.Is(e, context.Canceled) {
			fmt.Fprintln(os.Stderr, "fixture worker stopped; control remains available")
		}
		<-ctx.Done()
		return nil
	case <-ctx.Done():
		e = <-done
		if errors.Is(e, context.Canceled) {
			return nil
		}
		return e
	}
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
