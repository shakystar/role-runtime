// runtime-workflow hosts the control API and a bounded cooperative document pipeline.
// The fixture backend is explicit; no model or business outcome is simulated as real.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	sdk "github.com/shakystar/codex-adapter"
	rpc "github.com/shakystar/codex-adapter/rpc"
	"github.com/shakystar/role-runtime/internal/control"
	"github.com/shakystar/role-runtime/internal/engine"
	"github.com/shakystar/role-runtime/internal/lifecycle"
	"github.com/shakystar/role-runtime/internal/workflowmodel"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
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
	backend := flag.String("backend", "fixture", "fixture or codex")
	codex := flag.String("codex", "codex", "local Codex executable")
	root := flag.String("state", "", "owned state directory")
	socket := flag.String("socket", "", "local Unix socket")
	runID := flag.String("run-id", "workflow-v1", "immutable run")
	delay := flag.Duration("fixture-delay", 200*time.Millisecond, "synthetic producer duration")
	mode := flag.String("fixture-mode", "normal", "normal, early, wrong, missing, corrupt, failed")
	workers := flag.Int("workers", 2, "bounded execution workers")
	limit := flag.Int("ledger-limit", 0, "lifecycle bytes (zero defaults 64 MiB)")
	contractPath := flag.String("contract", "", "optional v1 work contract for the model-backed handoff")
	flag.Parse()
	if *root == "" || *delay < 0 || *delay > time.Minute || (*backend != "fixture" && *backend != "codex") || (*backend == "codex" && *mode != "normal") || *workers < 1 || *workers > 64 {
		return errors.New("invalid workflow configuration")
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
	binding := map[string]string{"backend": *backend, "codex": *codex, "brief": engine.Digest(workflowmodel.Brief, workflowmodel.WriterInstruction, workflowmodel.ReviewerInstruction)}
	var contract *workflowmodel.WorkContract
	var contractJSON []byte
	if *contractPath != "" {
		if *backend != "codex" {
			return errors.New("work contract requires codex backend")
		}
		contract, e = workflowmodel.LoadContract(*contractPath)
		if e != nil {
			return e
		}
		contractJSON, _ = json.Marshal(contract)
		binding["contract"] = engine.Digest(string(contractJSON), workflowmodel.ProposalInstruction)
	}
	b, _ := json.Marshal(binding)
	bindingPath := filepath.Join(s.Root, "workflow-backend.json")
	old, bindingErr := os.ReadFile(bindingPath)
	if bindingErr == nil && !bytes.Equal(old, b) {
		return errors.New("backend configuration changed; use a new state")
	}
	if bindingErr != nil && !os.IsNotExist(bindingErr) {
		return bindingErr
	}
	if os.IsNotExist(bindingErr) {
		entries, readErr := os.ReadDir(filepath.Join(s.Root, "cooperation"))
		if readErr != nil {
			return readErr
		}
		if *backend == "codex" && len(entries) > 0 {
			return errors.New("legacy workflow requires a new state for codex")
		}
		if e = lifecycle.Write(bindingPath, b); e != nil {
			return e
		}
	}
	if e = os.MkdirAll(filepath.Join(s.Root, "workspace"), 0700); e != nil {
		return e
	}
	statusFile, e := os.OpenFile(filepath.Join(s.Root, "model-status.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer statusFile.Close()
	var statusMu sync.Mutex
	reportStatus := func(status sdk.Status) {
		statusMu.Lock()
		defer statusMu.Unlock()
		data, _ := json.Marshal(status)
		info, err := statusFile.Stat()
		if err == nil && info.Size()+int64(len(data)+1) <= 8<<20 {
			statusFile.Write(append(data, '\n'))
		}
	}

	if e = s.EnableLifecycle(*runID, *limit); e != nil {
		return e
	}
	var telemetry *workflowmodel.Telemetry
	if *backend == "codex" {
		telemetry, e = workflowmodel.NewTelemetry(s.Root)
		if e != nil {
			return e
		}
		defer telemetry.Close()
	}
	producer := engine.Task{ID: "producer", Project: "pipeline", Role: "producer", Version: "v1", Instance: "main", Input: "synthetic"}
	if *backend == "codex" {
		producer.Input = workflowmodel.Brief
		if contract != nil {
			producer.Input = string(contractJSON)
		}
	}
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
	json.NewEncoder(os.Stdout).Encode(map[string]any{"socket": server.Socket, "run_id": *runID, "backend": *backend})
	done := make(chan error, 1)
	go func() {
		pool := engine.CooperativePool{Workers: *workers, Store: s, Factory: func(int) (engine.StepExecutor, error) {
			if *backend == "fixture" {
				return fixture{s.Root, *mode, *delay}, nil
			}
			return &workflowmodel.Executor{Contract: contract, Store: s, RunID: *runID, Tracer: telemetry.Provider.Tracer("workflow-model"), OnStatus: reportStatus, Factory: func(ctx context.Context, status func(sdk.Status)) (workflowmodel.Model, error) {
				return sdk.NewContext(ctx, sdk.Config{StateDir: s.Root, RPC: rpc.Config{Command: *codex, Dir: filepath.Join(s.Root, "workspace")}, OnStatus: status, HeartbeatInterval: 15 * time.Second, HeartbeatTimeout: 10 * time.Second})
			}}, nil
		}, PollInterval: 10 * time.Millisecond}
		if *backend == "codex" {
			pool.HeartbeatInterval = 15 * time.Second
			pool.HeartbeatTimeout = 10 * time.Second
			pool.LeaseTTL = 45 * time.Second
		}
		_, e := pool.Run(ctx)
		done <- e
	}()
	select {
	case e = <-done:
		if e != nil && !errors.Is(e, context.Canceled) {
			fmt.Fprintln(os.Stderr, "workflow worker stopped; control remains available")
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
