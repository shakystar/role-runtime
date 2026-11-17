package pispike

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/shakystar/role-runtime/internal/engine"
	"github.com/shakystar/role-runtime/internal/piadapter"
	"github.com/shakystar/role-runtime/internal/pirpc"
)

type CooperationReport struct {
	engine.CooperativeStats
	ProcessesStarted int64 `json:"processesStarted"`
	AllFourWaiting   bool  `json:"allFourWaiting"`
}

// RunCooperation is a deterministic external work supplier: four requests first,
// then four response tasks only after every requester has released its worker.
func RunCooperation(parent context.Context, repository string) (report CooperationReport, err error) {
	return runCooperation(parent, repository, false)
}
func runCooperation(parent context.Context, repository string, restart bool) (report CooperationReport, err error) {
	repo, err := filepath.Abs(repository)
	if err != nil {
		return report, err
	}
	node, err := exec.LookPath("node")
	if err != nil {
		return report, err
	}
	root, err := os.MkdirTemp("", "role-cooperation-")
	if err != nil {
		return report, err
	}
	defer os.RemoveAll(root)
	store, err := engine.OpenStore(root)
	if err != nil {
		return report, err
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	workspace := filepath.Join(root, "workspace")
	if err = os.MkdirAll(workspace, 0700); err != nil {
		return report, err
	}
	if err = store.RegisterProject(engine.Project{ID: "project", Workspace: workspace, Rules: "Synthetic cooperation test"}); err != nil {
		return report, err
	}
	for i := 0; i < 4; i++ {
		for _, kind := range []string{"requester", "responder"} {
			role := fmt.Sprintf("%s-%d", kind, i)
			if err = store.RegisterRole(engine.Role{ID: role, Version: "v1", Instruction: "ROLE_CONTEXT_" + role + "_END\nReturn a JSON cooperative transition.", Tools: []string{"coop_probe"}}); err != nil {
				return report, err
			}
		}
	}
	task := func(kind string, i int) engine.Task {
		role := fmt.Sprintf("%s-%d", kind, i)
		id := fmt.Sprintf("request-%d", i)
		if kind == "responder" {
			id = fmt.Sprintf("response-%d", i)
		}
		return engine.Task{ID: id, Project: "project", Role: role, Version: "v1", Instance: "default", Input: "cooperate"}
	}
	for i := 0; i < 4; i++ {
		if err = store.SubmitCooperative(task("requester", i)); err != nil {
			return report, err
		}
	}
	var starts atomic.Int64
	factory := func(slot int) (engine.StepExecutor, error) {
		agentDir := filepath.Join(root, fmt.Sprintf("agent-%d", slot))
		if err := os.MkdirAll(agentDir, 0700); err != nil {
			return nil, err
		}
		a, err := piadapter.New(piadapter.Config{Resolver: store, Store: store, TaskTimeout: 30 * time.Second, OnProcessStart: func(int) { starts.Add(1) }, RPC: pirpc.Config{Command: node, Dir: workspace,
			Args: []string{filepath.Join(repo, "experiments/pi/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js"), "--mode", "rpc", "--offline", "--no-session", "--no-approve", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "-e", filepath.Join(repo, "experiments/pi/cooperation-provider.mjs"), "-e", filepath.Join(repo, "adapters/pi/role-extension.mjs"), "--provider", "cooperation", "--model", "deterministic"},
			Env:  []string{"PATH=" + os.Getenv("PATH"), "HOME=" + agentDir, "PI_CODING_AGENT_DIR=" + agentDir, "PI_OFFLINE=1", "ROLE_RUNTIME_STATE=" + root},
		}})
		if err != nil {
			return nil, err
		}
		return &piadapter.Cooperative{Adapter: a}, nil
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	p := engine.CooperativePool{Workers: 4, Store: store, Factory: factory, HeartbeatInterval: 75 * time.Millisecond, HeartbeatTimeout: 2 * time.Second, LeaseTTL: 15 * time.Second, PollInterval: 20 * time.Millisecond}
	type finished struct {
		stats engine.CooperativeStats
		err   error
	}
	done := make(chan finished, 1)
	go func() { stats, err := p.Run(ctx); done <- finished{stats, err} }()
	var beforeRestart engine.CooperativeStats
	defer func() {
		cancel()
		out := <-done
		report.CooperativeStats = out.stats
		report.Steps += beforeRestart.Steps
		report.Completed += beforeRestart.Completed
		report.Suspended += beforeRestart.Suspended
		report.Resumed += beforeRestart.Resumed
		report.Heartbeats += beforeRestart.Heartbeats
		if beforeRestart.MaxActive > report.MaxActive {
			report.MaxActive = beforeRestart.MaxActive
		}
		report.ProcessesStarted = starts.Load()
		if err == nil && !errors.Is(out.err, context.Canceled) {
			err = out.err
		}
	}()
	await := func(kind, status string) error {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			all := true
			for i := 0; i < 4; i++ {
				r, e := store.CooperativeReceipt(task(kind, i).ID)
				if e != nil {
					return e
				}
				if r.Status == "uncertain" {
					return fmt.Errorf("%s: %s", r.Task.ID, r.Error)
				}
				if r.Status != status {
					all = false
				}
				if status == "completed" && r.Status == status && kind == "requester" && r.Text != "resumed:"+r.Task.ID {
					return errors.New("wrong resumed output")
				}
			}
			if all {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out := <-done:
				done <- out
				return fmt.Errorf("pool stopped: %w", out.err)
			case <-ticker.C:
			}
		}
	}
	if err = await("requester", "waiting"); err != nil {
		return report, err
	}
	report.AllFourWaiting = true
	if restart {
		cancel()
		first := <-done
		if !errors.Is(first.err, context.Canceled) {
			done <- first
			return report, first.err
		}
		beforeRestart = first.stats
		// Reopen the store and replace every pi process, retaining only disk state.
		if err = store.Close(); err != nil {
			done <- finished{}
			return report, err
		}
		store, err = engine.OpenStore(root)
		if err != nil {
			done <- finished{}
			return report, err
		}
		p.Store = store
		ctx, cancel = context.WithCancel(parent)
		defer cancel()
		go func() { stats, err := p.Run(ctx); done <- finished{stats, err} }()
	}

	for i := 0; i < 4; i++ {
		if err = store.SubmitCooperative(task("responder", i)); err != nil {
			return report, err
		}
	}
	if err = await("responder", "completed"); err != nil {
		return report, err
	}
	err = await("requester", "completed")
	return report, err
}
