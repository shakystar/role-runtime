// Package pispike wires the real Go runtime to pi with a synthetic provider.
// It is a reproducible integration/demo fixture, not a real model adapter config.
package pispike

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shakystar/role-runtime/internal/engine"
	"github.com/shakystar/role-runtime/internal/piadapter"
	"github.com/shakystar/role-runtime/internal/pirpc"
)

type Options struct {
	Repository, StateDir          string
	Roles, Workers, Rounds, Queue int
	Fresh                         bool
	PayloadBytes                  int
	ToolDelayMS                   int
}
type Report struct {
	Stats            engine.Stats `json:"stats"`
	Roles            int          `json:"roles"`
	Rounds           int          `json:"rounds"`
	Mode             string       `json:"mode"`
	ProcessesStarted int64        `json:"processesStarted"`
	UniquePIDs       int          `json:"uniquePids"`
	WorkerRoleCounts map[int]int  `json:"workerRoleCounts"`
	SetupSeconds     float64      `json:"setupSeconds"`
	PoolSeconds      float64      `json:"poolSeconds"`
	ExecuteP50MS     float64      `json:"executeP50Ms"`
	ExecuteP95MS     float64      `json:"executeP95Ms"`
}

func Run(ctx context.Context, opt Options) (report Report, err error) {
	started := time.Now()
	if opt.PayloadBytes < 0 || opt.PayloadBytes > 16384 || opt.ToolDelayMS < 0 || opt.ToolDelayMS > 10000 {
		return report, fmt.Errorf("invalid fixture payload/delay")
	}
	if opt.Roles < 1 || opt.Roles > 400 || opt.Rounds < 1 || opt.Rounds > 20 {
		return report, fmt.Errorf("roles must be 1..400 and rounds 1..20")
	}
	repo, err := filepath.Abs(opt.Repository)
	if err != nil {
		return report, err
	}
	node, err := exec.LookPath("node")
	if err != nil {
		return report, err
	}
	cli := filepath.Join(repo, "experiments/pi/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js")
	if _, err = os.Stat(cli); err != nil {
		return report, fmt.Errorf("install locked pi dependencies first: %w", err)
	}
	root := opt.StateDir
	if root == "" {
		root, err = os.MkdirTemp("", "role-runtime-demo-")
		if err != nil {
			return report, err
		}
		defer os.RemoveAll(root)
	}
	store, err := engine.OpenStore(root)
	if err != nil {
		return report, err
	}
	defer store.Close()
	workspace := filepath.Join(store.Root, "workspace")
	if err = os.MkdirAll(workspace, 0700); err != nil {
		return report, err
	}
	if err = os.WriteFile(filepath.Join(workspace, "project-marker"), []byte("project-one"), 0600); err != nil {
		return report, err
	}
	if err = store.RegisterProject(engine.Project{ID: "project-one", Workspace: workspace, Rules: "PROJECT_CONTEXT_project-one_END"}); err != nil {
		return report, err
	}
	for i := 0; i < opt.Roles; i++ {
		id := fmt.Sprintf("role-%02d", i)
		if err = store.RegisterRole(engine.Role{ID: id, Version: "v1", Instruction: "ROLE_CONTEXT_" + id + "_END", Tools: []string{fmt.Sprintf("probe_%02d", i)}}); err != nil {
			return report, err
		}
	}
	var starts atomic.Int64
	var timings executionTimings
	factory := func(slot int) (engine.Executor, error) {
		agentDir := filepath.Join(store.Root, fmt.Sprintf("agent-%d", slot))
		if err := os.MkdirAll(agentDir, 0700); err != nil {
			return nil, err
		}
		cfg := piadapter.Config{Resolver: store, Store: store, TaskTimeout: 30 * time.Second, RPC: pirpc.Config{
			Command: node, Dir: workspace, Args: []string{cli, "--mode", "rpc", "--offline", "--no-session", "--no-approve", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
				"-e", filepath.Join(repo, "experiments/pi/pool-provider.mjs"), "-e", filepath.Join(repo, "adapters/pi/role-extension.mjs"), "--provider", "role-pool", "--model", "deterministic"},
			Env: []string{"PATH=" + os.Getenv("PATH"), "HOME=" + agentDir, "PI_CODING_AGENT_DIR=" + agentDir, "PI_OFFLINE=1", "ROLE_RUNTIME_STATE=" + store.Root, "ROLE_POOL_ROLES=" + fmt.Sprint(opt.Roles), "ROLE_POOL_DELAY_MS=" + fmt.Sprint(opt.ToolDelayMS), "ROLE_POOL_LOG=" + filepath.Join(store.Root, fmt.Sprintf("worker-%d.jsonl", slot))},
		}}
		create := func() (engine.Executor, error) {
			cfg.OnProcessStart = func(int) { starts.Add(1) }
			return piadapter.New(cfg)
		}
		if opt.Fresh {
			return &timedExecutor{Executor: &freshExecutor{create: create}, timings: &timings}, nil
		}
		executor, err := create()
		if err != nil {
			return nil, err
		}
		return &timedExecutor{Executor: executor, timings: &timings}, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tasks := make(chan engine.Task)
	go func() {
		defer close(tasks)
		for round := 0; round < opt.Rounds; round++ {
			for i := 0; i < opt.Roles; i++ {
				role := fmt.Sprintf("role-%02d", i)
				id := fmt.Sprintf("round-%02d-%s", round, role)
				input, _ := json.Marshal(map[string]any{"id": id, "project": "project-one", "role": role, "round": round, "padding": strings.Repeat("x", opt.PayloadBytes)})
				task := engine.Task{ID: id, Project: "project-one", Role: role, Version: "v1", Instance: "default", Input: string(input)}
				select {
				case tasks <- task:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	pids := map[int]bool{}
	workerRoles := map[int]map[string]bool{}
	pool := engine.Pool{Workers: opt.Workers, QueueSize: opt.Queue, Store: store, Factory: factory}
	report.SetupSeconds = time.Since(started).Seconds()
	poolStarted := time.Now()
	report.Stats, err = pool.Run(ctx, tasks, func(out engine.Outcome) error {
		if out.Status != "succeeded" {
			return fmt.Errorf("task %s %s: %s", out.Task.ID, out.Status, out.Error)
		}
		if out.Result.Text != "completed:"+out.Task.ID {
			return fmt.Errorf("wrong output for %s: %q", out.Task.ID, out.Result.Text)
		}
		pids[out.Result.PID] = true
		if workerRoles[out.Result.Worker] == nil {
			workerRoles[out.Result.Worker] = map[string]bool{}
		}
		workerRoles[out.Result.Worker][out.Task.Role] = true
		return nil
	})
	report.PoolSeconds = time.Since(poolStarted).Seconds()
	report.ExecuteP50MS, report.ExecuteP95MS = timings.percentiles()
	report.Roles, report.Rounds, report.ProcessesStarted, report.UniquePIDs = opt.Roles, opt.Rounds, starts.Load(), len(pids)
	report.Mode = "reuse"
	if opt.Fresh {
		report.Mode = "fresh-process"
	}
	report.WorkerRoleCounts = map[int]int{}
	for id, roles := range workerRoles {
		report.WorkerRoleCounts[id] = len(roles)
	}
	return report, err
}

type freshExecutor struct {
	create func() (engine.Executor, error)
}

func (f *freshExecutor) Execute(ctx context.Context, t engine.Task) (engine.Result, error) {
	if err := ctx.Err(); err != nil {
		return engine.Result{}, err
	}
	e, err := f.create()
	if err != nil {
		return engine.Result{}, err
	}
	defer e.Close()
	return e.Execute(ctx, t)
}
func (f *freshExecutor) Close() error { return nil }

// Demo-only instrumentation: bounded by the fixture's maximum 8000 tasks, not
// retained in the production scheduler. Execute latency excludes queue/receipts.
type executionTimings struct {
	mu     sync.Mutex
	values []float64
}
type timedExecutor struct {
	engine.Executor
	timings *executionTimings
}

func (e *timedExecutor) Execute(ctx context.Context, t engine.Task) (engine.Result, error) {
	start := time.Now()
	result, err := e.Executor.Execute(ctx, t)
	e.timings.mu.Lock()
	e.timings.values = append(e.timings.values, float64(time.Since(start))/float64(time.Millisecond))
	e.timings.mu.Unlock()
	return result, err
}
func (t *executionTimings) percentiles() (float64, float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.values) == 0 {
		return 0, 0
	}
	sort.Float64s(t.values)
	return t.values[(len(t.values)-1)*50/100], t.values[(len(t.values)-1)*95/100]
}
