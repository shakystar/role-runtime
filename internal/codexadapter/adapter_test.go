package codexadapter

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shakystar/role-runtime/internal/codexrpc"
	"github.com/shakystar/role-runtime/internal/engine"
)

func TestCodexAdapterProcess(t *testing.T) {
	if os.Getenv("ROLE_RUNTIME_ADAPTER_HELPER") != "1" {
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	send := func(v any) {
		if err := encoder.Encode(v); err != nil {
			os.Exit(2)
		}
	}
	for scanner.Scan() {
		var req struct {
			ID     json.RawMessage            `json:"id"`
			Method string                     `json:"method"`
			Params map[string]json.RawMessage `json:"params"`
		}
		if json.Unmarshal(scanner.Bytes(), &req) != nil {
			os.Exit(3)
		}
		if len(req.ID) == 0 {
			continue
		}
		var result any = map[string]any{}
		switch req.Method {
		case "thread/start", "thread/resume":
			var cwd, instructions, id string
			json.Unmarshal(req.Params["cwd"], &cwd)
			json.Unmarshal(req.Params["baseInstructions"], &instructions)
			json.Unmarshal(req.Params["threadId"], &id)
			if id == "" {
				id = "thread-" + engine.Digest(instructions)[:12]
			}
			result = map[string]any{"thread": map[string]any{"id": id, "cwd": cwd}}
		case "turn/start":
			var id string
			var input []struct {
				Text string `json:"text"`
			}
			json.Unmarshal(req.Params["threadId"], &id)
			json.Unmarshal(req.Params["input"], &input)
			if input[0].Text == "lost-ack" {
				os.Exit(0)
			}
			send(map[string]any{"id": req.ID, "result": map[string]any{"turn": map[string]any{"id": "turn-1", "status": "inProgress"}}})
			if input[0].Text == "hang" {
				continue
			}
			notify := func(method string, p any) { send(map[string]any{"method": method, "params": p}) }
			if input[0].Text == "tool" {
				notify("item/started", map[string]any{"threadId": id, "turnId": "turn-1", "item": map[string]any{"type": "commandExecution"}})
				continue
			}
			notify("thread/tokenUsage/updated", map[string]any{"threadId": id, "turnId": "turn-1", "tokenUsage": map[string]any{"last": map[string]any{"inputTokens": 10, "outputTokens": 3, "cachedInputTokens": 2}}})
			status := "completed"
			if input[0].Text == "fail" {
				status = "failed"
			}
			notify("turn/completed", map[string]any{"threadId": id, "turn": map[string]any{"id": "turn-1", "status": status, "items": []any{map[string]any{"type": "agentMessage", "phase": "final_answer", "text": id + ":" + input[0].Text}}}})
			continue
		case "thread/unsubscribe":
			result = map[string]any{"status": "unsubscribed"}
		}
		send(map[string]any{"id": req.ID, "result": result})
	}
	os.Exit(0)
}
func fixture(t *testing.T) (Config, engine.Task) {
	t.Helper()
	store, err := engine.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.RegisterProject(engine.Project{ID: "project", Workspace: workspace, Rules: "Use only supplied information."}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alpha", "beta"} {
		if err = store.RegisterRole(engine.Role{ID: name, Version: "v1", Instruction: "You are " + name, Tools: []string{"advisory"}}); err != nil {
			t.Fatal(err)
		}
	}
	cfg := Config{Store: store, TaskTimeout: time.Second, RPC: codexrpc.Config{Command: os.Args[0], Args: []string{"-test.run=^TestCodexAdapterProcess$"}, Env: append(os.Environ(), "ROLE_RUNTIME_ADAPTER_HELPER=1")}}
	return cfg, engine.Task{ID: "t1", Project: "project", Role: "alpha", Version: "v1", Instance: "default", Input: "hello"}
}
func TestRoleSwitchAndRestartMapping(t *testing.T) {
	cfg, task := fixture(t)
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	first, err := a.Execute(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if u := a.Usage(); !u.Known || u.InputTokens != 10 {
		t.Fatalf("usage: %+v", u)
	}
	task.Role = "beta"
	task.ID = "t2"
	second, err := a.Execute(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	task.Role = "alpha"
	task.ID = "t3"
	third, err := a.Execute(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if first.Session == second.Session || first.Session != third.Session || first.PID != second.PID || a.Starts != 1 {
		t.Fatal("role mapping or process reuse failed")
	}
	a.Close()
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	task.ID = "t4"
	resumed, err := b.Execute(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Session != first.Session {
		t.Fatal("restart lost mapping")
	}
	entries, err := os.ReadDir(filepath.Join(cfg.Store.Root, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("created pi session")
	}
}
func TestUncertainTurnsQuarantine(t *testing.T) {
	for _, input := range []string{"lost-ack", "hang", "fail", "tool"} {
		t.Run(input, func(t *testing.T) {
			cfg, task := fixture(t)
			cfg.TaskTimeout = 150 * time.Millisecond
			a, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			task.Input = input
			if _, err = a.Execute(context.Background(), task); err == nil {
				t.Fatal("uncertain execution succeeded")
			}
			task.Input = "hello"
			task.ID = "t2"
			if _, err = a.Execute(context.Background(), task); err == nil {
				t.Fatal("quarantined session replayed")
			}
			m, err := readMapping(filepath.Join(cfg.Store.Root, "codex-sessions", task.Key()+".json"))
			if err != nil {
				t.Fatal(err)
			}
			if m.ActiveTask == "" {
				t.Fatal("lost durable intent")
			}
		})
	}
}
func TestRestartWithActiveIntentFailsClosed(t *testing.T) {
	cfg, task := fixture(t)
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err = a.Execute(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.Store.Root, "codex-sessions", task.Key()+".json")
	m, err := readMapping(path)
	if err != nil {
		t.Fatal(err)
	}
	m.ActiveTask = "interrupted-task"
	if err = writeMapping(path, m); err != nil {
		t.Fatal(err)
	}
	task.ID = "t2"
	if _, err = a.Execute(context.Background(), task); err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("expected failclosed: %v", err)
	}
}
func TestUnsupportedToolPolicy(t *testing.T) {
	cfg, task := fixture(t)
	if err := cfg.Store.RegisterRole(engine.Role{ID: "shell", Version: "v1", Instruction: "shell", Tools: []string{"bash"}}); err != nil {
		t.Fatal(err)
	}
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	task.Role = "shell"
	if _, err = a.Execute(context.Background(), task); err == nil {
		t.Fatal(fmt.Errorf("unsupported tool policy accepted"))
	}
}
