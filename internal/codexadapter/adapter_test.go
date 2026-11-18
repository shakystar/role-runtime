package codexadapter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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
	var activeID, mode string
	beats := 0
	complete := func() {
		send(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": activeID, "turn": map[string]any{"id": "turn-1", "status": "completed", "items": []any{map[string]any{"type": "agentMessage", "phase": "final_answer", "text": "delayed success"}}}}})
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
		if os.Getenv("ROLE_RUNTIME_STALL_CONTROL") == req.Method {
			continue
		}
		var result any = map[string]any{}
		switch req.Method {
		case "thread/loaded/list":
			beats++
			if mode == "probe-dies" {
				os.Exit(0)
			}
			if mode == "probe-stalls" {
				continue
			}
			if mode == "complete-during-probe" {
				complete()
				time.Sleep(60 * time.Millisecond)
				mode = ""
			}
			if mode == "heartbeat-delayed" && beats == 3 {
				send(map[string]any{"method": "thread/status/changed", "params": map[string]any{"threadId": activeID, "status": map[string]any{"type": "active", "activeFlags": []string{"waitingOnUserInput"}}}})
				complete()
			}
			if mode == "progress-delayed" {
				for i := 0; i < 25; i++ {
					send(map[string]any{"method": "item/reasoning/textDelta", "params": map[string]any{"threadId": activeID, "turnId": "turn-1", "delta": "private-reasoning-must-not-appear-in-status"}})
					time.Sleep(time.Millisecond)
				}
				complete()
			}
			result = map[string]any{"data": []string{activeID}}
		case "thread/start", "thread/resume":
			var cwd, instructions, id string
			json.Unmarshal(req.Params["cwd"], &cwd)
			json.Unmarshal(req.Params["baseInstructions"], &instructions)
			json.Unmarshal(req.Params["threadId"], &id)
			var config map[string]any
			json.Unmarshal(req.Params["config"], &config)
			wantSearch := "disabled"
			if strings.Contains(instructions, "Web research task:") {
				wantSearch = "live"
			}
			if config["web_search"] != wantSearch || config["features.shell_tool"] != false {
				os.Exit(9)
			}
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
			activeID = id
			mode = input[0].Text
			beats = 0
			if input[0].Text == "require-schema" {
				var schema map[string]any
				if json.Unmarshal(req.Params["outputSchema"], &schema) != nil || schema["type"] != "object" || schema["additionalProperties"] != false {
					send(map[string]any{"id": req.ID, "error": map[string]any{"code": -32602}})
					continue
				}
			}
			if input[0].Text == "lost-ack" {
				os.Exit(0)
			}
			send(map[string]any{"id": req.ID, "result": map[string]any{"turn": map[string]any{"id": "turn-1", "status": "inProgress"}}})
			if input[0].Text == "hang" {
				continue
			}
			if strings.HasPrefix(mode, "probe-") || strings.HasSuffix(mode, "-delayed") || mode == "complete-during-probe" {
				continue
			}
			notify := func(method string, p any) { send(map[string]any{"method": method, "params": p}) }
			var webFinalItems []any
			if mode == "web-tool" || mode == "web-limit" || mode == "web-no-id" {
				webItem := func(itemID, action string) map[string]any {
					return map[string]any{"id": itemID, "type": "webSearch", "query": "private-query-secret", "action": map[string]any{"type": action, "url": "https://private-url-secret.example"}, "results": []any{map[string]any{"private": "private-result-secret"}}}
				}
				for _, pair := range [][2]string{{"other-thread", "turn-1"}, {id, "other-turn"}} {
					notify("item/completed", map[string]any{"threadId": pair[0], "turnId": pair[1], "item": webItem("irrelevant", "search")})
				}
				if mode == "web-limit" {
					for i := 0; i < 257; i++ {
						webFinalItems = append(webFinalItems, webItem(fmt.Sprint(i), "search"))
					}
				} else if mode == "web-no-id" {
					webFinalItems = []any{webItem("", "search")}
				}
				w := webItem("search-1", "search")
				notify("item/started", map[string]any{"threadId": id, "turnId": "turn-1", "item": w})
				for i := 0; i < 2; i++ {
					notify("item/completed", map[string]any{"threadId": id, "turnId": "turn-1", "item": w})
				}
				if mode == "web-tool" {
					webFinalItems = []any{w, webItem("open-1", "openPage"), webItem("find-1", "findInPage"), webItem("other-1", "private-action-secret")}
				}
			}
			if mode == "backend-errors" {
				for i, code := range []any{"serverOverloaded", map[string]any{"responseStreamDisconnected": map[string]any{"httpStatusCode": 503, "private": "private-error-secret"}}, "private-error-secret"} {
					notify("error", map[string]any{"threadId": id, "turnId": "turn-1", "willRetry": i != 2, "error": map[string]any{"codexErrorInfo": code, "message": "private-error-secret", "additionalDetails": "private-error-secret", "misalignment": map[string]any{"detailedExplanation": "private-error-secret"}}})
				}
			}
			if mode == "string-status" {
				notify("mcpServer/startupStatus/updated", map[string]any{"threadId": id, "status": "ready", "delta": map[string]any{"other": "shape"}, "turn": "unrelated"})
			}
			if input[0].Text == "tool" {
				notify("item/started", map[string]any{"threadId": id, "turnId": "turn-1", "item": map[string]any{"type": "commandExecution"}})
				continue
			}
			notify("thread/tokenUsage/updated", map[string]any{"threadId": id, "turnId": "turn-1", "tokenUsage": map[string]any{"last": map[string]any{"inputTokens": 10, "outputTokens": 3, "cachedInputTokens": 2}}})
			status := "completed"
			if input[0].Text == "fail" {
				status = "failed"
			}
			notify("turn/completed", map[string]any{"threadId": id, "turn": map[string]any{"id": "turn-1", "status": status, "items": append(webFinalItems, map[string]any{"type": "agentMessage", "phase": "final_answer", "text": id + ":" + input[0].Text})}})
			continue
		case "thread/unsubscribe":
			result = map[string]any{"status": "unsubscribed"}
		}
		send(map[string]any{"id": req.ID, "result": result})
	}
	os.Exit(0)
}

func TestUnlimitedTurnHeartbeatAndProgress(t *testing.T) {
	for _, mode := range []string{"heartbeat-delayed", "progress-delayed", "complete-during-probe"} {
		t.Run(mode, func(t *testing.T) {
			cfg, task := fixture(t)
			cfg.TaskTimeout = 0
			cfg.HeartbeatInterval = 30 * time.Millisecond
			cfg.HeartbeatTimeout = time.Second
			cfg.RPC.NotificationBuffer = 8
			var statuses []Status
			cfg.OnStatus = func(s Status) { statuses = append(statuses, s) }
			a, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			if a.cfg.TaskTimeout != 0 {
				t.Fatal("default task timeout was introduced")
			}
			task.Input = mode
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, err := a.Execute(ctx, task)
			if err != nil {
				t.Fatal(err)
			}
			final := statuses[len(statuses)-1]
			if final.Phase != "completed" || final.HeartbeatCount == 0 || final.LastHeartbeat.IsZero() {
				t.Fatalf("missing heartbeat: %+v", final)
			}
			if mode == "heartbeat-delayed" && (final.HeartbeatCount < 3 || !final.LastProgress.IsZero()) {
				t.Fatalf("lifecycle is not progress: %+v", final)
			}
			if mode == "heartbeat-delayed" && (final.ThreadState != "active" || len(final.WaitingFlags) != 1 || final.WaitingFlags[0] != "waitingOnUserInput") {
				t.Fatal("thread wait state missing")
			}
			if mode == "progress-delayed" && final.LastProgress.IsZero() {
				t.Fatal("missing progress")
			}
			data, _ := json.Marshal(statuses)
			if strings.Contains(string(data), "private-reasoning") {
				t.Fatal("status leaked content")
			}
			task.ID = "next"
			task.Input = "hello"
			next, err := a.Execute(ctx, task)
			if err != nil {
				t.Fatal(err)
			}
			if result.PID != next.PID || a.Starts != 1 {
				t.Fatal("normal completion killed healthy probe/process")
			}
		})
	}
}

func TestControlPlaneDeadlineSeparateFromUnlimitedTurn(t *testing.T) {
	cfg, task := fixture(t)
	cfg.TaskTimeout = 0
	cfg.StartupTimeout = 300 * time.Millisecond
	cfg.RPC.Env = append(cfg.RPC.Env, "ROLE_RUNTIME_STALL_CONTROL=config/read")
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	_, err = a.Execute(context.Background(), task)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "control config/read") {
		t.Fatalf("control deadline: %v", err)
	}
}

func TestPublicHeartbeat(t *testing.T) {
	cfg, _ := fixture(t)
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = a.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if !a.client.Alive() {
		t.Fatal("probe killed process")
	}
}

func TestHeartbeatFailureAndUserCancel(t *testing.T) {
	for _, mode := range []string{"probe-dies", "probe-stalls", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			cfg, task := fixture(t)
			cfg.TaskTimeout = 0
			cfg.HeartbeatInterval = 20 * time.Millisecond
			cfg.HeartbeatTimeout = 100 * time.Millisecond
			a, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			task.Input = mode
			if mode == "cancel" {
				task.Input = "probe-stalls"
				timer := time.AfterFunc(60*time.Millisecond, cancel)
				defer timer.Stop()
			}
			began := time.Now()
			_, err = a.Execute(ctx, task)
			if err == nil {
				t.Fatal("failed heartbeat/cancel succeeded")
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel: %v", err)
			}
			if time.Since(began) > 2*time.Second {
				t.Fatal("probe cleanup blocked")
			}
			if a.client != nil {
				t.Fatal("failed execution retained process")
			}
		})
	}
}

func TestRejectNegativeTaskTimeout(t *testing.T) {
	cfg, _ := fixture(t)
	cfg.TaskTimeout = -time.Second
	if a, err := New(cfg); err == nil {
		a.Close()
		t.Fatal("negative timeout accepted")
	}
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

func TestStructuredOutputSchemaForwarded(t *testing.T) {
	cfg, task := fixture(t)
	cfg.OutputSchema = json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`)
	task.Input = "require-schema"
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := a.Execute(context.Background(), task); err != nil {
		t.Fatal(err)
	}
}

func TestUnrelatedNotificationStringStatus(t *testing.T) {
	cfg, task := fixture(t)
	task.Input = "string-status"
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err = a.Execute(context.Background(), task); err != nil {
		t.Fatal(err)
	}
}

func TestBackendErrorMetadataObservedWithoutContentOrRetry(t *testing.T) {
	cfg, task := fixture(t)
	task.Input = "backend-errors"
	var statuses []Status
	cfg.OnStatus = func(s Status) { statuses = append(statuses, s) }
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err = a.Execute(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	var backend []Status
	for _, s := range statuses {
		if s.Phase == "backend_retrying" || s.Phase == "backend_error" {
			backend = append(backend, s)
		}
	}
	if len(backend) != 3 {
		t.Fatalf("backend events=%d", len(backend))
	}
	if backend[0].BackendErrorCode != "serverOverloaded" || backend[1].BackendHTTPStatus != 503 || backend[2].BackendErrorCode != "unknown" {
		t.Fatalf("wrong sanitized metadata: %+v", backend)
	}
	if backend[2].BackendWillRetry == nil || *backend[2].BackendWillRetry || backend[2].BackendErrorCount != 3 || backend[2].LastBackendErrorAt.IsZero() {
		t.Fatal("missing retry/count/time")
	}
	if !backend[2].LastProgress.IsZero() {
		t.Fatal("backend retry is not model progress")
	}
	data, _ := json.Marshal(statuses)
	if strings.Contains(string(data), "private-error-secret") {
		t.Fatal("private error leaked")
	}
	if statuses[len(statuses)-1].Phase != "completed" || a.Starts != 1 || a.Usage().CompletedTurns != 1 {
		t.Fatal("error observation changed execution")
	}
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

func TestResearchProfileValidation(t *testing.T) {
	for _, tools := range [][]string{nil, {}, {"shell"}, {"advisory", "web-research"}, {"web-research", "web-research"}} {
		if _, err := researchProfile(tools); err == nil {
			t.Fatalf("accepted unsupported profile %v", tools)
		}
	}
	for _, name := range []string{"advisory", "web-research"} {
		enabled, err := researchProfile([]string{name})
		if err != nil || enabled != (name == "web-research") {
			t.Fatal(name, enabled, err)
		}
	}
}

func TestWebResearchSwitchResumeAndPrivateTelemetry(t *testing.T) {
	cfg, task := fixture(t)
	if err := cfg.Store.RegisterRole(engine.Role{ID: "research", Version: "v1", Instruction: "Research", Tools: []string{"web-research"}}); err != nil {
		t.Fatal(err)
	}
	var statuses []Status
	cfg.OnStatus = func(s Status) { statuses = append(statuses, s) }
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ctx := context.Background()
	first, err := a.Execute(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	for i, role := range []string{"research", "beta", "research", "alpha"} {
		task.ID = fmt.Sprintf("switch-%d", i)
		task.Role = role
		task.Input = "hello"
		if role == "research" {
			task.Input = "web-tool"
		}
		got, err := a.Execute(ctx, task)
		if err != nil {
			t.Fatal(err)
		}
		if got.PID != first.PID || a.Starts != 1 {
			t.Fatal("role switch restarted process")
		}
		last := statuses[len(statuses)-1]
		if role == "research" {
			if last.WebSearchCount != 4 || last.WebSearchQueries != 1 || last.WebPageOpens != 1 || last.WebPageFinds != 1 || last.LastWebAction != "other" || last.LastProgress.IsZero() {
				t.Fatalf("incorrect web counts %+v", last)
			}
		} else if last.WebSearchCount != 0 {
			t.Fatal("research telemetry leaked into advisory role")
		}
	}
	b, _ := json.Marshal(statuses)
	if strings.Contains(string(b), "private-") {
		t.Fatal("web metadata leaked query, URL, results or unknown action")
	}
}

func TestRoleToolRejection(t *testing.T) {
	for _, tc := range []struct{ role, mode string }{{"alpha", "web-tool"}, {"research", "tool"}} {
		t.Run(tc.role, func(t *testing.T) {
			cfg, task := fixture(t)
			if err := cfg.Store.RegisterRole(engine.Role{ID: "research", Version: "v1", Instruction: "Research", Tools: []string{"web-research"}}); err != nil {
				t.Fatal(err)
			}
			task.Role = tc.role
			task.Input = tc.mode
			a, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			if _, err = a.Execute(context.Background(), task); err == nil || !strings.Contains(err.Error(), "unsupported tool") {
				t.Fatalf("tool not rejected: %v", err)
			}
		})
	}
}

func TestWebTelemetryBounded(t *testing.T) {
	for _, mode := range []string{"web-limit", "web-no-id"} {
		t.Run(mode, func(t *testing.T) {
			cfg, task := fixture(t)
			if err := cfg.Store.RegisterRole(engine.Role{ID: "research", Version: "v1", Instruction: "Research", Tools: []string{"web-research"}}); err != nil {
				t.Fatal(err)
			}
			task.Role = "research"
			task.Input = mode
			a, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			if _, err = a.Execute(context.Background(), task); err == nil || !strings.Contains(err.Error(), "web search") {
				t.Fatalf("missing bounded telemetry rejection: %v", err)
			}
		})
	}
}
