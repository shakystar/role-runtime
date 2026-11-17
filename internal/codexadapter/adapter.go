// Package codexadapter multiplexes durable role threads over one Codex app-server.
// Execute and Close are owned by one worker; Usage may be read concurrently.
// Advisory is a restricted operating mode, not a complete tool capability or
// filesystem isolation boundary. Tool notifications cause failure after detection.
package codexadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/shakystar/role-runtime/internal/codexrpc"
	"github.com/shakystar/role-runtime/internal/engine"
)

const maxOutput = 256 << 10

type Config struct {
	RPC               codexrpc.Config
	Store             *engine.Store
	TaskTimeout       time.Duration
	StartupTimeout    time.Duration
	Model             string
	OutputSchema      json.RawMessage
	OnProcessStart    func(int)
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	// OnStatus runs on the execution worker and must return promptly without blocking.
	// It receives metadata only, never prompts, output, reasoning, or config values.
	OnStatus func(Status)
}
type Status struct {
	TaskID             string        `json:"taskId"`
	Role               string        `json:"role"`
	Phase              string        `json:"phase"`
	PID                int           `json:"pid"`
	Elapsed            time.Duration `json:"elapsed"`
	LastNotification   string        `json:"lastNotification,omitempty"`
	LastNotificationAt time.Time     `json:"lastNotificationAt,omitempty"`
	LastProgress       time.Time     `json:"lastProgress,omitempty"`
	LastHeartbeat      time.Time     `json:"lastHeartbeat,omitempty"`
	HeartbeatCount     int           `json:"heartbeatCount"`
	ThreadState        string        `json:"threadState,omitempty"`
	WaitingFlags       []string      `json:"waitingFlags,omitempty"`
	BackendErrorCode   string        `json:"backendErrorCode,omitempty"`
	BackendHTTPStatus  int           `json:"backendHttpStatus,omitempty"`
	BackendWillRetry   *bool         `json:"backendWillRetry,omitempty"`
	BackendErrorCount  int           `json:"backendErrorCount"`
	LastBackendErrorAt time.Time     `json:"lastBackendErrorAt,omitempty"`
}
type Usage struct {
	// Counts sum the final tokenUsage.last notification of successful turns.
	// They exclude failed turns and may omit earlier requests in a tool turn.
	Scope             string `json:"scope"`
	InputTokens       int64  `json:"inputTokens"`
	OutputTokens      int64  `json:"outputTokens"`
	CachedInputTokens int64  `json:"cachedInputTokens"`
	Known             bool   `json:"known"`
	CompletedTurns    int64  `json:"completedTurns"`
	MeasuredTurns     int64  `json:"measuredTurns"`
}
type Adapter struct {
	cfg             Config
	client          *codexrpc.Client
	mu              sync.Mutex
	usage           Usage
	usageIncomplete bool
	Starts          int
}
type mapping struct {
	Key          string `json:"key"`
	ThreadID     string `json:"threadId"`
	Workspace    string `json:"workspace"`
	Instructions string `json:"instructionDigest"`
	ActiveTask   string `json:"activeTask,omitempty"`
}
type item struct {
	Type  string `json:"type"`
	Text  string `json:"text"`
	Phase string `json:"phase"`
}
type turn struct {
	ID     string          `json:"id"`
	Status string          `json:"status"`
	Items  []item          `json:"items"`
	Error  json.RawMessage `json:"error"`
}

func New(cfg Config) (*Adapter, error) {
	if cfg.Store == nil {
		return nil, errors.New("codex adapter requires store")
	}
	if cfg.RPC.Command == "" {
		cfg.RPC.Command = "codex"
	}
	if len(cfg.RPC.Args) == 0 {
		cfg.RPC.Args = []string{"app-server", "--stdio", "-c", "thread_unload_delay_secs=0", "-c", "web_search=\"disabled\""}
		for _, feature := range []string{"shell_tool", "apps", "plugins", "multi_agent", "browser_use", "computer_use", "image_generation", "view_image"} {
			cfg.RPC.Args = append(cfg.RPC.Args, "-c", "features."+feature+"=false")
		}
	}
	if cfg.TaskTimeout < 0 || cfg.HeartbeatInterval < 0 || cfg.HeartbeatTimeout < 0 {
		return nil, errors.New("codex timeouts and intervals cannot be negative")
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 15 * time.Second
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 10 * time.Second
	}
	if cfg.StartupTimeout <= 0 {
		cfg.StartupTimeout = 30 * time.Second
	}
	if err := os.MkdirAll(filepath.Join(cfg.Store.Root, "codex-sessions"), 0700); err != nil {
		return nil, err
	}
	a := &Adapter{cfg: cfg, usage: Usage{Scope: "successful-turn-last-notification"}}
	if err := a.start(context.Background()); err != nil {
		return nil, err
	}
	return a, nil
}
func (a *Adapter) start(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, a.cfg.StartupTimeout)
	defer cancel()
	c, err := codexrpc.Start(ctx, a.cfg.RPC)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.client = c
	a.mu.Unlock()
	a.Starts++
	if a.cfg.OnProcessStart != nil {
		a.cfg.OnProcessStart(c.PID())
	}
	return nil
}
func (a *Adapter) Close() error {
	a.mu.Lock()
	client := a.client
	a.client = nil
	a.mu.Unlock()
	if client == nil {
		return nil
	}
	return client.Close()
}

// Heartbeat probes actual app-server responsiveness; it does not prove model progress.
// Its caller must stop and join monitors before normal Close. RPC cancellation
// intentionally terminates the process, so do not cancel a healthy in-flight probe.
func (a *Adapter) Heartbeat(ctx context.Context) error {
	a.mu.Lock()
	client := a.client
	a.mu.Unlock()
	if client == nil {
		return errors.New("codex process unavailable")
	}
	return probeHeartbeat(ctx, client)
}
func probeHeartbeat(ctx context.Context, client *codexrpc.Client) error {
	var response struct {
		Data *[]string `json:"data"`
	}
	err := client.Call(ctx, "thread/loaded/list", map[string]any{"limit": 1}, &response)
	if err == nil && response.Data == nil {
		return errors.New("codex heartbeat response missing data")
	}
	return err
}
func (a *Adapter) Usage() Usage { a.mu.Lock(); defer a.mu.Unlock(); return a.usage }
func (a *Adapter) Execute(parent context.Context, t engine.Task) (result engine.Result, err error) {
	ctx, cancel := context.WithCancel(parent)
	if a.cfg.TaskTimeout > 0 {
		cancel()
		ctx, cancel = context.WithTimeout(parent, a.cfg.TaskTimeout)
	}
	defer cancel()
	began := time.Now()
	status := Status{TaskID: t.ID, Role: t.Role}
	emit := func(phase string) {
		status.Phase = phase
		status.Elapsed = time.Since(began)
		if a.cfg.OnStatus != nil {
			a.cfg.OnStatus(status)
		}
	}
	defer func() {
		if err != nil {
			if errors.Is(err, context.Canceled) {
				emit("canceled")
			} else {
				emit("failed")
			}
		}
	}()
	emit("resolving")
	// Bound control-plane replies separately from the model's execution time.
	controlCall := func(method string, params, out any) error {
		callCtx, stop := context.WithTimeout(ctx, a.cfg.StartupTimeout)
		defer stop()
		if callErr := a.client.Call(callCtx, method, params, out); callErr != nil {
			return fmt.Errorf("codex control %s: %w", method, callErr)
		}
		return nil
	}
	activation, err := a.cfg.Store.ResolveDefinitions(ctx, t)
	if err != nil {
		return result, err
	}
	if len(activation.Role.Tools) != 1 || activation.Role.Tools[0] != "advisory" {
		return result, errors.New("codex advisory adapter only supports tools=[advisory]")
	}
	path := filepath.Join(a.cfg.Store.Root, "codex-sessions", t.Key()+".json")
	instructions := activation.Project.Rules + "\n\n" + activation.Role.Instruction + "\nAdvisory task: reason only from supplied inputs. Do not invoke tools, contact anyone, modify files, or claim external research."
	m := mapping{Key: t.Key(), Workspace: activation.Project.Workspace, Instructions: engine.Digest(instructions)}
	prior, readErr := readMapping(path)
	if readErr == nil {
		if prior.Key != m.Key || prior.Workspace != m.Workspace || prior.Instructions != m.Instructions || prior.ThreadID == "" {
			return result, errors.New("codex thread mapping identity mismatch")
		}
		m = prior
		if m.ActiveTask != "" {
			err = errors.New("codex session has interrupted turn intent")
			return result, errors.Join(err, a.cfg.Store.Quarantine(t, err))
		}
	} else if !os.IsNotExist(readErr) {
		return result, readErr
	}
	if a.client == nil || !a.client.Alive() {
		if err = a.start(ctx); err != nil {
			return result, err
		}
	}
	result.PID = a.client.PID()
	status.PID = result.PID
	// A single worker owns notification consumption. Old unloaded-thread lifecycle
	// notifications are ignored by exact thread and turn matching below.
	threadConfig := map[string]any{"features.shell_tool": false, "web_search": "disabled"}
	// Inspect only configured server names, never log configuration values. The
	// app-server has no ignore-user-config flag in the pinned CLI version.
	var effective struct {
		Config struct {
			Servers map[string]json.RawMessage `json:"mcp_servers"`
		} `json:"config"`
	}
	emit("configuring")
	if err = controlCall("config/read", map[string]any{"cwd": m.Workspace, "includeLayers": false}, &effective); err != nil {
		a.Close()
		return result, err
	}
	for name := range effective.Config.Servers {
		if !regexp.MustCompile(`^[A-Za-z0-9_-]+$`).MatchString(name) {
			a.Close()
			return result, errors.New("cannot safely override MCP server config key")
		}
		threadConfig["mcp_servers."+name+".enabled"] = false
	}
	params := map[string]any{"cwd": m.Workspace, "baseInstructions": instructions, "approvalPolicy": "never", "sandbox": "read-only", "config": threadConfig}
	if a.cfg.Model != "" {
		params["model"] = a.cfg.Model
	}
	method := "thread/start"
	if m.ThreadID != "" {
		method = "thread/resume"
		params["threadId"] = m.ThreadID
		params["excludeTurns"] = true
	}
	var response struct {
		Thread struct {
			ID  string `json:"id"`
			Cwd string `json:"cwd"`
		} `json:"thread"`
	}
	emit("loading")
	if err = controlCall(method, params, &response); err != nil {
		a.Close()
		return result, err
	}
	if response.Thread.ID == "" || response.Thread.Cwd != m.Workspace || (m.ThreadID != "" && response.Thread.ID != m.ThreadID) {
		a.Close()
		return result, errors.New("codex activation identity mismatch")
	}
	m.ThreadID = response.Thread.ID
	result.Session = m.ThreadID
	// The durable intent precedes any model turn. A crash before its removal
	// must never be silently replayed against this role thread.
	m.ActiveTask = t.ID
	if err = writeMapping(path, m); err != nil {
		a.Close()
		return result, err
	}
	defer func() {
		if err != nil {
			a.mu.Lock()
			a.usageIncomplete = true
			a.usage.Known = false
			a.mu.Unlock()
			a.Close()
			err = errors.Join(err, a.cfg.Store.Quarantine(t, err))
		}
	}()
	var started struct {
		Turn turn `json:"turn"`
	}
	turnParams := map[string]any{"threadId": m.ThreadID, "input": []any{map[string]any{"type": "text", "text": t.Input}}, "approvalPolicy": "never", "sandboxPolicy": map[string]any{"type": "readOnly", "networkAccess": false}}
	if len(a.cfg.OutputSchema) > 0 {
		turnParams["outputSchema"] = a.cfg.OutputSchema
	}
	emit("starting")
	if err = controlCall("turn/start", turnParams, &started); err != nil {
		return result, err
	}
	if started.Turn.ID == "" {
		return result, errors.New("codex missing turn ID")
	}
	var text string
	var usage Usage
	ticker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer ticker.Stop()
	var probe <-chan error
	var probeCancel context.CancelFunc
	// Normal completion drains an in-flight probe before leaving the loop. Only
	// cancellation/failure cancels its Call, because that poisons the shared RPC.
	defer func() {
		if probe != nil {
			probeCancel()
			<-probe
		}
	}()
	completed := false
	lastProgressEmit := time.Time{}
	emit("running")
waitTurn:
	for {
		select {
		case <-ticker.C:
			if completed {
				emit("finishing")
				continue
			}
			emit("waiting")
			if probe == nil {
				probeCtx, stop := context.WithTimeout(ctx, a.cfg.HeartbeatTimeout)
				probeCancel = stop
				ch := make(chan error, 1)
				probe = ch
				client := a.client
				go func() {
					defer stop()
					ch <- probeHeartbeat(probeCtx, client)
				}()
			}
		case probeErr := <-probe:
			probe = nil
			if probeErr != nil {
				emit("heartbeat_failed")
				return result, fmt.Errorf("codex heartbeat: %w", probeErr)
			}
			status.HeartbeatCount++
			status.LastHeartbeat = time.Now()
			emit("heartbeat_ok")
			if completed {
				break waitTurn
			}
		case <-ctx.Done():
			return result, ctx.Err()
		case <-a.client.Done():
			return result, errors.Join(errors.New("codex exited during turn"), a.client.Err())
		case n, ok := <-a.client.Notifications():
			if !ok {
				return result, errors.New("codex notification stream closed")
			}
			// Notification payloads are a tagged union, not one shared schema.
			// Unknown methods may reuse field names with unrelated JSON types.
			if !supportedNotification(n.Method) {
				continue
			}
			if n.Method == "error" {
				// Observe retry metadata only. Neither backend error text nor open-ended
				// codes are safe to forward, and this event is not a turn completion.
				var backend struct {
					ThreadID  string `json:"threadId"`
					TurnID    string `json:"turnId"`
					WillRetry *bool  `json:"willRetry"`
					Error     struct {
						Code json.RawMessage `json:"codexErrorInfo"`
					} `json:"error"`
				}
				if json.Unmarshal(n.Params, &backend) != nil || backend.ThreadID != m.ThreadID || backend.TurnID != started.Turn.ID {
					continue
				}
				status.LastNotification = n.Method
				status.LastNotificationAt = time.Now()
				status.BackendErrorCode, status.BackendHTTPStatus = backendErrorMetadata(backend.Error.Code)
				status.BackendWillRetry = backend.WillRetry
				status.BackendErrorCount++
				status.LastBackendErrorAt = status.LastNotificationAt
				if backend.WillRetry != nil && *backend.WillRetry {
					emit("backend_retrying")
				} else {
					emit("backend_error")
				}
				continue
			}
			var e struct {
				ThreadID   string          `json:"threadId"`
				TurnID     string          `json:"turnId"`
				Turn       turn            `json:"turn"`
				Item       item            `json:"item"`
				Delta      string          `json:"delta"`
				Status     json.RawMessage `json:"status"`
				TokenUsage struct {
					Last Usage `json:"last"`
				} `json:"tokenUsage"`
			}
			if err = json.Unmarshal(n.Params, &e); err != nil {
				return result, fmt.Errorf("codex notification: %w", err)
			}
			if e.ThreadID != m.ThreadID {
				continue
			}
			if e.TurnID != "" && e.TurnID != started.Turn.ID {
				continue
			}
			status.LastNotification = n.Method
			status.LastNotificationAt = time.Now()
			if progressNotification(n.Method, e.Item, e.Delta != "") {
				status.LastProgress = status.LastNotificationAt
				if time.Since(lastProgressEmit) >= time.Second {
					emit("progress")
					lastProgressEmit = time.Now()
				}
			}
			switch n.Method {
			case "thread/status/changed":
				var threadStatus struct {
					Type        string   `json:"type"`
					ActiveFlags []string `json:"activeFlags"`
				}
				if err = json.Unmarshal(e.Status, &threadStatus); err != nil {
					return result, fmt.Errorf("codex thread status: %w", err)
				}
				switch threadStatus.Type {
				case "notLoaded", "idle", "systemError", "active":
					status.ThreadState = threadStatus.Type
				}
				status.WaitingFlags = nil
				for _, flag := range threadStatus.ActiveFlags {
					if flag == "waitingOnApproval" || flag == "waitingOnUserInput" {
						status.WaitingFlags = append(status.WaitingFlags, flag)
					}
				}
				emit("thread_state")
			case "item/started", "item/completed":
				if e.TurnID != started.Turn.ID {
					continue
				}
				if unsupportedItem(e.Item.Type) {
					return result, errors.New("codex advisory role attempted unsupported tool")
				}
				if n.Method == "item/completed" && e.Item.Type == "agentMessage" && (e.Item.Phase == "final_answer" || e.Item.Phase == "") {
					text = e.Item.Text
					if len(text) > maxOutput {
						return result, errors.New("codex output exceeds 256KiB")
					}
				}
			case "thread/tokenUsage/updated":
				if e.TurnID == started.Turn.ID {
					usage = e.TokenUsage.Last
					usage.Known = true
				}
			case "turn/completed":
				if e.Turn.ID != started.Turn.ID {
					continue
				}
				if e.Turn.Status != "completed" {
					return result, fmt.Errorf("codex turn ended %s", e.Turn.Status)
				}
				for _, v := range e.Turn.Items {
					if unsupportedItem(v.Type) {
						return result, errors.New("codex advisory role reported unsupported tool")
					}
					if v.Type == "agentMessage" && (v.Phase == "final_answer" || v.Phase == "") {
						text = v.Text
					}
				}
				if text == "" || len(text) > maxOutput {
					return result, errors.New("codex empty or oversized final output")
				}
				completed = true
				emit("finishing")
				if probe == nil {
					break waitTurn
				}
			}
		}
	}
	var unsubscribed struct {
		Status string `json:"status"`
	}
	emit("unloading")
	if err = controlCall("thread/unsubscribe", map[string]any{"threadId": m.ThreadID}, &unsubscribed); err != nil {
		return result, err
	}
	if unsubscribed.Status != "unsubscribed" && unsubscribed.Status != "notSubscribed" && unsubscribed.Status != "notLoaded" {
		return result, errors.New("codex invalid unsubscribe response")
	}
	m.ActiveTask = ""
	if err = writeMapping(path, m); err != nil {
		return result, err
	}
	a.mu.Lock()
	a.usage.CompletedTurns++
	if usage.Known {
		a.usage.MeasuredTurns++
		a.usage.InputTokens += usage.InputTokens
		a.usage.OutputTokens += usage.OutputTokens
		a.usage.CachedInputTokens += usage.CachedInputTokens
	}
	a.usage.Known = !a.usageIncomplete && a.usage.CompletedTurns == a.usage.MeasuredTurns
	a.mu.Unlock()
	result.Text = text
	emit("completed")
	return result, nil
}

func supportedNotification(method string) bool {
	switch method {
	case "error", "thread/status/changed", "item/started", "item/completed", "thread/tokenUsage/updated", "turn/completed", "item/agentMessage/delta", "item/reasoning/textDelta", "item/reasoning/summaryTextDelta", "item/commandExecution/outputDelta":
		return true
	}
	return false
}

func backendErrorMetadata(raw json.RawMessage) (string, int) {
	var code string
	if json.Unmarshal(raw, &code) == nil {
		switch code {
		case "contextWindowExceeded", "sessionBudgetExceeded", "usageLimitExceeded", "rateLimitExceeded", "flexUnavailable", "serverOverloaded", "modelNotFound", "policyCapacityUnavailable", "cyberPolicy", "misalignmentPolicyViolation", "tooManyDenials", "internalServerError", "unauthorized", "badRequest", "threadRollbackFailed", "sandboxError", "other":
			return code, 0
		}
		return "unknown", 0
	}
	var variants map[string]json.RawMessage
	if json.Unmarshal(raw, &variants) == nil && len(variants) == 1 {
		for _, name := range []string{"httpConnectionFailed", "responseStreamConnectionFailed", "responseStreamDisconnected", "responseTooManyFailedAttempts", "activeTurnNotSteerable"} {
			if data, ok := variants[name]; ok {
				var details struct {
					HTTPStatus *int `json:"httpStatusCode"`
				}
				if json.Unmarshal(data, &details) == nil && details.HTTPStatus != nil && *details.HTTPStatus >= 100 && *details.HTTPStatus <= 599 {
					return name, *details.HTTPStatus
				}
				return name, 0
			}
		}
	}
	return "unknown", 0
}

func progressNotification(method string, v item, hasDelta bool) bool {
	switch method {
	case "item/agentMessage/delta", "item/reasoning/textDelta", "item/reasoning/summaryTextDelta", "item/commandExecution/outputDelta":
		return hasDelta
	case "item/completed":
		return v.Type == "agentMessage" || v.Type == "reasoning" || v.Type == "commandExecution"
	}
	return false
}

func unsupportedItem(kind string) bool {
	switch kind {
	case "commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall", "webSearch", "collabAgentToolCall", "subAgentActivity", "imageGeneration", "imageView":
		return true
	}
	return false
}

func readMapping(path string) (m mapping, err error) {
	f, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 8193))
	if err != nil {
		return m, err
	}
	if len(b) > 8192 {
		return m, errors.New("oversized codex mapping")
	}
	err = json.Unmarshal(b, &m)
	return m, err
}
func writeMapping(path string, m mapping) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".codex-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
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
