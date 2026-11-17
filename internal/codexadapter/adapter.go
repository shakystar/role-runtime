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
	RPC            codexrpc.Config
	Store          *engine.Store
	TaskTimeout    time.Duration
	StartupTimeout time.Duration
	Model          string
	OnProcessStart func(int)
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
	if cfg.TaskTimeout <= 0 {
		cfg.TaskTimeout = 2 * time.Minute
	}
	if cfg.StartupTimeout <= 0 {
		cfg.StartupTimeout = 20 * time.Second
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
	a.client = c
	a.Starts++
	if a.cfg.OnProcessStart != nil {
		a.cfg.OnProcessStart(c.PID())
	}
	return nil
}
func (a *Adapter) Close() error {
	if a.client == nil {
		return nil
	}
	err := a.client.Close()
	a.client = nil
	return err
}
func (a *Adapter) Usage() Usage { a.mu.Lock(); defer a.mu.Unlock(); return a.usage }
func (a *Adapter) Execute(parent context.Context, t engine.Task) (result engine.Result, err error) {
	ctx, cancel := context.WithTimeout(parent, a.cfg.TaskTimeout)
	defer cancel()
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
	if err = a.client.Call(ctx, "config/read", map[string]any{"cwd": m.Workspace, "includeLayers": false}, &effective); err != nil {
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
	if err = a.client.Call(ctx, method, params, &response); err != nil {
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
	if err = a.client.Call(ctx, "turn/start", map[string]any{"threadId": m.ThreadID, "input": []any{map[string]any{"type": "text", "text": t.Input}}, "approvalPolicy": "never", "sandboxPolicy": map[string]any{"type": "readOnly", "networkAccess": false}}, &started); err != nil {
		return result, err
	}
	if started.Turn.ID == "" {
		return result, errors.New("codex missing turn ID")
	}
	var text string
	var usage Usage
	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-a.client.Done():
			return result, errors.Join(errors.New("codex exited during turn"), a.client.Err())
		case n, ok := <-a.client.Notifications():
			if !ok {
				return result, errors.New("codex notification stream closed")
			}
			var e struct {
				ThreadID   string `json:"threadId"`
				TurnID     string `json:"turnId"`
				Turn       turn   `json:"turn"`
				Item       item   `json:"item"`
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
			switch n.Method {
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
				var unsubscribed struct {
					Status string `json:"status"`
				}
				if err = a.client.Call(ctx, "thread/unsubscribe", map[string]any{"threadId": m.ThreadID}, &unsubscribed); err != nil {
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
				return result, nil
			}
		}
	}
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
