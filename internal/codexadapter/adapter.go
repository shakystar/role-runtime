// Package codexadapter binds the reusable adapter to role-runtime definitions.
package codexadapter

import (
	"context"
	"encoding/json"
	"errors"
	sdk "github.com/shakystar/codex-adapter"
	codexrpc "github.com/shakystar/codex-adapter/rpc"
	"github.com/shakystar/role-runtime/internal/engine"
	"time"
)

type Config struct {
	StartupAttempts   int
	RetryBackoff      time.Duration
	RPC               codexrpc.Config
	Store             *engine.Store
	TaskTimeout       time.Duration
	StartupTimeout    time.Duration
	Model             string
	ReasoningEffort   string
	OutputSchema      json.RawMessage
	OnProcessStart    func(int)
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	// OnStatus runs on the execution worker and must return promptly without blocking.
	// It receives metadata only, never prompts, output, reasoning, or config values.
	OnStatus func(Status)
}

type Status = sdk.Status
type Usage = sdk.Usage
type Adapter struct {
	*sdk.Adapter
	store *engine.Store
}

func New(c Config) (*Adapter, error) {
	if c.Store == nil {
		return nil, errors.New("codex adapter requires store")
	}
	a, err := sdk.New(sdk.Config{RPC: c.RPC, StateDir: c.Store.Root, TaskTimeout: c.TaskTimeout, StartupTimeout: c.StartupTimeout, StartupAttempts: c.StartupAttempts, RetryBackoff: c.RetryBackoff, Model: c.Model, ReasoningEffort: c.ReasoningEffort, OutputSchema: c.OutputSchema, OnProcessStart: c.OnProcessStart, HeartbeatInterval: c.HeartbeatInterval, HeartbeatTimeout: c.HeartbeatTimeout, OnStatus: c.OnStatus})
	if err != nil {
		return nil, err
	}
	return &Adapter{a, c.Store}, nil
}
func (a *Adapter) Execute(ctx context.Context, t engine.Task) (engine.Result, error) {
	activation, err := a.store.ResolveDefinitions(ctx, t)
	if err != nil {
		return engine.Result{}, err
	}
	r, err := a.Adapter.Execute(ctx, sdk.Request{ID: t.ID, Role: t.Role, SessionKey: t.Key(), Workspace: activation.Project.Workspace, Instructions: activation.Project.Rules + "\n\n" + activation.Role.Instruction, Tools: activation.Role.Tools, Input: t.Input})
	if err != nil && requiresQuarantine(err) {
		err = errors.Join(err, a.store.Quarantine(t, err))
	}
	result := engine.Result{Text: r.Text, Session: r.Session, PID: r.PID, Failure: engine.FailureMetadata(err)}
	if err == nil {
		result.ActualMemberID = activation.Role.ID
		result.ActualRoleVersion = activation.Role.Version
		result.TurnID = r.TurnID
		if r.TurnID != "" && r.TurnUsage != nil {
			result.TurnUsage = &engine.TurnUsage{
				InputTokens:       r.TurnUsage.InputTokens,
				OutputTokens:      r.TurnUsage.OutputTokens,
				CachedInputTokens: r.TurnUsage.CachedInputTokens,
			}
		}
	}
	return result, err
}

// Pre-dispatch failures do not poison a role session. Unknown/unclassified errors
// remain conservative. This does not resubmit failed tasks or clear receipts.
func requiresQuarantine(err error) bool {
	f := sdk.FailureOf(err)
	return f == nil || f.Execution != sdk.NotStarted
}
