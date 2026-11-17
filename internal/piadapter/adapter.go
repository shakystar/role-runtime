// Package piadapter binds one reusable pi process to role-runtime activations.
package piadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/shakystar/role-runtime/internal/engine"
	"github.com/shakystar/role-runtime/internal/pirpc"
)

type Config struct {
	RPC            pirpc.Config
	Resolver       engine.Resolver
	Store          *engine.Store
	TaskTimeout    time.Duration
	StartupTimeout time.Duration
	OnProcessStart func(int)
}
type Adapter struct {
	mu     sync.Mutex
	cfg    Config
	client *pirpc.Client
	Starts int
}

func New(cfg Config) (*Adapter, error) {
	if cfg.Resolver == nil || cfg.Store == nil {
		return nil, errors.New("adapter requires resolver and store")
	}
	if cfg.TaskTimeout <= 0 {
		cfg.TaskTimeout = time.Minute
	}
	if cfg.StartupTimeout <= 0 {
		cfg.StartupTimeout = 15 * time.Second
	}
	a := &Adapter{cfg: cfg}
	if err := a.start(context.Background()); err != nil {
		return nil, err
	}
	return a, nil
}
func (a *Adapter) start(parent context.Context) error {
	client, err := pirpc.Start(a.cfg.RPC)
	if err != nil {
		return err
	}
	a.Starts++
	if a.cfg.OnProcessStart != nil {
		a.cfg.OnProcessStart(client.PID())
	}
	ctx, cancel := context.WithTimeout(parent, a.cfg.StartupTimeout)
	defer cancel()
	if err = client.Call(ctx, "get_state", nil, nil); err != nil {
		client.Close()
		return err
	}
	a.mu.Lock()
	a.client = client
	a.mu.Unlock()
	return nil
}
func (a *Adapter) Close() error {
	if a.client != nil {
		err := a.client.Close()
		a.mu.Lock()
		a.client = nil
		a.mu.Unlock()
		return err
	}
	return nil
}
func (a *Adapter) Execute(parent context.Context, t engine.Task) (result engine.Result, err error) {
	ctx, cancel := context.WithTimeout(parent, a.cfg.TaskTimeout)
	defer cancel()
	activation, err := a.cfg.Resolver.Resolve(ctx, t)
	if err != nil {
		return result, err
	}
	if a.client == nil || !a.client.Alive() {
		if err = a.start(ctx); err != nil {
			return result, err
		}
	}
	result.PID = a.client.PID()
	result.Session = activation.SessionPath
	var switched struct {
		Cancelled bool `json:"cancelled"`
	}
	if err = a.client.Call(ctx, "switch_session", map[string]any{"sessionPath": activation.SessionPath}, &switched); err != nil {
		a.Close()
		return result, err
	}
	if switched.Cancelled {
		a.Close()
		return result, errors.New("pi activation canceled")
	}
	var state struct {
		SessionID   string `json:"sessionId"`
		SessionFile string `json:"sessionFile"`
		IsStreaming bool   `json:"isStreaming"`
	}
	if err = a.client.Call(ctx, "get_state", nil, &state); err != nil {
		a.Close()
		return result, err
	}
	if state.SessionID != activation.SessionID || state.SessionFile != activation.SessionPath || state.IsStreaming {
		a.Close()
		return result, errors.New("pi activation identity/idle mismatch")
	}
	result.Text, err = a.client.Prompt(ctx, t.Input)
	if err != nil {
		// An interrupted tool may already have produced side effects. Do not silently
		// replay the same logical role session on a subsequent task.
		if qerr := a.cfg.Store.Quarantine(t, err); qerr != nil {
			return result, fmt.Errorf("%w; quarantine: %v", err, qerr)
		}
	}
	return result, err
}

// Heartbeat probes the actual RPC process. It is safe concurrently with Execute;
// Close remains owned by the execution worker after that execution has stopped.
func (a *Adapter) Heartbeat(ctx context.Context) error {
	a.mu.Lock()
	client := a.client
	a.mu.Unlock()
	if client == nil {
		return errors.New("pi process unavailable")
	}
	return client.Call(ctx, "get_state", nil, nil)
}
