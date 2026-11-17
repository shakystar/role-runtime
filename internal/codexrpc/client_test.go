package codexrpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// A real subprocess with a fake Codex protocol: no model, account, or network.
func TestProtocolPeer(t *testing.T) {
	mode := os.Getenv("ROLE_CODEX_RPC_PEER")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	send := func(v any) { _ = json.NewEncoder(os.Stdout).Encode(v) }
	respond := func(r record, v any) { send(map[string]any{"id": r.ID, "result": v}) }
	var first *record
	var denied *record
	initialized := false
	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		var r record
		if json.Unmarshal(s.Bytes(), &r) != nil {
			os.Exit(2)
		}
		if r.Method == "initialize" {
			if mode == "startup-hang" {
				continue
			}
			if mode == "startup-error" {
				send(map[string]any{"id": r.ID, "error": map[string]any{"code": -32000, "message": "private-secret"}})
				continue
			}
			var params struct {
				ClientInfo struct {
					Name    string `json:"name"`
					Version string `json:"version"`
				} `json:"clientInfo"`
			}
			if json.Unmarshal(r.Params, &params) != nil || params.ClientInfo.Name != "role_runtime" || params.ClientInfo.Version == "" {
				os.Exit(3)
			}
			respond(r, map[string]any{"userAgent": "fake", "platformFamily": "unix", "platformOs": "fake", "codexHome": "/fake"})
			continue
		}
		if r.Method == "initialized" {
			initialized = true
			continue
		}
		if !initialized {
			os.Exit(4)
		}
		if r.Method == "" && denied != nil {
			if r.Error == nil || r.Error.Code != -32601 || string(r.ID) != "77" {
				os.Exit(5)
			}
			respond(*denied, "denied")
			denied = nil
			continue
		}
		switch r.Method {
		case "echo":
			respond(r, r.Params)
		case "reverse":
			if first == nil {
				copy := r
				first = &copy
			} else {
				respond(r, r.Params)
				respond(*first, first.Params)
				first = nil
			}
		case "wait":
			send(map[string]any{"method": "waiting", "params": nil})
		case "error":
			send(map[string]any{"id": r.ID, "error": map[string]any{"code": -32001, "message": "private-secret"}})
		case "approve":
			copy := r
			denied = &copy
			send(map[string]any{"id": 77, "method": "item/commandExecution/requestApproval", "params": map[string]string{"command": "do not execute"}})
		case "notify":
			send(map[string]any{"method": "turn/completed", "params": map[string]string{"id": "turn-1"}})
			respond(r, true)
		case "flood":
			for i := 0; i < 1000; i++ {
				send(map[string]any{"method": "delta", "params": "x"})
			}
			respond(r, nil)
		case "oversize":
			respond(r, strings.Repeat("x", 8192))
		case "invalid":
			fmt.Fprintln(os.Stdout, "not-json")
		case "shape":
			send(map[string]any{"id": r.ID})
		case "null-id":
			send(map[string]any{"id": nil, "result": true})
		case "unknown":
			send(map[string]any{"id": "never-issued", "result": true})
		case "exit":
			os.Exit(7)
		case "stderr":
			fmt.Fprint(os.Stderr, strings.Repeat("private", 10000))
			respond(r, nil)
		default:
			respond(r, nil)
		}
	}
	os.Exit(0)
}

func peerConfig(mode string) Config {
	return Config{Command: os.Args[0], Args: []string{"-test.run=^TestProtocolPeer$"}, Env: append(os.Environ(), "ROLE_CODEX_RPC_PEER="+mode), StartupTimeout: 3 * time.Second}
}
func peer(t *testing.T, edit func(*Config)) *Client {
	t.Helper()
	cfg := peerConfig("normal")
	if edit != nil {
		edit(&cfg)
	}
	c, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestHandshakeAndCorrelation(t *testing.T) {
	c := peer(t, nil)
	ctx := testContext(t)
	var got string
	if err := c.Call(ctx, "echo", "ready", &got); err != nil || got != "ready" {
		t.Fatalf("handshake: %q %v", got, err)
	}
	var wg sync.WaitGroup
	for _, v := range []string{"a", "b"} {
		wg.Add(1)
		go func(v string) {
			defer wg.Done()
			var out string
			if err := c.Call(ctx, "reverse", v, &out); err != nil || out != v {
				t.Errorf("correlation: %q %v", out, err)
			}
		}(v)
	}
	wg.Wait()
	if c.PID() <= 0 || !c.Alive() {
		t.Fatal("process missing")
	}
}

func TestNotificationsAndServerRequests(t *testing.T) {
	c := peer(t, nil)
	ctx := testContext(t)
	if err := c.Call(ctx, "notify", nil, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-c.Notifications():
		if n.Method != "turn/completed" {
			t.Fatal(n.Method)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var out string
	if err := c.Call(ctx, "approve", nil, &out); err != nil || out != "denied" {
		t.Fatalf("server request was not rejected: %q %v", out, err)
	}
}

func TestRPCErrorDoesNotExposeContents(t *testing.T) {
	c := peer(t, nil)
	err := c.Call(testContext(t), "error", nil, nil)
	var rpc *RPCError
	if !errors.As(err, &rpc) || rpc.Code != -32001 || strings.Contains(err.Error(), "private") {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := c.Call(testContext(t), "echo", nil, nil); err != nil {
		t.Fatal("ordinary RPC error should not poison process", err)
	}
}

func TestCancelReapsAndCannotReuse(t *testing.T) {
	c := peer(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Call(ctx, "wait", nil, nil) }()
	select {
	case <-c.Notifications():
	case <-time.After(3 * time.Second):
		t.Fatal("not dispatched")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if c.Alive() {
		t.Fatal("returned before reap")
	}
	if c.Call(context.Background(), "echo", nil, nil) == nil {
		t.Fatal("reused poisoned client")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAlreadyCancelledDoesNotPoison(t *testing.T) {
	c := peer(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Call(ctx, "echo", nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := c.Call(testContext(t), "echo", nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestBoundsAndProtocolFailures(t *testing.T) {
	for _, method := range []string{"oversize", "invalid", "shape", "null-id", "unknown", "exit", "flood"} {
		t.Run(method, func(t *testing.T) {
			c := peer(t, func(cfg *Config) { cfg.MaxRecordBytes = 512; cfg.NotificationBuffer = 1 })
			if err := c.Call(testContext(t), method, nil, nil); err == nil {
				t.Fatal("expected failure")
			}
			c.Close()
			if c.Alive() {
				t.Fatal("not reaped")
			}
		})
	}
}

func TestRequestLimitAndPendingLimit(t *testing.T) {
	c := peer(t, func(cfg *Config) { cfg.MaxRecordBytes = 512; cfg.MaxPending = 1 })
	if err := c.Call(testContext(t), "echo", strings.Repeat("x", 1024), nil); err == nil {
		t.Fatal("oversized request accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Call(ctx, "wait", nil, nil) }()
	select {
	case <-c.Notifications():
	case <-time.After(3 * time.Second):
		t.Fatal("not dispatched")
	}
	if err := c.Call(testContext(t), "echo", nil, nil); err == nil || !strings.Contains(err.Error(), "pending limit") {
		t.Fatal(err)
	}
	cancel()
	<-done
}

func TestStartupFailure(t *testing.T) {
	for _, mode := range []string{"startup-hang", "startup-error"} {
		t.Run(mode, func(t *testing.T) {
			cfg := peerConfig(mode)
			cfg.StartupTimeout = 100 * time.Millisecond
			c, err := Start(context.Background(), cfg)
			if err == nil || c != nil {
				if c != nil {
					c.Close()
				}
				t.Fatal("startup unexpectedly succeeded")
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("private contents exposed")
			}
		})
	}
}

func TestCloseConcurrentAndStderrBounded(t *testing.T) {
	c := peer(t, func(cfg *Config) { cfg.StderrBytes = 32 })
	if err := c.Call(testContext(t), "stderr", nil, nil); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Close() }()
	}
	wg.Wait()
	c.stderr.mu.Lock()
	defer c.stderr.mu.Unlock()
	if len(c.stderr.data) > 32 {
		t.Fatal("stderr unbounded")
	}
	if c.Alive() {
		t.Fatal("not reaped")
	}
}

func TestConfigLimits(t *testing.T) {
	for _, cfg := range []Config{{MaxPending: -1}, {MaxRecordBytes: 1}, {NotificationBuffer: 257}, {StderrBytes: 65537}} {
		if c, err := Start(context.Background(), cfg); err == nil {
			c.Close()
			t.Fatal("invalid limits accepted")
		}
	}
}
