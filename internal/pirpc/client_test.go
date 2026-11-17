package pirpc

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

// Real subprocess, fake protocol peer. Tests command ordering and lifecycle
// failures independently of pi or an LLM provider.
func TestProtocolPeer(t *testing.T) {
	mode := os.Getenv("ROLE_RPC_PEER")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	var mu sync.Mutex
	send := func(v any) { mu.Lock(); defer mu.Unlock(); _ = json.NewEncoder(os.Stdout).Encode(v) }
	response := func(r map[string]any, data any) {
		send(map[string]any{"type": "response", "id": r["id"], "command": r["type"], "success": true, "data": data})
	}
	finish := func(text string) {
		send(map[string]any{"type": "message_end", "message": map[string]any{"role": "assistant", "stopReason": "stop", "content": []any{map[string]any{"type": "text", "text": text}}}})
		send(map[string]any{"type": "agent_settled", "aborted": false})
	}
	var first map[string]any
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var r map[string]any
		if json.Unmarshal(scanner.Bytes(), &r) != nil {
			os.Exit(2)
		}
		switch r["type"] {
		case "reverse":
			if first == nil {
				first = r
			} else {
				response(r, r["value"])
				response(first, first["value"])
				first = nil
			}
		case "get_state":
			response(r, map[string]any{"ready": true})
		case "prompt":
			input, _ := r["message"].(string)
			if input == "fast" {
				finish("hello\u2028world")
				response(r, map[string]string{"disposition": "started"})
				continue
			}
			response(r, map[string]string{"disposition": "started"})
			switch input {
			case "flood":
				for i := 0; i < 10000; i++ {
					send(map[string]any{"type": "message_update", "delta": "ignored"})
				}
				finish("bounded")
			case "large":
				finish(strings.Repeat("x", 2048))
			case "error":
				send(map[string]any{"type": "message_end", "message": map[string]any{"role": "assistant", "stopReason": "error", "errorMessage": "provider failed", "content": []any{}}})
				send(map[string]any{"type": "agent_settled"})
			}
		case "release":
			response(r, nil)
			finish("released")
		case "abort":
			response(r, nil)
			if mode != "stuck" {
				time.Sleep(40 * time.Millisecond)
				send(map[string]any{"type": "agent_settled", "aborted": true})
			}
		case "oversize":
			response(r, strings.Repeat("x", 10000))
		case "invalid":
			fmt.Fprintln(os.Stdout, "not-json")
		case "exit":
			os.Exit(7)
		default:
			response(r, nil)
		}
	}
	os.Exit(0)
}
func peer(t *testing.T, mode string, edit func(*Config)) *Client {
	t.Helper()
	cfg := Config{Command: os.Args[0], Args: []string{"-test.run=^TestProtocolPeer$"}, Env: append(os.Environ(), "ROLE_RPC_PEER="+mode), CancelGrace: 100 * time.Millisecond}
	if edit != nil {
		edit(&cfg)
	}
	c, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func TestCorrelatesOutOfOrderResponses(t *testing.T) {
	c := peer(t, "normal", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, value := range []string{"alpha", "beta"} {
		wg.Add(1)
		go func(v string) {
			defer wg.Done()
			var got string
			if err := c.Call(ctx, "reverse", map[string]any{"value": v}, &got); err != nil {
				t.Error(err)
			} else if got != v {
				t.Errorf("got %q want %q", got, v)
			}
		}(value)
	}
	wg.Wait()
}
func TestCompletionBeforeAcknowledgement(t *testing.T) {
	c := peer(t, "normal", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	got, err := c.Prompt(ctx, "fast")
	if err != nil || got != "hello\u2028world" {
		t.Fatalf("%q %v", got, err)
	}
}
func TestAcceptanceIsNotCompletion(t *testing.T) {
	c := peer(t, "normal", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := c.Prompt(ctx, "wait"); result <- err }()
	// Wait until a prompt is registered; an acknowledgement alone cannot finish it.
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		active := c.active != nil
		c.mu.Unlock()
		if active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("prompt not started")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := c.Prompt(ctx, "fast"); !errors.Is(err, ErrBusy) {
		t.Fatalf("concurrent prompt: %v", err)
	}
	select {
	case err := <-result:
		t.Fatalf("completed before settled: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := c.Call(ctx, "release", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
func TestCancellationWaitsForSettled(t *testing.T) {
	c := peer(t, "normal", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Prompt(ctx, "wait")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if time.Since(start) < 110*time.Millisecond {
		t.Fatal("slot released before abort settled")
	}
	if !c.Alive() {
		t.Fatal("cooperative cancellation should retain process")
	}
	next, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err := c.Prompt(next, "fast"); err != nil {
		t.Fatal(err)
	}
}
func TestUncooperativeCancellationKillsAndReaps(t *testing.T) {
	c := peer(t, "stuck", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, err := c.Prompt(ctx, "wait"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if c.Alive() {
		t.Fatal("unsettled process still alive when slot returned")
	}
}
func TestProtocolFailures(t *testing.T) {
	for _, command := range []string{"oversize", "invalid", "exit"} {
		t.Run(command, func(t *testing.T) {
			c := peer(t, "normal", func(cfg *Config) { cfg.MaxRecordBytes = 512 })
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := c.Call(ctx, command, nil, nil); err == nil {
				t.Fatal("expected protocol failure")
			}
		})
	}
}
func TestBoundedOutputAndEventDrain(t *testing.T) {
	t.Run("flood", func(t *testing.T) {
		c := peer(t, "normal", nil)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if text, err := c.Prompt(ctx, "flood"); err != nil || text != "bounded" {
			t.Fatalf("%q %v", text, err)
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.pending) != 0 || c.active != nil {
			t.Fatal("retained completed run")
		}
	})
	t.Run("output", func(t *testing.T) {
		c := peer(t, "normal", func(cfg *Config) { cfg.MaxOutputBytes = 128 })
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := c.Prompt(ctx, "large"); err == nil {
			t.Fatal("oversized output accepted")
		}
	})
	t.Run("model failure", func(t *testing.T) {
		c := peer(t, "normal", nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := c.Prompt(ctx, "error"); err == nil {
			t.Fatal("model failure treated as success")
		}
	})
}
func TestStderrTailBound(t *testing.T) {
	tail := tail{limit: 10}
	for _, text := range []string{"abcdef", "ghijk", strings.Repeat("z", 100)} {
		tail.Write([]byte(text))
		if len(tail.data) > 10 {
			t.Fatal("stderr unbounded")
		}
	}
}
