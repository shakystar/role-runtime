// Package codexrpc owns one persistent Codex app-server stdio process.
// The wire format was checked against codex-cli 0.166.0 generated schemas:
// JSONL requests/responses have id/method/params/result/error, without a required
// jsonrpc field. Notifications are bounded and must be drained by the owner.
package codexrpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

var ErrClosed = errors.New("codex RPC closed")

type Config struct {
	Command                   string
	Args                      []string
	Dir                       string
	Env                       []string
	MaxRecordBytes            int
	MaxPending                int
	NotificationBuffer        int
	StderrBytes               int
	StartupTimeout            time.Duration
	OptOutNotificationMethods []string
}

type Notification struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// RPCError omits server-provided message/data from Error to avoid accidentally
// logging private prompt or tool contents. Callers may inspect Code.
type RPCError struct {
	Code int `json:"code"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("codex RPC error %d", e.Code) }

type record struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *RPCError       `json:"error"`
}
type writeRequest struct {
	data []byte
	ack  chan error
}
type Client struct {
	cfg           Config
	cmd           *exec.Cmd
	stdin         io.WriteCloser
	writes        chan writeRequest
	notifications chan Notification
	stop          chan struct{}
	done          chan struct{}
	writerDone    chan struct{}
	once          sync.Once
	mu            sync.Mutex
	seq           uint64
	pending       map[string]chan record
	terminal      error
	stderr        tail
}

// Start completes initialize and writes initialized before exposing the client.
// Context cancellation after Start returns is not a process lifetime signal;
// use Call contexts and Close. This lets a startup deadline expire harmlessly.
func Start(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.MaxRecordBytes == 0 {
		cfg.MaxRecordBytes = 1 << 20
	}
	if cfg.MaxPending == 0 {
		cfg.MaxPending = 8
	}
	if cfg.NotificationBuffer == 0 {
		cfg.NotificationBuffer = 64
	}
	if cfg.StderrBytes == 0 {
		cfg.StderrBytes = 16 << 10
	}
	if cfg.StartupTimeout == 0 {
		cfg.StartupTimeout = 15 * time.Second
	}
	if cfg.MaxRecordBytes < 256 || cfg.MaxRecordBytes > 8<<20 || cfg.MaxPending < 1 || cfg.MaxPending > 64 || cfg.NotificationBuffer < 1 || cfg.NotificationBuffer > 256 || cfg.StderrBytes < 1 || cfg.StderrBytes > 64<<10 || cfg.StartupTimeout <= 0 {
		return nil, errors.New("invalid codex RPC limits")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.Command == "" {
		cfg.Command = "codex"
	}
	if cfg.Args == nil {
		cfg.Args = []string{"app-server"}
	}
	c := &Client{cfg: cfg, writes: make(chan writeRequest, cfg.MaxPending), notifications: make(chan Notification, cfg.NotificationBuffer), stop: make(chan struct{}), done: make(chan struct{}), writerDone: make(chan struct{}), pending: make(map[string]chan record)}
	c.stderr.limit = cfg.StderrBytes
	c.cmd = exec.Command(cfg.Command, cfg.Args...)
	c.cmd.Dir, c.cmd.Env, c.cmd.Stderr = cfg.Dir, cfg.Env, &c.stderr
	c.cmd.WaitDelay = time.Second
	configureProcess(c.cmd)
	var err error
	c.stdin, err = c.cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := c.cmd.StdoutPipe()
	if err != nil {
		c.stdin.Close()
		return nil, err
	}
	if err = c.cmd.Start(); err != nil {
		c.stdin.Close()
		stdout.Close()
		return nil, err
	}
	go c.writeLoop()
	go func() {
		c.readLoop(stdout)
		c.fail(errors.New("codex RPC stdout closed"))
		_ = c.cmd.Wait()
		<-c.writerDone
		close(c.notifications)
		close(c.done)
	}()
	startup, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()
	params := map[string]any{"clientInfo": map[string]string{"name": "role_runtime", "version": "0.1.0"}, "capabilities": map[string]any{"experimentalApi": false, "explicitGatewayOauth": true, "optOutNotificationMethods": cfg.OptOutNotificationMethods}}
	if err = c.Call(startup, "initialize", params, nil); err == nil {
		err = c.send(startup, map[string]string{"method": "initialized"})
	}
	if err != nil {
		c.fail(err)
		<-c.done
		return nil, err
	}
	return c, nil
}

func (c *Client) PID() int { return c.cmd.Process.Pid }

// Alive remains true until the process is reaped. Err can become non-nil first.
func (c *Client) Alive() bool {
	select {
	case <-c.done:
		return false
	default:
		return true
	}
}
func (c *Client) Done() <-chan struct{}              { return c.done }
func (c *Client) Notifications() <-chan Notification { return c.notifications }
func (c *Client) Err() error                         { c.mu.Lock(); defer c.mu.Unlock(); return c.terminal }
func (c *Client) fail(err error) {
	c.mu.Lock()
	if c.terminal == nil {
		c.terminal = err
	}
	c.mu.Unlock()
	c.once.Do(func() { close(c.stop); killProcess(c.cmd); _ = c.stdin.Close() })
}

// Close is idempotent and waits for both process reap and protocol goroutines.
// No pending turn is considered successfully completed by closing its process.
func (c *Client) Close() error { c.fail(ErrClosed); <-c.done; return nil }

func (c *Client) writeLoop() {
	defer close(c.writerDone)
	for {
		select {
		case <-c.stop:
			return
		case request := <-c.writes:
			_, err := c.stdin.Write(request.data)
			if request.ack != nil {
				request.ack <- err
			}
			if err != nil {
				c.fail(errors.New("codex RPC stdin write failed"))
				return
			}
		}
	}
}

func (c *Client) encode(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, errors.New("codex RPC request encoding failed")
	}
	if len(data)+1 > c.cfg.MaxRecordBytes {
		return nil, errors.New("codex RPC request exceeds byte limit")
	}
	return append(data, '\n'), nil
}
func (c *Client) send(ctx context.Context, v any) error {
	data, err := c.encode(v)
	if err != nil {
		return err
	}
	r := writeRequest{data: data, ack: make(chan error, 1)}
	select {
	case c.writes <- r:
	case <-c.stop:
		<-c.done
		return c.Err()
	case <-ctx.Done():
		c.fail(ctx.Err())
		<-c.done
		return ctx.Err()
	}
	select {
	case err = <-r.ack:
		if err != nil {
			c.fail(errors.New("codex RPC stdin write failed"))
			<-c.done
			return c.Err()
		}
		return err
	case <-c.stop:
		<-c.done
		return c.Err()
	case <-ctx.Done():
		c.fail(ctx.Err())
		<-c.done
		return ctx.Err()
	}
}

// Call may run concurrently (up to MaxPending). Cancellation after dispatch
// poisons and reaps the whole process: its side effects are uncertain.
func (c *Client) Call(ctx context.Context, method string, params any, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if method == "" {
		return errors.New("empty codex RPC method")
	}
	c.mu.Lock()
	if c.terminal != nil {
		err := c.terminal
		c.mu.Unlock()
		return err
	}
	if len(c.pending) >= c.cfg.MaxPending {
		c.mu.Unlock()
		return errors.New("codex RPC pending limit")
	}
	c.seq++
	id := strconv.FormatUint(c.seq, 10)
	ch := make(chan record, 1)
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, id); c.mu.Unlock() }()
	if err := c.send(ctx, map[string]any{"id": id, "method": method, "params": params}); err != nil {
		return err
	}
	select {
	case r := <-ch:
		if err := c.Err(); err != nil {
			<-c.done
			return err
		}
		if err := ctx.Err(); err != nil {
			c.fail(err)
			<-c.done
			return err
		}
		if r.Error != nil {
			return r.Error
		}
		if out != nil {
			if err := json.Unmarshal(r.Result, out); err != nil {
				return errors.New("codex RPC result decode failed")
			}
		}
		return nil
	case <-ctx.Done():
		c.fail(ctx.Err())
		<-c.done
		return ctx.Err()
	case <-c.stop:
		<-c.done
		return c.Err()
	}
}

func validID(id json.RawMessage) bool {
	if string(id) == "null" {
		return false
	}
	var s string
	if json.Unmarshal(id, &s) == nil {
		return true
	}
	var n int64
	return json.Unmarshal(id, &n) == nil && string(id) != "null"
}

func (c *Client) readLoop(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	initial := min(4096, c.cfg.MaxRecordBytes)
	scanner.Buffer(make([]byte, initial), c.cfg.MaxRecordBytes)
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		var r record
		if json.Unmarshal(scanner.Bytes(), &r) != nil {
			c.fail(errors.New("invalid codex RPC JSONL"))
			return
		}
		hasID := len(r.ID) > 0
		if hasID && !validID(r.ID) {
			c.fail(errors.New("invalid codex RPC id"))
			return
		}
		if r.Method != "" {
			if r.Result != nil || r.Error != nil {
				c.fail(errors.New("invalid codex RPC method record"))
				return
			}
			if hasID {
				// Includes approval, dynamic-tool, elicitation and auth-refresh requests.
				// Explicitly reject, never approve or wait for an unattended user.
				data, err := c.encode(map[string]any{"id": r.ID, "error": map[string]any{"code": -32601, "message": "Client does not support server requests"}})
				if err != nil {
					c.fail(err)
					return
				}
				select {
				case c.writes <- writeRequest{data: data}:
				case <-c.stop:
					return
				default:
					c.fail(errors.New("codex RPC server request queue limit"))
					return
				}
			} else {
				select {
				case c.notifications <- Notification{Method: r.Method, Params: r.Params}:
				case <-c.stop:
					return
				default:
					c.fail(errors.New("codex RPC notification buffer limit"))
					return
				}
			}
			continue
		}
		if !hasID || (r.Error == nil) == (r.Result == nil) {
			c.fail(errors.New("invalid codex RPC response"))
			return
		}
		// We issue string IDs; accepting numeric equivalents would hide a peer bug.
		var id string
		if json.Unmarshal(r.ID, &id) != nil {
			c.fail(errors.New("unexpected codex RPC response id"))
			return
		}
		c.mu.Lock()
		ch := c.pending[id]
		c.mu.Unlock()
		if ch == nil {
			c.fail(errors.New("unknown codex RPC response id"))
			return
		}
		select {
		case ch <- r:
		default:
			c.fail(errors.New("duplicate codex RPC response"))
			return
		}
	}
	if scanner.Err() != nil {
		c.fail(errors.New("codex RPC record read or byte limit"))
	}
}

// Bounded private diagnostic tail; never interpolated into returned errors.
type tail struct {
	mu    sync.Mutex
	data  []byte
	limit int
}

func (t *tail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := len(p)
	if len(p) >= t.limit {
		t.data = append(t.data[:0], p[len(p)-t.limit:]...)
		return n, nil
	}
	if excess := len(t.data) + len(p) - t.limit; excess > 0 {
		t.data = t.data[excess:]
	}
	t.data = append(t.data, p...)
	return n, nil
}
