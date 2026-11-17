// Package pirpc controls one pi JSONL process. Streaming deltas are drained,
// not retained. Only bounded command responses and the latest final message live in memory.
package pirpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"
)

var ErrBusy = errors.New("pi already has an active prompt")

type Config struct {
	Command        string
	Args           []string
	Dir            string
	Env            []string
	MaxRecordBytes int
	MaxOutputBytes int
	MaxPending     int
	StderrBytes    int
	CancelGrace    time.Duration
}
type response struct {
	Type    string          `json:"type"`
	ID      string          `json:"id"`
	Command string          `json:"command"`
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
	Error   string          `json:"error"`
	Aborted bool            `json:"aborted"`
	Message json.RawMessage `json:"message"`
}
type run struct {
	done    chan struct{}
	settled bool
	output  string
	err     error
	final   bool
}
type writeRequest struct{ data []byte }
type Client struct {
	cfg      Config
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	writes   chan writeRequest
	done     chan struct{}
	stop     chan struct{}
	stopOnce sync.Once
	mu       sync.Mutex
	seq      uint64
	pending  map[string]chan response
	active   *run
	terminal error
	stderr   tail
}

func Start(cfg Config) (*Client, error) {
	if cfg.MaxRecordBytes <= 0 {
		cfg.MaxRecordBytes = 1 << 20
	}
	if cfg.MaxOutputBytes <= 0 {
		cfg.MaxOutputBytes = 256 << 10
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = 8
	}
	if cfg.StderrBytes <= 0 {
		cfg.StderrBytes = 16 << 10
	}
	if cfg.CancelGrace <= 0 {
		cfg.CancelGrace = 2 * time.Second
	}
	c := &Client{cfg: cfg, writes: make(chan writeRequest, cfg.MaxPending), done: make(chan struct{}), stop: make(chan struct{}), pending: make(map[string]chan response)}
	c.stderr.limit = cfg.StderrBytes
	c.cmd = exec.Command(cfg.Command, cfg.Args...)
	c.cmd.Dir, c.cmd.Env, c.cmd.Stderr = cfg.Dir, cfg.Env, &c.stderr
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
		// A process closing its protocol stream cannot safely be reused.
		c.kill(errors.New("pi stdout closed"))
		err := c.cmd.Wait()
		c.mu.Lock()
		if c.terminal == nil {
			c.terminal = fmt.Errorf("pi exited: %v", err)
		}
		c.mu.Unlock()
		close(c.done) // Only after the process has actually been reaped.
	}()
	return c, nil
}
func (c *Client) PID() int { return c.cmd.Process.Pid }
func (c *Client) Alive() bool {
	select {
	case <-c.done:
		return false
	default:
		return true
	}
}
func (c *Client) failure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal != nil {
		return c.terminal
	}
	return errors.New("pi unavailable")
}
func (c *Client) kill(err error) {
	c.mu.Lock()
	if c.terminal == nil {
		c.terminal = err
	}
	c.mu.Unlock()
	c.stopOnce.Do(func() { close(c.stop); _ = c.stdin.Close(); killProcess(c.cmd) })
}
func (c *Client) Close() error {
	_ = c.stdin.Close()
	timer := time.NewTimer(c.cfg.CancelGrace)
	defer timer.Stop()
	select {
	case <-c.done:
		return nil
	case <-timer.C:
		c.kill(errors.New("pi shutdown timeout"))
	}
	<-c.done
	return nil
}
func (c *Client) writeLoop() {
	for {
		select {
		case <-c.stop:
			return
		case request := <-c.writes:
			if _, err := c.stdin.Write(request.data); err != nil {
				c.kill(fmt.Errorf("pi stdin: %w", err))
				return
			}
		}
	}
}
func (c *Client) readLoop(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), c.cfg.MaxRecordBytes)
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		var record response
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			c.kill(fmt.Errorf("invalid pi JSONL: %w", err))
			return
		}
		c.mu.Lock()
		if record.Type == "response" {
			if ch := c.pending[record.ID]; ch != nil {
				select {
				case ch <- record:
				default:
				}
			}
		} else if r := c.active; r != nil && !r.settled {
			switch record.Type {
			case "message_end":
				var message struct {
					Role         string `json:"role"`
					StopReason   string `json:"stopReason"`
					ErrorMessage string `json:"errorMessage"`
					Content      []struct {
						Type string `json:"type"`
						Text string `json:"text"`
					} `json:"content"`
				}
				if json.Unmarshal(record.Message, &message) == nil && message.Role == "assistant" {
					var text strings.Builder
					for _, part := range message.Content {
						if part.Type == "text" {
							text.WriteString(part.Text)
						}
					}
					if text.Len() > c.cfg.MaxOutputBytes {
						c.mu.Unlock()
						c.kill(errors.New("pi output exceeded byte limit"))
						return
					}
					r.output = text.String()
					r.err = nil
					r.final = message.StopReason == "stop" || message.StopReason == "length"
					if message.StopReason == "error" || message.StopReason == "aborted" {
						r.err = fmt.Errorf("pi model %s: %s", message.StopReason, message.ErrorMessage)
					}
				}
			case "agent_settled":
				if record.Aborted {
					r.err = errors.New("pi run aborted")
				}
				if r.err == nil && !r.final {
					r.err = errors.New("pi settled without a final assistant message")
				}
				r.settled = true
				close(r.done)
			}
		}
		c.mu.Unlock()
	}
	if err := scanner.Err(); err != nil {
		c.kill(fmt.Errorf("pi record read/limit: %w", err))
	}
}

// Call correlates responses by ID. A command timeout poisons the process: its
// side effects are uncertain, so later jobs must never reuse it.
func (c *Client) Call(ctx context.Context, command string, fields map[string]any, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.terminal != nil {
		err := c.terminal
		c.mu.Unlock()
		return err
	}
	if len(c.pending) >= c.cfg.MaxPending {
		c.mu.Unlock()
		return errors.New("pi pending command limit")
	}
	c.seq++
	id := fmt.Sprint(c.seq)
	ch := make(chan response, 1)
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, id); c.mu.Unlock() }()
	message := make(map[string]any, len(fields)+2)
	for k, v := range fields {
		message[k] = v
	}
	message["id"], message["type"] = id, command
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if len(data)+1 > c.cfg.MaxRecordBytes {
		return errors.New("pi command exceeded byte limit")
	}
	select {
	case c.writes <- writeRequest{append(data, '\n')}:
	case <-ctx.Done():
		c.kill(ctx.Err())
		return ctx.Err()
	case <-c.done:
		return c.failure()
	}
	select {
	case result := <-ch:
		if result.Command != command {
			c.kill(errors.New("pi response command mismatch"))
			return c.failure()
		}
		if !result.Success {
			return fmt.Errorf("pi %s: %s", command, result.Error)
		}
		if out != nil && len(result.Data) > 0 {
			return json.Unmarshal(result.Data, out)
		}
		return nil
	case <-ctx.Done():
		c.kill(ctx.Err())
		return ctx.Err()
	case <-c.done:
		return c.failure()
	}
}

func (c *Client) Prompt(ctx context.Context, input string) (string, error) {
	r := &run{done: make(chan struct{})}
	c.mu.Lock()
	if c.active != nil {
		c.mu.Unlock()
		return "", ErrBusy
	}
	c.active = r
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.active = nil; c.mu.Unlock() }()
	var accepted struct {
		Disposition string `json:"disposition"`
	}
	if err := c.Call(ctx, "prompt", map[string]any{"message": input}, &accepted); err != nil {
		// Even a rejected prompt may have crossed an extension boundary.
		c.kill(err)
		<-c.done
		return "", err
	}
	if accepted.Disposition != "started" {
		err := fmt.Errorf("unexpected pi prompt disposition: %s", accepted.Disposition)
		c.kill(err)
		<-c.done
		return "", err
	}
	select {
	case <-r.done:
		c.mu.Lock()
		output, err := r.output, r.err
		c.mu.Unlock()
		return output, err
	case <-c.done:
		return "", c.failure()
	case <-ctx.Done():
		grace, cancel := context.WithTimeout(context.Background(), c.cfg.CancelGrace)
		defer cancel()
		if err := c.Call(grace, "abort", nil, nil); err == nil {
			select {
			case <-r.done:
				return "", ctx.Err()
			case <-c.done:
				return "", ctx.Err()
			case <-grace.Done():
			}
		}
		c.kill(errors.New("pi abort did not settle"))
		<-c.done
		return "", ctx.Err()
	}
}

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
