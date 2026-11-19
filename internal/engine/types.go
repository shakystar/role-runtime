package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

var (
	ErrDuplicate = errors.New("task ID already recorded")
	ErrConflict  = errors.New("immutable definition conflict")
)
var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

type Role struct {
	ID          string   `json:"id"`
	Version     string   `json:"version"`
	Instruction string   `json:"instruction"`
	Tools       []string `json:"tools"`
}
type Project struct {
	ID        string `json:"id"`
	Workspace string `json:"workspace"`
	Rules     string `json:"rules"`
}
type Task struct {
	ID       string `json:"id"`
	Project  string `json:"project"`
	Role     string `json:"role"`
	Version  string `json:"version"`
	Instance string `json:"instance"`
	Input    string `json:"input"`
}

func (t Task) Key() string { return Digest(t.Project, t.Role, t.Version, t.Instance) }
func (t Task) Validate() error {
	for _, id := range []string{t.ID, t.Project, t.Role, t.Version, t.Instance} {
		if !identifier.MatchString(id) {
			return errors.New("invalid or missing task identity")
		}
	}
	if len(t.Input) == 0 || len(t.Input) > 64<<10 {
		return errors.New("task input must be 1..65536 bytes")
	}
	return nil
}
func Digest(parts ...string) string {
	data, _ := json.Marshal(parts)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type Activation struct {
	Role        Role
	Project     Project
	SessionPath string
	SessionID   string
}

// Resolver is the context-construction boundary. Sprint 2 resolves immutable
// definitions and a complete pi session; it does not page or summarize memory.
type Resolver interface {
	Resolve(context.Context, Task) (Activation, error)
}
type Result struct {
	Failure *ExecutionFailure `json:"failure,omitempty"`
	Text    string            `json:"text,omitempty"`
	Session string            `json:"session,omitempty"`
	Worker  int               `json:"worker"`
	PID     int               `json:"pid,omitempty"`
}
type Outcome struct {
	Task   Task   `json:"task"`
	Result Result `json:"result"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}
type Executor interface {
	Execute(context.Context, Task) (Result, error)
	Close() error
}
type Record struct {
	Task    Task      `json:"task"`
	Status  string    `json:"status"`
	Result  Result    `json:"result"`
	Error   string    `json:"error,omitempty"`
	Updated time.Time `json:"updated"`
}
