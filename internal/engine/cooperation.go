package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/shakystar/role-runtime/internal/lifecycle"
	"time"
)

var ErrStaleLease = errors.New("expired or superseded execution lease")

// LeaseError captures only fencing metadata, never task inputs or message bodies.
// It preserves errors.Is(err, ErrStaleLease) while distinguishing expiry from ownership loss.
type LeaseError struct {
	Operation          string
	Status             string
	ExpectedGeneration uint64
	ActualGeneration   uint64
	Now                time.Time
	Deadline           time.Time
	LastHeartbeat      time.Time
}

func (e *LeaseError) Error() string {
	return fmt.Sprintf("%s: %s (status=%s generation=%d expected=%d now=%s deadline=%s lastHeartbeat=%s)", e.Operation, ErrStaleLease, e.Status, e.ActualGeneration, e.ExpectedGeneration, e.Now.Format(time.RFC3339Nano), e.Deadline.Format(time.RFC3339Nano), e.LastHeartbeat.Format(time.RFC3339Nano))
}
func (e *LeaseError) Unwrap() error { return ErrStaleLease }
func staleLease(operation string, r CooperativeRecord, g uint64, now time.Time) error {
	return &LeaseError{Operation: operation, Status: r.Status, ExpectedGeneration: g, ActualGeneration: r.Generation, Now: now, Deadline: r.Deadline, LastHeartbeat: r.LastHeartbeat}
}

// Address names a persistent role instance, never a worker. Versions match exactly.
type Address struct {
	Project  string `json:"project"`
	Role     string `json:"role"`
	Version  string `json:"version"`
	Instance string `json:"instance"`
}

func AddressOf(t Task) Address { return Address{t.Project, t.Role, t.Version, t.Instance} }
func (a Address) valid() bool {
	for _, v := range []string{a.Project, a.Role, a.Version, a.Instance} {
		if !identifier.MatchString(v) {
			return false
		}
	}
	return true
}
func (a Address) key() string { return Digest(a.Project, a.Role, a.Version, a.Instance) }

// One immutable message occupies a (recipient, sender, correlation) mailbox slot.
// Repeating the exact message is idempotent; conflicting contents are rejected.
type Message struct {
	ID          string  `json:"id"`
	From        Address `json:"from"`
	To          Address `json:"to"`
	Correlation string  `json:"correlation"`
	Body        string  `json:"body"`
}

func (m Message) validate() error {
	data, err := json.Marshal(m)
	if err != nil || len(data) > 64<<10 {
		return errors.New("encoded SMS exceeds 64KiB")
	}
	if !identifier.MatchString(m.ID) || !identifier.MatchString(m.Correlation) || !m.From.valid() || !m.To.valid() || m.From.Project != m.To.Project || len(m.Body) > 16<<10 {
		return errors.New("invalid SMS: identity, project or 16KiB body limit")
	}
	return nil
}
func (m Message) key() string { return Digest(m.To.key(), m.From.key(), m.Correlation) }

type WaitFor struct {
	From        Address `json:"from"`
	Correlation string  `json:"correlation"`
}

// Transition is committed only after the harness finishes its current turn.
// Outbox delivery is replayable; arbitrary tool side effects are not.
type Transition struct {
	ExecutionState string    `json:"execution_state,omitempty"`
	Checkpoint     string    `json:"checkpoint,omitempty"`
	Send           []Message `json:"send,omitempty"`
	Wait           *WaitFor  `json:"wait,omitempty"`
	Text           string    `json:"text,omitempty"`
}

func (t Transition) validate(task Task) error {
	if t.ExecutionState != "" && t.ExecutionState != "completed" {
		return errors.New("invalid execution evidence")
	}
	if t.Wait == nil && t.Text == "" {
		return errors.New("completion requires nonempty text")
	}
	if len(t.Checkpoint) > 16<<10 || len(t.Text) > 64<<10 || len(t.Send) > 8 {
		return errors.New("transition exceeds checkpoint/text/outbox limit")
	}
	if t.Wait != nil && (!t.Wait.From.valid() || t.Wait.From.Project != task.Project || !identifier.MatchString(t.Wait.Correlation) || t.Checkpoint == "") {
		return errors.New("wait requires checkpoint and same-project sender/correlation")
	}
	seen := map[string]bool{}
	for _, m := range t.Send {
		if err := m.validate(); err != nil {
			return err
		}
		if m.From != AddressOf(task) {
			return errors.New("SMS sender differs from executing role")
		}
		if seen[m.key()] {
			return errors.New("duplicate outbox slot")
		}
		seen[m.key()] = true
	}
	return nil
}

type Execution struct {
	Task       Task     `json:"task"`
	Generation uint64   `json:"generation"`
	Checkpoint string   `json:"checkpoint,omitempty"`
	Inbox      *Message `json:"inbox,omitempty"`
}
type CooperativeRecord struct {
	ExecutionState string            `json:"execution_state,omitempty"`
	Failure        *ExecutionFailure `json:"failure,omitempty"`
	AttemptID      string            `json:"attempt_id,omitempty"`
	SchemaVersion  int               `json:"schema_version,omitempty"`
	Revision       uint64            `json:"revision,omitempty"`
	Events         []lifecycle.Event `json:"event_intents,omitempty"`
	Execution
	Status        string    `json:"status"`
	Deadline      time.Time `json:"deadline,omitempty"`
	LastHeartbeat time.Time `json:"lastHeartbeat,omitempty"`
	Wait          *WaitFor  `json:"wait,omitempty"`
	Outbox        []Message `json:"outbox,omitempty"`
	Text          string    `json:"text,omitempty"`
	Error         string    `json:"error,omitempty"`
}

// StepExecutor must honor cancellation and permit Heartbeat concurrently with Step.
// Heartbeat measures harness responsiveness, not semantic task progress.
type StepExecutor interface {
	Step(context.Context, Execution) (Transition, error)
	Heartbeat(context.Context) error
	Close() error
}

func encodeCooperative(r CooperativeRecord) ([]byte, error) {
	data, err := json.Marshal(r)
	if err == nil && len(data) > 512<<10 {
		err = errors.New("cooperative record exceeds 512KiB")
	}
	return data, err
}
