// Package codexcooperation bridges completed Codex turns to durable engine SMS
// transitions. Waiting ends the model turn and returns its worker slot; it does
// not leave a model call blocked waiting for another role.
package codexcooperation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/shakystar/role-runtime/internal/codexadapter"
	"github.com/shakystar/role-runtime/internal/engine"
)

// Cooperative exposes the underlying adapter's usage, process-start count and
// heartbeat. Role instructions must teach the transition protocol explicitly.
// The engine validates addresses, sender identity, byte limits and wait rules
// when committing the returned transition; the schema is not that authority.
type Cooperative struct{ *codexadapter.Adapter }

var _ engine.StepExecutor = (*Cooperative)(nil)

// New overrides OutputSchema with the cooperative protocol. It does not append
// hidden role instructions or choose the next task/role for the caller.
func New(cfg codexadapter.Config) (*Cooperative, error) {
	cfg.OutputSchema = TransitionSchema()
	return newCooperative(cfg)
}

// NewPromptJSON explicitly selects prompt-only JSON generation for backends
// that cannot complete structured-output turns. It clears OutputSchema but uses
// the identical strict response validation before any engine commit. The role
// must request the protocol in its instructions; generation is not guaranteed
// to conform. This constructor never retries or automatically falls back.
func NewPromptJSON(cfg codexadapter.Config) (*Cooperative, error) {
	cfg.OutputSchema = nil
	return newCooperative(cfg)
}

func newCooperative(cfg codexadapter.Config) (*Cooperative, error) {
	a, err := codexadapter.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Cooperative{Adapter: a}, nil
}

// TransitionSchema returns a fresh schema so callers cannot mutate other runs.
// String lengths are intentionally checked as UTF-8 bytes by the engine, not
// approximated by JSON Schema character limits. Complete/wait conditional rules
// likewise remain the engine's responsibility.
func TransitionSchema() json.RawMessage { return json.RawMessage(transitionSchema) }

const transitionSchema = `{
 "type":"object","additionalProperties":false,
 "required":["checkpoint","send","wait","text"],
 "properties":{
  "checkpoint":{"type":"string"},
  "send":{"type":"array","maxItems":8,"items":{"$ref":"#/$defs/message"}},
  "wait":{"anyOf":[{"$ref":"#/$defs/wait"},{"type":"null"}]},
  "text":{"type":"string"}
 },
 "$defs":{
  "address":{"type":"object","additionalProperties":false,
   "required":["project","role","version","instance"],
   "properties":{"project":{"type":"string"},"role":{"type":"string"},"version":{"type":"string"},"instance":{"type":"string"}}},
  "message":{"type":"object","additionalProperties":false,
   "required":["id","from","to","correlation","body"],
   "properties":{"id":{"type":"string"},"from":{"$ref":"#/$defs/address"},"to":{"$ref":"#/$defs/address"},"correlation":{"type":"string"},"body":{"type":"string"}}},
  "wait":{"type":"object","additionalProperties":false,
   "required":["from","correlation"],
   "properties":{"from":{"$ref":"#/$defs/address"},"correlation":{"type":"string"}}}
 }
}`

func (c *Cooperative) Step(ctx context.Context, e engine.Execution) (engine.Transition, error) {
	return step(ctx, e, c.Execute)
}

type executeFunc func(context.Context, engine.Task) (engine.Result, error)

func step(ctx context.Context, e engine.Execution, execute executeFunc) (engine.Transition, error) {
	if err := ctx.Err(); err != nil {
		return engine.Transition{}, err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return engine.Transition{}, errors.New("cannot encode cooperative execution")
	}
	if len(data) > 64<<10 {
		return engine.Transition{}, errors.New("resume envelope exceeds 64KiB")
	}
	task := e.Task
	task.Input = string(data)
	result, err := execute(ctx, task)
	if err != nil {
		return engine.Transition{}, err
	}
	if err = ctx.Err(); err != nil {
		return engine.Transition{}, err
	}
	return decode(result.Text)
}

func decode(text string) (engine.Transition, error) {
	var transition engine.Transition
	if len(text) > 256<<10 {
		return transition, errors.New("cooperative response exceeds 256KiB")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(text))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&transition); err != nil {
		return engine.Transition{}, errors.New("invalid cooperative response JSON or fields")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return engine.Transition{}, errors.New("cooperative response has trailing content")
	}
	// Go unmarshalling alone accepts missing fields and null strings/slices.
	// Require the top-level wire shape promised by structured output as well.
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(text), &fields) != nil || len(fields) != 4 {
		return engine.Transition{}, errors.New("cooperative response requires checkpoint/send/wait/text")
	}
	for _, name := range []string{"checkpoint", "text"} {
		raw := bytes.TrimSpace(fields[name])
		if len(raw) == 0 || raw[0] != '"' {
			return engine.Transition{}, errors.New("cooperative checkpoint/text must be strings")
		}
	}
	send := bytes.TrimSpace(fields["send"])
	wait := bytes.TrimSpace(fields["wait"])
	if len(send) == 0 || send[0] != '[' || len(wait) == 0 || (wait[0] != '{' && string(wait) != "null") {
		return engine.Transition{}, errors.New("cooperative send must be array and wait object/null")
	}
	return transition, nil
}
