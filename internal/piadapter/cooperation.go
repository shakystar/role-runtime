package piadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/shakystar/role-runtime/internal/engine"
)

// Cooperative uses an explicit JSON turn protocol. A wait is a completed model
// turn with a checkpoint, never an RPC call that blocks waiting for another role.
// Role instructions must teach this protocol; ordinary text executors are unchanged.
type Cooperative struct{ *Adapter }

func (a *Cooperative) Step(ctx context.Context, e engine.Execution) (engine.Transition, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return engine.Transition{}, err
	}
	task := e.Task
	task.Input = string(data)
	if len(task.Input) > 64<<10 {
		return engine.Transition{}, fmt.Errorf("resume envelope exceeds 64KiB")
	}
	result, err := a.Execute(ctx, task)
	if err != nil {
		return engine.Transition{}, err
	}
	var transition engine.Transition
	decoder := json.NewDecoder(strings.NewReader(result.Text))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&transition); err != nil {
		return transition, fmt.Errorf("cooperative response: %w", err)
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return transition, fmt.Errorf("cooperative response has trailing content")
	}
	return transition, nil
}
