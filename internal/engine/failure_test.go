package engine

import (
	"context"
	"errors"
	"testing"
)

type classifiedFailure struct{ state string }

func (e classifiedFailure) Error() string { return "synthetic failure" }
func (e classifiedFailure) ExecutionFailure() (string, string, string, bool) {
	return "account_read", "workspace_routing_timeout", e.state, true
}

type failureExecutor struct {
	calls *int
	state string
}

func (e failureExecutor) Execute(context.Context, Task) (Result, error) {
	*e.calls++
	return Result{}, errors.Join(classifiedFailure{e.state}, errors.New("wrapped"))
}
func (e failureExecutor) Close() error { return nil }
func TestDurableFailureEvidenceWithoutReplay(t *testing.T) {
	for _, state := range []string{"not_started", "unknown", "started", "completed"} {
		t.Run(state, func(t *testing.T) {
			store := fixtureStore(t)
			calls := 0
			p := Pool{Workers: 1, QueueSize: 1, Store: store, Factory: func(int) (Executor, error) { return failureExecutor{&calls, state}, nil }}
			for i := 0; i < 2; i++ {
				input := make(chan Task, 1)
				input <- task(0)
				close(input)
				if _, err := p.Run(context.Background(), input, func(Outcome) error { return nil }); err != nil {
					t.Fatal(err)
				}
			}
			if calls != 1 {
				t.Fatalf("replayed failed work %d", calls)
			}
			receipt, err := store.Receipt(task(0).ID)
			if err != nil {
				t.Fatal(err)
			}
			f := receipt.Result.Failure
			if f == nil || f.Execution != state || f.Retryable != (state == "not_started") {
				t.Fatalf("lost failure evidence %+v", f)
			}
		})
	}
}
