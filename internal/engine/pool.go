package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Stats struct {
	Submitted int `json:"submitted"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Rejected  int `json:"rejected"`
	MaxActive int `json:"maxActive"`
	MaxQueued int `json:"maxQueued"`
	Workers   int `json:"workers"`
}
type Pool struct {
	Workers   int
	QueueSize int
	Store     *Store
	Factory   func(int) (Executor, error)
}
type completion struct {
	slot    int
	outcome Outcome
}

// Run consumes externally chosen tasks. Backpressure is applied by disabling
// input receives when the bounded queue is full. No goroutine is created per task
// or role. The synchronous sink must return promptly and must not reenter Run.
func (p Pool) Run(parent context.Context, input <-chan Task, sink func(Outcome) error) (stats Stats, runErr error) {
	if p.Workers < 1 || p.Workers > 64 || p.QueueSize < 1 || p.QueueSize > 4096 || p.Store == nil || p.Factory == nil || sink == nil {
		return stats, errors.New("invalid pool configuration")
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	executors := make([]Executor, 0, p.Workers)
	for i := 0; i < p.Workers; i++ {
		executor, err := p.Factory(i)
		if err != nil {
			for _, e := range executors {
				_ = e.Close()
			}
			return stats, err
		}
		executors = append(executors, executor)
	}
	stats.Workers = len(executors)
	jobs := make([]chan Task, p.Workers)
	done := make(chan completion, p.Workers)
	var wg sync.WaitGroup
	for i, e := range executors {
		jobs[i] = make(chan Task)
		wg.Add(1)
		go func(slot int, executor Executor) {
			defer wg.Done()
			defer executor.Close()
			for task := range jobs[slot] {
				result, err := executor.Execute(ctx, task)
				if len(result.Text) > 256<<10 {
					result.Text = ""
					err = errors.New("executor result exceeds 256KiB")
				}
				result.Worker = slot
				o := Outcome{Task: task, Result: result, Status: "succeeded"}
				if err != nil {
					o.Result.Failure = FailureMetadata(err)
					o.Status = "failed"
					o.Error = err.Error()
					if len(o.Error) > 8192 {
						o.Error = o.Error[:8192]
					}
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						o.Status = "canceled"
					}
				}
				done <- completion{slot: slot, outcome: o}
			}
		}(i, e)
	}
	defer func() {
		for _, ch := range jobs {
			close(ch)
		}
		wg.Wait()
	}()
	queue := make([]Task, 0, p.QueueSize)
	idle := make([]bool, p.Workers)
	for i := range idle {
		idle[i] = true
	}
	activeKeys := map[string]bool{}
	active := 0
	inputOpen := true
	stopping := false
	emit := func(o Outcome, persist bool) {
		if persist {
			if err := p.Store.Update(o); err != nil && runErr == nil {
				runErr = fmt.Errorf("task receipt: %w", err)
				cancel()
			}
		}
		if err := sink(o); err != nil && runErr == nil {
			runErr = err
			cancel()
		}
	}
	for inputOpen || len(queue) > 0 || active > 0 {
		if ctx.Err() != nil && !stopping {
			stopping = true
			inputOpen = false
			if runErr == nil {
				runErr = ctx.Err()
			}
			for _, t := range queue {
				stats.Failed++
				emit(Outcome{Task: t, Status: "canceled", Error: ctx.Err().Error()}, true)
			}
			queue = nil
		}
		if !stopping {
			for slot := range idle {
				if !idle[slot] {
					continue
				}
				index := -1
				for i, t := range queue {
					if !activeKeys[t.Key()] {
						index = i
						break
					}
				}
				if index < 0 {
					break
				}
				task := queue[index]
				copy(queue[index:], queue[index+1:])
				queue[len(queue)-1] = Task{}
				queue = queue[:len(queue)-1]
				if err := p.Store.Update(Outcome{Task: task, Status: "running"}); err != nil {
					runErr = err
					cancel()
					stats.Failed++
					emit(Outcome{Task: task, Status: "failed", Error: err.Error()}, false)
					break
				}
				idle[slot] = false
				activeKeys[task.Key()] = true
				active++
				if active > stats.MaxActive {
					stats.MaxActive = active
				}
				jobs[slot] <- task
			}
		}
		if !inputOpen && len(queue) == 0 && active == 0 {
			break
		}
		var incoming <-chan Task
		if inputOpen && len(queue) < p.QueueSize {
			incoming = input
		}
		var canceled <-chan struct{}
		if !stopping {
			canceled = ctx.Done()
		}
		select {
		case <-canceled:
		case t, ok := <-incoming:
			if !ok {
				inputOpen = false
				continue
			}
			stats.Submitted++
			if err := p.Store.Claim(t); err != nil {
				stats.Rejected++
				emit(Outcome{Task: t, Status: "rejected", Error: err.Error()}, false)
				continue
			}
			queue = append(queue, t)
			if len(queue) > stats.MaxQueued {
				stats.MaxQueued = len(queue)
			}
		case completed := <-done:
			active--
			idle[completed.slot] = true
			delete(activeKeys, completed.outcome.Task.Key())
			if completed.outcome.Status == "succeeded" {
				stats.Succeeded++
			} else {
				stats.Failed++
			}
			emit(completed.outcome, true)
		}
	}
	return stats, runErr
}
