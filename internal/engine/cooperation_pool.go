package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type CooperativeStats struct {
	Steps      int   `json:"steps"`
	Completed  int   `json:"completed"`
	Suspended  int   `json:"suspended"`
	Resumed    int   `json:"resumed"`
	MaxActive  int   `json:"maxActive"`
	Heartbeats int64 `json:"heartbeats"`
}
type CooperativePool struct {
	Workers           int
	Store             *Store
	Factory           func(int) (StepExecutor, error)
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	LeaseTTL          time.Duration
	StepTimeout       time.Duration
}
type stepCompletion struct {
	slot       int
	record     CooperativeRecord
	transition Transition
	err        error
}

// Run services a disk-backed queue until cancellation or failure. Only K records
// are leased at once; waiting roles have neither a worker nor a goroutine.
// Executors must stop on cancellation. A slot is never reassigned until Step and
// its heartbeat loop have returned, even when its lease has expired.
func (p CooperativePool) Run(parent context.Context) (stats CooperativeStats, runErr error) {
	if p.Workers < 1 || p.Workers > 64 || p.Store == nil || p.Factory == nil {
		return stats, errors.New("invalid cooperative pool")
	}
	if p.PollInterval <= 0 {
		p.PollInterval = 50 * time.Millisecond
	}
	if p.HeartbeatInterval <= 0 {
		p.HeartbeatInterval = time.Second
	}
	if p.HeartbeatTimeout <= 0 {
		p.HeartbeatTimeout = 2 * time.Second
	}
	if p.LeaseTTL <= 0 {
		p.LeaseTTL = 10 * time.Second
	}
	if p.StepTimeout <= 0 {
		p.StepTimeout = time.Minute
	}
	if p.LeaseTTL <= p.HeartbeatInterval+p.HeartbeatTimeout {
		return stats, errors.New("lease must exceed heartbeat interval plus timeout")
	}
	p.Store.mu.Lock()
	if p.Store.cooperating {
		p.Store.mu.Unlock()
		return stats, errors.New("cooperative pool already running")
	}
	p.Store.cooperating = true
	p.Store.mu.Unlock()
	defer func() { p.Store.mu.Lock(); p.Store.cooperating = false; p.Store.mu.Unlock() }()
	if err := parent.Err(); err != nil {
		return stats, err
	}
	if err := p.Store.RecoverCooperative(); err != nil {
		return stats, err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	executors := make([]StepExecutor, 0, p.Workers)
	defer func() {
		for _, e := range executors {
			_ = e.Close()
		}
	}()
	for i := 0; i < p.Workers; i++ {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		e, err := p.Factory(i)
		if err != nil {
			return stats, err
		}
		if e == nil {
			return stats, errors.New("nil step executor")
		}
		executors = append(executors, e)
	}
	var beats atomic.Int64
	defer func() { stats.Heartbeats = beats.Load() }()
	jobs := make([]chan CooperativeRecord, p.Workers)
	done := make(chan stepCompletion, p.Workers)
	var workers sync.WaitGroup
	for i, e := range executors {
		jobs[i] = make(chan CooperativeRecord)
		workers.Add(1)
		go func(slot int, e StepExecutor) {
			defer workers.Done()
			for r := range jobs[slot] {
				t, err := p.step(ctx, e, r, &beats)
				done <- stepCompletion{slot, r, t, err}
			}
		}(i, e)
	}
	defer func() {
		for _, ch := range jobs {
			close(ch)
		}
		workers.Wait()
	}()
	active := make(map[int]string, p.Workers)
	keys := make(map[string]bool, p.Workers)
	ticker := time.NewTicker(p.PollInterval)
	defer ticker.Stop()
	stopping := false
	for {
		if ctx.Err() != nil {
			stopping = true
			if runErr == nil {
				runErr = ctx.Err()
			}
		}
		if !stopping && len(active) < p.Workers {
			err := p.Store.ScanCooperative(func(r CooperativeRecord) (bool, error) {
				if ctx.Err() != nil {
					return true, nil
				}
				if r.Status != "ready" || keys[r.Task.Key()] {
					return false, nil
				}
				slot := 0
				for {
					if _, ok := active[slot]; !ok {
						break
					}
					slot++
				}
				leased, err := p.Store.LeaseCooperative(r.Task.ID, time.Now(), p.LeaseTTL)
				if err != nil {
					return true, err
				}
				active[slot] = r.Task.ID
				keys[r.Task.Key()] = true
				if len(active) > stats.MaxActive {
					stats.MaxActive = len(active)
				}
				if leased.Inbox != nil {
					stats.Resumed++
				}
				jobs[slot] <- leased
				return len(active) == p.Workers, nil
			})
			if err != nil {
				runErr = err
				cancel()
				stopping = true
			}
		}
		if stopping && len(active) == 0 {
			return stats, runErr
		}
		var canceled <-chan struct{}
		if !stopping {
			canceled = ctx.Done()
		}
		select {
		case <-canceled:
		case <-ticker.C:
		case c := <-done:
			delete(active, c.slot)
			delete(keys, c.record.Task.Key())
			stats.Steps++
			err := c.err
			if err == nil {
				err = p.Store.FinishCooperative(c.record.Task.ID, c.record.Generation, c.transition, time.Now())
			}
			if err != nil {
				fenceErr := p.Store.FenceCooperative(c.record.Task.ID, c.record.Generation, err)
				if fenceErr != nil {
					err = fmt.Errorf("%w; fence: %v", err, fenceErr)
				}
				if runErr == nil {
					runErr = err
				}
				cancel()
				stopping = true
			} else if c.transition.Wait != nil {
				stats.Suspended++
			} else {
				stats.Completed++
			}
		}
	}
}
func (p CooperativePool) step(parent context.Context, e StepExecutor, r CooperativeRecord, beats *atomic.Int64) (Transition, error) {
	ctx, cancel := context.WithTimeout(parent, p.StepTimeout)
	defer cancel()
	// Probe before executing: starting a Go timer is not a worker heartbeat.
	probe := func() error {
		c, stop := context.WithTimeout(ctx, p.HeartbeatTimeout)
		defer stop()
		if err := e.Heartbeat(c); err != nil {
			return err
		}
		if err := p.Store.RenewCooperative(r.Task.ID, r.Generation, time.Now(), p.LeaseTTL); err != nil {
			return err
		}
		beats.Add(1)
		return nil
	}
	if err := probe(); err != nil {
		return Transition{}, err
	}
	monitor := make(chan error, 1)
	stopMonitor := make(chan struct{})
	go func() {
		ticker := time.NewTicker(p.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopMonitor:
				monitor <- nil
				return
			case <-ctx.Done():
				monitor <- nil
				return
			case <-ticker.C:
				if err := probe(); err != nil {
					if ctx.Err() != nil {
						monitor <- nil
						return
					}
					monitor <- err
					cancel()
					return
				}
			}
		}
	}()
	t, err := e.Step(ctx, r.Execution)
	if err == nil {
		err = ctx.Err()
	}
	close(stopMonitor)
	if monitorErr := <-monitor; monitorErr != nil {
		err = fmt.Errorf("heartbeat: %w", monitorErr)
	}
	return t, err
}
