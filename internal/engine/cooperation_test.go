package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func coopTask(id, role string) Task {
	return Task{ID: id, Project: "p", Role: role, Version: "v1", Instance: "default", Input: "work"}
}
func replyFor(t Task) Message {
	return Message{ID: "reply-" + t.ID, From: Address{t.Project, "responder", "v1", "default"}, To: AddressOf(t), Correlation: t.ID, Body: "answer"}
}
func mustSubmit(t *testing.T, s *Store, task Task) {
	t.Helper()
	if err := s.SubmitCooperative(task); err != nil {
		t.Fatal(err)
	}
}
func mustLease(t *testing.T, s *Store, id string) CooperativeRecord {
	t.Helper()
	r, err := s.LeaseCooperative(id, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func mustScan(t *testing.T, s *Store) {
	t.Helper()
	if err := s.ScanCooperative(func(CooperativeRecord) (bool, error) { return false, nil }); err != nil {
		t.Fatal(err)
	}
}
func TestMailboxEarlyDeliveryRestartAndFencing(t *testing.T) {
	root := t.TempDir()
	s, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	task := coopTask("request", "dto")
	mustSubmit(t, s, task)
	r := mustLease(t, s, task.ID)
	m := replyFor(task)
	if err = s.Deliver(m); err != nil {
		t.Fatal(err)
	}
	if err = s.Deliver(m); err != nil {
		t.Fatal("duplicate", err)
	}
	conflict := m
	conflict.Body = "different"
	if !errors.Is(s.Deliver(conflict), ErrConflict) {
		t.Fatal("conflicting duplicate accepted")
	}
	foreign := m
	foreign.From.Project = "other"
	if s.Deliver(foreign) == nil {
		t.Fatal("cross project SMS accepted")
	}
	transition := Transition{Checkpoint: "resume here", Wait: &WaitFor{From: m.From, Correlation: m.Correlation}}
	if err = s.FinishCooperative(task.ID, r.Generation, transition, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = s.FinishCooperative(task.ID, r.Generation, Transition{}, time.Now()); !errors.Is(err, ErrStaleLease) {
		t.Fatal("stale completion", err)
	}
	s.Close()
	s, err = OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.RecoverCooperative(); err != nil {
		t.Fatal(err)
	}
	resumed := mustLease(t, s, task.ID)
	if resumed.Inbox == nil || resumed.Inbox.Body != "answer" || resumed.Checkpoint != "resume here" || resumed.Generation <= r.Generation {
		t.Fatalf("lost resume: %+v", resumed)
	}
	if err = s.RenewCooperative(task.ID, r.Generation, time.Now(), time.Minute); !errors.Is(err, ErrStaleLease) {
		t.Fatal("old renewal", err)
	}
	if err = s.FenceCooperative(task.ID, resumed.Generation, errors.New("worker lost")); err != nil {
		t.Fatal(err)
	}
	if err = s.FinishCooperative(task.ID, resumed.Generation, Transition{Text: "late"}, time.Now()); !errors.Is(err, ErrStaleLease) {
		t.Fatal("late result accepted", err)
	}
}
func TestOutboxRecoveryAndRunningQuarantine(t *testing.T) {
	root := t.TempDir()
	s, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	task := coopTask("response", "responder")
	request := coopTask("request", "dto")
	m := replyFor(request)
	mustSubmit(t, s, task)
	r := mustLease(t, s, task.ID)
	if err = s.FinishCooperative(task.ID, r.Generation, Transition{Send: []Message{m}, Text: "sent"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Simulate crash after delivery but before clearing the transactional outbox.
	if err = s.Deliver(m); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, s, request)
	running := mustLease(t, s, request.ID)
	s.Close()
	s, err = OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.RecoverCooperative(); err != nil {
		t.Fatal(err)
	}
	receipt, err := s.CooperativeReceipt(task.ID)
	if err != nil || len(receipt.Outbox) != 0 {
		t.Fatal(receipt, err)
	}
	lost, err := s.CooperativeReceipt(request.ID)
	if err != nil || lost.Status != "uncertain" || lost.Generation <= running.Generation {
		t.Fatal(lost, err)
	}
	if _, err = os.Stat(s.Root + "/uncertain/" + request.Key() + ".json"); err != nil {
		t.Fatal(err)
	}
}
func TestLeaseExpiryAndSenderBinding(t *testing.T) {
	s := fixtureStore(t)
	task := coopTask("request", "dto")
	mustSubmit(t, s, task)
	now := time.Now()
	tick := now
	s.leaseClock = func() time.Time { return tick }
	r, err := s.LeaseCooperative(task.ID, now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tick = now.Add(time.Second)
	if err = s.RenewCooperative(task.ID, r.Generation, now.Add(time.Second), time.Second); !errors.Is(err, ErrStaleLease) {
		t.Fatal(err)
	}
	if err = s.FinishCooperative(task.ID, r.Generation, Transition{}, now.Add(time.Second)); !errors.Is(err, ErrStaleLease) {
		t.Fatal(err)
	}
	if err = s.FinishCooperative(task.ID, r.Generation, Transition{Send: []Message{replyFor(task)}}, now); err == nil {
		t.Fatal("forged sender")
	}
}

type cooperationFixture struct {
	ready  chan struct{}
	count  atomic.Int64
	active atomic.Int64
	max    atomic.Int64
	closed atomic.Int64
	once   sync.Once
}
type cooperativeExecutor struct {
	fixture   *cooperationFixture
	failProbe bool
	hang      bool
	failAfter int64
	probes    atomic.Int64
}

func (e *cooperativeExecutor) Close() error { e.fixture.closed.Add(1); return nil }
func (e *cooperativeExecutor) Heartbeat(ctx context.Context) error {
	if e.failProbe || (e.failAfter > 0 && e.probes.Add(1) > e.failAfter) {
		<-ctx.Done()
		return ctx.Err()
	}
	return ctx.Err()
}
func (e *cooperativeExecutor) Step(ctx context.Context, x Execution) (Transition, error) {
	f := e.fixture
	n := f.active.Add(1)
	defer f.active.Add(-1)
	for old := f.max.Load(); n > old; old = f.max.Load() {
		if f.max.CompareAndSwap(old, n) {
			break
		}
	}
	if e.hang {
		<-ctx.Done()
		return Transition{}, ctx.Err()
	}
	if x.Task.Role == "responder" {
		request := coopTask(x.Task.Input, x.Task.Input)
		m := replyFor(request)
		return Transition{Send: []Message{m}, Text: "sent"}, nil
	}
	if x.Inbox != nil {
		if x.Checkpoint != "saved" || x.Inbox.Body != "answer" {
			return Transition{}, errors.New("lost state")
		}
		return Transition{Text: "resumed"}, nil
	}
	if f.count.Add(1) == 4 {
		f.once.Do(func() { close(f.ready) })
	}
	select {
	case <-f.ready:
	case <-ctx.Done():
		return Transition{}, ctx.Err()
	}
	// Allow heartbeat to run while all four workers are active.
	select {
	case <-time.After(150 * time.Millisecond):
	case <-ctx.Done():
		return Transition{}, ctx.Err()
	}
	m := replyFor(x.Task)
	return Transition{Checkpoint: "saved", Wait: &WaitFor{From: m.From, Correlation: m.Correlation}}, nil
}
func waitReceipt(t *testing.T, s *Store, id, status string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, err := s.CooperativeReceipt(id)
		if err == nil && r.Status == status {
			return
		}
		if err == nil && r.Status == "uncertain" {
			t.Fatalf("uncertain: %+v", r)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not become %s", id, status)
}
func TestFourWaitingRolesReleaseAllWorkers(t *testing.T) {
	s := fixtureStore(t)
	f := &cooperationFixture{ready: make(chan struct{})}
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("request-%d", i)
		mustSubmit(t, s, coopTask(id, id))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := CooperativePool{Store: s, Workers: 4, Factory: func(int) (StepExecutor, error) { return &cooperativeExecutor{fixture: f}, nil }, HeartbeatInterval: 30 * time.Millisecond, HeartbeatTimeout: time.Second, LeaseTTL: 10 * time.Second, PollInterval: 10 * time.Millisecond}
	result := make(chan struct {
		stats CooperativeStats
		err   error
	}, 1)
	go func() {
		stats, err := p.Run(ctx)
		result <- struct {
			stats CooperativeStats
			err   error
		}{stats, err}
	}()
	defer func() {
		cancel()
		select {
		case <-result:
		case <-time.After(20 * time.Second):
			t.Error("pool did not stop")
		}
	}()
	for i := 0; i < 4; i++ {
		waitReceipt(t, s, fmt.Sprintf("request-%d", i), "waiting")
	}
	if f.active.Load() != 0 {
		t.Fatal("waiting roles retained workers")
	}
	// Responses are externally assigned only after every worker has yielded.
	for i := 0; i < 4; i++ {
		task := coopTask(fmt.Sprintf("response-%d", i), "responder")
		task.Input = fmt.Sprintf("request-%d", i)
		mustSubmit(t, s, task)
	}
	for i := 0; i < 4; i++ {
		waitReceipt(t, s, fmt.Sprintf("request-%d", i), "completed")
	}
	cancel()
	out := <-result
	result <- out
	if !errors.Is(out.err, context.Canceled) || out.stats.Completed != 8 || out.stats.Suspended != 4 || out.stats.Resumed != 4 || out.stats.MaxActive != 4 || out.stats.Heartbeats < 12 || f.max.Load() != 4 || f.closed.Load() != 4 {
		t.Fatalf("%+v %v max=%d closed=%d", out.stats, out.err, f.max.Load(), f.closed.Load())
	}
}
func TestHeartbeatFailureAndStepDeadlineFence(t *testing.T) {
	for _, probeFail := range []bool{true, false} {
		t.Run(fmt.Sprint(probeFail), func(t *testing.T) {
			s := fixtureStore(t)
			task := coopTask("hung", "dto")
			mustSubmit(t, s, task)
			f := &cooperationFixture{}
			p := CooperativePool{Store: s, Workers: 1, Factory: func(int) (StepExecutor, error) {
				return &cooperativeExecutor{fixture: f, failProbe: probeFail, hang: true}, nil
			}, HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 50 * time.Millisecond, LeaseTTL: time.Second, StepTimeout: 150 * time.Millisecond}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := p.Run(ctx)
			if err == nil {
				t.Fatal("failure hidden")
			}
			r, err := s.CooperativeReceipt(task.ID)
			if err != nil || r.Status != "uncertain" || f.active.Load() != 0 || f.closed.Load() != 1 {
				t.Fatalf("%+v %v", r, err)
			}
		})
	}
}

type deadlineInspectExecutor struct{}

func (deadlineInspectExecutor) Close() error                        { return nil }
func (deadlineInspectExecutor) Heartbeat(ctx context.Context) error { return ctx.Err() }
func (deadlineInspectExecutor) Step(ctx context.Context, _ Execution) (Transition, error) {
	if _, ok := ctx.Deadline(); ok {
		return Transition{}, errors.New("unexpected step deadline")
	}
	return Transition{Text: "complete without elapsed deadline"}, ctx.Err()
}
func TestZeroStepTimeoutHasNoDeadline(t *testing.T) {
	s := fixtureStore(t)
	task := coopTask("no-deadline", "dto")
	mustSubmit(t, s, task)
	r := mustLease(t, s, task.ID)
	p := CooperativePool{Store: s, HeartbeatInterval: time.Second, HeartbeatTimeout: time.Second, LeaseTTL: 5 * time.Second}
	var beats atomic.Int64
	tr, err := p.step(context.Background(), deadlineInspectExecutor{}, r, &beats)
	if err != nil || tr.Text == "" || beats.Load() != 1 {
		t.Fatalf("%+v %v beats=%d", tr, err, beats.Load())
	}
}

func TestMailboxExactRoleVersionAndConcurrentDelivery(t *testing.T) {
	s := fixtureStore(t)
	task := coopTask("request", "dto")
	mustSubmit(t, s, task)
	r := mustLease(t, s, task.ID)
	m := replyFor(task)
	if err := s.FinishCooperative(task.ID, r.Generation, Transition{Checkpoint: "saved", Wait: &WaitFor{From: m.From, Correlation: m.Correlation}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	wrong := m
	wrong.To.Version = "v2"
	if err := s.Deliver(wrong); err != nil {
		t.Fatal(err)
	}
	mustScan(t, s)
	receipt, _ := s.CooperativeReceipt(task.ID)
	if receipt.Status != "waiting" {
		t.Fatal("wrong version woke role")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Deliver(m); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	mustScan(t, s)
	receipt, _ = s.CooperativeReceipt(task.ID)
	if receipt.Status != "ready" || receipt.Inbox == nil || receipt.Inbox.ID != m.ID {
		t.Fatal("message lost")
	}
}
func TestHeartbeatLostDuringExecution(t *testing.T) {
	s := fixtureStore(t)
	task := coopTask("lost", "dto")
	mustSubmit(t, s, task)
	f := &cooperationFixture{}
	p := CooperativePool{Store: s, Workers: 1, Factory: func(int) (StepExecutor, error) {
		return &cooperativeExecutor{fixture: f, failAfter: 1, hang: true}, nil
	}, HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 50 * time.Millisecond, LeaseTTL: time.Second, StepTimeout: 5 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stats, err := p.Run(ctx)
	if err == nil || stats.Heartbeats != 1 || f.active.Load() != 0 || f.closed.Load() != 1 {
		t.Fatalf("%+v %v", stats, err)
	}
	r, err := s.CooperativeReceipt(task.ID)
	if err != nil || r.Status != "uncertain" || r.LastHeartbeat.IsZero() {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestLeaseErrorReportsPersistedFence(t *testing.T) {
	s := fixtureStore(t)
	task := coopTask("lease-diagnosis", "dto")
	mustSubmit(t, s, task)
	now := time.Now()
	tick := now
	s.leaseClock = func() time.Time { return tick }
	r, err := s.LeaseCooperative(task.ID, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	renewed := now.Add(15 * time.Second)
	tick = renewed
	if err = s.RenewCooperative(task.ID, r.Generation, renewed, time.Minute); err != nil {
		t.Fatal(err)
	}
	expired := renewed.Add(time.Minute)
	tick = expired
	err = s.RenewCooperative(task.ID, r.Generation, expired, time.Minute)
	var detail *LeaseError
	if !errors.Is(err, ErrStaleLease) || !errors.As(err, &detail) {
		t.Fatalf("missing typed fence: %v", err)
	}
	if detail.Operation != "renew" || detail.Status != "running" || detail.ActualGeneration != r.Generation || !detail.Deadline.Equal(expired) || !detail.LastHeartbeat.Equal(renewed) {
		t.Fatalf("bad fence metadata: %+v", detail)
	}
	stored, e := s.CooperativeReceipt(task.ID)
	if e != nil || !stored.Deadline.Equal(expired) {
		t.Fatal("rejected renewal changed state", e)
	}
	err = s.FinishCooperative(task.ID, r.Generation+1, Transition{}, renewed)
	if !errors.As(err, &detail) || detail.Operation != "finish" || detail.ExpectedGeneration != r.Generation+1 {
		t.Fatalf("missing generation mismatch: %v", err)
	}
}

func TestLeaseSurvivesWallClockJumpButNotElapsedExpiry(t *testing.T) {
	for _, jump := range []time.Duration{3 * time.Hour, -3 * time.Hour} {
		t.Run(jump.String(), func(t *testing.T) {
			s := fixtureStore(t)
			task := coopTask("clock-jump", "dto")
			mustSubmit(t, s, task)
			base := time.Now()
			tick := base
			s.leaseClock = func() time.Time { return tick }
			r, err := s.LeaseCooperative(task.ID, base, 45*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			tick = base.Add(15 * time.Second)
			wall := base.Add(jump)
			if err = s.RenewCooperative(task.ID, r.Generation, wall, 45*time.Second); err != nil {
				t.Fatalf("wall jump expired live owner: %v", err)
			}
			tick = base.Add(61 * time.Second)
			if err = s.RenewCooperative(task.ID, r.Generation, wall, 45*time.Second); !errors.Is(err, ErrStaleLease) {
				t.Fatalf("elapsed expiry ignored: %v", err)
			}
			if err = s.FinishCooperative(task.ID, r.Generation, Transition{}, wall); !errors.Is(err, ErrStaleLease) {
				t.Fatal("expired result accepted")
			}
		})
	}
}
func TestReopenedStoreCannotRenewOldLease(t *testing.T) {
	root := t.TempDir()
	s, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	task := coopTask("old-owner", "dto")
	mustSubmit(t, s, task)
	r := mustLease(t, s, task.ID)
	s.Close()
	s, err = OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.RenewCooperative(task.ID, r.Generation, time.Now(), time.Minute); !errors.Is(err, ErrStaleLease) {
		t.Fatal("new owner revived old execution")
	}
	if err = s.RecoverCooperative(); err != nil {
		t.Fatal(err)
	}
	receipt, err := s.CooperativeReceipt(task.ID)
	if err != nil || receipt.Status != "uncertain" {
		t.Fatal("restart did not fence", err)
	}
}
