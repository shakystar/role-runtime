package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type tracker struct {
	mu                          sync.Mutex
	active, max, starts, closes int
	keys                        map[string]bool
	conflict                    bool
	ready                       chan struct{}
	readyOnce                   sync.Once
}
type fakeExecutor struct{ tracker *tracker }

func (f *fakeExecutor) Execute(ctx context.Context, task Task) (Result, error) {
	tr := f.tracker
	tr.mu.Lock()
	tr.active++
	if tr.active > tr.max {
		tr.max = tr.active
	}
	if tr.keys[task.Key()] {
		tr.conflict = true
	}
	tr.keys[task.Key()] = true
	if tr.active == 4 && tr.ready != nil {
		tr.readyOnce.Do(func() { close(tr.ready) })
	}
	tr.mu.Unlock()
	defer func() { tr.mu.Lock(); tr.active--; delete(tr.keys, task.Key()); tr.mu.Unlock() }()
	if tr.ready != nil {
		select {
		case <-tr.ready:
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-time.After(2 * time.Millisecond):
		return Result{Text: task.ID}, nil
	}
}
func (f *fakeExecutor) Close() error {
	f.tracker.mu.Lock()
	defer f.tracker.mu.Unlock()
	f.tracker.closes++
	return nil
}
func fixtureStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func task(i int) Task {
	return Task{ID: fmt.Sprintf("task-%04d", i), Project: "project", Role: fmt.Sprintf("role-%02d", i%40), Version: "v1", Instance: "default", Input: "work"}
}
func TestFortyRolesFourWorkersAndDuplicateReceipts(t *testing.T) {
	s := fixtureStore(t)
	tr := &tracker{keys: map[string]bool{}, ready: make(chan struct{})}
	p := Pool{Workers: 4, QueueSize: 8, Store: s, Factory: func(int) (Executor, error) { tr.starts++; return &fakeExecutor{tr}, nil }}
	input := make(chan Task)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	go func() {
		defer close(input)
		for i := 0; i < 80; i++ {
			select {
			case input <- task(i):
			case <-ctx.Done():
				return
			}
		}
		select {
		case input <- task(0):
		case <-ctx.Done():
		}
	}()
	count := 0
	stats, err := p.Run(ctx, input, func(o Outcome) error { count++; return nil })
	if err != nil {
		t.Fatalf("%v stats=%+v", err, stats)
	}
	if stats.Succeeded != 80 || stats.Rejected != 1 || count != 81 || stats.MaxActive != 4 || stats.MaxQueued > 8 {
		t.Fatalf("stats: %+v count=%d", stats, count)
	}
	if tr.max != 4 || tr.active != 0 || tr.conflict || tr.starts != 4 || tr.closes != 4 {
		t.Fatalf("tracker: %+v", tr)
	}
	receipt, err := s.Receipt(task(0).ID)
	if err != nil || receipt.Status != "succeeded" {
		t.Fatalf("duplicate overwrote receipt: %+v %v", receipt, err)
	}
}
func TestSameRoleSerialized(t *testing.T) {
	s := fixtureStore(t)
	tr := &tracker{keys: map[string]bool{}}
	input := make(chan Task, 20)
	for i := 0; i < 20; i++ {
		job := task(i)
		job.Role = "one-role"
		input <- job
	}
	close(input)
	p := Pool{Workers: 4, QueueSize: 4, Store: s, Factory: func(int) (Executor, error) { return &fakeExecutor{tr}, nil }}
	stats, err := p.Run(context.Background(), input, func(Outcome) error { return nil })
	if err != nil || stats.MaxActive != 1 || tr.conflict {
		t.Fatalf("%+v %v", stats, err)
	}
}
func TestCancellationAndSinkFailureDrainWorkers(t *testing.T) {
	for _, sinkFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(sinkFailure), func(t *testing.T) {
			s := fixtureStore(t)
			tr := &tracker{keys: map[string]bool{}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := make(chan Task)
			go func() {
				defer close(input)
				for i := 0; i < 1000; i++ {
					select {
					case input <- task(i):
					case <-ctx.Done():
						return
					}
				}
			}()
			p := Pool{Workers: 4, QueueSize: 8, Store: s, Factory: func(int) (Executor, error) { return &fakeExecutor{tr}, nil }}
			count := 0
			_, err := p.Run(ctx, input, func(Outcome) error {
				count++
				if count == 1 {
					if sinkFailure {
						return errors.New("sink unavailable")
					}
					cancel()
				}
				return nil
			})
			cancel()
			if err == nil || tr.active != 0 || tr.closes != 4 {
				t.Fatalf("cleanup failed: %v %+v", err, tr)
			}
		})
	}
}
func TestStoreContracts(t *testing.T) {
	s := fixtureStore(t)
	if second, err := OpenStore(s.Root); err == nil {
		second.Close()
		t.Fatal("second owner allowed")
	}
	role := Role{ID: "dto", Version: "v1", Instruction: "DTO", Tools: []string{"read"}}
	if err := s.RegisterRole(role); err != nil {
		t.Fatal(err)
	}
	role.Instruction = "changed"
	if err := s.RegisterRole(role); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	project := Project{ID: "project", Workspace: t.TempDir(), Rules: "rules"}
	if err := s.RegisterProject(project); err != nil {
		t.Fatal(err)
	}
	job := task(0)
	job.Role = "dto"
	a, err := s.Resolve(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(a.SessionPath) != filepath.Join(s.Root, "sessions") {
		t.Fatal("session outside store")
	}
	other := job
	other.Project = "missing"
	if _, err = s.Resolve(context.Background(), other); err == nil {
		t.Fatal("unknown project")
	}
	other = job
	other.Version = "v2"
	if _, err = s.Resolve(context.Background(), other); err == nil {
		t.Fatal("unknown version")
	}
	other = job
	other.ID = "../../escape"
	if err = s.Claim(other); err == nil {
		t.Fatal("unsafe ID")
	}
	if err = os.WriteFile(a.SessionPath, []byte("broken\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Resolve(context.Background(), job); err == nil {
		t.Fatal("corrupt session accepted")
	}
}
func TestDuplicateSurvivesStoreRestart(t *testing.T) {
	root := t.TempDir()
	s, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Claim(task(1)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Claim(task(1)); !errors.Is(err, ErrDuplicate) {
		t.Fatal(err)
	}
}
