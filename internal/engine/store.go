package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/shakystar/role-runtime/internal/lifecycle"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store is a local, single-owner file store. The original Pool stores receipts;
// CooperativePool additionally persists ready/waiting state and transactional
// outboxes. Interrupted running work is never automatically replayed.
type Store struct {
	ledger                *lifecycle.Ledger
	lifecycleRun          string
	lifecycleReservations map[string]int
	lifecycleStarted      map[string]time.Time
	lifecycleWaiting      map[string]time.Time
	lifecycleError        error
	lifecycleIncomplete   bool
	Root                  string
	lock                  *os.File
	mu                    sync.Mutex
	cooperating           bool
	liveLeases            map[string]liveLease
	leaseClock            func() time.Time
}

func OpenStore(root string) (*Store, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	for _, dir := range []string{"", "roles", "projects", "sessions", "tasks", "uncertain", "cooperation", "mailbox"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			return nil, err
		}
	}
	lock, err := acquireLock(filepath.Join(root, "owner.lock"))
	if err != nil {
		return nil, err
	}
	return &Store{Root: root, lock: lock, liveLeases: make(map[string]liveLease), leaseClock: time.Now}, nil
}
func (s *Store) Close() error { return s.lock.Close() }
func (s *Store) RegisterRole(r Role) error {
	if !identifier.MatchString(r.ID) || !identifier.MatchString(r.Version) || r.Instruction == "" || len(r.Tools) == 0 {
		return errors.New("invalid role definition")
	}
	for _, name := range r.Tools {
		if !identifier.MatchString(name) {
			return errors.New("invalid role tool")
		}
	}
	return s.immutable(filepath.Join(s.Root, "roles", Digest(r.ID, r.Version)+".json"), r)
}
func (s *Store) RegisterProject(p Project) error {
	if !identifier.MatchString(p.ID) || !filepath.IsAbs(p.Workspace) {
		return errors.New("project needs ID and absolute workspace")
	}
	info, err := os.Stat(p.Workspace)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("workspace is not a directory")
	}
	p.Workspace, err = filepath.EvalSymlinks(p.Workspace)
	if err != nil {
		return err
	}
	return s.immutable(filepath.Join(s.Root, "projects", Digest(p.ID)+".json"), p)
}
func (s *Store) immutable(path string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(data) > 64<<10 {
		return errors.New("definition exceeds 64KiB")
	}
	prior, err := os.ReadFile(path)
	if err == nil {
		if !bytes.Equal(prior, data) {
			return ErrConflict
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	return atomicWrite(path, data)
}
func readJSON(path string, out any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil {
		return err
	}
	if len(data) > 64<<10 {
		return errors.New("definition exceeds 64KiB")
	}
	return json.Unmarshal(data, out)
}
func (s *Store) Resolve(ctx context.Context, t Task) (Activation, error) {
	a, err := s.ResolveDefinitions(ctx, t)
	if err != nil {
		return a, err
	}
	return s.resolvePiSession(t, a)
}

// ResolveDefinitions validates immutable identities and quarantine without
// creating a harness-specific session. Adapters own their session formats.
func (s *Store) ResolveDefinitions(ctx context.Context, t Task) (Activation, error) {
	if err := ctx.Err(); err != nil {
		return Activation{}, err
	}
	if err := t.Validate(); err != nil {
		return Activation{}, err
	}
	var a Activation
	if err := readJSON(filepath.Join(s.Root, "roles", Digest(t.Role, t.Version)+".json"), &a.Role); err != nil {
		return a, fmt.Errorf("role version: %w", err)
	}
	if a.Role.ID != t.Role || a.Role.Version != t.Version {
		return a, errors.New("role identity mismatch")
	}
	if err := readJSON(filepath.Join(s.Root, "projects", Digest(t.Project)+".json"), &a.Project); err != nil {
		return a, err
	}
	if a.Project.ID != t.Project {
		return a, errors.New("project identity mismatch")
	}
	if _, err := os.Stat(filepath.Join(s.Root, "uncertain", t.Key()+".json")); err == nil {
		return a, errors.New("session quarantined after an incomplete run; explicit recovery required")
	} else if !os.IsNotExist(err) {
		return a, err
	}
	return a, nil
}

func (s *Store) resolvePiSession(t Task, a Activation) (Activation, error) {
	a.SessionID = t.Key()
	a.SessionPath = filepath.Join(s.Root, "sessions", a.SessionID+".jsonl")
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, err := os.Stat(a.SessionPath); err == nil {
		if info.Size() > 16<<20 {
			return a, errors.New("session exceeds 16MiB; paging is not implemented")
		}
		f, err := os.Open(a.SessionPath)
		if err != nil {
			return a, err
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 4096), 64<<10)
		var header struct {
			Type    string `json:"type"`
			ID      string `json:"id"`
			Version int    `json:"version"`
			CWD     string `json:"cwd"`
		}
		if !scanner.Scan() {
			return a, errors.New("session header missing")
		}
		if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
			return a, err
		}
		if header.Type != "session" || header.Version != 3 || header.ID != a.SessionID || header.CWD != a.Project.Workspace {
			return a, errors.New("session identity/cwd mismatch")
		}
		return a, nil
	} else if !os.IsNotExist(err) {
		return a, err
	}
	// pi's documented v3 bootstrap format only. Pi owns all subsequent appends.
	now := time.Now().UTC().Format(time.RFC3339Nano)
	entries := []any{
		map[string]any{"type": "session", "version": 3, "id": a.SessionID, "timestamp": now, "cwd": a.Project.Workspace},
		map[string]any{"type": "custom", "id": "role-ref", "parentId": nil, "timestamp": now, "customType": "role-runtime/v1", "data": map[string]string{"project": t.Project, "role": t.Role, "version": t.Version, "instance": t.Instance}},
	}
	var buffer bytes.Buffer
	for _, entry := range entries {
		if err := json.NewEncoder(&buffer).Encode(entry); err != nil {
			return a, err
		}
	}
	return a, atomicWrite(a.SessionPath, buffer.Bytes())
}
func (s *Store) Claim(t Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.Root, "tasks", Digest(t.ID)+".json")
	if _, err := os.Stat(path); err == nil {
		return ErrDuplicate
	} else if !os.IsNotExist(err) {
		return err
	}
	return s.saveRecord(path, Record{Task: t, Status: "accepted", Updated: time.Now().UTC()})
}
func (s *Store) Update(o Outcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveRecord(filepath.Join(s.Root, "tasks", Digest(o.Task.ID)+".json"), Record{Task: o.Task, Status: o.Status, Result: o.Result, Error: o.Error, Updated: time.Now().UTC()})
}
func (s *Store) Receipt(id string) (Record, error) {
	var r Record
	f, err := os.Open(filepath.Join(s.Root, "tasks", Digest(id)+".json"))
	if err != nil {
		return r, err
	}
	defer f.Close()
	err = json.NewDecoder(io.LimitReader(f, 1<<20)).Decode(&r)
	return r, err
}
func (s *Store) Quarantine(t Task, err error) error {
	data, _ := json.Marshal(map[string]string{"taskID": t.ID, "reason": err.Error()})
	return atomicWrite(filepath.Join(s.Root, "uncertain", t.Key()+".json"), data)
}
func (s *Store) saveRecord(path string, r Record) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return atomicWrite(path, data)
}
func atomicWrite(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".write-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
