package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (s *Store) coopPath(id string) string {
	return filepath.Join(s.Root, "cooperation", Digest(id)+".json")
}
func (s *Store) saveCooperative(r CooperativeRecord) error {
	data, err := encodeCooperative(r)
	if err != nil {
		return err
	}
	return atomicWrite(s.coopPath(r.Task.ID), data)
}
func readCooperative(path string) (CooperativeRecord, error) {
	var r CooperativeRecord
	f, err := os.Open(path)
	if err != nil {
		return r, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (512<<10)+1))
	if err != nil {
		return r, err
	}
	if len(data) > 512<<10 {
		return r, errors.New("oversize cooperative record")
	}
	err = json.Unmarshal(data, &r)
	return r, err
}
func (s *Store) CooperativeReceipt(id string) (CooperativeRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return readCooperative(s.coopPath(id))
}
func (s *Store) SubmitCooperative(t Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(s.coopPath(t.ID)); err == nil {
		return ErrDuplicate
	} else if !os.IsNotExist(err) {
		return err
	}
	return s.saveCooperative(CooperativeRecord{Execution: Execution{Task: t}, Status: "ready"})
}
func (s *Store) deliver(m Message) error {
	if err := m.validate(); err != nil {
		return err
	}
	path := filepath.Join(s.Root, "mailbox", m.key()+".json")
	data, err := json.Marshal(m)
	if len(data) > 64<<10 {
		return errors.New("encoded SMS exceeds 64KiB")
	}
	if err != nil {
		return err
	}
	prior, err := os.ReadFile(path)
	if err == nil {
		if !bytes.Equal(data, prior) {
			return ErrConflict
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	return atomicWrite(path, data)
}

// Deliver accepts a trusted external sender. Agent-originated messages go through
// FinishCooperative, which binds sender identity to the fenced execution.
func (s *Store) Deliver(m Message) error { s.mu.Lock(); defer s.mu.Unlock(); return s.deliver(m) }
func (s *Store) reconcile(r *CooperativeRecord) error {
	changed := false
	for _, m := range r.Outbox {
		if err := s.deliver(m); err != nil {
			return err
		}
	}
	if len(r.Outbox) > 0 {
		r.Outbox = nil
		changed = true
	}
	if r.Status == "waiting" && r.Wait != nil {
		lookup := Message{To: AddressOf(r.Task), From: r.Wait.From, Correlation: r.Wait.Correlation}
		var m Message
		err := readJSON(filepath.Join(s.Root, "mailbox", lookup.key()+".json"), &m)
		if err == nil {
			if err = m.validate(); err != nil {
				return err
			}
			if m.key() != lookup.key() {
				return errors.New("mailbox identity mismatch")
			}
			r.Inbox = &m
			r.Wait = nil
			r.Status = "ready"
			changed = true
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if changed {
		return s.saveCooperative(*r)
	}
	return nil
}

// ScanCooperative streams filenames in bounded batches; histories never all load
// into RAM. The caller may stop after finding enough runnable work.
func (s *Store) ScanCooperative(visit func(CooperativeRecord) (bool, error)) error {
	dir, err := os.Open(filepath.Join(s.Root, "cooperation"))
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		names, err := dir.Readdirnames(32)
		if err != nil && err != io.EOF {
			return err
		}
		for _, name := range names {
			if !strings.HasSuffix(name, ".json") {
				continue
			}
			s.mu.Lock()
			r, e := readCooperative(filepath.Join(s.Root, "cooperation", name))
			if e == nil {
				e = s.reconcile(&r)
			}
			s.mu.Unlock()
			if e != nil {
				return e
			}
			stop, e := visit(r)
			if e != nil {
				return e
			}
			if stop {
				return nil
			}
		}
		if err == io.EOF {
			return nil
		}
	}
}

// liveLease retains Go's monotonic clock component, which JSON cannot preserve.
// Only this Store owner may renew it; reopening requires explicit recovery/fencing.
type liveLease struct {
	generation uint64
	deadline   time.Time
}

// now supplies wall-clock audit metadata. Expiry uses the owner's monotonic clock.
func (s *Store) LeaseCooperative(id string, now time.Time, ttl time.Duration) (CooperativeRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := readCooperative(s.coopPath(id))
	if err != nil {
		return r, err
	}
	if r.Status != "ready" || len(r.Outbox) > 0 {
		return r, ErrStaleLease
	}
	// Uncertain sessions cannot be silently resumed by another task.
	if _, err := os.Stat(filepath.Join(s.Root, "uncertain", r.Task.Key()+".json")); err == nil {
		return r, errors.New("role session quarantined")
	} else if !os.IsNotExist(err) {
		return r, err
	}
	if ttl <= 0 {
		return r, errors.New("lease TTL must be positive")
	}
	if len(s.liveLeases) >= 64 {
		return r, errors.New("active lease limit reached")
	}
	liveDeadline := s.leaseClock().Add(ttl)
	r.Generation++
	r.Status = "running"
	r.Deadline = now.Add(ttl)
	r.LastHeartbeat = time.Time{}
	if err := s.saveCooperative(r); err != nil {
		return r, err
	}
	s.liveLeases[id] = liveLease{r.Generation, liveDeadline}
	return r, nil
}
func (s *Store) leaseValid(r CooperativeRecord, g uint64) bool {
	lease, ok := s.liveLeases[r.Task.ID]
	return ok && r.Status == "running" && r.Generation == g && lease.generation == g && s.leaseClock().Before(lease.deadline)
}
func (s *Store) RenewCooperative(id string, g uint64, now time.Time, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := readCooperative(s.coopPath(id))
	if err != nil {
		return err
	}
	if !s.leaseValid(r, g) {
		return staleLease("renew", r, g, now)
	}
	if ttl <= 0 {
		return errors.New("lease TTL must be positive")
	}
	liveDeadline := s.leaseClock().Add(ttl)
	r.LastHeartbeat = now
	r.Deadline = now.Add(ttl)
	if err := s.saveCooperative(r); err != nil {
		return err
	}
	s.liveLeases[id] = liveLease{g, liveDeadline}
	return nil
}
func (s *Store) FinishCooperative(id string, g uint64, t Transition, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := readCooperative(s.coopPath(id))
	if err != nil {
		return err
	}
	if !s.leaseValid(r, g) {
		return staleLease("finish", r, g, now)
	}
	if err = t.validate(r.Task); err != nil {
		return err
	}
	r.Checkpoint = t.Checkpoint
	r.Outbox = t.Send
	r.Wait = t.Wait
	r.Text = t.Text
	r.Inbox = nil
	r.Status = "completed"
	r.Deadline = time.Time{}
	if t.Wait != nil {
		r.Status = "waiting"
	}
	// Single atomic commit: checkpoint, wait condition and outbox. Reconciliation
	// may run after a crash, including after partial delivery of this outbox.
	if err := s.saveCooperative(r); err != nil {
		return err
	}
	delete(s.liveLeases, id)
	return nil
}
func (s *Store) FenceCooperative(id string, g uint64, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := readCooperative(s.coopPath(id))
	if err != nil {
		return err
	}
	if r.Generation != g || r.Status != "running" {
		return ErrStaleLease
	}
	r.Generation++
	r.Status = "uncertain"
	r.Error = cause.Error()
	if len(r.Error) > 8192 {
		r.Error = r.Error[:8192]
	}
	r.Deadline = time.Time{}
	// Quarantine first: a crash in between still prevents unsafe new activation.
	if err = s.Quarantine(r.Task, cause); err != nil {
		return err
	}
	if err := s.saveCooperative(r); err != nil {
		return err
	}
	delete(s.liveLeases, id)
	return nil
}

// Recovery never replays running work: tools may have executed before a crash.
func (s *Store) RecoverCooperative() error {
	return s.ScanCooperative(func(r CooperativeRecord) (bool, error) {
		if r.Status == "running" {
			return false, s.FenceCooperative(r.Task.ID, r.Generation, errors.New("runtime restarted during execution"))
		}
		return false, nil
	})
}
