package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/shakystar/role-runtime/internal/lifecycle"
	"os"
	"path/filepath"
	"time"
)

const terminalReserve = 4 * lifecycle.MaxEntry

func activeStatus(status string) bool {
	return status == "ready" || status == "running" || status == "waiting"
}
func (s *Store) reserved() int {
	n := 0
	for _, v := range s.lifecycleReservations {
		n += v
	}
	return n
}

// EnableLifecycle must precede submission/worker startup. Existing legacy state is
// explicitly incomplete; running work is still fenced by RecoverCooperative.
func (s *Store) EnableLifecycle(run string, limit int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !identifier.MatchString(run) || s.ledger != nil {
		return errors.New("invalid lifecycle binding")
	}
	l, e := lifecycle.Open(filepath.Join(s.Root, "lifecycle"), run, limit)
	if e != nil {
		return e
	}
	s.ledger = l
	s.lifecycleRun = run
	s.lifecycleReservations = map[string]int{}
	s.lifecycleStarted = map[string]time.Time{}
	s.lifecycleWaiting = map[string]time.Time{}
	if _, e := os.Stat(filepath.Join(s.Root, "lifecycle", "history-incomplete")); e == nil {
		s.lifecycleIncomplete = true
	} else if !os.IsNotExist(e) {
		return e
	}
	paths, e := filepath.Glob(filepath.Join(s.Root, "cooperation", "*.json"))
	if e != nil {
		return e
	}
	for _, path := range paths {
		r, e := readCooperative(path)
		if e != nil {
			return e
		}
		if r.SchemaVersion != 1 {
			s.lifecycleIncomplete = true
		}
		if activeStatus(r.Status) {
			s.lifecycleReservations[r.Task.ID] = terminalReserve
		} else {
			s.lifecycleReservations[r.Task.ID] = s.pendingCost(r)
		}
	}
	if s.lifecycleIncomplete {
		if e = lifecycle.Write(filepath.Join(s.Root, "lifecycle", "history-incomplete"), []byte("true")); e != nil {
			return e
		}
	}
	if l.Used()+s.reserved() > l.Limit() {
		return lifecycle.ErrCapacity
	}
	for _, path := range paths {
		r, e := readCooperative(path)
		if e != nil {
			return e
		}
		if e = s.project(&r); e != nil {
			return e
		}
	}
	return nil
}
func (s *Store) pendingCost(r CooperativeRecord) int {
	n := 0
	for _, event := range r.Events {
		if !s.ledger.Has(event.ID) {
			b, _ := json.Marshal(event)
			entry, _ := json.Marshal(lifecycle.Entry{ID: event.ID, Kind: "event", Data: b})
			n += len(entry)
		}
	}
	return n
}
func (s *Store) project(r *CooperativeRecord) error {
	terminal := !activeStatus(r.Status)
	if terminal {
		s.lifecycleReservations[r.Task.ID] = s.pendingCost(*r)
	}
	for _, event := range r.Events {
		b, e := json.Marshal(event)
		if e != nil {
			return e
		}
		reserved := s.reserved()
		if terminal {
			reserved -= s.lifecycleReservations[r.Task.ID]
		}
		if e = s.ledger.Append(lifecycle.Entry{ID: event.ID, Kind: "event", Data: b}, reserved); e != nil {
			return e
		}
		if terminal {
			s.lifecycleReservations[r.Task.ID] = s.pendingCost(*r)
		}
	}
	if len(r.Events) > 0 {
		r.Events = nil
		b, e := encodeCooperative(*r)
		if e != nil {
			return e
		}
		return atomicWrite(s.coopPath(r.Task.ID), b)
	}
	return nil
}
func (s *Store) saveLifecycle(r CooperativeRecord) error { return s.saveLifecycleExtra(r, "", nil, "") }
func (s *Store) saveLifecycleExtra(r CooperativeRecord, extra string, extraPayload map[string]any, extraID string) error {
	prior, e := readCooperative(s.coopPath(r.Task.ID))
	fresh := os.IsNotExist(e)
	if e != nil && !fresh {
		return e
	}
	if !fresh {
		if e = s.project(&prior); e != nil {
			s.lifecycleError = e
			return e
		}
	}
	r.SchemaVersion = 1
	r.AttemptID = prior.AttemptID
	if r.Status == "running" && prior.Status != "running" {
		r.AttemptID = Digest(s.lifecycleRun, r.Task.ID, fmt.Sprint(r.Generation))
	}
	r.Revision = prior.Revision
	r.Events = nil
	kinds := []string{}
	if fresh {
		kinds = append(kinds, "task.accepted")
	} else if prior.Status != r.Status {
		if prior.Status == "running" {
			kinds = append(kinds, "attempt.finished")
		}
		switch r.Status {
		case "running":
			kinds = append(kinds, "attempt.started")
		case "waiting":
			kinds = append(kinds, "task.waiting")
		case "ready":
			kinds = append(kinds, "task.ready")
		}
	}
	if extra != "" {
		kinds = append(kinds, extra)
	}
	oldReserve := s.lifecycleReservations[r.Task.ID]
	if activeStatus(r.Status) {
		s.lifecycleReservations[r.Task.ID] = terminalReserve
	} else {
		delete(s.lifecycleReservations, r.Task.ID)
	}
	rollback := func() {
		if oldReserve > 0 {
			s.lifecycleReservations[r.Task.ID] = oldReserve
		} else {
			delete(s.lifecycleReservations, r.Task.ID)
		}
	}
	pendingBytes := 0
	for _, kind := range kinds {
		r.Revision++
		generation := r.Generation
		if kind == "attempt.finished" {
			generation = prior.Generation
		}
		payload := map[string]any{"status": r.Status, "history_complete": !s.lifecycleIncomplete, "project": r.Task.Project}
		if kind == "attempt.started" || kind == "task.ready" {
			payload["wait_duration_ns"] = nil
			payload["wait_duration_complete"] = false
			if began, ok := s.lifecycleWaiting[r.Task.ID]; ok {
				payload["wait_duration_ns"] = time.Since(began).Nanoseconds()
				payload["wait_duration_complete"] = true
			}
		}
		if kind == "attempt.finished" {
			payload["outcome"] = r.Status
			payload["execution"] = "unknown"
			if r.ExecutionState != "" {
				payload["execution"] = r.ExecutionState
			}
			if r.Failure != nil {
				payload["failure"] = r.Failure
			}
			payload["duration_ns"] = nil
			payload["duration_complete"] = false
			if began, ok := s.lifecycleStarted[r.Task.ID]; ok {
				payload["duration_ns"] = time.Since(began).Nanoseconds()
				payload["duration_complete"] = true
			}
		}
		event := lifecycle.Event{SchemaVersion: 1, ID: Digest(s.lifecycleRun, r.Task.ID, fmt.Sprint(r.Revision)), RunID: s.lifecycleRun, TaskID: r.Task.ID, AttemptID: Digest(s.lifecycleRun, r.Task.ID, fmt.Sprint(generation)), Source: "runtime", Revision: r.Revision, Kind: kind, OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Payload: payload}
		if extra != "" && kind == extra {
			event.ID = extraID
			for key, value := range extraPayload {
				event.Payload[key] = value
			}
		}

		b, e := json.Marshal(event)
		if e != nil {
			rollback()
			return e
		}
		entry, _ := json.Marshal(lifecycle.Entry{ID: event.ID, Kind: "event", Data: b})
		if len(entry) > lifecycle.MaxEntry {
			rollback()
			return lifecycle.ErrCapacity
		}
		pendingBytes += len(entry)
		r.Events = append(r.Events, event)
	}
	extraBytes := pendingBytes
	if !activeStatus(r.Status) {
		s.lifecycleReservations[r.Task.ID] = pendingBytes
		extraBytes = 0
	}
	if s.ledger.Used()+s.reserved()+extraBytes > s.ledger.Limit() {
		rollback()
		return lifecycle.ErrCapacity
	}
	b, e := encodeCooperative(r)
	if e != nil {
		rollback()
		return e
	}
	if e = atomicWrite(s.coopPath(r.Task.ID), b); e != nil {
		rollback()
		return e
	}
	if fresh || prior.Status != r.Status {
		if r.Status == "ready" || r.Status == "waiting" {
			s.lifecycleWaiting[r.Task.ID] = time.Now()
		} else {
			delete(s.lifecycleWaiting, r.Task.ID)
		}
	}
	if prior.Status != "running" && r.Status == "running" {
		s.lifecycleStarted[r.Task.ID] = time.Now()
	}
	if prior.Status == "running" && r.Status != "running" {
		delete(s.lifecycleStarted, r.Task.ID)
	}
	// State and intents are durable already. A projection failure must not turn a
	// completed task into a failed/replayed task. Subsequent transitions retry it.
	s.lifecycleError = s.project(&r)
	return nil
}
func (s *Store) LifecyclePage(cursor *string, limit int) ([]lifecycle.Entry, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ledger == nil {
		return nil, "", errors.New("unavailable")
	}
	paths, e := filepath.Glob(filepath.Join(s.Root, "cooperation", "*.json"))
	if e != nil {
		return nil, "", e
	}
	for _, path := range paths {
		r, e := readCooperative(path)
		if e != nil {
			return nil, "", e
		}
		if e = s.project(&r); e != nil {
			s.lifecycleError = e
			return nil, "", e
		}
	}
	s.lifecycleError = nil
	return s.ledger.Page(cursor, limit)
}
func (s *Store) LifecycleHealth() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ledger == nil {
		return map[string]any{"enabled": false}
	}
	return map[string]any{"enabled": true, "used_bytes": s.ledger.Used(), "limit_bytes": s.ledger.Limit(), "reserved_bytes": s.reserved(), "projection_ok": s.lifecycleError == nil, "history_complete": !s.lifecycleIncomplete}
}

// RecordStartupRetry attaches preflight recovery to an already admitted attempt.
// Worker construction before task admission has no attempt and is not attributed.
func (s *Store) RecordStartupRetry(taskID string, generation uint64, index int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ledger == nil {
		return nil
	}
	if index < 1 || index > 5 {
		return errors.New("invalid retry index")
	}
	r, e := readCooperative(s.coopPath(taskID))
	if e != nil {
		return e
	}
	if r.Status != "running" || r.Generation != generation {
		return ErrStaleLease
	}
	return s.recordAux(r, "startup.retry", map[string]any{"retry_index": index}, fmt.Sprint(generation, "/", index))
}
func (s *Store) RecordCacheHit(taskID, delivery string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ledger == nil {
		return nil
	}
	if !identifier.MatchString(delivery) {
		return errors.New("invalid delivery identity")
	}
	r, e := readCooperative(s.coopPath(taskID))
	if e != nil {
		return e
	}
	if r.Status != "completed" {
		return errors.New("not completed")
	}
	return s.recordAux(r, "result.cache_hit", map[string]any{"delivery_id": delivery}, delivery)
}
func (s *Store) recordAux(r CooperativeRecord, kind string, payload map[string]any, key string) error {
	if e := s.project(&r); e != nil {
		return e
	}
	id := Digest(s.lifecycleRun, r.Task.ID, kind, key)
	if s.ledger.Has(id) {
		return nil
	}
	return s.saveLifecycleExtra(r, kind, payload, id)
}
