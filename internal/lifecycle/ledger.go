// Package lifecycle stores bounded, replayable runtime facts, independently of telemetry.
package lifecycle

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var ErrCapacity = errors.New("capacity")
var ErrConflict = errors.New("conflict")
var ErrCursor = errors.New("cursor_expired")

const MaxEntry = 16 << 10
const DefaultLimit = 64 << 20

type Event struct {
	SchemaVersion int            `json:"schema_version"`
	ID            string         `json:"event_id"`
	RunID         string         `json:"run_id"`
	TaskID        string         `json:"task_id"`
	AttemptID     string         `json:"attempt_id"`
	Source        string         `json:"source"`
	Revision      uint64         `json:"revision"`
	Kind          string         `json:"kind"`
	OccurredAt    string         `json:"occurred_at"`
	Payload       map[string]any `json:"payload"`
}
type Entry struct {
	ID   string          `json:"id"`
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}
type Ledger struct {
	mu          sync.Mutex
	root, epoch string
	limit, used int
	rows        []Entry
	ids         map[string][]byte
}

func Write(path string, b []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".pending-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	if ce != nil {
		return ce
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func Open(root, epoch string, limit int) (*Ledger, error) {
	if limit <= 0 {
		limit = DefaultLimit
	}
	if limit > DefaultLimit {
		return nil, ErrCapacity
	}
	if e := os.MkdirAll(root, 0700); e != nil {
		return nil, e
	}
	binding := filepath.Join(root, "binding.json")
	b, _ := json.Marshal(epoch)
	old, e := os.ReadFile(binding)
	if e == nil && !bytes.Equal(old, b) {
		return nil, ErrConflict
	}
	if e != nil && !os.IsNotExist(e) {
		return nil, e
	}
	if os.IsNotExist(e) {
		if e = Write(binding, b); e != nil {
			return nil, e
		}
	}
	generationPath := filepath.Join(root, "epoch.json")
	generationBytes, e := os.ReadFile(generationPath)
	var generation string
	if os.IsNotExist(e) {
		random := make([]byte, 16)
		if _, e = rand.Read(random); e != nil {
			return nil, e
		}
		generation = hex.EncodeToString(random)
		generationBytes, _ = json.Marshal(generation)
		if e = Write(generationPath, generationBytes); e != nil {
			return nil, e
		}
	} else if e != nil {
		return nil, e
	} else if json.Unmarshal(generationBytes, &generation) != nil || len(generation) != 32 {
		return nil, ErrConflict
	}
	l := &Ledger{root: root, epoch: generation, limit: limit, ids: map[string][]byte{}}
	names, e := filepath.Glob(filepath.Join(root, "*.entry"))
	if e != nil {
		return nil, e
	}
	sort.Strings(names)
	for i, path := range names {
		if filepath.Base(path) != fmt.Sprintf("%012d.entry", i+1) {
			return nil, ErrConflict
		}
		info, e := os.Stat(path)
		if e != nil {
			return nil, e
		}
		if info.Size() > MaxEntry || l.used+int(info.Size()) > limit {
			return nil, ErrCapacity
		}
		data, e := os.ReadFile(path)
		if e != nil {
			return nil, e
		}
		var row Entry
		if json.Unmarshal(data, &row) != nil || row.ID == "" {
			return nil, ErrConflict
		}
		if _, ok := l.ids[row.ID]; ok {
			return nil, ErrConflict
		}
		l.ids[row.ID] = data
		l.rows = append(l.rows, row)
		l.used += len(data)
	}
	return l, nil
}
func (l *Ledger) Used() int  { l.mu.Lock(); defer l.mu.Unlock(); return l.used }
func (l *Ledger) Limit() int { return l.limit }
func (l *Ledger) Append(row Entry, reserved int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	data, e := json.Marshal(row)
	if e != nil {
		return e
	}
	if old, ok := l.ids[row.ID]; ok {
		if !bytes.Equal(old, data) {
			return ErrConflict
		}
		return nil
	}
	if len(data) > MaxEntry || l.used+len(data)+reserved > l.limit {
		return ErrCapacity
	}
	path := filepath.Join(l.root, fmt.Sprintf("%012d.entry", len(l.rows)+1))
	if prior, readErr := os.ReadFile(path); readErr == nil {
		if !bytes.Equal(prior, data) {
			return ErrConflict
		}
	} else if !os.IsNotExist(readErr) {
		return readErr
	}
	if e = Write(path, data); e != nil {
		return e
	}
	l.rows = append(l.rows, row)
	l.ids[row.ID] = data
	l.used += len(data)
	return nil
}
func (l *Ledger) Page(cursor *string, limit int) ([]Entry, string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if limit < 1 || limit > 100 {
		return nil, "", errors.New("invalid_request")
	}
	pos := 0
	if cursor != nil {
		var epoch string
		var n int
		if _, e := fmt.Sscanf(*cursor, "%s", &epoch); e != nil {
			return nil, "", ErrCursor
		}
		prefix := l.epoch + ":"
		if len(*cursor) <= len(prefix) || (*cursor)[:len(prefix)] != prefix {
			return nil, "", ErrCursor
		}
		if _, e := fmt.Sscanf((*cursor)[len(prefix):], "%d", &n); e != nil || n < 0 || n > len(l.rows) || fmt.Sprintf("%s%d", prefix, n) != *cursor {
			return nil, "", ErrCursor
		}
		pos = n
	}
	end := pos + limit
	if end > len(l.rows) {
		end = len(l.rows)
	}
	rows := append([]Entry{}, l.rows[pos:end]...)
	return rows, fmt.Sprintf("%s:%d", l.epoch, end), nil
}

func (l *Ledger) Has(id string) bool { l.mu.Lock(); defer l.mu.Unlock(); _, ok := l.ids[id]; return ok }
