// Package control is a local bounded API hosted by the runtime's Store owner.
package control

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/shakystar/role-runtime/internal/engine"
	"github.com/shakystar/role-runtime/internal/lifecycle"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sync"
	"syscall"
	"time"
)

const maxRequest = 64 << 10

var ident = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type Artifact struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
}
type Notice struct {
	DeliveryID string         `json:"delivery_id"`
	Message    engine.Message `json:"message"`
	Producer   string         `json:"producer_task_id"`
	Artifact   Artifact       `json:"artifact"`
}
type Reference struct {
	Producer string   `json:"producer_task_id"`
	Artifact Artifact `json:"artifact"`
}
type Config struct {
	RunID     string                    `json:"run_id"`
	Project   string                    `json:"project"`
	Consumers         map[string]engine.Address `json:"consumers"`
	ApprovedRevisions bool                      `json:"approved_revisions,omitempty"`
}
type Server struct {
	stop      chan struct{}
	closeOnce sync.Once
	Store     *engine.Store
	Config    Config
	Socket    string
	mu        sync.Mutex
	notices   *lifecycle.Ledger
	listener  net.Listener
	lock      *os.File
	wg        sync.WaitGroup
}
type request struct {
	Version   int             `json:"version"`
	RequestID string          `json:"request_id"`
	RunID     string          `json:"run_id"`
	Method    string          `json:"method"`
	Params    json.RawMessage `json:"params"`
}

func strict(data []byte, out any) error {
	var scan func(*json.Decoder) error
	scan = func(d *json.Decoder) error {
		tok, e := d.Token()
		if e != nil {
			return e
		}
		switch tok {
		case json.Delim('{'):
			seen := map[string]bool{}
			for d.More() {
				key, e := d.Token()
				if e != nil {
					return e
				}
				k, ok := key.(string)
				if !ok || seen[k] {
					return errors.New("duplicate key")
				}
				seen[k] = true
				if e = scan(d); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		case json.Delim('['):
			for d.More() {
				if e = scan(d); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		}
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if e := scan(d); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return errors.New("extra JSON")
	}
	// encoding/json matches field names case-insensitively; the wire does not.
	if e := exactFields(data, reflect.TypeOf(out).Elem()); e != nil {
		return e
	}

	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	return d.Decode(out)
}
func exactFields(data []byte, typ reflect.Type) error {
	if typ == reflect.TypeOf(json.RawMessage{}) {
		return nil
	}
	if typ.Kind() == reflect.Pointer {
		if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
			return nil
		}
		return exactFields(data, typ.Elem())
	}
	if typ.Kind() != reflect.Struct {
		return nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return errors.New("object required")
	}
	allowed := map[string]reflect.Type{}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("json")
		if tag != "" {
			allowed[tag] = field.Type
		}
	}
	for key, value := range fields {
		child, ok := allowed[key]
		if !ok {
			return errors.New("unknown field")
		}
		if e := exactFields(value, child); e != nil {
			return e
		}
	}
	return nil
}

func (s *Server) Start(ctx context.Context) error {
	if s.Store == nil || !ident.MatchString(s.Config.RunID) || !ident.MatchString(s.Config.Project) || len(s.Config.Consumers) == 0 {
		return errors.New("invalid config")
	}
	for id, a := range s.Config.Consumers {
		if !ident.MatchString(id) || a.Project != s.Config.Project {
			return errors.New("invalid consumer")
		}
	}
	dir := filepath.Join(s.Store.Root, "control")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.OpenFile(filepath.Join(dir, "owner.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return e
	}
	s.lock = f
	success := false
	defer func() {
		if !success {
			f.Close()
		}
	}()
	config, _ := json.Marshal(s.Config)
	path := filepath.Join(dir, "binding.json")
	old, e := os.ReadFile(path)
	if e == nil && !bytes.Equal(old, config) {
		return lifecycle.ErrConflict
	}
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	if os.IsNotExist(e) {
		if e = lifecycle.Write(path, config); e != nil {
			return e
		}
	}
	s.notices, e = lifecycle.Open(filepath.Join(dir, "notifications"), s.Config.RunID, 0)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Join(dir, "acks"), 0700); e != nil {
		return e
	}
	expectedSocket := filepath.Join(dir, "runtime.sock")
	if s.Socket == "" {
		s.Socket = expectedSocket
	}
	if filepath.Clean(s.Socket) != expectedSocket {
		return errors.New("socket must be inside owned control directory")
	}
	// Store and control ownership locks are held before removing a stale socket.
	if info, e := os.Lstat(s.Socket); e == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("not a socket")
		}
		if e = os.Remove(s.Socket); e != nil {
			return e
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	l, e := net.Listen("unix", s.Socket)
	if e != nil {
		return e
	}
	s.listener = l
	s.stop = make(chan struct{})
	if e = os.Chmod(s.Socket, 0600); e != nil {
		l.Close()
		return e
	}
	slots := make(chan struct{}, 4)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			select {
			case slots <- struct{}{}:
				s.wg.Add(1)
				go func() { defer s.wg.Done(); defer func() { <-slots }(); s.serve(c) }()
			default:
				c.SetWriteDeadline(time.Now().Add(time.Second))
				io.WriteString(c, "{\"version\":1,\"ok\":false,\"error\":{\"code\":\"busy\",\"retryable\":true}}\n")
				c.Close()
			}
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
			l.Close()
		case <-s.stop:
		}
	}()
	success = true
	return nil
}
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		if s.stop != nil {
			close(s.stop)
		}
		if s.listener != nil {
			s.listener.Close()
		}
		s.wg.Wait()
		if s.lock != nil {
			s.lock.Close()
		}
		os.Remove(s.Socket)
	})
}

func (s *Server) serve(c net.Conn) {
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(io.LimitReader(c, maxRequest+1))
	data, e := r.ReadBytes('\n')
	var req request
	code := ""
	var result any
	if e != nil || len(data) > maxRequest || strict(data, &req) != nil || !ident.MatchString(req.RequestID) || len(req.Params) == 0 {
		code = "invalid_request"
	} else if req.Version != 1 {
		code = "unsupported_version"
	} else if req.RunID != s.Config.RunID {
		code = "scope_mismatch"
	} else {
		result, code = s.dispatch(req)
	}
	out := map[string]any{"version": 1, "request_id": req.RequestID, "run_id": req.RunID, "ok": code == ""}
	if code == "" {
		out["result"] = result
	} else {
		out["error"] = map[string]any{"code": code, "retryable": code == "busy" || code == "unavailable"}
	}
	b, _ := json.Marshal(out)
	if len(b) > 256<<10 {
		b = []byte(`{"version":1,"ok":false,"error":{"code":"capacity","retryable":false}}`)
	}
	c.Write(append(b, '\n'))
}
func errCode(e error) string {
	if e == nil {
		return ""
	}
	switch {
	case errors.Is(e, lifecycle.ErrCapacity):
		return "capacity"
	case errors.Is(e, lifecycle.ErrCursor):
		return "cursor_expired"
	case errors.Is(e, lifecycle.ErrConflict), errors.Is(e, engine.ErrConflict):
		return "conflict"
	case os.IsNotExist(e):
		return "not_found"
	}
	return "unavailable"
}
func (s *Server) artifactOK(a Artifact) bool {
	if !ident.MatchString(a.ID) || !ident.MatchString(a.Version) || len(a.SHA256) != 64 {
		return false
	}
	root, e := os.OpenRoot(filepath.Join(s.Store.Root, "artifacts"))
	if e != nil {
		return false
	}
	defer root.Close()
	f, e := root.Open(a.ID)
	if e != nil {
		return false
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if e != nil || len(b) > 1<<20 {
		return false
	}
	hash := sha256.Sum256(b)
	return a.SHA256 == hex.EncodeToString(hash[:])
}
func (s *Server) receipt(id string) (map[string]any, string) {
	if !ident.MatchString(id) {
		return nil, "invalid_request"
	}
	r, e := s.Store.CooperativeReceipt(id)
	if e != nil {
		return nil, errCode(e)
	}
	if r.Task.Project != s.Config.Project {
		return nil, "scope_mismatch"
	}
	var ref Reference
	valid := strict([]byte(r.Text), &ref) == nil && ref.Producer == id
	if valid {
		if s.Config.ApprovedRevisions && r.Task.Role == "producer" && id == s.approvedRevisionID() {
			_, code := s.readApprovedArtifact(ref.Artifact, "revision-"+id+".json")
			valid = code == ""
		} else {
			valid = s.artifactOK(ref.Artifact)
		}
	}
	return map[string]any{"task": r.Task, "status": r.Status, "receipt_revision": r.Revision, "attempt_id": r.AttemptID, "artifact": ref.Artifact, "artifact_valid": valid, "generation": r.Generation, "failure": r.Failure, "execution": r.ExecutionState}, ""
}
func (s *Server) syncNotices(to engine.Address) error {
	return s.Store.Messages(to, func(m engine.Message) error {
		var ref Reference
		if strict([]byte(m.Body), &ref) != nil || !ident.MatchString(ref.Producer) {
			return nil
		}
		id := engine.Digest(s.Config.RunID, m.From.Project, m.From.Role, m.From.Version, m.From.Instance, m.To.Project, m.To.Role, m.To.Version, m.To.Instance, m.Correlation)
		n := Notice{id, m, ref.Producer, ref.Artifact}
		data, e := json.Marshal(n)
		if e != nil {
			return e
		}
		return s.notices.Append(lifecycle.Entry{ID: id, Kind: "notification", Data: data}, 0)
	})
}
func (s *Server) dispatch(req request) (any, string) {
	switch req.Method {
	case "health.get":
		var p struct{}
		if strict(req.Params, &p) != nil {
			return nil, "invalid_request"
		}
		return s.Store.LifecycleHealth(), ""
	case "receipts.get":
		var p struct {
			TaskID string `json:"task_id"`
		}
		if strict(req.Params, &p) != nil {
			return nil, "invalid_request"
		}
		return s.receipt(p.TaskID)
	case "events.list":
		var p struct {
			After *string `json:"after_cursor"`
			Limit int     `json:"limit"`
		}
		if strict(req.Params, &p) != nil || p.Limit < 1 || p.Limit > 100 {
			return nil, "invalid_request"
		}
		rows, next, e := s.Store.LifecyclePage(p.After, p.Limit)
		if e != nil {
			return nil, errCode(e)
		}
		filtered := []lifecycle.Entry{}
		for _, row := range rows {
			var event lifecycle.Event
			if json.Unmarshal(row.Data, &event) != nil {
				return nil, "unavailable"
			}
			if event.Payload["project"] == s.Config.Project {
				filtered = append(filtered, row)
			}
		}
		return map[string]any{"items": filtered, "next_cursor": next}, ""
	case "notifications.list":
		var p struct {
			Consumer string  `json:"consumer_id"`
			After    *string `json:"after_cursor"`
			Limit    int     `json:"limit"`
		}
		if strict(req.Params, &p) != nil || p.Limit < 1 || p.Limit > 100 {
			return nil, "invalid_request"
		}
		to, ok := s.Config.Consumers[p.Consumer]
		if !ok {
			return nil, "scope_mismatch"
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if e := s.syncNotices(to); e != nil {
			return nil, errCode(e)
		}
		rows, next, e := s.notices.Page(p.After, p.Limit)
		if e != nil {
			return nil, errCode(e)
		}
		items := []Notice{}
		for _, row := range rows {
			var n Notice
			if json.Unmarshal(row.Data, &n) != nil {
				return nil, "unavailable"
			}
			if n.Message.To == to {
				items = append(items, n)
			}
		}
		return map[string]any{"items": items, "next_cursor": next}, ""
	case "notifications.ack":
		var p struct {
			Consumer   string `json:"consumer_id"`
			Delivery   string `json:"delivery_id"`
			Checkpoint string `json:"checkpoint_id"`
		}
		if strict(req.Params, &p) != nil || !ident.MatchString(p.Delivery) || !ident.MatchString(p.Checkpoint) {
			return nil, "invalid_request"
		}
		to, ok := s.Config.Consumers[p.Consumer]
		if !ok {
			return nil, "scope_mismatch"
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		found := false
		var cur *string
		for {
			rows, next, e := s.notices.Page(cur, 100)
			if e != nil {
				return nil, errCode(e)
			}
			for _, row := range rows {
				if row.ID == p.Delivery {
					var n Notice
					if json.Unmarshal(row.Data, &n) != nil {
						return nil, "unavailable"
					}
					if n.Message.To != to {
						return nil, "scope_mismatch"
					}
					found = true
				}
			}
			if len(rows) < 100 {
				break
			}
			cur = &next
		}
		if !found {
			return nil, "not_found"
		}
		path := filepath.Join(s.Store.Root, "control", "acks", engine.Digest(p.Consumer, p.Delivery)+".json")
		b, _ := json.Marshal(p)
		old, e := os.ReadFile(path)
		if e == nil {
			if !bytes.Equal(old, b) {
				return nil, "conflict"
			}
			return map[string]bool{"acknowledged": true}, ""
		}
		if !os.IsNotExist(e) {
			return nil, errCode(e)
		}
		e = lifecycle.Write(path, b)
		return map[string]bool{"acknowledged": e == nil}, errCode(e)
	case "revisions.approve":
		if !s.Config.ApprovedRevisions {
			return nil, "invalid_request"
		}
		return s.approveRevision(req)
	case "tasks.submit":
		var p struct {
			Consumer string      `json:"consumer_id"`
			Task     engine.Task `json:"task"`
		}
		if strict(req.Params, &p) != nil {
			return nil, "invalid_request"
		}
		to, ok := s.Config.Consumers[p.Consumer]
		if !ok || engine.AddressOf(p.Task) != to {
			return nil, "scope_mismatch"
		}
		if p.Task.Validate() != nil {
			return nil, "invalid_request"
		}
		e := s.Store.SubmitCooperative(p.Task)
		if errors.Is(e, engine.ErrDuplicate) {
			r, readErr := s.Store.CooperativeReceipt(p.Task.ID)
			if readErr != nil {
				return nil, errCode(readErr)
			}
			if r.Task != p.Task {
				return nil, "conflict"
			}
			if r.Status == "completed" {
				if e = s.Store.RecordCacheHit(r.Task.ID, req.RequestID); e != nil {
					return nil, errCode(e)
				}
			}
			return map[string]any{"cached": true, "status": r.Status}, ""
		}
		return map[string]bool{"cached": false}, errCode(e)
	}
	return nil, "invalid_request"
}

func (s *Server) approvedRevisionID() string {
	return engine.Digest(s.Config.RunID, "consumer")
}

func (s *Server) submitApprovedRevision(task engine.Task, input string) (any, string) {
	task.ID = s.approvedRevisionID()
	task.Input = input
	if task.Validate() != nil {
		return nil, "invalid_request"
	}
	err := s.Store.SubmitCooperative(task)
	cached := errors.Is(err, engine.ErrDuplicate)
	if err != nil && !cached {
		return nil, errCode(err)
	}
	r, err := s.Store.CooperativeReceipt(task.ID)
	if err != nil {
		return nil, errCode(err)
	}
	if r.Task != task {
		return nil, "conflict"
	}
	return map[string]any{"task_id": task.ID, "cached": cached, "status": r.Status}, ""
}

// DecodeJSON shares the strict bounded-caller JSON shape checks with runtime-owned artifacts.
func DecodeJSON(data []byte, out any) error { return strict(data, out) }
