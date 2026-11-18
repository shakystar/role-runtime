// Package researchflow supplies bounded, committed predecessor evidence to roles.
package researchflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/shakystar/role-runtime/internal/engine"
)

type RoleSpec struct {
	ID                       string
	Dependencies, Recipients []string
	Web                      bool
}
type Source struct {
	ID          string `json:"id"`
	URL         string `json:"url"`
	Title       string `json:"title"`
	Access      string `json:"access"`
	Observation string `json:"observation"`
}
type Document struct {
	Role           string            `json:"role"`
	Summary        string            `json:"summary"`
	Sources        []Source          `json:"sources"`
	Findings       string            `json:"findings"`
	Limitations    []string          `json:"limitations"`
	Decision       string            `json:"decision"`
	NextExperiment string            `json:"nextExperiment"`
	Readbacks      map[string]string `json:"readbacks"`
}
type Reference struct {
	Role   string `json:"role"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}
type Input struct {
	Instruction string               `json:"instruction"`
	Documents   map[string]Document  `json:"documents"`
	References  map[string]Reference `json:"references"`
	Missing     []string             `json:"missing"`
}
type InputAudit struct {
	Role         string               `json:"role"`
	Generation   uint64               `json:"generation"`
	References   map[string]Reference `json:"references"`
	SourceCounts map[string]int       `json:"sourceCounts"`
}

func (f *Flow) Audits() []InputAudit {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, _ := json.Marshal(f.audits)
	var out []InputAudit
	json.Unmarshal(b, &out)
	return out
}

type Flow struct {
	store  *engine.Store
	specs  map[string]RoleSpec
	tasks  map[string]engine.Task
	mu     sync.Mutex
	audits []InputAudit
}

func New(s *engine.Store, specs []RoleSpec, tasks map[string]engine.Task) (*Flow, error) {
	f := &Flow{store: s, specs: map[string]RoleSpec{}, tasks: map[string]engine.Task{}}
	if s == nil || len(specs) == 0 || len(specs) > 16 {
		return nil, errors.New("invalid flow")
	}
	for _, v := range specs {
		if !regexp.MustCompile(`^[a-z][a-z0-9-]{0,40}$`).MatchString(v.ID) || f.specs[v.ID].ID != "" || tasks[v.ID].Role != v.ID {
			return nil, errors.New("invalid role spec")
		}
		f.specs[v.ID] = v
		f.tasks[v.ID] = tasks[v.ID]
	}
	visiting, done := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if done[id] {
			return nil
		}
		if visiting[id] {
			return errors.New("cycle")
		}
		v, ok := f.specs[id]
		if !ok {
			return errors.New("unknown role")
		}
		visiting[id] = true
		seen := map[string]bool{}
		for _, dep := range v.Dependencies {
			if seen[dep] {
				return errors.New("duplicate dependency")
			}
			seen[dep] = true
			if err := visit(dep); err != nil {
				return err
			}
		}
		visiting[id] = false
		done[id] = true
		return nil
	}
	for id, v := range f.specs {
		if err := visit(id); err != nil {
			return nil, err
		}
		seen := map[string]bool{}
		for _, r := range v.Recipients {
			target, ok := f.specs[r]
			if !ok || seen[r] || !contains(target.Dependencies, id) {
				return nil, errors.New("invalid recipient")
			}
			seen[r] = true
		}
		for _, dep := range v.Dependencies {
			if !contains(f.specs[dep].Recipients, id) {
				return nil, errors.New("missing dependency delivery")
			}
		}
	}
	return f, os.MkdirAll(filepath.Join(s.Root, "research-documents"), 0700)
}
func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
func decode(b []byte, v any) error {
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func reference(role string, b []byte) Reference {
	h := sha256.Sum256(b)
	return Reference{role, role + ".json", hex.EncodeToString(h[:])}
}
func (f *Flow) LoadDocument(role string) (Document, Reference, error) {
	var d Document
	t, ok := f.tasks[role]
	if !ok {
		return d, Reference{}, errors.New("unknown role")
	}
	r, e := f.store.CooperativeReceipt(t.ID)
	if e != nil {
		return d, Reference{}, e
	}
	if r.Status != "completed" {
		return d, Reference{}, errors.New("document not committed")
	}
	file, e := os.Open(filepath.Join(f.store.Root, "research-documents", role+".json"))
	if e != nil {
		return d, Reference{}, e
	}
	defer file.Close()
	b, e := io.ReadAll(io.LimitReader(file, 8193))
	if e != nil {
		return d, Reference{}, e
	}
	if len(b) > 8192 || string(b) != r.Text {
		return d, Reference{}, errors.New("artifact differs from committed receipt")
	}
	if e = decode(b, &d); e != nil {
		return d, Reference{}, e
	}
	return d, reference(role, b), nil
}
func (f *Flow) Wrap(e engine.StepExecutor) engine.StepExecutor { return &executor{f, e} }

type executor struct {
	f     *Flow
	inner engine.StepExecutor
}

func (e *executor) Heartbeat(c context.Context) error { return e.inner.Heartbeat(c) }
func (e *executor) Close() error                      { return e.inner.Close() }
func (e *executor) Step(c context.Context, x engine.Execution) (engine.Transition, error) {
	fail := func(err error) (engine.Transition, error) { return engine.Transition{}, err }
	spec, ok := e.f.specs[x.Task.Role]
	if !ok || x.Task != e.f.tasks[x.Task.Role] {
		return fail(errors.New("task identity mismatch"))
	}
	in := Input{Instruction: x.Task.Input, Documents: map[string]Document{}, References: map[string]Reference{}, Missing: []string{}}
	seen := map[string]bool{}
	var load func(string) error
	load = func(role string) error {
		if seen[role] {
			return nil
		}
		seen[role] = true
		task := e.f.tasks[role]
		r, err := e.f.store.CooperativeReceipt(task.ID)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if r.Status != "completed" {
			return nil
		}
		d, ref, err := e.f.LoadDocument(role)
		if err != nil {
			return err
		}
		in.Documents[role] = d
		in.References[role] = ref
		for _, dep := range e.f.specs[role].Dependencies {
			if err := load(dep); err != nil {
				return err
			}
		}
		return nil
	}
	for _, dep := range spec.Dependencies {
		if err := load(dep); err != nil {
			return fail(err)
		}
		if _, ok := in.Documents[dep]; !ok {
			in.Missing = append(in.Missing, dep)
		}
	}
	if x.Inbox != nil {
		m := x.Inbox
		dep := m.From.Role
		ref, exists := in.References[dep]
		var got Reference
		if !exists || !contains(spec.Dependencies, dep) || m.From != engine.AddressOf(e.f.tasks[dep]) || m.To != engine.AddressOf(e.f.tasks[spec.ID]) || m.Correlation != dep+"-v1" || m.ID != dep+"-to-"+spec.ID || decode([]byte(m.Body), &got) != nil || got != ref {
			return fail(errors.New("inbox artifact reference mismatch"))
		}
	}
	b, err := json.Marshal(in)
	if err != nil || len(b) > 40<<10 {
		return fail(errors.New("input exceeds 40KiB"))
	}
	x.Task.Input = string(b)
	envelope, _ := json.Marshal(x)
	if len(envelope) > 64<<10 {
		return fail(errors.New("execution exceeds 64KiB"))
	}
	audit := InputAudit{Role: spec.ID, Generation: x.Generation, References: in.References, SourceCounts: map[string]int{}}
	for role, d := range in.Documents {
		audit.SourceCounts[role] = len(d.Sources)
	}
	e.f.mu.Lock()
	if len(e.f.audits) >= 128 {
		e.f.mu.Unlock()
		return fail(errors.New("input audit limit exceeded"))
	}
	e.f.audits = append(e.f.audits, audit)
	e.f.mu.Unlock()
	tr, err := e.inner.Step(c, x)
	if err != nil {
		return fail(err)
	}
	if tr.Wait != nil {
		valid := false
		for _, dep := range in.Missing {
			if tr.Wait.From == engine.AddressOf(e.f.tasks[dep]) && tr.Wait.Correlation == dep+"-v1" {
				valid = true
			}
		}
		if !valid || tr.Checkpoint == "" || len(tr.Send) > 0 || tr.Text != "" {
			return fail(errors.New("invalid dependency wait"))
		}
		return tr, nil
	}
	if len(in.Missing) > 0 {
		return fail(errors.New("completion before dependencies"))
	}
	var doc Document
	b = []byte(tr.Text)
	if len(b) > 8192 {
		return fail(errors.New("document exceeds 8KiB"))
	}
	if err := decode(b, &doc); err != nil {
		return fail(err)
	}
	if doc.Role != spec.ID || strings.TrimSpace(doc.Summary) == "" || strings.TrimSpace(doc.Findings) == "" || doc.Sources == nil || len(doc.Sources) > 5 || len(doc.Limitations) == 0 || doc.Readbacks == nil || len(doc.Readbacks) != len(spec.Dependencies) {
		return fail(errors.New("invalid document"))
	}
	for _, dep := range spec.Dependencies {
		if doc.Readbacks[dep] != in.Documents[dep].Summary {
			return fail(errors.New("dependency readback mismatch"))
		}
	}
	ids := map[string]bool{}
	for _, s := range doc.Sources {
		u, er := url.Parse(s.URL)
		if er != nil || u.Hostname() == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") || s.ID == "" || ids[s.ID] || s.Title == "" || s.Observation == "" || !contains([]string{"body-read", "search-snippet", "access-failed"}, s.Access) {
			return fail(errors.New("invalid source"))
		}
		ids[s.ID] = true
	}
	if len(tr.Send) != len(spec.Recipients) {
		return fail(errors.New("recipient count mismatch"))
	}
	ref := reference(spec.ID, b)
	body, _ := json.Marshal(ref)
	recipients := map[string]bool{}
	for i, m := range tr.Send {
		r := m.To.Role
		if !contains(spec.Recipients, r) || recipients[r] || m.To != engine.AddressOf(e.f.tasks[r]) || m.From != engine.AddressOf(e.f.tasks[spec.ID]) || m.ID != spec.ID+"-to-"+r || m.Correlation != spec.ID+"-v1" || m.Body != "ready" {
			return fail(errors.New("invalid ready SMS"))
		}
		recipients[r] = true
		tr.Send[i].Body = string(body)
	}
	// Artifact is prepared before engine commit. Readers require completed receipt
	// and exact bytes, so interrupted preparation never publishes evidence.
	e.f.mu.Lock()
	defer e.f.mu.Unlock()
	path := filepath.Join(e.f.store.Root, "research-documents", ref.Path)
	if old, err := os.ReadFile(path); err == nil {
		if string(old) != string(b) {
			return fail(errors.New("immutable artifact conflict"))
		}
		return tr, nil
	} else if !os.IsNotExist(err) {
		return fail(err)
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".prepare-")
	if err != nil {
		return fail(err)
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err = file.Write(b); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return fail(err)
	}
	if closeErr != nil {
		return fail(closeErr)
	}
	if err = os.Rename(name, path); err != nil {
		return fail(err)
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fail(err)
	}
	err = dir.Sync()
	dir.Close()
	if err != nil {
		return fail(fmt.Errorf("sync artifact directory: %w", err))
	}
	return tr, nil
}
