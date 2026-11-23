package workflowmodel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	sdk "github.com/shakystar/codex-adapter"
	"github.com/shakystar/role-runtime/internal/control"
	"github.com/shakystar/role-runtime/internal/engine"
)

const (
	approvalOriginalTitle = "Session receipt handoff"
	approvalOriginalBody  = "One pilot team could not reconcile a revision with its original session."
	approvalRevisedTitle  = "Session receipt handoff with a checkpoint"
	approvalRevisedBody   = "Match the revision receipt to the original session before advancing the checkpoint."
	approvalReason        = "Add a checkpoint that explicitly verifies the original session receipt."
	approvalObservation   = "The pilot team found a revision receipt but could not identify its source session."
)

type approvalModel struct {
	fakeModel
	contract *WorkContract
	action   string
}

func (m *approvalModel) Execute(_ context.Context, r sdk.Request) (sdk.Result, error) {
	m.requests = append(m.requests, r)
	switch len(m.requests) {
	case 1:
		var contract WorkContract
		if r.Role != "producer" || json.Unmarshal([]byte(r.Input), &contract) != nil || !reflect.DeepEqual(contract, *m.contract) {
			return sdk.Result{}, errors.New("original producer did not receive the work contract")
		}
		return sdk.Result{Text: `{"title":"` + approvalOriginalTitle + `","body":"` + approvalOriginalBody + `"}`}, nil
	case 2:
		var input struct {
			Document struct {
				Contract WorkContract `json:"task_contract"`
			} `json:"document"`
			Hash string `json:"source_sha256"`
		}
		if r.Role != "consumer" || r.Instructions != ProposalInstruction || json.Unmarshal([]byte(r.Input), &input) != nil ||
			!reflect.DeepEqual(input.Document.Contract, *m.contract) || !strings.Contains(r.Input, approvalOriginalBody) {
			return sdk.Result{}, errors.New("reviewer did not receive the original document and work contract")
		}
		digest, err := hex.DecodeString(input.Hash)
		if err != nil || len(digest) != sha256.Size {
			return sdk.Result{}, fmt.Errorf("invalid source digest %q", input.Hash)
		}
		reason, decisionTest := approvalReason, "Verify that the checkpoint compares the two session receipts."
		if m.action == "complete" {
			reason, decisionTest = "The pilot observation is documented.", ""
		}
		data, err := json.Marshal(proposal{m.action, reason, []string{"receipt-gap"}, decisionTest, input.Hash})
		return sdk.Result{Text: string(data)}, err
	case 3:
		if m.action != "revise" || r.Role != "producer" {
			return sdk.Result{}, fmt.Errorf("unexpected third model request: %v", r.Role)
		}
		if reflect.ValueOf(&r.SessionKey).Elem().IsZero() || !reflect.DeepEqual(r.SessionKey, m.requests[0].SessionKey) {
			return sdk.Result{}, errors.New("revision did not reuse the original producer SDK session key")
		}
		for _, want := range []string{approvalOriginalTitle, approvalOriginalBody, m.contract.ID, m.contract.Objective, m.contract.RequiredItems[0], "receipt-gap", approvalObservation, approvalReason} {
			if !strings.Contains(r.Input, want) {
				return sdk.Result{}, fmt.Errorf("revision input is missing %q", want)
			}
		}
		return sdk.Result{Text: `{"title":"` + approvalRevisedTitle + `","body":"` + approvalRevisedBody + `"}`}, nil
	default:
		return sdk.Result{}, fmt.Errorf("unexpected model request %d: %v", len(m.requests), r.Role)
	}
}

type approvalCLI struct {
	python, cwd, socket, state string
}

type approvalProcess struct {
	stdout, stderr bytes.Buffer
	err            error
}

func (c approvalCLI) run(operation, digest string) *approvalProcess {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := []string{"-m", "role_orchestrator.approval", operation, "--socket", c.socket, "--run-id", "run", "--review-task-id", "consumer", "--state", c.state}
	if operation == "approve" {
		args = append(args, "--artifact-sha256", digest)
	}
	result := &approvalProcess{}
	cmd := exec.CommandContext(ctx, c.python, args...)
	cmd.Dir, cmd.Stdout, cmd.Stderr = c.cwd, &result.stdout, &result.stderr
	result.err = cmd.Run()
	if ctx.Err() != nil {
		result.err = ctx.Err()
	}
	return result
}

func (p *approvalProcess) diagnostic() string {
	return fmt.Sprintf("process error: %v\nstdout:\n%s\nstderr:\n%s", p.err, p.stdout.Bytes(), p.stderr.Bytes())
}

func (p *approvalProcess) requireStatus(t *testing.T, allowed ...string) (string, map[string]any) {
	t.Helper()
	var output map[string]any
	if err := json.Unmarshal(p.stdout.Bytes(), &output); err != nil {
		t.Fatalf("invalid approval CLI response: %v\n%s", err, p.diagnostic())
	}
	status, _ := output["status"].(string)
	for _, want := range allowed {
		if status != want {
			continue
		}
		if p.err != nil {
			var exitErr *exec.ExitError
			domainOutcome := status == "conflict" || status == "unsupported" || status == "uncertain"
			if !domainOutcome || !errors.As(p.err, &exitErr) {
				t.Fatalf("approval CLI failed\n%s", p.diagnostic())
			}
		}
		return status, output
	}
	t.Fatalf("approval status = %q, want one of %v\n%s", status, allowed, p.diagnostic())
	return "", nil
}

func approvalArtifacts(t *testing.T, s *engine.Store) map[string][]byte {
	t.Helper()
	root := filepath.Join(s.Root, "artifacts")
	files := make(map[string][]byte)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err == nil {
			files[filepath.ToSlash(name)] = data
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func approvalArtifactID(t *testing.T, text string) string {
	t.Helper()
	var ref control.Reference
	if err := json.Unmarshal([]byte(text), &ref); err != nil || ref.Artifact.ID == "" {
		t.Fatalf("missing artifact receipt: %v; text=%q", err, text)
	}
	return ref.Artifact.ID
}

func approvalReportedArtifact(value any, id, digest string) bool {
	switch value := value.(type) {
	case map[string]any:
		if (value["id"] == id || value["artifact_id"] == id) && (value["sha256"] == digest || value["artifact_sha256"] == digest) {
			return true
		}
		for _, child := range value {
			if approvalReportedArtifact(child, id, digest) {
				return true
			}
		}
	case []any:
		for _, child := range value {
			if approvalReportedArtifact(child, id, digest) {
				return true
			}
		}
	case string:
		// A runtime receipt can be carried as JSON text inside the CLI response.
		value = strings.TrimSpace(value)
		if strings.HasPrefix(value, "{") || strings.HasPrefix(value, "[") {
			var child any
			return json.Unmarshal([]byte(value), &child) == nil && approvalReportedArtifact(child, id, digest)
		}
	}
	return false
}

type approvalFixture struct {
	store     *engine.Store
	executor  *Executor
	model     *approvalModel
	cli       approvalCLI
	reviewSHA string
	originals map[string][]byte
}

func newApprovalFixture(t *testing.T, action string) approvalFixture {
	t.Helper()
	python := os.Getenv("APPROVAL_PYTHON")
	cwd := os.Getenv("APPROVAL_ORCHESTRATOR")
	if !filepath.IsAbs(python) || !filepath.IsAbs(cwd) {
		t.Skip("approval integration requires absolute APPROVAL_PYTHON (Python executable) and APPROVAL_ORCHESTRATOR (repository directory)")
	}
	// Keep the real Unix socket path short even when the test name is long.
	root, err := os.MkdirTemp("/tmp", "ar-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	s, err := engine.OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.EnableLifecycle("run", 0); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"workspace", "control"} {
		if err := os.MkdirAll(filepath.Join(s.Root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	contract := &WorkContract{
		Version: 1, ID: "receipt-checkpoint", Objective: "Make revision provenance verifiable.",
		RequiredItems: []string{"Describe how a checkpoint verifies a session receipt."},
		Evidence:      []Observation{{"receipt-gap", approvalObservation}},
	}
	m := &approvalModel{contract: contract, action: action}
	e := &Executor{
		Store: s, Contract: contract, RunID: "run",
		Factory: func(context.Context, func(sdk.Status)) (Model, error) { return m, nil },
	}
	t.Cleanup(func() { e.Close() })
	write(t, s, e)
	beforeReview := approvalArtifacts(t, s)
	foundOriginal := false
	for _, data := range beforeReview {
		if json.Valid(data) && bytes.Contains(data, []byte(approvalOriginalTitle)) && bytes.Contains(data, []byte(approvalOriginalBody)) {
			foundOriginal = true
		}
	}
	if !foundOriginal {
		t.Fatal("missing original producer artifact")
	}
	consumer := lease(t, s, "consumer")
	transition, err := e.Step(context.Background(), consumer.Execution)
	if err != nil || transition.ExecutionState != "completed" || len(transition.Send) != 0 || transition.Wait != nil {
		t.Fatalf("unexpected review transition: %v %+v", err, transition)
	}
	if err := s.FinishCooperative("consumer", consumer.Generation, transition, time.Now()); err != nil {
		t.Fatal(err)
	}
	if id := approvalArtifactID(t, transition.Text); id != "review.json" {
		t.Fatalf("review artifact ID = %q, want review.json", id)
	}
	originals := approvalArtifacts(t, s)
	for name, data := range beforeReview {
		if current, ok := originals[name]; !ok || !bytes.Equal(current, data) {
			t.Fatalf("review changed the original producer artifact %q", name)
		}
	}
	var review struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(originals["review.json"], &review); err != nil || review.Action != action {
		t.Fatalf("invalid saved %q review: %v; artifact=%s", action, err, originals["review.json"])
	}
	if len(m.requests) != 2 {
		t.Fatalf("original workflow made %d model requests, want 2", len(m.requests))
	}
	socket := filepath.Join(s.Root, "control", "runtime.sock")
	server := &control.Server{
		Store: s, Socket: socket,
		Config: control.Config{
			RunID: "run", Project: "pipeline", ApprovedRevisions: true,
			Consumers: map[string]engine.Address{"graph": {Project: "pipeline", Role: "consumer", Version: "v1", Instance: "main"}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := server.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); server.Close() })
	return approvalFixture{
		store: s, executor: e, model: m, originals: originals,
		cli:       approvalCLI{python: python, cwd: cwd, socket: socket, state: filepath.Join(root, "approval.sqlite3")},
		reviewSHA: fmt.Sprintf("%x", sha256.Sum256(originals["review.json"])),
	}
}

func requireNoApprovalLease(t *testing.T, s *engine.Store, taskID string) {
	t.Helper()
	if _, err := s.LeaseCooperative(taskID, time.Now(), time.Minute); err == nil {
		t.Fatalf("unexpected lease for revision %q", taskID)
	}
}

func TestPythonApprovalRevisionOverUnixRPC(t *testing.T) {
	f := newApprovalFixture(t, "revise")
	taskID := engine.Digest("run", "consumer")
	f.cli.run("status", "").requireStatus(t, "unapproved")
	requireNoApprovalLease(t, f.store, taskID)

	wrongSHA := strings.Repeat("0", sha256.Size*2)
	if wrongSHA == f.reviewSHA {
		wrongSHA = strings.Repeat("1", sha256.Size*2)
	}
	f.cli.run("approve", wrongSHA).requireStatus(t, "conflict")
	requireNoApprovalLease(t, f.store, taskID)

	start := make(chan struct{})
	results := make(chan *approvalProcess, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			results <- f.cli.run("approve", f.reviewSHA)
		}()
	}
	close(start)
	first, second := <-results, <-results
	// A racing caller may see only the durable intent. Do not replay it.
	firstStatus, _ := first.requireStatus(t, "pending", "uncertain")
	secondStatus, _ := second.requireStatus(t, "pending", "uncertain")
	if firstStatus != "pending" && secondStatus != "pending" {
		t.Fatalf("neither concurrent approval reported a pending revision\nfirst: %s\nsecond: %s", first.diagnostic(), second.diagnostic())
	}
	if len(f.model.requests) != 2 {
		t.Fatal("approval executed a model before the revision was leased")
	}

	revision, err := f.store.LeaseCooperative(taskID, time.Now(), time.Minute)
	if err != nil {
		t.Fatalf("lease durable revision %q: %v", taskID, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	transition, err := f.executor.Step(ctx, revision.Execution)
	if err != nil || transition.ExecutionState != "completed" {
		t.Fatalf("unexpected revision transition: %v %+v", err, transition)
	}
	if err := f.store.FinishCooperative(taskID, revision.Generation, transition, time.Now()); err != nil {
		t.Fatal(err)
	}

	id := approvalArtifactID(t, transition.Text)
	if _, reused := f.originals[id]; reused || filepath.Ext(id) != ".json" {
		t.Fatalf("revision receipt must name a distinct JSON artifact, got %q", id)
	}
	afterRevision := approvalArtifacts(t, f.store)
	for name, original := range f.originals {
		if current, ok := afterRevision[name]; !ok || !bytes.Equal(current, original) {
			t.Fatalf("revision changed original artifact %q", name)
		}
	}
	data := afterRevision[id]
	if !json.Valid(data) || !bytes.Contains(data, []byte(approvalRevisedTitle)) || !bytes.Contains(data, []byte(approvalRevisedBody)) {
		t.Fatalf("artifact named in revision receipt (%q) is missing or does not contain the revised document: %s", id, data)
	}
	if len(afterRevision) != len(f.originals)+1 {
		t.Fatalf("revision added %d artifacts, want exactly one", len(afterRevision)-len(f.originals))
	}
	revisionSHA := fmt.Sprintf("%x", sha256.Sum256(data))
	for _, operation := range []string{"status", "approve"} {
		result := f.cli.run(operation, f.reviewSHA)
		_, output := result.requireStatus(t, "completed")
		if !approvalReportedArtifact(output, id, revisionSHA) {
			t.Fatalf("%s did not report revision artifact %q with SHA-256 %s\n%s", operation, id, revisionSHA, result.diagnostic())
		}
	}
	f.cli.run("approve", wrongSHA).requireStatus(t, "conflict")
	requireNoApprovalLease(t, f.store, taskID)
	if len(f.model.requests) != 3 {
		t.Fatalf("model requests = %d, want original producer, reviewer, and one revision", len(f.model.requests))
	}
	if !reflect.DeepEqual(approvalArtifacts(t, f.store), afterRevision) {
		t.Fatal("repeated or conflicting approval changed the saved artifacts")
	}
}

func TestPythonApprovalCompleteProposalIsUnsupported(t *testing.T) {
	f := newApprovalFixture(t, "complete")
	f.cli.run("approve", f.reviewSHA).requireStatus(t, "unsupported")
	requireNoApprovalLease(t, f.store, engine.Digest("run", "consumer"))
	if len(f.model.requests) != 2 {
		t.Fatalf("unsupported approval made %d extra model requests", len(f.model.requests)-2)
	}
	if !reflect.DeepEqual(approvalArtifacts(t, f.store), f.originals) {
		t.Fatal("unsupported approval changed the original artifacts")
	}
}

