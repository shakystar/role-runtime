package workflowmodel

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/shakystar/role-runtime/internal/engine"
)

// The fixture model is synthetic. The Python processes, socket, store, and
// cooperative leases are real. Measure revision leases, not model calls or
// approval callers. The test is the sole cooperative worker for this store.
type approvalNativeLease struct {
	TaskID     string `json:"task_id"`
	Generation any    `json:"generation"`
}

type approvalLeaseHistory []approvalNativeLease

func (f *approvalFixture) revisionID() string {
	return engine.Digest(f.cli.runID, "consumer")
}

// Sample only at quiescent boundaries. A runnable revision here is a test
// failure, not a zero-dispatch observation. Retain successful lease events
// after completion: a completed task is no longer leaseable.
func (f *approvalFixture) leaseSnapshot(t *testing.T) approvalLeaseHistory {
	t.Helper()
	requireNoApprovalLease(t, f.store, f.revisionID())
	return append(approvalLeaseHistory{}, f.leases...)
}

func (h approvalLeaseHistory) ids(t *testing.T) []string {
	t.Helper()
	ids := make([]string, 0, len(h))
	seen := make(map[string]bool, len(h))
	for _, lease := range h {
		// Lossless string encoding of the native composite lease key. A new
		// generation of the same task must not collapse to the old attempt.
		b, err := json.Marshal([2]any{lease.TaskID, lease.Generation})
		if err != nil {
			t.Fatal(err)
		}
		id := string(b)
		if seen[id] {
			t.Fatalf("native lease observed twice: %s", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids
}

type approvalSavedArtifact struct {
	ID     string `json:"id"`
	SHA256 string `json:"sha256"`
	data   []byte
}

func (f *approvalFixture) requireFiles(t *testing.T, want map[string][]byte) {
	t.Helper()
	if !reflect.DeepEqual(approvalArtifacts(t, f.store), want) {
		t.Fatal("approval changed the saved artifact set or bytes")
	}
}

func (p *approvalProcess) requireCompletedArtifact(t *testing.T, a *approvalSavedArtifact) string {
	t.Helper()
	status, output := p.requireStatus(t, "completed")
	if !approvalReportedArtifact(output, a.ID, a.SHA256) {
		t.Fatalf("completed without the saved revision %q (%s)\n%s", a.ID, a.SHA256, p.diagnostic())
	}
	return status
}

// Poll through the local approval CLI, never by resubmitting approval.
func (f *approvalFixture) pollCompleted(t *testing.T, a *approvalSavedArtifact) string {
	t.Helper()
	var p *approvalProcess
	for i := 0; i < 3; i++ {
		p = f.cli.run("status", "")
		status, _ := p.requireStatus(t, "completed", "pending", "uncertain")
		if status == "completed" {
			return p.requireCompletedArtifact(t, a)
		}
		if i < 2 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	t.Fatalf("status did not recover the native receipt\n%s", p.diagnostic())
	return ""
}

// Only export is optional; none of the scenario actions or assertions depend
// on the evidence directory. Pass this directory as the verifier's ref root.
// Each case.json is one row, not a manifest with guessed refs or M3 counts.
func (f *approvalFixture) recordCase(t *testing.T, id, status string, before, after approvalLeaseHistory, a *approvalSavedArtifact) {
	t.Helper()
	root := os.Getenv("APPROVAL_EVIDENCE_DIR")
	if root == "" {
		return
	}
	runDir := filepath.Join(root, f.cli.runID)
	if err := os.MkdirAll(runDir, 0700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(runDir, id)
	// Do not replace or silently combine records from different captures.
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	row := map[string]any{
		"id": id, "run_id": f.cli.runID, "model_backend": "fixture",
		"observed_status": status, "review_sha256": f.reviewSHA,
		"attempts_before": before.ids(t), "attempts_after": after.ids(t),
		"native_leases_before": before, "native_leases_after": after,
		"artifact": a,
	}
	if a != nil {
		// data came from os.ReadFile on the ID named in the native receipt;
		// never serialize the parsed document as a substitute for those bytes.
		if err := os.WriteFile(filepath.Join(dir, "artifact.bin"), a.data, 0600); err != nil {
			t.Fatal(err)
		}
		row["artifact_file"] = filepath.ToSlash(filepath.Join(f.cli.runID, id, "artifact.bin"))
	}
	b, err := json.MarshalIndent(row, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, ".case.tmp")
	if err := os.WriteFile(tmp, append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, "case.json")); err != nil {
		t.Fatal(err)
	}
}

func TestPythonApprovalFirstAndRepeat(t *testing.T) {
	f := newApprovalFixture(t, "revise")
	before := f.leaseSnapshot(t)
	f.cli.run("approve", f.reviewSHA).requireStatus(t, "pending")
	a := f.finishRevision(t)
	files := approvalArtifacts(t, f.store)
	status := f.pollCompleted(t, a)
	after := f.leaseSnapshot(t)
	f.requireFiles(t, files)
	f.recordCase(t, "first", status, before, after, a)

	// Repeat is intentionally in the very same run and state database.
	before = f.leaseSnapshot(t)
	status = f.cli.run("approve", f.reviewSHA).requireCompletedArtifact(t, a)
	after = f.leaseSnapshot(t)
	f.requireFiles(t, files)
	if len(f.model.requests) != 3 {
		t.Fatalf("first and repeat made %d model requests, want 3 including setup", len(f.model.requests))
	}
	f.recordCase(t, "repeat", status, before, after, a)
}

func TestPythonApprovalStaleDigest(t *testing.T) {
	f := newApprovalFixture(t, "revise")
	wrong := strings.Repeat("0", len(f.reviewSHA))
	if wrong == f.reviewSHA {
		wrong = strings.Repeat("1", len(f.reviewSHA))
	}
	before := f.leaseSnapshot(t)
	status, _ := f.cli.run("approve", wrong).requireStatus(t, "conflict")
	after := f.leaseSnapshot(t)
	f.requireFiles(t, f.originals)
	if len(f.model.requests) != 2 {
		t.Fatal("stale approval executed the model")
	}
	f.recordCase(t, "stale", status, before, after, nil)
}

func requireApprovalCrash(t *testing.T, p *approvalProcess, code int) {
	t.Helper()
	var exit *exec.ExitError
	if !errors.As(p.err, &exit) || exit.ExitCode() != code {
		t.Fatalf("did not reach approval crash point %d\n%s", code, p.diagnostic())
	}
}

func TestPythonApprovalInterruptedBeforeRPC(t *testing.T) {
	f := newApprovalFixture(t, "revise")
	before := f.leaseSnapshot(t)
	// Use the real intent transaction in a disposable process. Supply no
	// client or socket: the crashing writer cannot issue even one RPC.
	const interrupt = `import os, sys
from role_orchestrator.approval import Approval
state, run, digest = sys.argv[1:]
a = Approval(state, None, run, 'consumer')
if not a.insert_intent(digest, 'pipeline'):
    raise RuntimeError('expected a fresh intent')
i = a.read_intent()
if i is None or i.phase != 'intent' or i.artifact_sha256 != digest:
    raise RuntimeError('intent was not committed')
os._exit(73)
`
	requireApprovalCrash(t, f.cli.command("-c", interrupt, f.cli.state, f.cli.runID, f.reviewSHA), 73)
	f.cli.run("status", "").requireStatus(t, "uncertain")
	status, _ := f.cli.run("approve", f.reviewSHA).requireStatus(t, "uncertain")
	after := f.leaseSnapshot(t)
	f.requireFiles(t, f.originals)
	if len(f.model.requests) != 2 {
		t.Fatal("interrupted approval replayed model work")
	}
	f.recordCase(t, "interrupted", status, before, after, nil)
}

func TestPythonApprovalRecoverNativeReceipt(t *testing.T) {
	f := newApprovalFixture(t, "revise")
	// Execute the real CLI and admission RPC. Stop on entry to the local
	// acknowledgement write, leaving the committed intent unchanged. Do not
	// synthesize a completed receipt or prime the local DB with one.
	const loseAck = `import os, runpy, sys
def cut(frame, event, arg):
    if event != 'call' or frame.f_code.co_qualname != 'Approval.set_phase':
        return
    spec = frame.f_globals.get('__spec__')
    if getattr(spec, 'name', None) != 'role_orchestrator.approval' or frame.f_locals.get('phase') != 'accepted':
        return
    i = frame.f_locals['self'].read_intent()
    if i is None or i.phase != 'intent':
        raise RuntimeError('no outstanding local intent at acknowledgement')
    os._exit(74)
sys.setprofile(cut)
runpy.run_module('role_orchestrator.approval', run_name='__main__', alter_sys=True)
`
	args := append([]string{"-c", loseAck}, f.cli.flags("approve", f.reviewSHA)...)
	requireApprovalCrash(t, f.cli.command(args...), 74)
	a := f.finishRevision(t)
	files := approvalArtifacts(t, f.store)
	// The native completion is setup for recovery. Its actual lease must
	// already appear in the before sample, not as a recovery dispatch.
	before := f.leaseSnapshot(t)
	status := f.pollCompleted(t, a)
	after := f.leaseSnapshot(t)
	f.requireFiles(t, files)
	if len(f.model.requests) != 3 {
		t.Fatal("receipt recovery executed extra model work")
	}
	f.recordCase(t, "recover", status, before, after, a)
}
