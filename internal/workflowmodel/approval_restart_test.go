package workflowmodel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shakystar/role-runtime/internal/engine"
)

// The model and injected error are fixtures. The native processes, persisted
// store, terminal receipts, and Python commands are real when these tests run.
// This proposal was not compiled or run against the checkout.
func TestPythonApprovalRecoversReceiptAfterNativeRestart(t *testing.T) {
	runApprovalRestart(t, "completed")
}

func TestPythonApprovalFailedReceiptIsNotReplayed(t *testing.T) {
	runApprovalRestart(t, "failed")
}

func runApprovalRestart(t *testing.T, outcome string) {
	t.Helper()
	root := approvalTempRoot(t)
	approvalCLIAt(t, root) // Apply prerequisite skips in the parent, not a child.
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"finish", "recover"} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		cmd := exec.CommandContext(ctx, exe, "-test.run=^TestApprovalNativeRestartProcess$")
		cmd.Env = append(os.Environ(), "APPROVAL_TEST_PHASE="+phase,
			"APPROVAL_TEST_ROOT="+root, "APPROVAL_TEST_OUTCOME="+outcome)
		out, err := cmd.CombinedOutput()
		deadline := ctx.Err()
		cancel()
		if err != nil || deadline != nil {
			t.Fatalf("native %s process: %v (context: %v)\n%s", phase, err, deadline, out)
		}
		// CombinedOutput has waited for the entire process, not just for its
		// listener to close. No store handle survives into the second phase.
	}
}

type approvalRestartCheckpoint struct {
	RunID, TaskID, ReviewSHA string
	Leases                   approvalLeaseHistory
	Receipt                  json.RawMessage
	Files                    map[string][]byte
	Artifact                 *approvalSavedArtifact
	StatusRequests           []approvalRPCFrame
	ApproveRequest           json.RawMessage
}

func TestApprovalNativeRestartProcess(t *testing.T) {
	phase := os.Getenv("APPROVAL_TEST_PHASE")
	if phase == "" {
		return
	}
	root, outcome := os.Getenv("APPROVAL_TEST_ROOT"), os.Getenv("APPROVAL_TEST_OUTCOME")
	if !filepath.IsAbs(root) || (outcome != "completed" && outcome != "failed") {
		t.Fatal("invalid approval restart helper configuration")
	}
	path := filepath.Join(root, "restart-checkpoint.json")
	switch phase {
	case "finish":
		f := seedApprovalFixture(t, root, "revise")
		startApprovalControl(t, f.store, f.cli)
		requireNoApprovalLease(t, f.store, f.revisionID())
		p, writes := traceApprovalCommand(t, f.cli, "approve", f.reviewSHA)
		p.requireStatus(t, "pending")
		var approveRequest json.RawMessage
		for _, frame := range writes {
			if frame.Method == "revisions.approve" {
				approveRequest = frame.Wire
				break
			}
		}
		if len(approveRequest) == 0 {
			t.Fatalf("initial approval did not send revisions.approve: %q\n%s", writes, p.diagnostic())
		}
		// Calibrate the read trace while no terminal receipt can be cached.
		p, reads := traceApprovalCommand(t, f.cli, "status", "")
		p.requireStatus(t, "pending", "uncertain")
		if len(reads) == 0 || len(writes) == 0 || approvalRequestsWithin(writes, reads) {
			t.Fatalf("could not distinguish initial approval traffic from pending status traffic: approve=%q status=%q", writes, reads)
		}
		var artifact *approvalSavedArtifact
		if outcome == "failed" {
			f.failApprovalRevision(t)
			f.requireFiles(t, f.originals)
		} else {
			artifact = f.finishRevision(t)
		}
		leases := f.leaseSnapshot(t)
		cp := approvalRestartCheckpoint{
			RunID: f.cli.runID, TaskID: f.revisionID(), ReviewSHA: f.reviewSHA,
			Leases: leases, Receipt: approvalNativeReceipt(t, f.store, f.revisionID(), outcome, leases),
			Files: approvalArtifacts(t, f.store), Artifact: artifact, StatusRequests: reads,
			ApproveRequest: approveRequest,
		}
		// Do not run Python status again in this process. The next status
		// command must recover the terminal outcome from the restarted server.
		if err := os.WriteFile(path, approvalRestartJSON(t, cp), 0600); err != nil {
			t.Fatal(err)
		}
	case "recover":
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var cp approvalRestartCheckpoint
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber() // Preserve the native generation in the lease key.
		if err := dec.Decode(&cp); err != nil {
			t.Fatal(err)
		}
		s, err := engine.OpenStore(root)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		f := approvalFixture{store: s, cli: approvalCLIAt(t, root), reviewSHA: cp.ReviewSHA, leases: cp.Leases}
		startApprovalControl(t, s, f.cli)
		if f.cli.runID != cp.RunID || f.revisionID() != cp.TaskID {
			t.Fatalf("restart changed the run or revision task ID: run=%q task=%q", f.cli.runID, f.revisionID())
		}
		requireUnchanged := func() approvalLeaseHistory {
			t.Helper()
			leases := f.leaseSnapshot(t)
			if got := approvalNativeReceipt(t, s, cp.TaskID, outcome, leases); !bytes.Equal(got, cp.Receipt) {
				t.Fatalf("native receipt changed: before=%s after=%s", cp.Receipt, got)
			}
			f.requireFiles(t, cp.Files)
			return leases
		}
		requireUnchanged()
		p, requests := traceApprovalCommand(t, f.cli, "status", "")
		if len(requests) == 0 || !approvalRequestsWithin(requests, cp.StatusRequests) {
			t.Fatalf("fresh status did not use only the calibrated native status requests: %q\n%s", requests, p.diagnostic())
		}
		var status, caseID string
		if outcome == "completed" {
			if cp.Artifact == nil {
				t.Fatal("missing pre-restart revision artifact")
			}
			status = p.requireCompletedArtifact(t, cp.Artifact)
			caseID = "restart"
			cp.Artifact.data = approvalArtifacts(t, s)[cp.Artifact.ID]
		} else {
			status, _ = p.requireStatus(t, "failed")
			caseID = "failure"
			requireUnchanged()
			repeat, replay := traceApprovalCommand(t, f.cli, "approve", f.reviewSHA)
			repeat.requireStatus(t, "failed")
			if !approvalRequestsWithin(replay, cp.StatusRequests) {
				t.Fatalf("reapproval sent traffic other than the calibrated status lookups: %q\n%s", replay, repeat.diagnostic())
			}
		}
		requireUnchanged()
		// Bypass the Python command and its local cache. Send the original
		// production request directly to the restarted Go server.
		requireCachedApprovalRPC(t, f.cli.socket, cp.ApproveRequest, cp.TaskID, outcome)
		after := requireUnchanged()
		f.recordCase(t, caseID, status, cp.Leases, after, cp.Artifact)
	default:
		t.Fatalf("unknown approval restart phase %q", phase)
	}
}

func (f *approvalFixture) failApprovalRevision(t *testing.T) {
	t.Helper()
	id := f.revisionID()
	lease, err := f.store.LeaseCooperative(id, time.Now(), time.Minute)
	if err != nil {
		t.Fatalf("lease revision %q for failure: %v", id, err)
	}
	f.leases = append(f.leases, approvalNativeLease{TaskID: id, Generation: lease.Generation})
	f.model.failRevision = true
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, stepErr := f.executor.Step(ctx, lease.Execution)
	if !f.model.injectedRevisionFailure || stepErr == nil || ctx.Err() != nil {
		t.Fatalf("did not reach the injected revision failure: step=%v context=%v", stepErr, ctx.Err())
	}
	failure := engine.FailureMetadata(stepErr)
	if failure == nil || failure.Execution != "not_started" {
		t.Fatalf("model error must carry not_started execution metadata: error=%v failure=%+v", stepErr, failure)
	}
	// Use the cooperative pool's error path, not a synthetic transition.
	if err := f.store.FenceCooperative(id, lease.Generation, stepErr); err != nil {
		t.Fatalf("fence failed native lease %q: %v", id, err)
	}
	r := approvalRevisionRecord(t, f.store, id)
	if r.Status != "failed" || r.ExecutionState != "not_started" || r.Error != stepErr.Error() || r.Generation != lease.Generation+1 {
		t.Fatalf("fenced revision %q: status=%q execution=%q error=%q generation=%d; want failed/not_started, error=%q generation=%d",
			id, r.Status, r.ExecutionState, r.Error, r.Generation, stepErr.Error(), lease.Generation+1)
	}
}

func approvalRestartJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func approvalRevisionRecord(t *testing.T, s *engine.Store, id string) engine.CooperativeRecord {
	t.Helper()
	var record engine.CooperativeRecord
	var found bool
	if err := s.ScanCooperative(func(r engine.CooperativeRecord) (bool, error) {
		if r.Task.ID != id {
			return false, nil
		}
		record, found = r, true
		return true, nil
	}); err != nil {
		t.Fatalf("scan native cooperative records: %v", err)
	}
	if !found {
		t.Fatalf("no native cooperative record for %q", id)
	}
	return record
}

func approvalNativeReceipt(t *testing.T, s *engine.Store, id, outcome string, leases approvalLeaseHistory) []byte {
	t.Helper()
	if len(leases) != 1 || leases[0].TaskID != id {
		t.Fatalf("expected exactly one native lease of %q, got %+v", id, leases)
	}
	var generation uint64
	if err := json.Unmarshal(approvalRestartJSON(t, leases[0].Generation), &generation); err != nil {
		t.Fatalf("decode native lease generation: %v", err)
	}
	execution := "completed"
	if outcome == "failed" {
		execution = "not_started"
		generation++ // Fencing invalidates the original lease generation.
	}
	r, err := s.CooperativeReceipt(id)
	if err != nil {
		t.Fatalf("read native terminal receipt for %q: %v", id, err)
	}
	if r.Task.ID != id || r.Task.Project != "pipeline" || r.Task.Role != "producer" ||
		r.Status != outcome || r.ExecutionState != execution || strings.TrimSpace(r.AttemptID) == "" ||
		!bytes.Equal(approvalRestartJSON(t, r.Generation), approvalRestartJSON(t, generation)) {
		t.Fatalf("native receipt does not match the leased %s revision %q (want execution=%q generation=%d): %+v", outcome, id, execution, generation, r)
	}
	record := approvalRevisionRecord(t, s, id)
	if record.Status != outcome || record.ExecutionState != execution || record.Generation != generation {
		t.Fatalf("native record for %q does not match its receipt: %+v", id, record)
	}
	if outcome == "failed" {
		if r.Failure == nil || r.Failure.Execution != "not_started" || strings.TrimSpace(record.Error) == "" {
			t.Fatalf("missing native failure for %q: metadata=%+v error=%q", id, r.Failure, record.Error)
		}
	} else if r.Failure != nil || record.Error != "" {
		t.Fatalf("completed revision %q carries a failure: metadata=%+v error=%q", id, r.Failure, record.Error)
	}
	return approvalRestartJSON(t, map[string]any{
		"task_id": id, "task": r.Task, "status": r.Status, "receipt_revision": r.Revision,
		"attempt_id": r.AttemptID, "generation": r.Generation, "execution": r.ExecutionState,
		"failure": r.Failure, "error": record.Error, "text": r.Text,
	})
}

func requireCachedApprovalRPC(t *testing.T, socket string, request json.RawMessage, id, status string) {
	t.Helper()
	if len(request) == 0 {
		t.Fatal("missing pre-restart revisions.approve request")
	}
	conn, err := net.DialTimeout("unix", socket, 5*time.Second)
	if err != nil {
		t.Fatalf("dial restarted approval control socket: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(append(append([]byte(nil), request...), '\n')); err != nil {
		t.Fatalf("send direct revisions.approve: %v", err)
	}
	if err := conn.(*net.UnixConn).CloseWrite(); err != nil {
		t.Fatalf("finish writing direct revisions.approve: %v", err)
	}
	var response json.RawMessage
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatalf("read direct revisions.approve: %v", err)
	}
	var envelope struct {
		OK     *bool           `json:"ok"`
		Error  json.RawMessage `json:"error"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatalf("decode direct revisions.approve response: %v: %s", err, response)
	}
	if envelope.OK != nil && !*envelope.OK {
		t.Fatalf("direct revisions.approve rejected: %s", response)
	}
	switch string(envelope.Error) {
	case "", "null", "\"\"":
	default:
		t.Fatalf("direct revisions.approve returned an error: %s", response)
	}
	payload := envelope.Result
	if len(payload) == 0 {
		payload = response
	}
	var result struct {
		TaskID string `json:"task_id"`
		Cached bool   `json:"cached"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(payload, &result); err != nil {
		t.Fatalf("decode direct revisions.approve result: %v: %s", err, response)
	}
	if !result.Cached || result.TaskID != id || result.Status != status {
		t.Fatalf("direct revisions.approve must return cached=true, task_id=%q, status=%q: %s", id, status, response)
	}
}

type approvalRPCFrame struct {
	Method    string
	Canonical string
	Wire      json.RawMessage
}

func approvalRequestsWithin(got, allowed []approvalRPCFrame) bool {
	set := make(map[string]bool, len(allowed))
	for _, req := range allowed {
		set[req.Canonical] = true
	}
	for _, req := range got {
		if !set[req.Canonical] {
			return false
		}
	}
	return true
}

// Forward both directions to the production control server. Receipt and lease
// equality alone cannot distinguish an idempotent resubmit from no submit.
// The independent production status command supplies the read-only allowlist;
// replay may make those same native lookups or no native calls at all. The
// comparison is deliberately narrow: it covers JSON requests on this socket,
// not internal calls, and fails closed on new status request shapes or framing.
func traceApprovalCommand(t *testing.T, cli approvalCLI, operation, digest string) (*approvalProcess, []approvalRPCFrame) {
	t.Helper()
	native := cli.socket
	cli.socket = filepath.Join(filepath.Dir(native), "approval-tap.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: cli.socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var mu sync.Mutex
	var frames []approvalRPCFrame
	var traceErr error
	add := func(reqs []approvalRPCFrame, err error) {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, reqs...)
		if traceErr == nil {
			traceErr = err
		}
	}
	var workers sync.WaitGroup
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			client, err := listener.AcceptUnix()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					add(nil, err)
				}
				return
			}
			workers.Add(1)
			go func(c *net.UnixConn) {
				defer workers.Done()
				defer c.Close()
				add(approvalForwardRequests(c, native))
			}(client)
		}
	}()
	p := cli.run(operation, digest)
	listener.Close()
	<-accepted
	workers.Wait()
	if traceErr != nil {
		t.Fatalf("trace %s approval RPC: %v\n%s", operation, traceErr, p.diagnostic())
	}
	return p, frames
}

func approvalForwardRequests(client *net.UnixConn, native string) ([]approvalRPCFrame, error) {
	conn, err := net.DialTimeout("unix", native, 5*time.Second)
	if err != nil {
		return nil, err
	}
	upstream := conn.(*net.UnixConn)
	defer upstream.Close()
	deadline := time.Now().Add(35 * time.Second)
	if err := client.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if err := upstream.SetDeadline(deadline); err != nil {
		return nil, err
	}
	var input bytes.Buffer
	forwarded := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.MultiWriter(upstream, &input), client)
		upstream.CloseWrite()
		forwarded <- err
	}()
	_, receiveErr := io.Copy(client, upstream)
	client.CloseWrite()
	if err := <-forwarded; err != nil {
		return nil, err
	}
	if receiveErr != nil {
		return nil, receiveErr
	}
	var frames []approvalRPCFrame
	dec := json.NewDecoder(&input)
	for {
		var wire json.RawMessage
		if err := dec.Decode(&wire); err == io.EOF {
			return frames, nil
		} else if err != nil {
			return nil, err
		}
		var req map[string]any
		frameDecoder := json.NewDecoder(bytes.NewReader(wire))
		frameDecoder.UseNumber()
		if err := frameDecoder.Decode(&req); err != nil {
			return nil, err
		}
		if len(req) == 0 {
			return nil, errors.New("empty approval control request")
		}
		method, _ := req["method"].(string)
		// Ignore correlation metadata for comparisons, but retain the
		// original envelope so the direct retry uses the real RPC shape.
		if req["jsonrpc"] == "2.0" {
			delete(req, "id")
		}
		delete(req, "request_id")
		b, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		frames = append(frames, approvalRPCFrame{Method: method, Canonical: string(b), Wire: wire})
	}
}
