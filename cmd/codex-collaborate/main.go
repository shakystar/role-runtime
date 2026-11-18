// codex-collaborate supplies two real-model jobs to one cooperative worker.
// The generated document is the durable SMS body, not an independently saved file.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	codexrpc "github.com/shakystar/codex-adapter/rpc"
	"github.com/shakystar/role-runtime/internal/codexadapter"
	"github.com/shakystar/role-runtime/internal/codexcooperation"
	"github.com/shakystar/role-runtime/internal/engine"
)

const correlation = "regional-brief-v1"

type document struct {
	Version           string `json:"version"`
	Title             string `json:"title"`
	Region            string `json:"region"`
	RegionDecision    string `json:"regionDecision"`
	FirstCustomerTask string `json:"firstCustomerTask"`
	Proposal          string `json:"proposal"`
	DistinctiveDetail string `json:"distinctiveDetail"`
}
type review struct {
	DocumentVersion      string `json:"documentVersion"`
	ObservedDetail       string `json:"observedDetail"`
	ObservedRegion       string `json:"observedRegion"`
	ObservedCustomerTask string `json:"observedCustomerTask"`
	Assessment           string `json:"assessment"`
	Risk                 string `json:"risk"`
}
type report struct {
	OutputMode         string                  `json:"outputMode"`
	Completed          bool                    `json:"completed"`
	Error              string                  `json:"error,omitempty"`
	Workers            int                     `json:"workers"`
	ProcessesStarted   int64                   `json:"processesStarted"`
	WaitingObserved    bool                    `json:"waitingObserved"`
	PlannerAfterWait   bool                    `json:"plannerSubmittedAfterWait"`
	DocumentVerified   bool                    `json:"documentVerifiedInDurableMailbox"`
	ReviewMatched      bool                    `json:"reviewMatchedGeneratedDetail"`
	DocumentSHA256     string                  `json:"documentSHA256,omitempty"`
	Document           document                `json:"document"`
	Review             review                  `json:"review"`
	WaitingCheckpoint  string                  `json:"waitingCheckpoint,omitempty"`
	ReviewerGeneration uint64                  `json:"reviewerGeneration"`
	Stats              engine.CooperativeStats `json:"stats"`
	Usage              codexadapter.Usage      `json:"usage"`
	Seconds            float64                 `json:"seconds"`
	ArtifactStorage    string                  `json:"artifactStorage"`
}

func main() {
	state := flag.String("state", "", "new persistent state directory; contains model outputs")
	binary := flag.String("codex", "codex", "Codex executable")
	model := flag.String("model", "", "optional model override; empty uses installed default")
	structured := flag.Bool("structured-output", true, "use backend output schema; false explicitly selects prompt-JSON diagnostic mode")
	timeout := flag.Duration("timeout", 0, "optional overall deadline; zero waits until signal or failure")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *timeout < 0 {
		fmt.Fprintln(os.Stderr, "timeout cannot be negative")
		os.Exit(2)
	}
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	began := time.Now()
	r, err := run(ctx, *state, *binary, *model, *structured)
	r.Seconds = time.Since(began).Seconds()
	if err != nil {
		r.Error = err.Error()
	}
	if e := json.NewEncoder(os.Stdout).Encode(r); e != nil {
		fmt.Fprintln(os.Stderr, "report write failed")
		os.Exit(1)
	}
	if err != nil {
		os.Exit(1)
	}
}

func run(parent context.Context, root, binary, model string, structured bool) (r report, err error) {
	r.OutputMode = "backend-schema"
	if !structured {
		r.OutputMode = "prompt-json"
	}
	r.Workers = 1
	r.ArtifactStorage = "versioned document JSON in durable SMS body; no prior standalone document file"
	if root == "" {
		return r, errors.New("new --state directory required")
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return r, err
	}
	// Mkdir is exclusive: never mix this experiment with an existing state.
	if err = os.Mkdir(root, 0700); err != nil {
		return r, fmt.Errorf("create new state: %w", err)
	}
	workspace := filepath.Join(root, "workspace")
	if err = os.Mkdir(workspace, 0700); err != nil {
		return r, err
	}
	store, err := engine.OpenStore(root)
	if err != nil {
		return r, err
	}
	defer store.Close()
	project := "regional-collaboration"
	if err = store.RegisterProject(engine.Project{ID: project, Workspace: workspace, Rules: "This is a limited document handoff collaboration test, not the final four-role regional business meeting. The original proposed starting scope is Gyeongju, Daegu and Pohang (경주·대구·포항); roles may adopt, revise or defer it based on explicit assumptions. This is an advisory regional tourism proposal experiment. Use only supplied inputs. No external research, tools, file edits, contacts, purchases or actual deployment. Generated proposals are hypotheses, not observed market facts. Input is a JSON Execution object with task, generation, checkpoint and inbox. Output is exactly a Transition object with all four keys: checkpoint(string), send(array), wait(null or an object with from Address and correlation), text(string). Wait requires a nonempty checkpoint; completion requires wait=null and nonempty text. Each message has id, from, to, correlation and body. Address fields are project, role, version, instance."}); err != nil {
		return r, err
	}
	planner := engine.Task{ID: "plan-document", Project: project, Role: "planner", Version: "v1", Instance: "main"}
	reviewer := engine.Task{ID: "review-document", Project: project, Role: "reviewer", Version: "v1", Instance: "main"}
	plannerAddress, _ := json.Marshal(engine.AddressOf(planner))
	reviewerAddress, _ := json.Marshal(engine.AddressOf(reviewer))
	for _, role := range []engine.Role{
		{ID: "planner", Version: "v1", Tools: []string{"advisory"}, Instruction: "Create a short original regional proposal when assigned. Deliver the complete document through exactly one Transition.send message. Never claim the document was written to a separate file."},
		{ID: "reviewer", Version: "v1", Tools: []string{"advisory"}, Instruction: "Review only an actual supplied inbox document. When inbox is absent, return a wait transition with a nonempty checkpoint; do not invent the missing document. After resuming, inspect the inbox body and critically assess its exact content."},
	} {
		if err = store.RegisterRole(role); err != nil {
			return r, err
		}
	}
	reviewer.Input = `You are the reviewer. If execution.inbox is absent/null, set wait={"from":` + string(plannerAddress) + `,"correlation":"` + correlation + `"}, checkpoint to a short explanation of the pending review, send=[] and text="". End your turn immediately to release the worker. If inbox is present, parse inbox.body as the real document JSON; do not wait again and do not send messages. Complete with wait=null and text containing a JSON object with exactly documentVersion (copy document.version), observedDetail (copy document.distinctiveDetail verbatim), observedRegion (copy document.region verbatim), observedCustomerTask (copy document.firstCustomerTask verbatim), assessment and risk (each a substantive Korean review of that specific proposal). No Markdown fences. The document is stored in the durable SMS body, not a previously created file.`
	planner.Input = `Create a short original Korean regional tourism proposal. Assess 경주·대구·포항 as the proposed first scope and choose adopt, revise or defer with a reason. Invent a concrete proposed service concept and first customer task; do not present unresearched demand, prices or tourism details as facts. Create JSON with exactly version="regional-brief-v1", title, region, regionDecision (one of adopt/revise/defer), firstCustomerTask, proposal, distinctiveDetail. The distinctiveDetail must be one specific original proposed operating rule (15-100 Korean characters), also included verbatim in proposal. Keep the document under 3000 characters. Send exactly one message: id="regional-brief-message-v1", from=` + string(plannerAddress) + `, to=` + string(reviewerAddress) + `, correlation="` + correlation + `", body=the JSON-encoded document string. Set wait=null, checkpoint="", and nonempty text summarizing completion. Return the cooperative Transition object, not the document alone. The durable SMS body itself is the versioned artifact; do not claim a standalone file was saved.`
	if err = store.SubmitCooperative(reviewer); err != nil {
		return r, err
	}
	var starts atomic.Int64
	var statusMu sync.Mutex
	statusLog := json.NewEncoder(os.Stderr)
	logStatus := func(v any) {
		statusMu.Lock()
		defer statusMu.Unlock()
		_ = statusLog.Encode(v)
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var adapter *codexadapter.Adapter
	pool := engine.CooperativePool{Workers: 1, Store: store, PollInterval: 50 * time.Millisecond, HeartbeatInterval: 15 * time.Second, HeartbeatTimeout: 10 * time.Second, LeaseTTL: 45 * time.Second, StepTimeout: 0,
		Factory: func(int) (engine.StepExecutor, error) {
			create := codexcooperation.New
			if !structured {
				create = codexcooperation.NewPromptJSON
			}
			wrapped, e := create(codexadapter.Config{Store: store, Model: model, TaskTimeout: 0, StartupTimeout: 30 * time.Second, RPC: codexrpc.Config{Command: binary, Dir: workspace}, OnProcessStart: func(int) { starts.Add(1) }, OnStatus: func(s codexadapter.Status) { logStatus(s) }})
			if e != nil {
				return nil, e
			}
			adapter = wrapped.Adapter
			return wrapped, nil
		},
	}
	type finished struct {
		stats engine.CooperativeStats
		err   error
	}
	done := make(chan finished, 1)
	go func() { s, e := pool.Run(ctx); done <- finished{s, e} }()
	defer func() {
		cancel()
		out := <-done
		r.Stats = out.stats
		r.ProcessesStarted = starts.Load()
		if adapter != nil {
			r.Usage = adapter.Usage()
		}
		if out.err != nil && !errors.Is(out.err, context.Canceled) {
			err = errors.Join(err, out.err)
		}
		if err == nil && parent.Err() != nil {
			err = parent.Err()
		}
		if err == nil && (r.Stats.MaxActive != 1 || r.Stats.Steps != 3 || r.Stats.Completed != 2 || r.Stats.Suspended != 1 || r.Stats.Resumed != 1 || r.ProcessesStarted != 1) {
			err = errors.New("unexpected worker, process or wait/resume counters")
		}
		r.Completed = err == nil && r.WaitingObserved && r.DocumentVerified && r.ReviewMatched
	}()
	await := func(task engine.Task, want string) (engine.CooperativeRecord, error) {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			record, e := store.CooperativeReceipt(task.ID)
			if e != nil {
				return record, e
			}
			if record.Status == "uncertain" {
				return record, fmt.Errorf("%s uncertain: %s", task.ID, record.Error)
			}
			if record.Status == want {
				return record, nil
			}
			if record.Status == "completed" && want != "completed" {
				return record, errors.New("reviewer completed without waiting for document")
			}
			select {
			case <-parent.Done():
				return record, parent.Err()
			case out := <-done:
				done <- out
				return record, errors.Join(errors.New("cooperative pool stopped prematurely"), out.err)
			case <-ticker.C:
			}
		}
	}
	waiting, err := await(reviewer, "waiting")
	if err != nil {
		return r, err
	}
	if waiting.Checkpoint == "" || waiting.Wait == nil || waiting.Wait.From != engine.AddressOf(planner) || waiting.Wait.Correlation != correlation || waiting.Inbox != nil {
		return r, errors.New("reviewer wait checkpoint or sender/correlation mismatch")
	}
	r.WaitingObserved = true
	r.WaitingCheckpoint = waiting.Checkpoint
	logStatus(map[string]any{"event": "reviewer_waiting", "generation": waiting.Generation, "workerReleased": true})
	if err = store.SubmitCooperative(planner); err != nil {
		return r, err
	}
	r.PlannerAfterWait = true
	logStatus(map[string]any{"event": "planner_submitted_after_wait"})
	if _, err = await(planner, "completed"); err != nil {
		return r, err
	}
	reviewed, err := await(reviewer, "completed")
	if err != nil {
		return r, err
	}
	r.ReviewerGeneration = reviewed.Generation
	if reviewed.Generation <= waiting.Generation {
		return r, errors.New("reviewer has no resumed generation")
	}
	// FinishCooperative clears Inbox on completion. The immutable mailbox remains
	// the document source; resumed generation plus the review's three exact copied
	// facts establish handoff evidence without requiring a completed receipt inbox.
	addressKey := func(a engine.Address) string { return engine.Digest(a.Project, a.Role, a.Version, a.Instance) }
	mailPath := filepath.Join(root, "mailbox", engine.Digest(addressKey(engine.AddressOf(reviewer)), addressKey(engine.AddressOf(planner)), correlation)+".json")
	mail, err := os.Open(mailPath)
	if err != nil {
		return r, err
	}
	b, readErr := io.ReadAll(io.LimitReader(mail, (64<<10)+1))
	closeErr := mail.Close()
	if readErr != nil || closeErr != nil || len(b) > 64<<10 {
		return r, errors.Join(errors.New("durable mailbox read failed"), readErr, closeErr)
	}
	var m engine.Message
	if err = decodeStrict(string(b), &m); err != nil {
		return r, err
	}
	if m.ID != "regional-brief-message-v1" || m.From != engine.AddressOf(planner) || m.To != engine.AddressOf(reviewer) || m.Correlation != correlation {
		return r, errors.New("durable document address or version mismatch")
	}
	if err = decodeStrict(m.Body, &r.Document); err != nil {
		return r, fmt.Errorf("generated document: %w", err)
	}
	if r.Document.Version != correlation || r.Document.Title == "" || r.Document.Region == "" || r.Document.FirstCustomerTask == "" || (r.Document.RegionDecision != "adopt" && r.Document.RegionDecision != "revise" && r.Document.RegionDecision != "defer") || r.Document.Proposal == "" || len([]rune(r.Document.DistinctiveDetail)) < 15 || !strings.Contains(r.Document.Proposal, r.Document.DistinctiveDetail) {
		return r, errors.New("generated document missing version, fields or distinctive proposal detail")
	}
	r.DocumentVerified = true
	sum := sha256.Sum256([]byte(m.Body))
	r.DocumentSHA256 = hex.EncodeToString(sum[:])
	if err = decodeStrict(reviewed.Text, &r.Review); err != nil {
		return r, fmt.Errorf("review output: %w", err)
	}
	if r.Review.DocumentVersion != r.Document.Version || r.Review.ObservedDetail != r.Document.DistinctiveDetail || r.Review.ObservedRegion != r.Document.Region || r.Review.ObservedCustomerTask != r.Document.FirstCustomerTask || strings.TrimSpace(r.Review.Assessment) == "" || strings.TrimSpace(r.Review.Risk) == "" {
		return r, errors.New("review did not reflect the generated document detail")
	}
	r.ReviewMatched = true
	logStatus(map[string]any{"event": "document_review_verified", "sha256": r.DocumentSHA256, "reviewerGeneration": reviewed.Generation})
	return r, nil
}

func decodeStrict(s string, out any) error {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return errors.New("unexpected trailing JSON content")
	}
	return nil
}
