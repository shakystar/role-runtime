// codex-pilot is an external task supplier for the real Codex adapter.
// It does not replace the runtime scheduler or fabricate model responses.
package main

import (
	"context"
	"crypto/rand"
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
	"time"
	"unicode/utf8"

	"github.com/shakystar/role-runtime/internal/codexadapter"
	"github.com/shakystar/role-runtime/internal/codexrpc"
	"github.com/shakystar/role-runtime/internal/engine"
)

type answer struct {
	Checkpoint     string `json:"checkpoint"`
	Recommendation string `json:"recommendation"`
	Risk           string `json:"risk"`
}
type stageReport struct {
	Stage    int                  `json:"stage"`
	Stats    engine.Stats         `json:"stats"`
	Outcomes []engine.Outcome     `json:"outcomes"`
	Usage    []codexadapter.Usage `json:"usage"`
}
type report struct {
	Completed        bool          `json:"completed"`
	Error            string        `json:"error,omitempty"`
	Workers          int           `json:"workers"`
	ProcessesStarted int           `json:"processesStarted"`
	MemoryChecks     int           `json:"memoryChecks"`
	Stages           []stageReport `json:"stages"`
	Seconds          float64       `json:"seconds"`
}

func main() {
	state := flag.String("state", "", "new persistent state directory (required; contains model outputs)")
	workers := flag.Int("workers", 2, "bounded workers, 1..3")
	model := flag.String("model", "", "optional installed Codex model override")
	binary := flag.String("codex", "codex", "Codex executable")
	timeout := flag.Duration("timeout", 0, "optional entire pilot deadline; 0 means no time deadline")
	probe := flag.Bool("probe", false, "one role and one model turn; no restoration claim")
	proposalFile := flag.String("proposal", "", "optional UTF-8 agenda file, at most 16KiB")
	structured := flag.Bool("structured-output", true, "request backend JSON schema; false uses validated prompt JSON")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *timeout < 0 {
		fmt.Fprintln(os.Stderr, "timeout must be nonnegative")
		os.Exit(2)
	}
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	started := time.Now()
	r, err := run(ctx, *state, *workers, *model, *binary, *probe, *proposalFile, *structured)
	r.Seconds = time.Since(started).Seconds()
	if err != nil {
		r.Error = err.Error()
	}
	json.NewEncoder(os.Stdout).Encode(r)
	if err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, root string, workers int, model, binary string, probe bool, proposalFile string, structured bool) (r report, err error) {
	r.Workers = workers
	customProposal := ""
	if proposalFile != "" {
		f, e := os.Open(proposalFile)
		if e != nil {
			return r, e
		}
		b, e := io.ReadAll(io.LimitReader(f, (16<<10)+1))
		f.Close()
		if e != nil {
			return r, e
		}
		if len(b) > 16<<10 || strings.TrimSpace(string(b)) == "" || !utf8.Valid(b) {
			return r, errors.New("proposal must be valid UTF-8, nonempty and at most 16KiB")
		}
		customProposal = string(b)
	}
	if root == "" || workers < 1 || workers > 3 {
		return r, errors.New("require new --state and --workers 1..3")
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return
	}
	if _, e := os.Stat(root); !os.IsNotExist(e) {
		return r, errors.New("state must not already exist")
	}
	workspace := filepath.Join(root, "workspace")
	if err = os.MkdirAll(workspace, 0700); err != nil {
		return
	}
	store, e := engine.OpenStore(root)
	if e != nil {
		return r, e
	}
	defer func() {
		if store != nil {
			store.Close()
		}
	}()
	roles := []string{"customer", "finance", "operations"}
	stages := 3
	if probe {
		roles = roles[:1]
		stages = 1
		workers = 1
		r.Workers = 1
	}
	instructions := []string{"고객 가치와 기존 대안 대비 차별성을 검토한다.", "광고 수익 가설, 비용과 투자 중단 조건을 검토한다.", "공공데이터 확보·갱신과 민간 광고 검수 가능성을 검토한다."}
	project := engine.Project{ID: "tourism-pilot", Workspace: workspace, Rules: "공급된 안건만 검토한다. 외부 도구·웹·파일·연락·지출을 사용하지 않는다. 시장 수치나 조사 결과를 만들지 않는다. 이 실행은 사업 가설 사전 심의다. JSON 객체만 출력한다. 최상위 키는 checkpoint, recommendation, risk 세 개만 사용하고 세 값은 모두 문자열이다. 설명이나 비교표도 이 문자열 안에 적고 추가 키나 Markdown 코드펜스를 만들지 않는다."}
	if err = store.RegisterProject(project); err != nil {
		return
	}
	markers := map[string]string{}
	for i, id := range roles {
		b := make([]byte, 12)
		if _, err = rand.Read(b); err != nil {
			return
		}
		markers[id] = hex.EncodeToString(b)
		if err = store.RegisterRole(engine.Role{ID: id, Version: "v1", Instruction: instructions[i], Tools: []string{"advisory"}}); err != nil {
			return
		}
	}
	if err = store.RegisterRole(engine.Role{ID: "chair", Version: "v1", Instruction: "부문별 의견과 반론을 비교하여 진행·추가 조사·범위 변경·중단 중 선택한다. 합의를 강요하지 말고 소수 의견과 재검토 조건을 남긴다.", Tools: []string{"advisory"}}); err != nil {
		return
	}
	// No model selection override by default: use the user's installed default.
	cfg := codexadapter.Config{Store: store, Model: model, StartupTimeout: 30 * time.Second, RPC: codexrpc.Config{Command: binary, Dir: workspace}}
	var statusMu sync.Mutex
	cfg.OnStatus = func(s codexadapter.Status) {
		statusMu.Lock()
		defer statusMu.Unlock()
		_ = json.NewEncoder(os.Stderr).Encode(map[string]any{"event": "worker_status", "status": s})
	}
	cfg.OutputSchema = json.RawMessage(`{"type":"object","properties":{"checkpoint":{"type":"string"},"recommendation":{"type":"string"},"risk":{"type":"string"}},"required":["checkpoint","recommendation","risk"],"additionalProperties":false}`)
	if !structured {
		cfg.OutputSchema = nil
	}
	previous := map[string]answer{}
	sessions := map[string]string{}
	proposal := "안건 Welcome to Korea: 국내 여행 정보를 폭넓게 탐색하고 원문으로 연결한다. 자동 공급은 공공기관 데이터로 한정하고 민간은 직접 등록하며 유료 광고를 수익 가설로 둔다. 고객군·데이터 이용 조건·광고 수요·단가는 아직 검증하지 않았다. 이전 대안은 다양한 브랜드 이벤트·혜택 통합검색이다. 사업 범위를 좁히거나 변경할 재량이 있다."
	if customProposal != "" {
		proposal = customProposal
	}
	for stage := 1; stage <= stages; stage++ {
		// Close/reopen the runtime store as well as all workers between stages.
		if stage > 1 {
			if err = store.Close(); err != nil {
				return
			}
			store, err = engine.OpenStore(root)
			if err != nil {
				return
			}
			cfg.Store = store
		}
		var tasks []engine.Task
		if stage < 3 {
			for _, id := range roles {
				input := proposal
				if stage == 1 {
					input += "\n역할 전용 확인값 " + markers[id] + " 를 기억하라. checkpoint에 이 값을 그대로 넣고 recommendation과 risk에 각 1000자 이내 독립 검토를 적어라."
				} else {
					others := map[string]answer{}
					for other, a := range previous {
						if other != id {
							a.Checkpoint = ""
							others[other] = a
						}
					}
					data, _ := json.Marshal(others)
					input += "\n다른 부문 검토: " + string(data) + "\n이 역할의 직전 대화에서 받은 전용 확인값을 checkpoint에 복원하라. 현재 입력에는 그 값이 없다. recommendation과 risk에 반론을 검토한 최종 의견을 각 1000자 이내로 적어라."
				}
				tasks = append(tasks, engine.Task{ID: fmt.Sprintf("s%d-%s", stage, id), Project: project.ID, Role: id, Version: "v1", Instance: "main", Input: input})
			}
		} else {
			views := map[string]answer{}
			for id, a := range previous {
				a.Checkpoint = ""
				views[id] = a
			}
			data, _ := json.Marshal(views)
			tasks = []engine.Task{{ID: "s3-chair", Project: project.ID, Role: "chair", Version: "v1", Instance: "main", Input: proposal + "\n최종 부문 검토: " + string(data) + "\ncheckpoint는 빈 문자열, recommendation은 결정과 이유, risk는 반대 의견과 다음 검증 조건. 각 2000자 이내 JSON."}}
		}
		input := make(chan engine.Task, len(tasks))
		for _, t := range tasks {
			input <- t
		}
		close(input)
		sr := stageReport{Stage: stage}
		activeWorkers := workers
		if stage == 3 {
			activeWorkers = 1
		}
		var adapters []*codexadapter.Adapter
		pool := engine.Pool{Workers: activeWorkers, QueueSize: 4, Store: store, Factory: func(slot int) (engine.Executor, error) {
			a, e := codexadapter.New(cfg)
			if e == nil {
				adapters = append(adapters, a)
			}
			return a, e
		}}
		sr.Stats, err = pool.Run(ctx, input, func(o engine.Outcome) error {
			sr.Outcomes = append(sr.Outcomes, o)
			fmt.Fprintf(os.Stderr, "stage=%d role=%s status=%s worker=%d\n", stage, o.Task.Role, o.Status, o.Result.Worker)
			return nil
		})
		for _, a := range adapters {
			r.ProcessesStarted += a.Starts
			sr.Usage = append(sr.Usage, a.Usage())
		}
		r.Stages = append(r.Stages, sr)
		if err != nil {
			return
		}
		for _, o := range sr.Outcomes {
			if o.Status != "succeeded" {
				return r, fmt.Errorf("%s: %s", o.Task.ID, o.Error)
			}
			var a answer
			dec := json.NewDecoder(strings.NewReader(o.Result.Text))
			dec.DisallowUnknownFields()
			if err = dec.Decode(&a); err != nil {
				return r, fmt.Errorf("%s response JSON: %w", o.Task.ID, err)
			}
			if a.Recommendation == "" || a.Risk == "" {
				return r, errors.New("empty deliberation output")
			}
			if stage < 3 {
				if a.Checkpoint != markers[o.Task.Role] {
					return r, fmt.Errorf("role memory mismatch: %s", o.Task.Role)
				}
				if stage == 2 {
					if o.Result.Session != sessions[o.Task.Role] {
						return r, errors.New("thread identity changed on restart")
					}
					r.MemoryChecks++
				}
				sessions[o.Task.Role] = o.Result.Session
				previous[o.Task.Role] = a
			}
		}
	}
	r.Completed = true
	return
}
