// Package eino_test exercises the real Eino agent/tool loop with a deterministic
// model boundary. It is a feasibility experiment, not the production runtime.
package eino_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

type role struct {
	Instruction string `json:"instruction"`
	Tool        string `json:"tool"`
}
type step struct {
	Project   string   `json:"project"`
	Role      string   `json:"role"`
	Prompt    string   `json:"prompt"`
	Required  []string `json:"required"`
	Forbidden []string `json:"forbidden"`
	Restart   bool     `json:"restart"`
}
type fixture struct {
	Roles map[string]role `json:"roles"`
	Steps []step          `json:"steps"`
}
type capture struct {
	Messages []*schema.Message
	Tools    []string
}

type syntheticModel struct{ calls []capture }

func (m *syntheticModel) Generate(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options := model.GetCommonOptions(nil, opts...)
	names := []string{}
	for _, info := range options.Tools {
		names = append(names, info.Name)
	}
	encoded, err := json.Marshal(messages)
	if err != nil {
		return nil, err
	}
	var copied []*schema.Message
	if err := json.Unmarshal(encoded, &copied); err != nil {
		return nil, err
	}
	m.calls = append(m.calls, capture{Messages: copied, Tools: names})
	if len(messages) > 0 && messages[len(messages)-1].Role == schema.Tool {
		return schema.AssistantMessage("synthetic-complete", nil), nil
	}
	if len(names) != 1 {
		return nil, fmt.Errorf("expected one role tool, got %v", names)
	}
	return schema.AssistantMessage("", []schema.ToolCall{{ID: "probe-call", Type: "function", Function: schema.FunctionCall{Name: names[0], Arguments: "{}"}}}), nil
}

func (m *syntheticModel) Stream(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	output, err := m.Generate(ctx, input, opts...)
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]*schema.Message{output}), nil
}

type probeTool struct {
	name, cwd  string
	executions []string
}

func (p *probeTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{Name: p.name, Desc: "Synthetic scoped file probe", ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{})}, nil
}
func (p *probeTool) InvokableRun(ctx context.Context, _ string, _ ...tool.Option) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	data, err := os.ReadFile(filepath.Join(p.cwd, "marker"))
	if err != nil {
		return "", err
	}
	p.executions = append(p.executions, string(data))
	return string(data), nil
}

// One reused slot, no role -> live agent map. Each activation reconstructs an
// Eino runner from a role definition and disk transcript, then drops references.
type worker struct{ root string }

func (w *worker) execute(ctx context.Context, s step, r role) ([]capture, []string, error) {
	key := s.Project + "-" + s.Role // Only trusted fixed fixture IDs in this spike.
	cwd := filepath.Join(w.root, s.Project)
	if err := os.MkdirAll(cwd, 0700); err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(filepath.Join(cwd, "marker"), []byte(s.Project), 0600); err != nil {
		return nil, nil, err
	}
	statePath := filepath.Join(w.root, key+"-state.json")
	var history []*schema.Message
	if data, err := os.ReadFile(statePath); err == nil {
		if err := json.Unmarshal(data, &history); err != nil {
			return nil, nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, nil, err
	}
	history = append(history, schema.UserMessage(s.Prompt))
	recorder := &syntheticModel{}
	probe := &probeTool{name: r.Tool, cwd: cwd}
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name: s.Role, Description: "Role switching spike", Instruction: r.Instruction, Model: recorder,
		MaxIterations: 3, ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{probe}}},
	})
	if err != nil {
		return nil, nil, err
	}
	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: agent})
	events := runner.Run(ctx, history)
	var final *schema.Message
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			return nil, nil, event.Err
		}
		if event.Output != nil && event.Output.MessageOutput != nil {
			message, err := event.Output.MessageOutput.GetMessage()
			if err != nil {
				return nil, nil, err
			}
			if message.Role == schema.Assistant && len(message.ToolCalls) == 0 {
				final = message
			}
		}
	}
	if final == nil || final.Content != "synthetic-complete" || len(recorder.calls) != 2 {
		return nil, nil, fmt.Errorf("incomplete model-tool-model loop: calls=%d final=%v", len(recorder.calls), final)
	}
	// Completed-turn transcript persistence is owned by this experiment. This
	// does NOT test Eino's interrupted-run checkpoint/resume semantics.
	persisted := []*schema.Message{}
	for _, msg := range recorder.calls[len(recorder.calls)-1].Messages {
		if msg.Role != schema.System {
			persisted = append(persisted, msg)
		}
	}
	persisted = append(persisted, final)
	encoded, err := json.Marshal(persisted)
	if err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(statePath+".tmp", encoded, 0600); err != nil {
		return nil, nil, err
	}
	if err := os.Rename(statePath+".tmp", statePath); err != nil {
		return nil, nil, err
	}
	return recorder.calls, probe.executions, nil
}

func loadFixture(t *testing.T) fixture {
	t.Helper()
	data, err := os.ReadFile("../fixtures/role-switch.json")
	if err != nil {
		t.Fatal(err)
	}
	var f fixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func runStep(t *testing.T, w *worker, f fixture, s step) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	calls, executions, err := w.execute(ctx, s, f.Roles[s.Role])
	if err != nil {
		t.Fatal(err)
	}
	r := f.Roles[s.Role]
	for _, call := range calls {
		if !reflect.DeepEqual(call.Tools, []string{r.Tool}) {
			t.Fatalf("wrong tools: %v", call.Tools)
		}
		found := false
		for _, msg := range call.Messages {
			if msg.Role == schema.System && msg.Content == r.Instruction {
				found = true
			}
		}
		if !found {
			t.Fatal("role instruction absent from actual model input")
		}
		encoded, err := json.Marshal(call.Messages)
		if err != nil {
			t.Fatal(err)
		}
		text := string(encoded)
		if !strings.Contains(text, s.Prompt) {
			t.Fatal("current task absent from model input")
		}
		for _, marker := range s.Required {
			if !strings.Contains(text, marker) {
				t.Fatalf("missing %s", marker)
			}
		}
		for _, marker := range s.Forbidden {
			if strings.Contains(text, marker) {
				t.Fatalf("leaked %s", marker)
			}
		}
		for id, other := range f.Roles {
			if id != s.Role && strings.Contains(text, other.Instruction) {
				t.Fatal("previous role instruction leaked")
			}
		}
	}
	if !reflect.DeepEqual(executions, []string{s.Project}) {
		t.Fatalf("wrong scoped tool execution: %v", executions)
	}
	t.Logf(`{"candidate":"eino","step":%q,"result":"pass","pid":%d}`, s.Prompt, os.Getpid())
}

func TestRoleSwitch(t *testing.T) {
	f := loadFixture(t)
	w := &worker{root: t.TempDir()}
	for _, s := range f.Steps {
		if s.Restart {
			continue
		}
		runStep(t, w, f, s)
	}
	// The helper is a genuinely fresh OS process with no prior role objects.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessResume$", "-test.v")
	cmd.Env = append(os.Environ(), "ROLE_SPIKE_STATE="+w.root)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("restart failed: %v\n%s", err, output)
	}
	t.Log(string(output))
}

func TestProcessResume(t *testing.T) {
	root := os.Getenv("ROLE_SPIKE_STATE")
	if root == "" {
		t.Skip("subprocess helper")
	}
	f := loadFixture(t)
	for _, s := range f.Steps {
		if s.Restart {
			runStep(t, &worker{root: root}, f, s)
		}
	}
}

func TestCorruptStateFailsClosed(t *testing.T) {
	f := loadFixture(t)
	s := f.Steps[0]
	w := &worker{root: t.TempDir()}
	if err := os.WriteFile(filepath.Join(w.root, s.Project+"-"+s.Role+"-state.json"), []byte("invalid-json"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.execute(context.Background(), s, f.Roles[s.Role]); err == nil {
		t.Fatal("corrupt state silently discarded")
	}
}
