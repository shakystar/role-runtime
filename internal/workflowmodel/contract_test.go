package workflowmodel

import (
	"context"
	"encoding/json"
	sdk "github.com/shakystar/codex-adapter"
	"github.com/shakystar/role-runtime/internal/control"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProposalAdmission(t *testing.T) {
	c := WorkContract{ID: "contract-a", Evidence: []Observation{{ID: "obs"}}}
	for _, test := range []struct {
		action, check, hash string
		refs                []string
		ok                  bool
	}{
		{"complete", "", "hash", []string{"obs"}, true},
		{"revise", "Correct the missing date", "hash", []string{"obs"}, true},
		{"investigate", "Compare an independent sample", "hash", []string{"obs"}, true},
		{"defer", "Wait until dates are published", "hash", []string{"obs"}, true},
		{"run", "", "hash", []string{"obs"}, false},
		{"investigate", "", "hash", []string{"obs"}, false},
		{"complete", "", "wrong", []string{"obs"}, false},
		{"complete", "", "hash", []string{"invented"}, false},
		{"complete", "", "hash", []string{"obs", "obs"}, false},
	} {
		raw, _ := json.Marshal(proposal{test.action, "reason", test.refs, test.check, test.hash})
		out, err := c.recordProposal(string(raw), "hash")
		if (err == nil) != test.ok {
			t.Fatalf("%+v: %v", test, err)
		}
		if test.ok {
			var saved map[string]any
			if json.Unmarshal(out, &saved) != nil || saved["action"] != test.action || saved["contract_id"] != c.ID {
				t.Fatal("lost decision binding")
			}
		}
	}
}

func TestContractInputValidation(t *testing.T) {
	c := WorkContract{Version: 1, ID: "test", Objective: "Decide", RequiredItems: []string{"Assess evidence"}, Evidence: []Observation{{"obs", "observed"}}}
	p := filepath.Join(t.TempDir(), "contract.json")
	for _, dup := range []bool{false, true} {
		if dup {
			c.Evidence = append(c.Evidence, c.Evidence[0])
		}
		raw, _ := json.Marshal(c)
		if err := os.WriteFile(p, raw, 0600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadContract(p)
		if (err != nil) != dup {
			t.Fatalf("duplicate=%v error=%v", dup, err)
		}
	}
}

type proposalModel struct {
	fakeModel
	t *testing.T
}

func (m *proposalModel) Execute(_ context.Context, r sdk.Request) (sdk.Result, error) {
	if r.Role == "producer" {
		var contract WorkContract
		if json.Unmarshal([]byte(r.Input), &contract) != nil || contract.ID != "contract-a" {
			m.t.Fatal("writer missing contract")
		}
		return sdk.Result{Text: `{"title":"Scope","body":"One group reported a difficulty."}`}, nil
	}
	var input struct {
		Document struct {
			Contract WorkContract `json:"task_contract"`
		}
		Hash string `json:"source_sha256"`
	}
	if json.Unmarshal([]byte(r.Input), &input) != nil || input.Document.Contract.ID != "contract-a" || r.Instructions != ProposalInstruction {
		m.t.Fatal("reviewer missing verified contract")
	}
	data, _ := json.Marshal(proposal{"investigate", "One group is insufficient", []string{"obs"}, "Check other groups", input.Hash})
	return sdk.Result{Text: string(data)}, nil
}

func TestInvestigationIsRecordedWithoutDispatch(t *testing.T) {
	s, e, _ := setup(t)
	e.Contract = &WorkContract{Version: 1, ID: "contract-a", Objective: "Decide", RequiredItems: []string{"Check sample size"}, Evidence: []Observation{{"obs", "One report"}}}
	e.Factory = func(context.Context, func(sdk.Status)) (Model, error) { return &proposalModel{t: t}, nil }
	write(t, s, e)
	consumer := lease(t, s, "consumer")
	result, err := e.Step(context.Background(), consumer.Execution)
	if err != nil || len(result.Send) != 0 || result.Wait != nil {
		t.Fatalf("unexpected transition: %v %+v", err, result)
	}
	if err = s.FinishCooperative("consumer", consumer.Generation, result, time.Now()); err != nil {
		t.Fatal(err)
	}
	var ref control.Reference
	if json.Unmarshal([]byte(result.Text), &ref) != nil || ref.Artifact.ID != "review.json" {
		t.Fatal("missing review receipt")
	}
	data, err := os.ReadFile(filepath.Join(s.Root, "artifacts", "review.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved map[string]any
	if json.Unmarshal(data, &saved) != nil || saved["action"] != "investigate" || saved["contract_id"] != "contract-a" {
		t.Fatal("proposal not recorded")
	}
}
