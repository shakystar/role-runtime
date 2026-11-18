package main

import (
	"github.com/shakystar/role-runtime/internal/researchflow"
	"strings"
	"testing"
	"time"
)

func TestOverlap(t *testing.T) {
	now := time.Now()
	a := []interval{{now, now.Add(time.Second)}}
	if !overlap(a, []interval{{now.Add(time.Millisecond), now.Add(2 * time.Second)}}) {
		t.Fatal("missed overlap")
	}
	if overlap(a, []interval{{now.Add(time.Second), now.Add(2 * time.Second)}}) {
		t.Fatal("touching intervals overlap")
	}
	if overlap(a, []interval{{time.Time{}, now}}) {
		t.Fatal("unknown start accepted")
	}
}
func TestPlanDependencyOrderAndFanIn(t *testing.T) {
	seen := map[string]bool{}
	specs := plan()
	for _, s := range specs {
		for _, d := range s.Dependencies {
			if !seen[d] {
				t.Fatal("not topological", s.ID, d)
			}
		}
		seen[s.ID] = true
		for _, to := range s.Recipients {
			found := false
			for _, next := range specs {
				if next.ID == to {
					for _, d := range next.Dependencies {
						if d == s.ID {
							found = true
						}
					}
				}
			}
			if !found {
				t.Fatal("unmatched SMS edge", s.ID, to)
			}
		}
	}
	if len(specs[3].Dependencies) != 2 {
		t.Fatal("lost fan-in")
	}
}
func TestRejectIncompleteMetrics(t *testing.T) {
	if validateRun(report{}) == nil {
		t.Fatal("empty report passed")
	}
}

func TestFinalEvidenceRequiresEveryOriginalReference(t *testing.T) {
	r := report{Documents: map[string]researchflow.Document{}, References: map[string]researchflow.Reference{}}
	a := researchflow.InputAudit{Role: "decision", Generation: 2, References: map[string]researchflow.Reference{}, SourceCounts: map[string]int{}}
	for _, role := range []string{"designer", "domestic", "international", "data", "critic"} {
		ref := researchflow.Reference{Role: role, Path: role + ".json", SHA256: "hash-" + role}
		r.References[role] = ref
		a.References[role] = ref
		r.Documents[role] = researchflow.Document{Sources: []researchflow.Source{{ID: "s1", URL: "https://example.com"}}}
		a.SourceCounts[role] = 1
	}
	r.InputAudits = []researchflow.InputAudit{a}
	if err := validateFinalEvidence(r); err != nil {
		t.Fatal(err)
	}
	changed := a.References["domestic"]
	changed.SHA256 = "altered"
	a.References["domestic"] = changed
	if validateFinalEvidence(r) == nil {
		t.Fatal("changed source artifact accepted")
	}
}
func TestDecisionFieldsRequired(t *testing.T) {
	r := report{Documents: map[string]researchflow.Document{"decision": {Decision: "defer", NextExperiment: " "}}}
	if e := validateRun(r); e == nil || !strings.Contains(e.Error(), "decision and next experiment") {
		t.Fatal(e)
	}
}
