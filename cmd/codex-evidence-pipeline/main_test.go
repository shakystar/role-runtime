package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shakystar/role-runtime/internal/engine"
)

func fixtureEvidence() evidence {
	e := evidence{Version: "e1", ObservedAt: "2026-11-18", Sources: []source{{ID: "s1", URL: "https://example.invalid/official", Access: "observed", Notes: "synthetic"}}, SNS: evidenceStatus{Status: "unavailable"}, LocalInput: evidenceStatus{Status: "one-person-report"}}
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		e.Candidates = append(e.Candidates, candidate{ID: id, Name: id, SourceIDs: []string{"s1"}, Facts: []string{"synthetic fact"}, Unknowns: []string{}})
	}
	return e
}
func writeEvidence(t *testing.T, e evidence) string {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "input.json")
	if err = os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	return p
}
func TestEvidenceValidation(t *testing.T) {
	e := fixtureEvidence()
	if _, _, err := loadEvidence(writeEvidence(t, e)); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*evidence){func(e *evidence) { e.Candidates[5].ID = "a" }, func(e *evidence) { e.Candidates[0].SourceIDs = []string{"foreign"} }, func(e *evidence) { e.Candidates = e.Candidates[:5] }, func(e *evidence) { e.Sources = append(e.Sources, e.Sources[0]) }} {
		e := fixtureEvidence()
		change(&e)
		if _, _, err := loadEvidence(writeEvidence(t, e)); err == nil {
			t.Fatal("invalid evidence accepted")
		}
	}
	p := filepath.Join(t.TempDir(), "oversize.json")
	if err := os.WriteFile(p, []byte(strings.Repeat(" ", (32<<10)+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadEvidence(p); err == nil {
		t.Fatal("oversized evidence accepted")
	}
}
func TestModelOutputsRequireExactIDsAndEvidenceVersion(t *testing.T) {
	e := fixtureEvidence()
	ids := []string{"f", "e", "d", "c", "b", "a"}
	d := discoveryResult{Version: candidatesVersion, EvidenceVersion: e.Version, CandidateIDs: ids, Summary: "evidence summary", LocalQuestions: []string{}}
	if err := validateDiscovery(d, e); err != nil {
		t.Fatal(err)
	}
	d.CandidateIDs = []string{"a", "a", "b", "c", "d", "e"}
	if validateDiscovery(d, e) == nil {
		t.Fatal("duplicate ID accepted")
	}
	d.CandidateIDs = ids
	d.EvidenceVersion = "stale"
	if validateDiscovery(d, e) == nil {
		t.Fatal("stale evidence accepted")
	}
	r := reviewResult{ObservedFinding: "generated finding", Version: "review-v1", EvidenceVersion: e.Version, CandidateIDs: ids, Decision: "request evidence", Rationale: "conflicting dates", LocalQuestions: []string{}, Limitations: []string{"no SNS body"}}
	if err := validateReview(r, e); err != nil {
		t.Fatal(err)
	}
	r.CandidateIDs = ids[:5]
	if validateReview(r, e) == nil {
		t.Fatal("missing reviewed candidate accepted")
	}
}
func TestVerifierRejectsInventedSourcesAndMissingFindings(t *testing.T) {
	e := fixtureEvidence()
	v := verificationResult{ObservedDiscoverySummary: "generated summary", Version: verifiedVersion, EvidenceVersion: e.Version, LocalQuestions: []string{}, Limitations: []string{"synthetic"}}
	for _, c := range e.Candidates {
		v.CandidateIDs = append(v.CandidateIDs, c.ID)
		v.Findings = append(v.Findings, finding{CandidateID: c.ID, Status: "unconfirmed", Reason: "insufficient source", SourceIDs: []string{"s1"}})
	}
	if err := validateVerification(v, e); err != nil {
		t.Fatal(err)
	}
	v.Findings[0].SourceIDs = []string{"invented"}
	if validateVerification(v, e) == nil {
		t.Fatal("invented source accepted")
	}
	v.Findings[0].SourceIDs = []string{"s1"}
	v.Findings[5].CandidateID = "a"
	if validateVerification(v, e) == nil {
		t.Fatal("duplicate finding accepted")
	}
}
func TestDurableHandoffChecksIdentityAndBodySize(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "mailbox"), 0700); err != nil {
		t.Fatal(err)
	}
	from := engine.Task{Project: "p", Role: "discovery", Version: "v1", Instance: "main"}
	to := from
	to.Role = "verifier"
	path := filepath.Join(root, "mailbox", engine.Digest(to.Key(), from.Key(), candidatesVersion)+".json")
	m := engine.Message{ID: candidatesVersion + "-message", From: engine.AddressOf(from), To: engine.AddressOf(to), Correlation: candidatesVersion, Body: `{"version":"candidates-v1"}`}
	save := func() {
		b, _ := json.Marshal(m)
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	save()
	if b, err := readHandoff(root, from, to, candidatesVersion); err != nil || b != m.Body {
		t.Fatal("valid handoff failed", err)
	}
	m.From.Role = "impostor"
	save()
	if _, err := readHandoff(root, from, to, candidatesVersion); err == nil {
		t.Fatal("spoofed sender accepted")
	}
	m.From = engine.AddressOf(from)
	m.Body = strings.Repeat("x", (16<<10)+1)
	save()
	if _, err := readHandoff(root, from, to, candidatesVersion); err == nil {
		t.Fatal("oversized SMS body accepted")
	}
}
func TestStrictJSONRejectsUnknownAndTrailingFields(t *testing.T) {
	for _, s := range []string{`{"version":"v","invented":true}`, `{"version":"v"} {"version":"w"}`} {
		var d discoveryResult
		if decodeStrict(s, &d) == nil {
			t.Fatal("invalid JSON accepted")
		}
	}
}

func TestReadbackRequiresGeneratedHandoffContent(t *testing.T) {
	d := discoveryResult{Summary: "model generated discovery summary"}
	v := verificationResult{ObservedDiscoverySummary: d.Summary, Findings: []finding{{Reason: "model generated assessment"}}}
	r := reviewResult{ObservedFinding: v.Findings[0].Reason}
	if err := validateReadback(d, v, r); err != nil {
		t.Fatal(err)
	}
	v.ObservedDiscoverySummary = "evidence-only inference"
	if validateReadback(d, v, r) == nil {
		t.Fatal("missing discovery handoff readback accepted")
	}
	v.ObservedDiscoverySummary = d.Summary
	r.ObservedFinding = "different finding"
	if validateReadback(d, v, r) == nil {
		t.Fatal("missing verification handoff readback accepted")
	}
	v.Findings = nil
	if validateReadback(d, v, r) == nil {
		t.Fatal("empty findings accepted")
	}
}
