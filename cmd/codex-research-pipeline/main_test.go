package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/shakystar/role-runtime/internal/engine"
)

func validSource() source {
	return source{ID: "S1", URL: "https://example.org/post/1", Title: "Question", Access: "body-read", Observation: "Location question; date not available."}
}
func TestSourceValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sources []source
		fail    bool
	}{
		{"empty honest evidence", []source{}, false},
		{"nil", nil, true},
		{"valid", []source{validSource()}, false},
		{"duplicate", []source{validSource(), validSource()}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if (validateSources(tc.sources) != nil) != tc.fail {
				t.Fatal("unexpected source validation")
			}
		})
	}
	for _, mutate := range []func(*source){
		func(s *source) { s.URL = "file:///private/test" },
		func(s *source) { s.URL = "https://name:password@example.org/" },
		func(s *source) { s.Access = "verified" },
		func(s *source) { s.Observation = " " },
	} {
		s := validSource()
		mutate(&s)
		if validateSources([]source{s}) == nil {
			t.Fatal("invalid source accepted")
		}
	}
	if bodyReadCount([]source{{Access: "search-snippet"}, {Access: "access-failed"}}) != 0 {
		t.Fatal("snippet/failure treated as body access")
	}
}
func TestActualWebActivityRequired(t *testing.T) {
	a := map[string]webActivity{"community-research": {Completed: 2}, "data-research": {Completed: 1}}
	if validateActivity(a) != nil {
		t.Fatal("valid activity rejected")
	}
	a["data-research"] = webActivity{}
	if validateActivity(a) == nil {
		t.Fatal("missing web calls accepted")
	}
	a["data-research"] = webActivity{Completed: 1}
	a["reviewer"] = webActivity{Completed: 1}
	if validateActivity(a) == nil {
		t.Fatal("advisory search accepted")
	}
}
func TestReadbackAndHonestInsufficiency(t *testing.T) {
	d := discoveryResult{Version: candidatesVersion, Summary: "No accessible bodies.", Sources: []source{}, Limitations: []string{"Access blocked."}}
	v := verificationResult{Version: verifiedVersion, ObservedCommunitySummary: d.Summary, Assessment: "Metadata only.", Sources: []source{}, Limitations: []string{"No rows retrieved."}}
	r := reviewResult{Version: "review-v1", ObservedDataAssessment: v.Assessment, Decision: "request evidence", Rationale: "Insufficient.", NextExperiment: "Obtain public sample.", Limitations: []string{"No demand proof."}}
	if validateDiscovery(d) != nil || validateVerification(v) != nil || validateReview(r) != nil || validateReadback(d, v, r) != nil {
		t.Fatal("honest insufficient report rejected")
	}
	if bodyReadCount(d.Sources) > 0 || bodyReadCount(v.Sources) > 0 {
		t.Fatal("insufficiency counted ready")
	}
	v.ObservedCommunitySummary = "invented"
	if validateReadback(d, v, r) == nil {
		t.Fatal("missing first handoff rejected incorrectly")
	}
	v.ObservedCommunitySummary = d.Summary
	r.ObservedDataAssessment = "invented"
	if validateReadback(d, v, r) == nil {
		t.Fatal("missing second handoff accepted")
	}
}
func TestDurableHandoffAndBounds(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "mailbox"), 0700); err != nil {
		t.Fatal(err)
	}
	from := engine.Task{Project: "p", Role: "community-research", Version: "v1", Instance: "main"}
	to := engine.Task{Project: "p", Role: "data-research", Version: "v1", Instance: "main"}
	m := engine.Message{ID: candidatesVersion + "-message", From: engine.AddressOf(from), To: engine.AddressOf(to), Correlation: candidatesVersion, Body: `{"version":"community-v1"}`}
	path := filepath.Join(root, "mailbox", engine.Digest(to.Key(), from.Key(), candidatesVersion)+".json")
	write := func() {
		b, _ := json.Marshal(m)
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	if b, e := readHandoff(root, from, to, candidatesVersion); e != nil || b != m.Body {
		t.Fatal(b, e)
	}
	m.To.Role = "wrong"
	write()
	if _, e := readHandoff(root, from, to, candidatesVersion); e == nil {
		t.Fatal("misaddressed handoff accepted")
	}
	m.To = engine.AddressOf(to)
	m.Body = string(make([]byte, (16<<10)+1))
	write()
	if _, e := readHandoff(root, from, to, candidatesVersion); e == nil {
		t.Fatal("oversized body accepted")
	}
}
func TestStrictJSON(t *testing.T) {
	var r reviewResult
	for _, s := range []string{`{"extra":1}`, `{} {}`} {
		if decodeStrict(s, &r) == nil {
			t.Fatal("invalid JSON shape accepted")
		}
	}
}
