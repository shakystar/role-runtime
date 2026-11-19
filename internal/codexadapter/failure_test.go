package codexadapter

import (
	"errors"
	sdk "github.com/shakystar/codex-adapter"
	"github.com/shakystar/role-runtime/internal/engine"
	"testing"
)

func TestFailureResponsibilityBoundary(t *testing.T) {
	for _, state := range []string{sdk.NotStarted, sdk.Unknown, sdk.Started, sdk.Completed} {
		err := errors.Join(&sdk.Failure{Version: 1, Stage: "account_read", Code: "workspace_routing_timeout", Execution: state, Retryable: state == sdk.NotStarted}, errors.New("wrapped"))
		if requiresQuarantine(err) != (state != sdk.NotStarted) {
			t.Fatalf("quarantine %s", state)
		}
		f := engine.FailureMetadata(err)
		if f == nil || f.Execution != state || f.Retryable != (state == sdk.NotStarted) {
			t.Fatalf("metadata %+v", f)
		}
	}
	if !requiresQuarantine(errors.New("unclassified")) {
		t.Fatal("unknown error must remain conservative")
	}
}
