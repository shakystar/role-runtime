package workflowmodel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExporterFailureDoesNotLoseLocalTrace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	t.Setenv("ROLE_TRACE_ENDPOINT", server.URL+"/v1/traces")
	t.Setenv("ROLE_TRACE_AUTH", "")
	root := t.TempDir()
	telemetry, err := NewTelemetry(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, parent := telemetry.Provider.Tracer("test").Start(context.Background(), "workflow.step")
	_, child := telemetry.Provider.Tracer("test").Start(ctx, "workflow.model")
	child.End()
	parent.End()
	telemetry.Close()
	if telemetry.log.failed != 2 {
		t.Fatalf("lost failure count: %d", telemetry.log.failed)
	}
	data, err := os.ReadFile(filepath.Join(root, "workflow-spans.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatal("local spans missing")
	}
	for _, line := range lines {
		var row map[string]any
		if json.Unmarshal([]byte(line), &row) != nil || row["trace_id"] != telemetry.TraceID {
			t.Fatal("invalid local span")
		}
		if _, ok := row["input"]; ok {
			t.Fatal("payload exported")
		}
	}
	t.Setenv("ROLE_TRACE_ENDPOINT", "")
	again, err := NewTelemetry(root)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if again.TraceID != telemetry.TraceID {
		t.Fatal("trace identity changed on restart")
	}
}
