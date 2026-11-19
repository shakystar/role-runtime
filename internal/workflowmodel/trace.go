package workflowmodel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/shakystar/role-runtime/internal/lifecycle"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	st "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type runIDs struct{ id trace.TraceID }

func (i runIDs) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	return i.id, i.NewSpanID(ctx, i.id)
}
func (i runIDs) NewSpanID(context.Context, trace.TraceID) trace.SpanID {
	var id trace.SpanID
	rand.Read(id[:])
	return id
}

type spanLog struct {
	mu     sync.Mutex
	file   *os.File
	remote st.SpanExporter
	failed int
}

func (l *spanLog) ExportSpans(ctx context.Context, spans []st.ReadOnlySpan) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range spans {
		attrs := map[string]any{}
		for _, a := range s.Attributes() {
			attrs[string(a.Key)] = a.Value.AsInterface()
		}
		data, _ := json.Marshal(map[string]any{"name": s.Name(), "trace_id": s.SpanContext().TraceID().String(), "span_id": s.SpanContext().SpanID().String(), "parent_span_id": s.Parent().SpanID().String(), "status": s.Status().Code.String(), "attributes": attrs})
		info, e := l.file.Stat()
		if e != nil || info.Size()+int64(len(data)+1) > 8<<20 {
			l.failed++
			continue
		}
		if _, e = l.file.Write(append(data, '\n')); e != nil {
			l.failed++
		}
	}
	if l.remote != nil {
		if e := l.remote.ExportSpans(ctx, spans); e != nil {
			l.failed += len(spans)
			return errors.New("workflow trace export failed")
		}
	}
	return nil
}
func (l *spanLog) Shutdown(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.file.Close()
	if l.remote != nil {
		return l.remote.Shutdown(ctx)
	}
	return nil
}

type Telemetry struct {
	Provider *st.TracerProvider
	TraceID  string
	log      *spanLog
}

func NewTelemetry(root string) (*Telemetry, error) {
	path := filepath.Join(root, "workflow-trace-id.json")
	var value string
	data, e := os.ReadFile(path)
	if os.IsNotExist(e) {
		id := make([]byte, 16)
		if _, e = rand.Read(id); e != nil {
			return nil, e
		}
		value = hex.EncodeToString(id)
		data, _ = json.Marshal(value)
		if e = lifecycle.Write(path, data); e != nil {
			return nil, e
		}
	} else if e != nil {
		return nil, e
	} else if json.Unmarshal(data, &value) != nil {
		return nil, errors.New("invalid trace identity")
	}
	id, e := trace.TraceIDFromHex(value)
	if e != nil {
		return nil, e
	}
	file, e := os.OpenFile(filepath.Join(root, "workflow-spans.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	log := &spanLog{file: file}
	if endpoint := os.Getenv("ROLE_TRACE_ENDPOINT"); endpoint != "" {
		u, e := url.Parse(endpoint)
		if e != nil || u.Scheme != "http" || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			file.Close()
			return nil, errors.New("explicit loopback trace endpoint required")
		}
		log.remote, e = otlptracehttp.New(context.Background(), otlptracehttp.WithEndpointURL(endpoint), otlptracehttp.WithHeaders(map[string]string{"Authorization": os.Getenv("ROLE_TRACE_AUTH")}), otlptracehttp.WithTimeout(2*time.Second), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
		if e != nil {
			file.Close()
			return nil, errors.New("trace configuration failed")
		}
	}
	provider := st.NewTracerProvider(st.WithIDGenerator(runIDs{id}), st.WithResource(resource.NewSchemaless(attribute.String("service.name", "role-runtime-workflow"))), st.WithBatcher(log, st.WithMaxQueueSize(64), st.WithMaxExportBatchSize(16), st.WithBatchTimeout(100*time.Millisecond), st.WithExportTimeout(2*time.Second)))
	return &Telemetry{provider, value, log}, nil
}
func (t *Telemetry) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	e := t.Provider.Shutdown(ctx)
	t.log.mu.Lock()
	defer t.log.mu.Unlock()
	json.NewEncoder(os.Stderr).Encode(map[string]any{"phase": "workflow_trace_summary", "failed": t.log.failed, "shutdown_ok": e == nil})
}
