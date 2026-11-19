package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type metadataExporter struct {
	mu                            sync.Mutex
	file                          *os.File
	remote                        sdktrace.SpanExporter
	exported, failed, localFailed int
}

func (e *metadataExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range spans {
		attrs := map[string]any{}
		for _, a := range s.Attributes() {
			attrs[string(a.Key)] = a.Value.AsInterface()
		}
		record := map[string]any{"name": s.Name(), "trace_id": s.SpanContext().TraceID().String(), "span_id": s.SpanContext().SpanID().String(), "parent_span_id": s.Parent().SpanID().String(), "start": s.StartTime(), "end": s.EndTime(), "status": s.Status().Code.String(), "attributes": attrs}
		b, _ := json.Marshal(record)
		if e.file == nil {
			e.localFailed++
			continue
		}
		info, err := e.file.Stat()
		if err != nil || info.Size()+int64(len(b)+1) > 8<<20 {
			e.localFailed++
			continue
		}
		if _, err = e.file.Write(append(b, '\n')); err != nil {
			e.localFailed++
		}
	}
	if e.remote != nil {
		if err := e.remote.ExportSpans(ctx, spans); err != nil {
			e.failed += len(spans)
			return errors.New("telemetry export failed")
		}
		e.exported += len(spans)
	}
	return nil
}
func (e *metadataExporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.file != nil {
		_ = e.file.Close()
	}
	if e.remote != nil {
		return e.remote.Shutdown(ctx)
	}
	return nil
}

type telemetry struct {
	provider *sdktrace.TracerProvider
	exporter *metadataExporter
}

func newTelemetry(root string) *telemetry {
	exporter := &metadataExporter{}
	exporter.file, _ = os.OpenFile(filepath.Join(root, "bridge-spans.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if endpoint := os.Getenv("ROLE_TRACE_ENDPOINT"); endpoint != "" {
		u, err := url.Parse(endpoint)
		if err == nil && u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1") && u.User == nil && u.RawQuery == "" && u.Fragment == "" {
			exporter.remote, err = otlptracehttp.New(context.Background(), otlptracehttp.WithEndpointURL(endpoint), otlptracehttp.WithHeaders(map[string]string{"Authorization": os.Getenv("ROLE_TRACE_AUTH")}), otlptracehttp.WithTimeout(2*time.Second), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
		} else {
			err = errors.New("invalid telemetry endpoint")
		}
		if err != nil {
			_ = json.NewEncoder(os.Stderr).Encode(map[string]string{"phase": "telemetry_disabled", "reason": "invalid_exporter_configuration"})
		}
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "role-runtime"))), sdktrace.WithSpanLimits(sdktrace.SpanLimits{AttributeCountLimit: 16, AttributeValueLengthLimit: 256, EventCountLimit: 8, AttributePerEventCountLimit: 4, LinkCountLimit: 4, AttributePerLinkCountLimit: 4}), sdktrace.WithBatcher(exporter, sdktrace.WithMaxQueueSize(64), sdktrace.WithMaxExportBatchSize(16), sdktrace.WithBatchTimeout(100*time.Millisecond), sdktrace.WithExportTimeout(2*time.Second)))
	return &telemetry{provider, exporter}
}
func (t *telemetry) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := t.provider.Shutdown(ctx)
	t.exporter.mu.Lock()
	defer t.exporter.mu.Unlock()
	_ = json.NewEncoder(os.Stderr).Encode(map[string]any{"phase": "telemetry_summary", "exported": t.exporter.exported, "failed": t.exporter.failed, "local_failed": t.exporter.localFailed, "shutdown_ok": err == nil})
}
