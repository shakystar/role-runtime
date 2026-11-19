# Experimental LangGraph bridge

2026-11-19: `cmd/runtime-bridge` exposes a small sequential JSONL interface to the independent Python [role-orchestrator](https://github.com/shakystar/role-orchestrator). This is a pilot, not a stable public API or formal adoption.

The external graph owns proposal/review/revision decisions and SQLite checkpoints. The bridge registers immutable role/project definitions and runs tasks through `engine.Pool` with one worker and queue slot. One bridge-owned executor is borrowed across submissions and closed at bridge shutdown. `--backend fixture` returns explicit synthetic responses; `--backend codex` uses the existing codex-adapter with advisory tools and heartbeat/status reporting. The core engine was not changed.

Completed matching receipts are reused on repeated task IDs. Changed inputs or role definitions are rejected. Existing non-success or malformed receipts fail closed; uncertain model work is never automatically replayed. There is no cross-store transaction or exactly-once side-effect guarantee.

```sh
./scripts/go.sh build -o bin/runtime-bridge ./cmd/runtime-bridge
./bin/runtime-bridge --state artifacts/bridge-new --backend fixture
```

Current default is **bridge v1**: send one JSON object per line containing integer `schema_version: 1` and string `run_id`, `attempt_id`, `traceparent`, `id`, `role`, `instruction`, `input`. Unknown, duplicate, incorrectly cased and missing fields are rejected. Unversioned requests require `--allow-legacy` and a separate state directory. Fixture input is a JSON string containing a `fixture` response object. Responses echo schema/run/attempt/task identity and include `status`, `text`, `cached`, `stats`, and optional sanitized `error`/`error_code`. Input is bounded to 128 KiB per line, task input to 64 KiB, executor output to 256 KiB. Stderr carries status; stdout only responses. Full protocol and Python commands are in role-orchestrator.

Validation: Go fmt/vet and `go test -race ./...` passed. Bridge tests exercise successful receipt reuse across store reopen, input/role conflicts, uncertain receipt rejection, and corrupted receipt rejection. Python tests exercise real bridge restart/resume, cached execution, accept-without-revision, bounded unresolved review and budget exhaustion. Live Codex completed three roles/four tasks across two Python invocations, with one revision and one active worker; successful receipt replay caused no new model execution. Synthetic evidence only: this does not validate travel demand or broad agent autonomy. SMS wakeups, parallel workers, dynamic graphs and production recovery remain outside this bridge pilot.

During live-receipt replay inspection, eager adapter initialization attempted to start Codex even for cached work and failed in the restricted environment (`codex RPC stdout closed`). The bridge now initializes the backend lazily. A regression test ensures cached receipts do not call the executor factory; actual completed model output was replayed successfully with a nonexistent Codex executable. The failed check is not counted as a successful replay.

## Versioned contract and cross-process tracing

The Python owner persists one run/trace identity; every client delivery creates a fresh
span. Go extracts W3C context, creates `runtime.request`, and creates `runtime.execute`
only when the executor actually runs. Cache replay creates no execution span. Protocol,
run and trace binding plus per-task attempt intents are durable. Intent-only and failed
receipts fail closed; there is no automated retry or cross-store transaction.

Metadata-only local logs are bounded to 8 MiB. Optional `ROLE_TRACE_ENDPOINT` (loopback
HTTP only) and `ROLE_TRACE_AUTH` configure OTLP export; Python passes these in the child
environment, never command arguments. Export failures do not change task receipts.
Bounded queues may lose telemetry; no durable export/replay guarantee is claimed.

The first new live probe hit `workspace_routing_timeout` during account preflight and
exposed a typed-nil executor retained after failed construction. The lazy wrapper now
stores only successfully initialized executors; a regression test verifies safe close.
This fix does not repair upstream routing. Full wire and ownership rules are maintained
in role-orchestrator `docs/protocol.md`; executable shared input vectors are in
`cmd/runtime-bridge/testdata/v1-vectors.json` and its Python counterpart.
