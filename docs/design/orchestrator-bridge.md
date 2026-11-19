# Experimental LangGraph bridge

2026-11-19: `cmd/runtime-bridge` exposes a small sequential JSONL interface to the independent Python [role-orchestrator](https://github.com/shakystar/role-orchestrator). This is a pilot, not a stable public API or formal adoption.

The external graph owns proposal/review/revision decisions and SQLite checkpoints. The bridge registers immutable role/project definitions and runs tasks through `engine.Pool` with one worker and queue slot. One bridge-owned executor is borrowed across submissions and closed at bridge shutdown. `--backend fixture` returns explicit synthetic responses; `--backend codex` uses the existing codex-adapter with advisory tools and heartbeat/status reporting. The core engine was not changed.

Completed matching receipts are reused on repeated task IDs. Changed inputs or role definitions are rejected. Existing non-success or malformed receipts fail closed; uncertain model work is never automatically replayed. There is no cross-store transaction or exactly-once side-effect guarantee.

```sh
./scripts/go.sh build -o bin/runtime-bridge ./cmd/runtime-bridge
./bin/runtime-bridge --state artifacts/bridge-new --backend fixture
```

Send one JSON object per line: `id`, `role`, `instruction`, `input` (all strings). Fixture input is a JSON string containing a `fixture` response object. Responses include `id`, `status`, `text`, `cached`, `stats`, and optional `error`. Input is bounded to 128 KiB per line, task input to 64 KiB, executor output to 256 KiB. Stderr carries status; stdout only responses. Full protocol and Python commands are in role-orchestrator.

Validation: Go fmt/vet and `go test -race ./...` passed. Bridge tests exercise successful receipt reuse across store reopen, input/role conflicts, uncertain receipt rejection, and corrupted receipt rejection. Python tests exercise real bridge restart/resume, cached execution, accept-without-revision, bounded unresolved review and budget exhaustion. Live Codex completed three roles/four tasks across two Python invocations, with one revision and one active worker; successful receipt replay caused no new model execution. Synthetic evidence only: this does not validate travel demand or broad agent autonomy. SMS wakeups, parallel workers, dynamic graphs and production recovery remain outside this bridge pilot.

During live-receipt replay inspection, eager adapter initialization attempted to start Codex even for cached work and failed in the restricted environment (`codex RPC stdout closed`). The bridge now initializes the backend lazily. A regression test ensures cached receipts do not call the executor factory; actual completed model output was replayed successfully with a nonexistent Codex executable. The failed check is not counted as a successful replay.
