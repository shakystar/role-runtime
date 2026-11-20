# Model-backed document handoff

2026-11-19. `runtime-workflow --backend codex` now runs the existing cooperative
control/SMS/lifecycle path with real writer and reviewer turns. The default remains
`fixture`. This is a fixed synthetic brief, not a general research or product workflow.

## Before → after

| Situation | Previous workflow host | This slice |
| --- | --- | --- |
| Document creation | Fixture writes predetermined JSON | Existing codex-adapter produces writer JSON; runtime validates and saves it |
| Review | Fixture consumer returns fixed text | Reviewer receives completed producer evidence and exact document bytes/hash |
| Failure evidence | Cooperative errors conservatively uncertain | Structured stage/code/execution retained; known not-started failure stops without role quarantine |
| Observation | Lifecycle/metrics with a startup-retry hook | Adapter startup retries connected to admitted attempt; bounded metadata-only model spans |

`internal/workflowmodel` provides an injectable model executor. Each bounded worker
lazily initializes one adapter and can reuse its process across separate role session
keys. Python does not invoke Codex. The runtime determines artifact paths, immutable
references and SMS addresses; model output cannot choose these. Adapter defaults use
read-only workspace policy, but role/session separation is not OS privilege isolation.

Writer JSON requires title/body. Reviewer JSON requires accept/revise, a reason and
the supplied source SHA-256. Completion means a valid review was produced, **not**
that the proposal was accepted. This slice does not automatically revise a document.
Artifacts are bounded to 16 KiB and existing files are never intentionally overwritten.
Dependency receipts, paths and content hashes are checked before reviewer model calls.

Artifact persistence precedes receipt commit. A crash between them can leave an orphan
artifact and uncertain work; no automatic replay or exactly-once claim is made. Failed
task IDs remain terminal even for known not-started failures. Unknown/started/completed
failures retain conservative role quarantine. Backend, executable and instruction digest
are pinned in each state directory; changing them requires a new run.

## Run

```sh
./scripts/go.sh build -o bin/runtime-workflow ./cmd/runtime-workflow
./bin/runtime-workflow --backend codex --state /tmp/role-model-demo \
  --run-id model-demo --codex /absolute/path/to/codex
```

Use the paired role-orchestrator `examples/model-document-workflow.json` with its
workflow poll CLI. `scripts/check_model_workflow.py` additionally projects metrics
and verifies completed receipts, model completion count and artifact/trace consistency.
The process stays available for receipt inspection after a worker failure. SIGTERM
stops it gracefully. There is no model-turn deadline; heartbeat is 15s, probe timeout
10s, lease 45s. Adapter initialization retains its existing bounded retry policy.

## Observation boundary

`workflow-spans.jsonl` contains metadata only (8 MiB limit). `workflow-trace-id.json`
persists the run identity. Each step has a model child span. Optional OTLP export uses
`ROLE_TRACE_ENDPOINT` (explicit localhost/127.0.0.1 HTTP URL) and `ROLE_TRACE_AUTH`.
For local Langfuse the endpoint is `/api/public/otel/v1/traces`; pass credentials via
the environment, never in committed files. Export failures are counted separately;
they do not decide task completion. Local log initialization/configuration can still
fail host startup. Model status logs are bounded to 8 MiB. No unlimited retention or
lossless tracing guarantee is provided.

This tracing covers the Go workflow executor; it does not newly connect Python socket
request spans into this trace. Existing JSONL bridge tracing remains separate. Actual
token usage, cost and graph wait duration remain unavailable rather than reported as zero.

## Validation

- Whole-repository Go race suite and vet passed; added executor tests cover handoff,
  dependency tampering before a model call, malformed output after model completion,
  and all four execution-failure states. Exporter-503 test preserves local spans and
  trace identity across reopen.
- Paired orchestrator: existing 19 acceptance cases passed; 39 Python tests passed.
- Actual Codex 0.169.1: writer/reviewer completed with one reused process, one startup
  retry, two model completions, two execution attempts and one SMS acknowledgment.
- Local Langfuse 4.82.0 readback matched all four span IDs and parents, with no input
  or output payloads. Restart/re-poll left model statuses, spans and artifacts unchanged.
- Initial doctor request failed with workspace_routing_timeout; subsequent admitted
  execution recovered under the adapter's existing bounded startup retry. Routing was
  not bypassed or declared permanently fixed.

See the paired orchestrator's `docs/model-workflow-validation.md` and sanitized report.
All model content was synthetic; this verifies integration, not business quality,
long-running operations, dynamic teams, memory paging or universal entrypoint adoption.

## Contract-bound writer/reviewer

Pass `--contract file.json --backend codex` on a fresh run to supply a v1
contract. The JSON object contains version (1), id, objective, required_items
(1–8 distinct strings), and evidence (1–16 unique id/text observations); the
file limit is 8 KiB. The canonical contract and proposal instruction are bound
to the state directory. A changed binding cannot resume an earlier run.
The producer receipt input and the hashed writer artifact retain the contract.
The reviewer receives the verified artifact and contract.

In this mode review.json records one action (complete, revise, investigate,
defer), reason, evidence_ids, decision_test, source_sha256 and the runtime-bound
contract_id. All actions require known nonduplicate evidence IDs. Non-complete
actions require a concrete next check in the model instruction and nonempty text
at validation. The runtime checks shape and reference identity; the factual
support and usefulness of a reason require a separate evaluation. This review
finishes the consumer without emitting another SMS or dispatching a task.
