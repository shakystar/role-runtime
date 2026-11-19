# Local workflow control and lifecycle records

2026-11-19. Implemented as opt-in cooperative-runtime components, with a two-task
**fixture** host. No model, network research or production-autonomy claim is made.
The existing sequential JSONL bridge remains unchanged.

## Ownership and API

`internal/control.Server` runs alongside workers using the same `engine.Store` owner.
Only the owner opens runtime state. A second control lock prevents duplicate listeners.
The socket is fixed under `<state>/control/runtime.sock`, mode 0600, in the private
owned directory. It never removes a socket in an unrelated state directory.

Requests carry version=1, request_id, run_id, method and params. Duplicate/unknown or
incorrectly cased object fields are rejected. Methods:

- health.get: lifecycle capacity, projection health and historical completeness.
- receipts.get: scoped cooperative receipt identity, status, revision and artifact check.
- notifications.list: immutable messages projected from the existing mailbox; opaque
  persistent ledger-generation cursors; explicit consumer/address binding.
- notifications.ack: durable consumer+delivery checkpoint acknowledgment, idempotent.
- events.list: durable lifecycle event pages filtered by configured project.
- tasks.submit: narrowly scoped, immutable cooperative submission for a registered
  consumer address. This addition lets a resumed graph submit its downstream task
  through the owner instead of reopening storage. An existing failed task is never
  requeued. Completed duplicate submission records a cache delivery, not another attempt.

Limits: four concurrent connections, 5-second connection deadline, 64 KiB request,
256 KiB response and 1..100 rows per page. An idle fifth connection receives busy.
Notifications are projected from retained immutable mailbox records, so a crash before
projection is repaired by scanning those records. Opaque cursors refer to projection
order, not causality. A page may contain zero matching items while advancing its cursor.
Consumers must continue until the cursor stops advancing, not infer EOF from a filtered
empty page. No automatic retention/deletion is implemented.

Artifact references contain producer_task_id and artifact {id,version,sha256}. Receipt
text and SMS body use the same reference. Checks use Go OpenRoot rooted at state/artifacts,
reject traversal/symlink escape and read at most 1 MiB. Hash equality proves byte identity,
not content quality. The Python workflow verifies the producer's role/project/version
and terminal receipt as well as the expected correlation and reference.

## Durable events and capacity

EnableLifecycle(runID,limit) enables cooperative-record schema_version=1, per-task
revision and bounded event_intents. Every lifecycle transition atomically persists its
state plus event intents; projection into numbered immutable ledger entries preserves
stable event IDs. Pending intents are cleared only after durable projection. A projection
failure after completion does not change task success. New transitions retry pending
intents; events.list reconciles them. Legacy history is explicitly incomplete and that
marker persists across restarts. Normal Store/JSONL receipts are not retroactively
converted into an invented event history.

64 MiB default/max ledger, 16 KiB maximum entry; each nonterminal admitted task reserves
64 KiB for bounded terminal events. Pending completed intents retain their byte reservation
until projection. Admission stops at capacity without deleting source records. This is
logical ledger reservation, not a guarantee against filesystem exhaustion; I/O errors
remain visible. Notification projection has a separate bounded 64 MiB ledger. Acks are
bounded by the projected delivery identities and are never silently dropped.

Events: task.accepted/waiting/ready, attempt.started/finished, startup.retry and
result.cache_hit. Attempt identity derives from run/task/cooperative generation.
An executor invocation is not proof of model dispatch. Fixture terminal execution
certainty is unknown. RecordStartupRetry is a fenced hook for a backend with a known
admitted attempt; startup before task admission is not falsely attributed. The fixture
host does not contact Codex or generate preflight retries. RecordCacheHit deduplicates
by delivery identity. Token/cost records are intentionally not implemented yet.

Queue/suspended segment and execution durations use process-local monotonic clocks.
Reopened intervals are unknown, never computed by subtracting persisted wall times.
Python graph dependency wait is a separate, currently unmeasured duration.

## Run and verify

```sh
./scripts/go.sh build -o bin/runtime-workflow ./cmd/runtime-workflow
./bin/runtime-workflow --state /tmp/role-flow-demo --run-id demo --fixture-delay 2s
```

This foreground fixture host serves the API while the bounded CooperativePool runs.
The producer writes a synthetic artifact and sends SMS. A Python workflow submits the
consumer after verifying the dependency. SIGINT/SIGTERM stops the host; uncertain
in-flight work is fenced rather than replayed. Use a fresh state for a fresh experiment;
do not delete failed receipts to force retry. Keep state paths short for Unix socket limits.

`--fixture-mode` includes early/wrong/missing/corrupt/failed fault cases. They are test
controls, not business modes. The reusable control/lifecycle packages can be hosted by
other cooperative entry points; this CLI intentionally supports fixtures only.

Tests cover projection failure/reopen, duplicate projection, completion capacity,
auxiliary-event deduplication, malformed control requests, address/run restrictions,
connection limits and confined artifact access. Python's evaluation runner supplies
cross-process graph/inbox/ack crash tests and 19 acceptance cases. See the paired
role-orchestrator `docs/workflow-implementation.md` for results and commands.
