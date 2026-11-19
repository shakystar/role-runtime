# Synthetic SMS/LangGraph feasibility fixture

Build with `./scripts/go.sh build -o bin/workflow-probe ./experiments/workflowprobe`.
The Python regression suite in role-orchestrator/tests/test_workflow_probe.py invokes
this fixture on temporary state. It exercises the real cooperative store, mailbox,
leases and receipts without a model. Actions intentionally include early/wrong SMS
and producer fencing for fault injection; this is not a service API or general executor.

`observe` reconciles mailbox/outbox state and checks the producer completion receipt,
exact inbox identity and SHA-256 of the fixed synthetic artifact. It is not a read-only
storage operation. `consume` is idempotent for an already completed fixture consumer.
Each invocation owns and closes the Go store; simultaneous production access would
need an API in the existing owner process rather than independent store opens.

The fixture does not implement performance, token, cost, or general event aggregation.
See role-orchestrator/docs/workflow-feasibility.md for measured checks and next steps.
