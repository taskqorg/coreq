# Shared connection client

Accepted implementation target under [coreq.md](../../coreq.md). Apply [shared rules](https://github.com/taskqorg/specq/blob/main/engineering/product-connections.md); [DashQ C1–C7](https://github.com/taskqorg/dashq/blob/main/docs/design/enrollment.md) owns wire routes, defaults and scenario evidence.

## Boundary

A linked module owns device pairing/polling, protected credentials, stable installation/grant generation, atomic connection activation, HTTPS authorization, retry timing, receipts and revocation recovery. Use existing curl transport without exposing secrets in argv. No daemon, generic connector framework, product state machine, source catalog or second telemetry queue.

Consumers provide their identity, product/schema scope, payload and existing service wake/storage integration. AgentQ retains its durable sender queue; ServerQ/TaskQ retain only the latest unsent snapshot. Installations do not inherit each other's grants. Unknown protocol majors fail explicitly; optional fields within a compatible schema survive.

## Implementation obligations

- Publish a versioned wire schema and executable client/server conformance traces before dependent integrations. Cover pending/slow-down/expiry, lost token response/ack, duplicate approval, crash recovery, destination switch, accepted/projected receipts, revoke/reconnect and mismatched replay payloads.
- Never count HTTP success as connection verification. Compare the returned projection receipt with the actual installation, generation, schema and snapshot ID.
- Keep credentials outside telemetry pruning. Secret storage is protected on every supported OS; fallback files need restrictive Unix permissions or Windows ACLs. Failure to protect storage refuses activation.
- Store only bounded local connection/revoke state, not task state. Offline disconnect stops sending immediately; only a revoke-only record can retry. Old-generation retries cannot enter a new workspace.
- Fixtures for AgentQ and an independently installed ServerQ or TaskQ must exercise the same client without product branches. OS storage/service evidence is required separately from protocol fixtures.
- The ingestion credential lifetime is the explicit accepted DashQ C3 exception; it does not apply to agent/forge/admin credentials.

Ship a tagged coreq release only after its platform checks and conformance traces pass. Consumers pin that release; no moving-branch dependency. Capability stays inactive until each product's complete release gate passes.
