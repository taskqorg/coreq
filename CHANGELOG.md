# Changelog

## Unreleased
- Telemetry (#100): TaskQ field names and JSON types come from the vendored TaskQ v4.6.53 release fixture `taskq-4.6.53-telemetry-fields-fixtures.json` instead of a hand-kept list; a newer TaskQ field reaches consumers by vendoring the next fixture, no code edit.
- Profiling (taskqorg/taskq#2011): `profile::pct`, `profile::summarize` and `profile::rank` let a consumer that keeps its own samples rank them by the largest of p95/budget, failure rate/budget and cv, worst first, instead of its own ranking code.
- Telemetry (#95): an IPC client waits 5 s, not 2 s, for the writer's commit receipt; a receipt that misses the deadline retries the same ids at the same writer instead of re-electing, so a slow bounded commit no longer loses batches.
- Telemetry (#92): `telemetry::Reader::storage` reports `database_bytes`, `wal_bytes`, `wal_live_bytes`, `shm_bytes` and `total_bytes` from a read-only snapshot, without the writer lock or a checkpoint, so a reader process can show live WAL growth while a writer runs.

## v0.13.0
- Host topology (#50): `service::host_unit` writes and loads one install-root unit (`com.<name>.service`, `<title>`, `<name>.service`); `service::legacy_unit` removes an idle per-project unit with native and file readback. Existing `unit` generators remain available. The consumer must drain before removal and verify host ownership before recording `launcher::set_rollback_floor`; host starts count separately and automatic rollback never selects a version below that floor. No live consumer migration is included.

## v0.12.0
- Profiling (#45): `profile::record` keeps always-on counters per operation (count, outcomes, p50/p95/max, error classes); coreq measures `forge.read`, `forge.write`, `release.read` and `token.read`, with `rate_limit` as its own class. `profile::flush` merges them into a bounded `<root>/.<name>/profile.json` of codes only; `<ENV>_PROFILE=trace` adds spans. `profile::report` ranks slow (p95 over budget), failing and unstable operations; no samples or a corrupt record read `unknown`. Incident reports gain the key `profile`: the measured window and operations over budget, as codes.

## v0.11.0
- Operation authority (#42): each pulled operation carries server-set `authority` `{actor, workspace, profiles}`, never taken from the payload and outside `digest`, so v0.10.0 results keep their identity. `pull` rejects locally, never for execution, an operation with missing or malformed authority (`authority`) or another workspace than the grant's (`workspace`). `admit(op, actor, workspace, profiles)` checks the consumer's payload claims: `wrong_actor`, `wrong_workspace`, `profile_out_of_scope`. The reference server queues only for an actor authorized in the grant's workspace; trace `agentq-operations.json` covers it.

## v0.10.0
- Operations (#40): `enroll::Request` gains `capabilities: &[&str]` (callers add `capabilities: &[]`); a requested capability such as `agentq.configure` is sent on `/connect/device`, needs its own approval and is kept only when granted. `pull` fetches pending `/operations` of the active grant once per service pass and rejects locally, never for execution, those of another capability, installation or generation, expired, over 16 KiB or with a wrong `digest`. `post_result` posts `{operation, digest, result}` to `/operations/result`; a repeat returns the same receipt. Conformance trace `agentq-operations.json`.

## v0.9.0
- `enroll::Request` gains `installation: Option<&str>`: a caller id (1-64 of `[A-Za-z0-9-]`, e.g. TaskQ's 64-hex id) is sent on `/connect/device` and must be echoed unchanged; a changed id re-pairs and revokes the old grant. `None` keeps the stored random UUID. Callers add `installation: None` (#38).

## v0.8.0
- `enroll::Request` gains optional `scope` `(name, value)`, e.g. `taskq-board/1` and the board repository; `pair` sends it on `/connect/device` for the approval page and re-pairs when it changes. `None` keeps v0.7.0 behavior (#36).

## v0.7.0
- Product connections (`enroll`): wire v1 contract `coreq-enrollment/1` with AgentQ and ServerQ conformance traces (#27); pairing client with protected, crash-safe `connection.json` (#28).
- Delivery (#29): `stamp` binds a record to its grant and event ID; `deliver` sends at most 100 records or 1 MiB, retries with jittered 1-60 s backoff, honors `429 Retry-After` and returns `accepted`/`rejected` receipts; `receipt` tells durable acceptance from projection. HTTPS requests time out after 10 s.
- Disconnect keeps at most 16 revoke-only records; `401` ends the active connection; records of an old grant are never sent after reenrollment.
- No consumer uses the connection client yet; consumers pin `v0.7.0` only in their own release tasks.

## v0.6.0
- Update prunes `releases/<ver>/` folders that are neither `current` nor `previous`, before and after installing; a release whose exe is in use on Windows is reported on stderr and kept (#20).

## v0.5.0
- Incident reports: an atomic claim `<install>/incidents/<fingerprint>` lets racing processes of one install open one issue per fingerprint; a sibling comments or skips (#14).
- Launcher counts unhealthy `service run` starts per project in `releases/<ver>/starts/<project>`; `healthy` stays install-wide. taskq 4.1.9 must bump its coreq pin from `v0.4.0` to `v0.5.0`.
