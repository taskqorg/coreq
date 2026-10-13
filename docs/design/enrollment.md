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

## Wire v1

Contract `coreq-enrollment/1` implements DashQ C3–C5 routes. Every machine route is `POST` with a JSON body; every body and response carries `"protocol": 1` (the major). Unknown fields are ignored, so minor additions are optional fields. Another or missing major: `400 unsupported_protocol`. Errors are `{"protocol": 1, "error": "<code>"}`. `Authorization: Bearer <credential>` authenticates every route after `/connect/token`: unknown `401 invalid_credential`, revoked `401 revoked`.

| Route | Request | Success | Errors |
|---|---|---|---|
| `/connect/device` | `product`, `installation`, `runtime` (`native`/`wsl`), `host_label`, `schemas` | `device_code`, `user_code`, `verification_uri`, `expires_in` 600, `interval` 5 | |
| browser `/connect` | approve or deny `user_code` as account/workspace | one grant; a repeated approval binds nothing new | expired, denied or unknown code: none |
| `/connect/token` | `device_code` | `credential`, `installation`, `generation`, `product`, `workspace`, `schemas`; the same until ack | `400`: `authorization_pending`, `slow_down` (interval +5 s), `access_denied`, `expired_token` (600 s), `invalid_grant` (unknown or acknowledged) |
| `/connect/ack` | (auth) | `state` `active`, `generation`; a repeat is the same; older generations of the installation become revoked | |
| `/ingest` | (auth) `records`: `schema`, `event_id`, `observed_at`, `payload` | `receipts`: `event_id`, `state` `accepted`, or `rejected` with `reason` `scope` (schema outside grant) or `replay_mismatch` | `403 not_active` before ack |
| `/connection/receipt` | (auth) `event_id` | `installation`, `generation`, `schema`, `event_id`, `state` `accepted`/`projected` | `404 unknown_event` |
| `/connection/revoke` | (auth) | `state` `revoked`, `generation`; a revoked credential gets the same reply, so offline retries end | |

Event identity is `(installation, generation, event_id)`: a replay returns the stored receipt; changed content is `rejected` and not stored. A new generation needs a new browser approval and starts a fresh event space, so old retries cannot enter it. The client reports `Connected` only when `enroll::verify` matches a `projected` receipt to its installation, generation, schema and event; `enroll::check` refuses any response major but 1.

## Conformance traces

`tests/enrollment/*.json` are executable traces `{contract, product, about, steps}`. A step is a machine call `{at, route, auth?, request, status, response, verify?, client?}`, a browser action `{at, browser: approve|deny, user_code, account, workspace, result}` or `{server: "project"}`. `at` is seconds since the trace start. A `"$name"` string binds the actual value on first use and must match it later; a new binding never repeats another's value, so new codes and credentials are distinct. Responses match exactly. On a receipt step, `client` is the `enroll::verify` outcome against `verify`. `tests/enrollment.rs` runs every trace on a reference server and through the same client functions; DashQ runs them against its handlers. AgentQ and ServerQ traces differ only in product and schema.

## Client

`enroll::pair(dir, destination, request, wire)` connects one installation; `dir` is the consumer's connection folder. `Wire` carries HTTPS `POST`s (`enroll::Https` through the OS `curl`: URL, credential and body on stdin, never in argv; a control character in them is refused), the clock, waits and showing the code. Browser mode opens the `https://` verification URI (no whitespace or control character; user code `[A-Za-z0-9-]`, at most 32); headless only prints it with the code for approval on another device. No secret is built in; errors and output name codes, never a credential or device code.

- **State.** `<dir>/connection.json`: a random `installation` made once and kept across destinations, `pending` (destination, product, schemas, device code, user code, URI, `expires_at`, `interval`, `staged` token response), `active` (token response plus destination) and `revoke` (destination, credential). Every change rewrites the whole file atomically (temp file, fsync, rename), so a crash leaves the old or the new state, never a mix; an unreadable file is an error and is never overwritten.
- **Protection.** Unix (macOS, Linux, WSL): folder 0700, file 0600, both checked after writing; Windows: `icacls` removes inheritance and grants only `whoami` full control on the folder. A mode that does not hold (WSL `/mnt/c` without metadata) or a failed `icacls` refuses to store, so nothing activates. `ponytail:` protected files, not the OS keychain; add Keychain/libsecret/DPAPI when a product needs more than per-user file access.
- **Pairing.** Same destination, product and schemas as `active`: return it, no request. Otherwise resume `pending` for the same request, else drop it and call `/connect/device`, storing `pending` before the code is shown. Poll `/connect/token` every `interval`; `slow_down` adds 5 s; `access_denied`, `expired_token`, `invalid_grant` or local expiry drop `pending` and return the code. A token response is stored as `staged` before `/connect/ack`; a lost token or ack reply is recovered by running `pair` again (the server repeats both). Ack `active` with the staged generation: one write makes it `active`, clears `pending` and queues the old `active` for revoke. Lost replies return an error and keep the state for the next run.
- **Destination switch.** The old `active` stays until the new one is acknowledged; then it moves to `revoke` in the same write. A `pending` for another request moves its staged credential, if any, to `revoke`.
- **Cancel, disconnect, revoke.** `cancel` drops `pending` (a staged credential goes to `revoke`): no active connection. `disconnect` moves `active` to `revoke` at once, so sending stops offline. `retry_revokes` posts `/connection/revoke` for each record; `200` or `401` ends it, anything else keeps it for the next service pass.
