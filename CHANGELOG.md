# Changelog

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
