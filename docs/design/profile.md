# Legacy profiling detail

Binding detail of [coreq.md § Legacy profiling](../../coreq.md#legacy-profiling-profile). The rules and budgets there take precedence; this file holds the operation budgets, the measured adapter boundary and the counter, store, trace, report and incident-link formats.

## Operation budgets

coreq measures its own calls below; a consumer records its operations with the same `profile::record` and passes their budgets to `profile::report`.

| Operation | Call | p95 budget | Error budget |
|---|---|---|---|
| `forge.read` | GitHub API `GET` (incident login, search, comments) | 2 s | 5% |
| `forge.write` | GitHub API `POST` (incident issue, comment) | 5 s | 5% |
| `release.read` | `update::download` (`latest.json`, `SHA256SUMS`, binary) | 60 s | 5% |
| `token.read` | `Tool::token` and the `gh` login of `incident::github_token` | 1 s | 1% |

## Measured adapter boundary

- The adapters above use one internal measured boundary: the existing capture operation records their logical result and fixed error class before the caller handles it, while the legacy memory counters retain the same measurements.
- Native subprocess observations remain child operations; a successful process is not a successful credential or HTTP result. No token, URL, response or error text enters capture.
- `record`, `flush`, `report` and `window` remain compatibility APIs with their existing formats; capture does not automatically flush or create `profile.json`. Consumers using the shared store need no compatibility-file writer.

## Counters

- Always on, in process memory: per operation count, outcomes `ok`, `failed`, `cancelled`, `retried`, max and the last 64 durations (p50, p95), error classes.
- Classes: `rate_limit` (HTTP 429, or 403 naming a rate limit), `auth` (401, other 403), `http`, `timeout` (curl exit 28), `network`, `json`, `spawn`, `missing` (no token).
- coreq never retries; a consumer that succeeds only after a retry records `retried`, not `ok`.

## Profile store

- `profile::flush` merges memory into `<root>/.<name>/profile.json` atomically; the consumer calls it once per service pass. A failed write keeps the measurements in memory for the next flush.
- Bounds: 32 operations (least recently seen pruned), 64 durations and 8 classes (the rest `other`, also for a stored record) per operation, 256 trace spans.
- Names and classes are codes of `[a-z0-9._-]`, at most 32 characters: an operation with another name is dropped, another class becomes `other`. No secrets, content or paths.
- An unreadable store is replaced. A corrupt operation record (zero count, outcome sum that disagrees or overflows, a sample above `max` or more samples than calls) is dropped on flush; counters saturate, never wrap.

## Traces

- Env `<ENV>_PROFILE=trace` at flush writes the spans `{op, ms, outcome, class, at}` recorded since the last flush; otherwise the store holds none. The store's `trace` key reports the switch.

## Report

- `profile::report` lists every budgeted or stored operation: count, window (`since`, `at`), p50, p95, max, failure and retried rates, spread (p95/p50), classes, budget and state `ok`, `over` or `unbudgeted`.
- `slow` ranks measured operations by p95 over their budget (unbudgeted after, by p95), `failing` by failure rate, `unstable` by retried rate, then spread.
- Budget states and ranks compare unrounded values; the report rounds rates to 3 places and spread to 2 for display only.
- An operation without samples reads `unknown`, reason `no samples`; a corrupt record `unknown`, reason `corrupt`; never zero. No store: state `unknown`, reason `no samples`; unreadable store: `unknown`, reason `corrupt store`.

## Incident link

- Every incident report carries `profile`, from `profile::window` over the store as of the last flush: `window:<since>-<at>` (else `window:unknown:<reason>`), then at most 7 operations over coreq's budgets as `<op>:over`, slowest first.
- The issue body's JSON and each `confirmed` or `regression` comment (line `profile <codes>`) carry it. Codes only, so the report's 4 KiB and code checks hold.
