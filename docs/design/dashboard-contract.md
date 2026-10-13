# Proposal: shared dashboard source and action contract

Status: proposal for owner review ([coreq#22](https://github.com/taskqorg/coreq/issues/22)). It does not amend `coreq.md`, specq rules, dashq D1–D7 or any producer contract, and it authorizes no implementation, deployment or auth change.

Consumers: [serverq#21](https://github.com/taskqorg/serverq/issues/21) (findings) and [dashq#24](https://github.com/taskqorg/dashq/issues/24) (adapters and answers). Transport inputs: [dashq#4](https://github.com/taskqorg/dashq/issues/4), [agentq#60](https://github.com/taskqorg/agentq/issues/60).

**Recommendation in one line:** no new shared code now. Each consumer wraps each producer's existing payload in one adapter-side *source frame* (below); dashq stays pull-only and links out for answers; coreq gains at most one additive common key (`install`) once two producers need it.

## 1. Inventory at immutable revisions (A1)

| Repo | Revision |
|---|---|
| coreq | [`2a74456`](https://github.com/taskqorg/coreq/tree/2a74456395548d967109f7d4c118f25faef633d2) (taskq pins tag `v0.5.0`; agentq, serverq, dashq pin `v0.6.0`) |
| taskq | [`4bda2d7`](https://github.com/taskqorg/taskq/tree/4bda2d790e449831c672f8c91561b416ab4ef370) |
| agentq | [`938b628`](https://github.com/taskqorg/agentq/tree/938b6288cbb0446b28e9caf7898e336047082369) |
| serverq | [`444e494`](https://github.com/taskqorg/serverq/tree/444e4944590f4bce96bfeba5893af6828a39a3c1) |
| dashq | [`cc2b41f`](https://github.com/taskqorg/dashq/tree/cc2b41f6d56d9c79401a3d1438062f4c8b10377a) |
| specq | [`f8f75c7`](https://github.com/taskqorg/specq/tree/f8f75c761b1279b6ac6c8a46d77dc9fc3e76c027) |

### coreq `status.json` common keys
`src/status.rs` `common`: `host`, `version`, `pid`, `at` (unix s), `project`, `error`, `update`. `read(max_age)` returns unknown when unreadable or stale. No install identity, schema, capabilities or remote delivery. The `host` value is a local hostname: not identity proof.

### taskq snapshot (`status --json`, taskq#886) and receipt (taskq#887)
- Snapshot (`src/snapshot.rs`, fixture `tests/fixtures/status-snapshot.json`): `schema` 1, `source` `board|host`, `observed` (board time), `complete`, `reason`, `board {kind, host, repo, url}`, `totals` (null when incomplete), `tasks[]`, `hosts[] {name, version, updated}`, `capabilities`.
- Task row: `n, title, state, labels, url, revision, session, attempt, role, deps, card, failed`. `revision` is the expected `--revision` of the next write.
- `card` is one string. Options are lines `K. text`, `★` marks the recommendation; only `core::options` parses them. Free text is `answer N --text`.
- `<root>/.taskq/status.json` adds the snapshot keys (no `board`, `hosts`, `capabilities`) plus `focus`, `kids`, `capacity`, `agents`, `incident`, `events_since`, `request`.
- Receipt (`src/dispatch.rs`): `{task, op_id, outcome, state, revision, reason, next}`; outcome `accepted|rejected|stale|unknown`. A repeated `--op-id` returns the first receipt and writes nothing; `unknown` reconciles with `taskq inbox N` and the same op id.

### agentq telemetry
- OTLP HTTP JSON to Langfuse `/api/public/otel/v1/traces` (`agentq.md` § 6, `telemetry.rs`). Resource: only `service.name=agentq`; identity sits in span attributes: `agentq.schema` 1, `agentq.event_id`, `agentq.installation` (random, `data/installation`), `host.name`, `agentq.runtime`, `agentq.version`, `agentq.ids_unknown`.
- Events: `agentq.installation` (hourly heartbeat, queue counters), `agentq.run`, `agentq.retrieval`, `agentq.incident`, `agentq.resource` (300 s buckets, filesystem id = keyed HMAC, `*_unknown` reasons), `agentq.verify`.
- `status.json` (90 s freshness): common keys, `queue`, `agents`. Spec also promises `mcp`, `hooks`, engine versions, newest incident: not written (gap).
- Team mode (device token, forward through dashq) is not implemented (agentq#60). No inbound command channel.

### serverq report
- `report.json` schema 1: `at` (float s), `revision` (rules.json hash + version), `filesystem`, `results {rule_id: {status ok|warn|unknown, detail, probe fields}}`, `incidents {rule_id: {first_seen, last_seen, occurrences, state observed|verified, verified_at}}`, `manual_rules[]`.
- Missing for display: host/install identity, per-result time (except cached `checked_at`), recommendation text (only in `rules.json` `instruction`), severity beyond status.
- `status.json`: common keys plus `check {at, exit, counts, report}`, `update_at`, `paused`. `check.report` is an absolute path: not exportable.
- Windows: every automated rule is `unknown`, `reason unsupported-os`; macOS: systemd and Docker rules likewise.

### dashq today
Pull only (D1): Langfuse observations (agentq), GitHub issues and `taskq host` heartbeats (taskq), local `report.json` (serverq, one configured host). Source states `ok|incomplete|stale|unconfirmed|unknown` with reason (D6). Host key `<host>/<runtime>`; taskq heartbeats join by bare host name; serverq joins by a configured host id. No answer actions.

## 2. Identity and source frame (A2)

### Identity
| Level | Key | Source today | Gap |
|---|---|---|---|
| Service | `Tool.name` (`taskq`, `agentq`, `serverq`) | all | none |
| Installation | random id per `<install>` | agentq `agentq.installation` | taskq, serverq: none |
| Board (taskq) | `board.url` | snapshot | none: board is global identity, not a host |
| Resource | `<service>:<installation>:<kind>:<id>` | agentq filesystem HMAC id; serverq `rule_id`; taskq `board.url` + `n` | serverq resource has no installation |
| Host | owner-verified mapping `installation → host label` | none | hostname match is display only, never a join |

Rules: identity is opaque; no paths, usernames or devices. Windows and WSL on one machine are two installations. A join across services needs a verified mapping; without one, rows stay separate (no double counting).

### Source frame
The consumer adapter builds one frame per read; producers keep their payloads unchanged:

```json
{"service": "serverq", "installation": "7f3c…", "schema": "serverq-report/1",
 "observed": 1760400000, "collected": 1760400042, "fresh_s": 7200,
 "state": "ok", "complete": true, "reason": null,
 "capabilities": ["findings", "incidents"],
 "unknown": [{"field": "results.failed-services", "reason": "unsupported-os"}],
 "payload": {"…": "producer document, verbatim"}}
```

- `observed` is the producer's time; `collected` is the consumer's. Freshness uses `observed` against `fresh_s`, which the producer contract owns (taskq 60 s status.json / board `ttl`, agentq 90 s, serverq 2 × `check_every`).
- `state` reuses dashq D6: `ok|incomplete|stale|unconfirmed|unknown`, plus `unsupported` for an unknown schema major. Only `ok` renders healthy.
- `unknown[]` keeps every producer unknown reason (`ids_unknown`, `*_unknown`, `unsupported-os`, `complete false`) instead of zero.
- `capabilities` drives views; a missing capability hides a view, never shows empty.

## 3. Observations, recommendations, questions (A3)

| Kind | Meaning | taskq | agentq | serverq |
|---|---|---|---|---|
| Observation | measured state with time | task rows, totals, heartbeats | samples, heartbeats, incidents | `results`, `incidents` |
| Recommendation | source advice, no effect | card `★`, PM `Next:` star | none | `rules.json` `instruction`, manual rules |
| Question | owner decision with choices | card in `ask` / `review` | none | none |

Question shape the adapter exposes, preserving the source card:

```json
{"id": "https://github.com/o/r/issues/2", "revision": 4,
 "text": "Done: read\nProblem: Which way?",
 "choices": [{"k": 1, "text": "left"}, {"k": 2, "text": "right"}],
 "recommended": 2, "free_text": true}
```

Choice numbers, order and text are the source's; the consumer never renumbers, reorders or rewords. Free text stays free text (`answer N --text`), never mapped to a number. A serverq warning or manual rule is a recommendation, never a question and never repair approval. Historical incidents stay apart from current warnings and unknown probes.

## 4. Authority per operation (A4)

| Operation | Authority | Expected revision | Operation id | Receipt | Reconcile |
|---|---|---|---|---|---|
| taskq `answer`, `requeue`, `later` | board via the `taskq` CLI with the board token | task `revision` from the snapshot | `<consumer>.<request>.<k>` | taskq receipt JSON | `unknown`: `taskq inbox N`, retry same op id; `stale`: re-read, re-show card |
| taskq `close`, `failed`, config | owner through PM or tray confirm | same | same | same | same |
| serverq remediation | none in serverq (read-only); separate authorized task | — | — | — | — |
| agentq config, enroll | local CLI on that installation only | — | — | readback by `agentq` | local |
| dashq | none: read-only (D1) | — | — | — | — |

- Telemetry, `status.json` and push tokens are read credentials. A device or host token never authorizes an answer, a repair or a config write.
- An `accepted` receipt proves the board entry, not that a worker resumed or a repair worked; those show up only as later observations.

## 5. Where rules and code go (A5)

### specq proposal (draft, to file after owner approval)
Problem: dashboards re-derive identity, freshness and choices per product. Rule for `design/dashboards.md`:
- **W16.** Every source read carries service, installation, schema, observed time, freshness limit, completeness, capabilities and unknown reasons; unknown or unsupported never renders healthy.
- **W17.** Join entities only by a verified mapping; matching names are display only.
- **W18.** Show source questions with their choice ids, order, recommendation and free-text option unchanged; actions go to the owning product with expected revision and operation id, and the UI shows its receipt.
- **W19.** Read and telemetry credentials never grant write authority.

Consumers: dashq, taskq tray. Verification: dashq fixtures for outage, stale, mixed schema; taskq tray already meets W16 for its own source.

### coreq Rust primitives
Shared code only where two consumers already map to it:

| Candidate | Consumer 1 | Consumer 2 | Verdict |
|---|---|---|---|
| Fresh-or-unknown read | taskq tray reads `status.json` via `status::read` | dashq D6 source state from its own cache | exists (`status::read`); no change |
| Installation id file + common key `install` | taskq status.json (needed for host join, has none) | serverq status.json/report (needed by serverq#21 A4, has none) | propose: `status::common` adds `install`, read from `<install>/installation`; agentq passes its existing id |
| Frame struct | dashq adapters | none outside dashq | no: lives in dashq |
| Remote report call (dashq#4 A1) | dashq ingest | no approved producer use (D1) | no: defer (§ 6) |
| taskq queue state machine | — | — | no: stays in taskq (scope) |

## 6. Transport options (A6)

| Option | Path | Size | Risk |
|---|---|---|---|
| P1 Pull (today) | dashq reads GitHub, Langfuse, local `report.json` | none | serverq only on the dashq host |
| P2 dashq#4 push | coreq report call, per-host token, dashq stores last status | new coreq HTTP call, enrollment, store; amends D1 | raw `status.json` leaks paths (`check.report`); needs a reviewed projection |
| P3 agentq#60 forward | device token, OTLP to dashq, dashq forwards to Langfuse | dashq endpoint, enrollment, revocation; amends D1 | agentq only |

Smallest: **P1 now**. When a second serverq host is real, use **one** enrollment for P2 and P3: one device token kind, one revocation list, one dashq ingest that accepts OTLP and reviewed frames. Never two channels.

Compatibility: frames are adapter-side, so producers change nothing. A new common key is additive (N/N-1 readers ignore it). Producers bump schema major only for breaking changes; a consumer seeing an unknown major shows `unsupported` with the last good frame, never a failure of the whole view.

## 7. Independence (A7)

- Each producer runs standalone: taskq (board + local service), agentq (standalone Langfuse mode), serverq (local `check`, `report.json`). None calls dashq; only agentq#60 team mode would, and it is opt-in.
- coreq is a crate pinned by tag at build time: no daemon, port or central service. The proposed `install` key is a local file.
- Removing dashq changes no producer state, queue or exit code. Disconnecting a source in dashq stops nothing on the host.

## 8. Cases: code vs adapter mapping (A8)

| Case | Behavior | Needs code | Adapter mapping only |
|---|---|---|---|
| One source | Its views from its capabilities; other services `not configured` | none | capability → view |
| Combined | Rows joined only by verified `installation → host`; else separate | install id (coreq key) + mapping config (dashq) | rows by `installation` |
| Outage | Last frame kept with age, `stale`/`unknown` + reason; totals state coverage | none (D6 exists) | producer error → `reason` |
| Mixed versions | Old producer lacks `install` or new keys: `unknown` field, view stays; unknown schema major: `unsupported` | none | key presence → `unknown[]` |
| Concurrent answers | Second answer gets `stale` receipt, card re-shown | none (taskq#887) | receipt → UI state |

Common behavior needing code: only the install id. Everything else is adapter mapping in each consumer.

### Consumer mappings
- **dashq#24:** taskq snapshot → frame (`state` from `complete`/`observed`), card → question shape; agentq Langfuse events → frames per `agentq.installation`; serverq `report.json` → frames, findings keyed `serverq:<installation>:rule:<rule_id>` with `rules.json` instruction as recommendation.
- **serverq#21:** add `install` and per-result `observed` to the report; keep `rules.json` the only threshold/recommendation catalog; drop the absolute `check.report` path from any export.

## 9. Owner decisions

1. coreq common key `install` from `<install>/installation` (agentq id reused): yes ★ / no.
2. taskq snapshot exposes parsed `choices` and `recommended` beside `card`, so consumers stop re-parsing: yes ★ / adapter parses.
3. Transport: P1 pull now, one enrollment for P2+P3 later ★ / build P2 now / build P3 now.
4. dashq answers: link out to the board until D1 is amended ★ / server-side `taskq answer` with a write token now.
5. File the W16–W19 specq proposal: yes ★ / keep in dashq `integration.md`.
