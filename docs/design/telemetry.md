# Universal telemetry detail

Binding detail of [coreq.md § Universal telemetry](../../coreq.md#universal-telemetry-telemetry). The rules there take precedence; this file holds the limits, formats and procedures that implement them.

## Shared implementation

- One shared implementation serves all consumers, local first and always available without a connection.
- Consumer execution adapters supply identities and observations; no product forks the storage or synchronization bookkeeping.
- The target coverage is the complete [shared contract](https://github.com/taskqorg/specq/blob/main/engineering/profiling.md#coverage-by-construction), including native/WebView input, windows, resizing, rendering, commands, agents, tools and context/compression. A storage API alone does not qualify that coverage.

## Store and schema

- Store: a consumer-selected stable path below its installation, SQLite WAL, `synchronous=FULL`, a prepared writer with a 1 s busy timeout.
- Initialization is serialized by `<database>.init.lock` with a 1 s deadline.
- Every store mutation and initialization takes the owned OS lock `<database>.write.lock` before SQLite. It queues cooperating connections/processes outside SQLite rather than starving another writer at `BEGIN IMMEDIATE`.
- External SQLite contention still obeys the 1 s busy timeout; async qualification flush keeps its 3 s deadline.
- One writer connection per `Store`, shared by callers; the process reuses that store. Group writes commit in one transaction and return ids only after commit.
- Unacknowledged writes may be lost on crash; acknowledged writes survive process restart.
- Corrupt or newer-schema stores are preserved and refused, never reset. Unknown schemas remain refused.
- SQL schema version is 6. Schema 5 upgrades atomically by backfilling a validated kind column and its kind/sequence index; payloads are not rewritten.
- Schema 4 upgrades without payload rewriting; schema 5 is the reader floor for partitioned count summaries.
- Schema 3 upgrades without payload rewrites; the new reader floor admits semantic metadata and explicitly unknown historical identities.
- Schema 2 upgrades transactionally without rewriting existing compressed payloads; older readers must refuse the new version.
- A completely empty, unowned schema-0 file is initialized in place after an interrupted bootstrap; schema-0 files with any object or application id are refused.
- Schema-1 upgrade is one FULL transaction: validate the original observations, repack rows with block/slot references in the same table, temporarily muting only this connection's triggers so retention counters and user callbacks do not observe deletion.
- The schema-1 upgrade preserves ids, sequences, source/receipt times, retention metadata, sync/import cursors and the events AUTOINCREMENT high-water mark.
- Pre-commit failure rolls back schema and rows; a post-commit checkpoint error preserves the committed schema-6 data and refuses this open.

## Shared physical budget

- `telemetry::Budget` names an existing installation lock, explicit peer files, a total byte ceiling and reserved text/metadata space.
- `Store::open_bounded` and `Runtime::install_bounded` reuse the normal store/writer. Every initialization and mutation holds that lock and measures peer file lengths.
- The budget fixes the shares: `total/2` is the working share and the other half is a reserve that steady retention never uses.
- Inside the working share, `reserved` holds legacy text, IPC metadata and peer stores; the rest splits 6:1 into the database cap (at most 384 MiB) and the WAL+SHM share.
- Consumers pass `telemetry::INSTALLATION_BYTES` (1 GiB) and `telemetry::PEER_BYTES` (64 MiB): database 384 MiB, WAL+SHM 64 MiB, peers 64 MiB, reserve 512 MiB. Peer growth never shrinks the database cap.
- A share over its limit (`peer-share`, `database-over-cap`, `wal-over-share`, `reserve-used`) or a capacity prune by this store (`capacity-pruned`, kept for the store's lifetime) is a degraded diagnostic in `storage()` plus one stderr line per process, never an error to callers.
- Missing peers count as zero; inaccessible, non-file or symlink peers refuse.
- A bounded store over its cap (a smaller budget, or one over the old 64 MiB budget) opens, prunes oldest whole observations to 60% of the cap and stays `degraded` until `reclaim` shrinks the file; no manual deletion.
- A changed cap checkpoints before reconfiguration.
- Bounded connections disable SQLite's implicit last-close checkpoint; explicit checkpoints run under the budget lock and committed WAL survives close/reopen.
- Peer writers must use the same lock and calculation; arbitrary external file writers are outside this contract. The lock has a 1 s admission deadline.
- The capture writer retries a batch that failed on contention (`budget-locked`, `sqlite-busy`, `transport-unavailable`) with the same event ids every 50 ms for up to 10 s. The bounded queue absorbs new arrivals meanwhile; only a batch still failing after that window, or any other failure, counts as lost.
- IPC binds the budget descriptor: a bounded client refuses a writer with a different or absent budget. Unbounded consumer APIs retain their existing limits.

## Compact payloads

- Portable zlib-rs level 1 blocks hold up to 1024 complete JSON payloads and 1 MiB decoded framing.
- The frame carries a version, count, decoded size and checksum-protected stream; decoding rejects unsupported, oversized, truncated, corrupt, non-UTF8 or trailing data.
- Events retain indexed identity/time fields and reference a block/slot; pruning the last reference releases the block. Eight decoded blocks at most are cached per read operation.
- Exported observations, order and receipts are unchanged.
- Already open schema-1 writers may still insert raw payloads; schema-6 readers validate/read these rows too. Old readers cannot interpret compressed rows and must not be used after cutover.
- A later open atomically compresses remaining raw rows from an older writer too. No source store or historical row is silently discarded.
- Capacity reservation covers incoming payload bytes plus 512 bytes per new indexed row and 32 pages of block/index slack; SQLite's physical cap and transactional rollback remain authoritative.

## Observations

- Observation: immutable random id (historical import derives it from a stable namespace and position); product/version, operation/kind, trace/span/parent identities, source UTC start and monotonic duration, outcome/error codes.
- Optional fields: project/host/runtime/task/attempt and model, context/token/compression measurements.
- Fixed codes and finite numbers only; no task text, arguments, prompts, output, keys or paths. Unknown values are null with an explicit reason.

## Retention and checkpoints

- Bounds: newest observations within 30 days and the physical database cap, whichever is smaller. The same writer prunes oldest whole observations transactionally; a single observation is at most 16 KiB.
- Database pages are reused; WAL checkpoints run after pruning.
- Unbounded `Store::open` keeps its 28 MiB database cap, 29 MiB WAL cap and 1 MiB SHM allowance; bounded stores use the budget shares above. SQLite pages must be at least 1024 bytes.
- Each cooperating mutation holds the shared writer lock and disables cache spill/automatic checkpointing.
- Append first reads SQLite NOOP checkpoint frame counts. It skips RESTART only when the current frames plus a worst-case transaction, 24-byte frame headers, WAL header and up to one 64 KiB sector of repeated commit frames fit the WAL cap.
- The worst case is a full database for unbounded stores; for bounded stores it is the actual transaction: its payload bytes plus 512 bytes per row and 32 pages.
- A bounded append also checkpoints once more than 4 MiB of WAL frames (or the WAL share, if smaller) are live, and truncates when the WAL file is over that 4 MiB. One checkpoint copies at most about 4 MiB under the budget lock, and the file does not stay at its high-water mark.
- A transaction alone larger than 4 MiB still commits into an empty WAL.
- Otherwise it checkpoints with RESTART before beginning; a bounded WAL already over its share truncates instead.
- An append that leaves WAL+SHM over the share truncates after its commit; a busy truncate leaves it degraded and never fails the committed append.
- An unbounded append's checkpoint waits for readers up to the busy timeout.
- A bounded append's checkpoint never waits for a reader. One that a pinned reader leaves unfinished is counted in `storage()` `unfinished_checkpoints` and retried by the first append after 1 s.
- Meanwhile bounded appends commit into the reserve while peers, database, WAL, SHM and one maximum append group fit the installation total; past that total the append refuses as `sqlite-busy`.
- Other mutations retain that checkpoint and open/explicit prune truncate.
- Checkpoint, migration, import, rebuild and large capacity prunes may exceed the WAL share transiently inside the reserve; the database cap bounds that transaction.
- The maximum transaction includes sector padding rounded up to a complete frame. This reuses bounded allocation without forcing database synchronization for every small append.
- A pinned reader may coexist with appends inside that allowance. When a checkpoint is required, a pinned reader refuses an unbounded append and every other mutation without advancing data or cursors.
- Capacity pruning and the new batch commit atomically. When used pages plus the batch reach 80% of the cap, oldest whole observations are removed until 60% remain; `poll` aggregates and `poll.change` records go last.
- Larger preexisting unbounded databases are refused and preserved, never reset.
- `storage()` reports file allocation, limits, retained payload and pruning; `wal_live_bytes` counts WAL frames not yet checkpointed, separate from the `wal_bytes` file length.
- Export states retained sequence/time bounds and pruned count; pruning is not evidence that an operation never occurred.

## Read-only analysis

- `telemetry::Reader` opens an existing schema-6 store read-only.
- Inside its snapshot `open` runs only cheap checks: schema and application id, non-negative retention metadata, and the sequence high-water mark against the retained maximum and pruned floor. It never scans the store, so an open reader does not hold the snapshot for a full validation.
- Each page decodes and validates the rows it reads and reports a corrupt row as an explicit error.
- The full scan (SQLite `quick_check`, retained bytes, orphan blocks, every row) runs only in the owner `Store` open and maintenance, never in writer promotion or in an open of a sealed, unchanged store (see [local writer transport](#local-writer-transport)).
- Its bounded export pages share the store decoder and retention metadata and read one committed SQLite snapshot until the reader is dropped.
- It never creates a missing store, upgrades, takes a writer lock, checkpoints or prunes.
- `Reader::storage` reports database, WAL and SHM file bytes and live WAL bytes (frames not yet checkpointed, read from the SQLite WAL-index header) without the writer lock or a checkpoint. A WAL-index header that stays in flux is an error, never zero.
- Missing, foreign, older/newer or corrupt stores are explicit errors.
- SQLite may use WAL shared-memory sidecars; callers must release the snapshot before slow external output, and existing storage limits still bound writers while a snapshot is pinned.
- This API requires existing file-read permission and grants no access by itself.
- `Reader::export_kinds` uses the same indexed decoder as `Store` within its read-only snapshot.

## Agent analysis and export

- Agent analysis reads committed rows through the shared JSON export, without board or network access. Indexed sequence and source time support bounded windows/batches.
- Source `started_ms` may be null only with an explicit unknown reason. The SQL source-time index reserves -1 for unknown (never a reported timestamp); canonical observations retain null, and known source timestamps remain nonnegative.
- Time-filtered exports include unknown-time observations conservatively and expose `unknown_started_count` for the retained history; known time bounds exclude the reserved key.
- Receipt time remains independently required and drives expiry.
- Schema 6 is the reader compatibility floor for indexed semantic observations.
- Historical version/trace/span can be null only with explicit reasons; adapters never invent a product version or trace.
- Semantic fields retain `open`, `from`, `to`, `source`, `usage_status`, numeric `run_id`/`process_id`/`span_id`/`parent_id`, `entry`, `previous_seconds`, `remaining`, `calls` and `tokens`.
- `counts` is a bounded-by-event-size object of fixed code keys and unsigned counts. Numeric metadata preserves u64; source timestamps alone must fit signed SQLite milliseconds.
- Project identity accepts owner/repository or nested board namespaces and the explicit `other` fallback, never absolute or dot-segment paths.
- Historical request summaries remain summaries, not invented per-call observations.
- `Store::export_kinds` selects 1..16 unique fixed kind codes through the kind/sequence index, with the existing 1..1024 row page and source-time rules. Metadata bounds/pruning/unknown counts remain store-wide.
- Already-open older writers can insert null indexed kinds; these candidates are decoded/filtered in bounded pages and counted as unindexed.
- The next cursor advances over examined candidates even when none match; callers follow `more`, not a nonempty events array.
- Reopening validates and backfills late rows. A nonnull indexed kind disagreeing with canonical metadata is corruption, never a healthy omission.
- Schema/index backfill, reader floor and legacy compaction share one transaction; failure preserves the old format.

## Maintenance

- `telemetry::maintenance(path)` returns the existing writer-election guard, refusing a live broker. The caller holds it before opening its one owner `Store`, through completion/drop; it never stops another writer.
- `Reader::import_cursor` reads committed source progress without writer authority.
- `Store::reclaim` first holds the same IPC writer-election lock as bootstrap; a live IPC owner refuses maintenance before any SQLite mutation.
- The lock stays held through success/error, fencing a new IPC writer until the operation exits. The existing database mutex/shared write lock serialize direct `Store` calls too.
- Reclaim then validates retained data, checkpoints the prior WAL, then VACUUMs only when free pages exist, with `temp_store=MEMORY` and the same FULL/cache-spill-off settings. The bundled SQLite build creates its temporary vacuum database in memory, not a second disk file.
- Ids, sequences, original clocks, metadata, cursors/import receipts and the AUTOINCREMENT floor remain unchanged; no observation is pruned.
- A final TRUNCATE checkpoint must complete before success; a blocked checkpoint reports failure with committed data preserved.
- Reclaim is explicit quiesced maintenance outside a live collector, never a per-append copy or proof that all producer processes are drained or that the total multi-store transition budget is satisfied.

## Historical import

- The source adapter validates records and supplies a stable installation namespace, external record key and bounded source cursor; `Event::identified` derives immutable ids without storing source paths.
- `Store::import` commits the validated batch and compare-and-set source cursor in one FULL transaction.
- Repeating the same committed cursor/batch is idempotent; a different payload or stale expected cursor is refused.
- At most 32 sources, 128-byte code cursors and 1024 events per import.
- Source files stay untouched until a separately verified retirement step.
- The optional `imports` checkpoint table is carried unchanged from the additive schema-1 extension into schema 2.
- Import cannot bypass normal metadata validation or retention.
- `Store::import_history` additionally takes one nonnegative original receipt millisecond value per event. Only direct store owners can use it; append-only IPC always stamps host receipt time.
- Historical receipt clocks enter the same transaction and import digest as the events/cursor; duplicate ids with another clock are refused before capacity pruning.
- Expiry uses the current clock, never the supplied historical clock, and replay does not rejuvenate or resurrect expired rows. Plain import receipts keep their existing format.

## Split count summaries

- Large fixed-code count summaries use shared `Event::split`/`split_identified` and `telemetry::assemble`. One logical summary is at most 128 KiB and 16 parts, each at most 16 KiB.
- Unsplit identified events keep their existing id; a split summary keeps that id on its first part, with stable derived ids for the rest.
- Every part carries group identity, full canonical payload digest, index and total.
- Assembly validates all parts, matching metadata, unique count keys and digest; missing, repeated, inconsistent or pruned parts are an explicit error, never a complete or zero summary.
- The shared `Assembler` carries at most 16 validated parts across export pages and latches failures until finish; a truncated query cannot yield a complete report.
- Producers append all parts contiguously in one batch. Historical adapters commit all parts of a source row with its cursor in one import transaction.
- Age, logical-byte and physical-capacity pruning remove the complete summary group, so ordinary retention cannot leave half a summary.
- Part ids are bound to their group/index during validation. Individual observations and existing queue/SQL/IPC budgets do not grow.

## Optional sync

- Optional sync uses at most 32 independent named destination cursors over committed rows, bounded batches and immutable event ids.
- A sender retries the same ids after an uncertain write. Local acknowledgment accepts only the exact next offered batch; a receipt cannot skip unseen rows.
- Sending/receipt transport and authentication remain consumer connection adapters; creating a store sends nothing.
- Synced rows remain locally readable until normal retention. Pruning an unsent range is explicitly reported as a gap, never silently acknowledged.

## Capture collector

- `telemetry::capture::Capture` supplies the common metadata-only tracing layer and one bounded writer per process.
- Up to 8192 observations and 16 MiB of encoded payload wait in memory. Collector group commits contain at most 8192 observations and 16 MiB of decoded payload, with a 5 ms collection window or explicit flush/shutdown.
- Already queued observations coalesce into one FULL transaction; 1024-row SQL chunks retain the existing capacity/pruning checks, and a late chunk failure rolls back the whole group.
- Input identities are checked before any capacity pruning, including conflicts across chunks or with retained rows. The public append/import/export limits remain 1024.
- Enqueue is not a durable acknowledgment. A full queue or failed batch increments collection-loss health, reports unknown and cannot yield a successful qualification flush.
- Span ids are `<collector run>.<n>` from one per-collector counter, never the tracing registry slot: slot generations wrap and repeat.
- Each enabled span enqueues a `method.start` observation immediately, with unknown outcome and duration, then a `method` terminal observation under the same span identity on close.
- A missing terminal remains unknown; starts become durable only after group commit or explicit flush.
- Source start, lifetime duration, active entered duration and parent trace/span identity are recorded; async polling uses tracing's enter/exit behavior.
- Unknown returns are explicit. Error values are never formatted or stored by the collector.

## Process lifecycle

- `capture::Runtime::install` opens the local store and installs the one global tracing subscriber before product execution. Its guard flushes on normal return/unwind.
- A second installation or an existing subscriber is refused, never replaced silently.
- Generated `std::process::exit` calls use shared `capture::exit`; shared launchers and consumer exec boundaries call `before_exec` before replacing an image.
- Flush has a 3 s total deadline, including queue admission.
- Collection failure reports unknown without changing product exit codes or preventing image replacement. Active spans without a terminal remain unknown.
- Protected sandbox clients still require the shared host writer; failed store access never widens permissions.

## Collector self-check and store diagnostics

- `capture::qualify` flushes the installed runtime, requires committed observations and reports loss/failure as an error; an absent or empty collector is unknown, never a passing qualification.
- Ordinary operation exit codes stay unchanged. This check proves local collection only, not inventory, UI, runtime adapters or host rollout.
- `COREQ_TELEMETRY_STORE_TRACE=1` enables numeric phase diagnostics for store opens and appends over 100 ms (initialization/write locks, connection, validation, schema, checkpoint, begin, reserve, insert, prune, commit) and initialization-lock failures. It is off by default; explicit diagnostic runs can enable it to diagnose slow drains, while normal CI qualifies default output.
- Collector health also reports completed append batches, total/maximum append microseconds and the in-flight batch size; a flush timeout includes these numbers so backlog is distinguishable from rejected or lost observations.
- Always-on spans `<product>.operation.telemetry.election`, `telemetry.open` and `telemetry.checkpoint` date writer election, owner open with its full scan, and each checkpoint.
- `from` names the trigger: election `bootstrap` or `promotion`; open `first-open`, `schema`, `promotion`, `orderly-exit` or `unclean-exit`; checkpoint `open`, `upgrade`, `capacity`, `seal`, `budget`, `wal-room`, `wal-share`, `import`, `acknowledge`, `prune` or `reclaim`.
- `to` names the outcome: `owner`/`client`, `scan`/`skip`, `complete`/`busy` with `remaining` frames. The span duration is the wait.
- `Runtime::install` opens its store under the new collector, whose writer waits for that store, so the bootstrap election and open are kept.
- `qualify` requires committed observations beyond these bootstrap spans, and an existing global subscriber is refused before the store is opened.
- Store spans need an active collector. A checkpoint span emitted by an append is committed by a later append, and a bounded append retries a busy checkpoint at most once a second, so a pinned reader cannot start a span feedback loop.

## Repeating roots

- A consumer marks a span with `coreq_repeating=true`. The root and its descendants defer start rows; steady successful results fold into `method.aggregate` rows per operation, project and result code in UTC 15-minute windows.
- `coreq_state` names the result code (default `ok`).
- A changed code after the first observation, failure, refusal or unknown retains its own start/result rows and materializes open ancestor links.
- Selected diagnostic spans remain detailed. Outside marked roots behavior is unchanged.
- `observe` and `observe_linked` fold validated JS `method.aggregate` and `collection` records received inside a marked root; linked records keep their original identities on exceptions; an absent project inherits the ingestion scope.
- Consumers must enter the marked refresh scope around ingestion, including asynchronous delivery.
- Aggregates retain calls, failures and total measured duration, plus summed fixed counters.
- Existing JS summaries do not measure duration: it remains null with `not-measured`, never invented as zero. Arbitrary semantic counter sets stay detailed.
- At most 256 operation/project/kind keys are resident; capacity drains them early.
- Window rollover and orderly shutdown persist pending counts through the existing bounded writer. Abrupt termination can lose the current window, as with poll aggregates; enqueue is not a durable acknowledgment.
- `Capture::finish` drains partial windows and flushes before shutdown; ordinary `flush` does not split windows. Shutdown/restart or capacity drains can produce partial aggregates in the same UTC window.
- Budgets, queue limits, contended-batch retry and unknown-on-loss behavior do not change.
- Qualification simulates six projects for one hour (60 s passes and 2 s refreshes with board, identity, status and JS collection); growth must be at most 384 MiB/720 without pruning, and disabling the fold must fail that bound.
- The consumer (TaskQ) owns root adoption and live verification.

## Polling

- `capture::poll(name, subject, action, state)` runs `action` on every call and returns its result unchanged; `state` names the observed result with a fixed code.
- `process::identity` uses it (`present`, `missing`, `denied`, `process-identity`), as does `status::write` (`written`, `status-write`); consumers wrap their own repeated status checks the same way and report `changed` themselves.
- `subject` (`u64`: the PID, a hash of the status root) separates fold keys and is never stored.
- Per operation, project and subject, a new result code writes one `poll.change` row with `from`/`to`, outcome, error and this call's trace/span/parent.
- Unchanged calls add no row and fold into one `poll` aggregate (`calls`, `counts.failed`, total `duration_us`, last outcome/error) written on the next change, after 15 minutes, or at shutdown (`Runtime` drop, `before_exec`, `exit`). A crash loses at most that window's counts.
- A poll span that gains a child becomes an ordinary span with start and terminal rows, so parent links resolve.
- At most 256 keys; more drain all windows first. No collector: the check runs and nothing is recorded.

## Local writer transport

- The first eligible process holds a protected writer-election lock and serves append-only batches in a thread of that process; later clients reuse its existing `Store` without opening SQLite.
- Unix uses an owner-only socket in `<database parent>/telemetry-ipc/`; if its pathname exceeds the native address bound, use `/tmp/coreq-ipc-<euid>-<database SHA256>/s`.
- The fallback directory must be a real directory owned by that user with mode 0700; refuse unsafe existing entries without changing their permissions. Readers derive and validate the same path from the canonical database identity.
- Only the elected writer removes a verified stale socket; writer-owner teardown removes its socket and empty fallback directory. Process exit may leave that fixed per-database address; the next elected owner reclaims it after validation.
- Endpoint credentials remain beside the database. Short paths and wire2 framing stay unchanged; older readers unable to validate the fallback fail explicitly, never attach to another store.
- Sandbox access to the fallback needs target-platform qualification.
- Windows uses IPv4 loopback with a random 256-bit key in the same owner-only directory.
- Endpoint state contains no task data, grants no SQL/read/delete/command access and is never logged.
- Schema, frame size (17 MiB), batch count (up to 8192), decoded payload (16 MiB), ids, endpoint and acknowledgment are validated.
- At most 8 connections run concurrently, with a 1 s request/write deadline and a 5 s receipt-read deadline that covers the writer's bounded commit (installation budget lock, write lock, busy timeout, checkpoint and FULL sync).
- On macOS the listener waits on its registered read event and a private shutdown event, so an idle owner has no 5-ms wake loop.
- Teardown explicitly wakes that event before joining; a failed wake emits a fixed diagnostic and the listener rechecks shutdown within its 1 s safety timeout, retaining writer election until exit.
- Linux and Windows retain 5 ms shutdown polling. Active requests retain their existing I/O deadlines.
- Endpoint metadata wire2 requires a private SHA256 binding to the canonical existing parent plus the exact requested database filename; computing it never opens or inspects SQLite data.
- One parent has one elected writer: another filename is refused while that writer lives, including cached-client retry after handover.
- Metadata wire1 lacks that binding and is refused until its writer quiesces; old clients reject wire2. The append frame/receipt layout is unchanged.
- `max_events` remains additive (missing means 1024); clients split batches at complete-summary boundaries; an endpoint too small for one summary is refused before sending the batch.
- Same-path clients still connect before any file creation or SQL access.
- A dead writer permits an explicit new selected filename under the election lock, never implicit redirection of a live client.
- Authentication probes do not mutate the store.
- A committed-batch digest is acknowledged only after SQLite FULL commit; rejected batches return a validated stable failure class (authentication, frame/observation validation, immutable-id conflict, SQLite busy/full/readonly/I/O), never a raw message.
- Uncertain clients retry the same immutable ids once; duplicates are idempotent.
- A receipt that misses its deadline on a live connection is a pending commit, not a lost writer: the client retries the same ids at the same writer without election. Only a failed connection, write or closed receipt re-elects.
- A live writer prevents takeover; a dead writer is replaced under the lock.
- A client that promotes to writer inside append opens a current schema-6 store with the `Reader` cheap checks only, so its next commit never waits for a full-store scan: an owner open already ran that scan, and each row read still validates. An older or raw store still takes the full owner open.
- Orderly shutdown (`capture::Runtime` drop, `before_exec`, `exit`) seals the owner after a complete flush: it stops serving, checkpoints, records the exact store state in `<database>.sealed`, then leaves writer election.
- The seal records an empty WAL, database file length and modification time, sequence high-water mark and retention metadata.
- After `before_exec` a later append of its own reconnects or promotes.
- `Runtime` drop retires the sink as well: later observations on the still-global collector never reconnect, re-elect or recreate the store; health counts them as `dropped`, and a required commit is refused. A new process installs and elects normally.
- Every owner open consumes the seal and skips the full scan only for a current schema-6 store whose state still matches it, so a new owner after an orderly exit publishes without that scan.
- An unclean stop, any later write, a failed flush or an upgrade leaves no matching seal, so the next owner runs the full scan.
- Protected clients connect before any attempt to create/write files. Neither an unreachable writer nor a sandbox denial widens a grant.
- Unix socket-path limits and Windows network-isolation behavior require target-host qualification.
- A process with no resident service can own the thread temporarily.

## Automatic source capture

- `coreq_instrument::generate` in the consumer build instruments the source tree: methods, default trait bodies, callbacks and async blocks as disabled-by-default diagnostic Rust tracing sites.
- Existing explicit tracing instrumentation marks always-on operation boundaries and is not duplicated. Arguments are never recorded.
- A generated `COREQ_TELEMETRY_COVERAGE` inventory names instrumented bodies and explicit const-evaluation exclusions; the inventory is evidence for review, not proof that an excluded runtime route is covered.
- Source/asset dependencies remain compiler-tracked; generated files preserve original line numbers, and the shared source map restores original relative file names.
- Typed `Res`/`Result` exits and `?` of enabled spans are observed without error formatting, extra trait bounds or closure capture; untyped callback returns stay unknown rather than healthy.
- Native UI semantic events and observed paint still require the shared UI adapter.
- A declared return that cannot be classified as a typed `Result`/`Res` or an infallible scalar remains unknown, never healthy by default.
- Item macro expansions appear as unobserved in the generated coverage inventory; consumers must qualify their generated entry points separately.
- Original/generated fixtures compare returned behavior; source instrumentation must preserve native errors.

## Diagnostic selection

- Generated sites use target `coreq::method`; all other explicit spans remain operation evidence.
- `Capture::diagnose` selects an exact method or module prefix, or `*`, for at most 60 s and 256 new spans; only one session may be active.
- Normal mode does no per-method timing, state allocation or persistence.
- Selection happens before span construction; a session started outside the current dispatcher still activates selected sites.
- Quota/expiry stops admission immediately; the next health/control observation restores the disabled callsite cache through the collector's weak dispatcher reference, outside subscriber callbacks.
- Existing admitted spans retain their terminal even after expiry; later calls stop recording.
- Budget refusal is deliberate diagnostic truncation, separate from collector loss. Health and control observations expose the mode and limits.
- No arguments or raw returns are captured. A disabled method cannot attribute a recovered internal error to its enclosing action.

## Execution boundaries and context

- `capture::operation` runs synchronous fallible work once, observes the typed result before returning it and retains its parent trace even if the caller handles/discards the error.
- Its classifier returns a fixed static error code; error values are never formatted. Invalid classes fall back to `returned-error`; panic wins.
- Shared operation, continuation, detached and typed-result boundaries do not initialize tracing callsite caches under an absent subscriber; later scoped capture must remain available.
- Typed method results update their owning span outside its dispatch scope, including older generated sites.
- Diagnostic expiry/quota flush uses the owning dispatcher to restore disabled method sites without suppressing action records.
- The boundary does not create a deadline or turn enqueue into durable acceptance. Dependency adapters own finite deadlines and background owners must still observe completion.
- `capture::detached_span` keeps explicit trace/parent identities without retaining the caller span. The caller can commit its terminal while the background span stays open/unknown; its owner records the eventual outcome. No payload or authority is inherited.
- `capture::TraceContext` captures the current trace/span and encodes only those opaque identities in a versioned, at most 128-byte string.
- Decode rejects malformed/noncanonical ids, extra fields and unknown versions before opening a continuation.
- A continuation has the remote trace and parent, a new local span id, and no inherited local ambient parent or remote payload; generated descendants inherit its identity.
- The trace link grants no authority and does not acknowledge a command or prove delivery. Consumers own bounded handoff transport, supersession and observed-result classification; missing delivery remains unknown.
- An invalid raw continuation field marks collection incomplete without recording its contents.
- Shared command execution replaces `COREQ_TRACE_CONTEXT` with the current validated trace/span identities, or removes it when no context exists. Independent launchers use `capture::inherit_context` at dispatch.
- Runtime bootstrap validates this bounded field and enters its continuation for the process owner thread; normal teardown closes it before flush.
- Invalid/non-Unicode context marks collection incomplete without recording its value or refusing the command. Other threads still require explicit context propagation.
- The process link proves neither durable acceptance nor effective command state; abrupt exit may leave its continuation open/unknown.
- A fixed-code `coreq_refusal` field on a span or event marks that span's terminal as refused with the given error class. It is not inherited by children and never overrides a typed error or panic.
- Raw refusal messages and invalid codes are ignored. Always-on operation boundaries and enabled diagnostic methods accept this field; it adds domain meaning when a return value alone cannot distinguish a refusal.

## Semantic observations and receipts

- `capture::observe` validates product metadata and enqueues through the current capture writer with its actual version and causal context.
- No collector, invalid metadata or full/disconnected queue returns an explicit error; no fallback store. Admission is optional collection, not durable acceptance.
- Large bounded count summaries enter as one queue item and commit contiguously as a group; rejection loses the whole group and increments health.
- `Capture::commit` submits one immutable event through the existing bounded writer and acknowledges only that event's FULL commit.
- Queue rejection admits nothing; a 3 s acknowledgment timeout is uncertain, since a queued event may commit later.
- Retry only the identical event, never a business effect on the strength of a missing acknowledgment. Prior optional collection loss remains visible but cannot change this event's commit result.
- `capture::checkpoint` records received/applied/observed/superseded/failed phases for the current explicit action, with stable per-span/phase ids and no arguments.
- Missing capture/action identity refuses; retention remains bounded and pruned history is unknown.
- Consumers own command ordering, safety fallbacks and visible receipts; a recorded phase is not evidence of any other phase.
- `capture::CommandReceipt` captures the current explicit action and encodes at most 1024 bytes of versioned metadata. Strict decoding rejects malformed, foreign-version or extra fields without echoing input.
- Its checkpoint uses the receiving process's collector but keeps the original product/version/operation, trace/span/parent and start clock; the collector must belong to the same product.
- Replay of the same phase after a process restart keeps the same immutable event id. `event_id` computes that phase's lookup key without a collector, storage access or acknowledgment.
- The descriptor carries no arguments, authority or proof of prior phases. Consumers own its protected storage/transport and decide when each phase is true; decoding never runs a command or acknowledges delivery.

## JavaScript wrapper

- Ordinary module methods inherit counters and bounded selected diagnostics from one shared module wrapper; events, commands, rendering and owned asynchronous work enter explicit shared execution boundaries.
- Runtime code is not rewritten by a build-time JS parser.
- Wrapped logic/view methods are synchronous and preserve receiver/arguments/return and thrown identity.
- Async effects have one owner, keep their initiating context, observe settlement and deliver results through a wrapped synchronous callback.
- Callback exceptions are recorded and forwarded through the platform `reportError` hook when available; components do not own detached promises.
- `observe_linked` preserves externally supplied identities and unlinked aggregates rather than inheriting the receiver; external diagnostic methods share the native quota/expiry.
- The Rust `capture::bind` adapter carries a dispatcher/link into an owned thread callback.
- Context-bound synchronous JS callbacks retain the initiating scope across owned timer/Promise handoff.
- Diagnostic selection emits its bounded metadata through the same transport. Promise settlement is distinct from submission, frames from paint.
- The same source is exported by the Rust constant and the pinned `@taskq/coreq` JS package.
- `shared()` reuses one realm-owned runtime across components and coalesces dirty method counters into one 2 s flush timer, including calls without DOM events; an idle runtime has no recurring timer.
- Health metadata uses this same flush; `connect()` supplies the existing synchronous transport, with absent delivery explicitly lost.
- The consumer transports bounded metadata into the existing capture writer; no second collector/store.
- The selected gate uses the runner's Node.js through the existing bounded executor for JS wrapper conformance.
- Full consumer module/boundary inventory, new-handler inheritance, errors, privacy and native/load checks remain required before claiming coverage.

## Consumer integration acceptance

Use the shared [construction and collection rules](https://github.com/taskqorg/specq/blob/main/engineering/profiling.md#collection-modes); the following matrix names CoreQ's implemented collection modes, not equivalent coverage claims.

| Owned call group | Normal mode | Selected detail |
|---|---|---|
| Explicit Rust actions/dependency boundaries | Causal start/result records and semantic operation observations | Existing action evidence remains on |
| Generated internal Rust methods | No per-method invocation counters or timings | Bounded method spans; unclassified returns remain unknown |
| Wrapped internal JS methods | Call/finish/failure/unknown aggregates; one activity-triggered 2 s flush | Bounded method timing/results |

- A request for continuous Rust method aggregates changes the second row and its implementation; diagnostic capability alone does not satisfy it ([coreq.md R34](../../coreq.md#universal-telemetry-telemetry)).
- Qualify wrapper integration through the ordinary consumer import/build path: one new method/event, a delayed callback after an unrelated action, a callback exception, method activity with no input event, and unavailable optional transport.
- Reuse existing conformance cases; do not require each consumer to duplicate library internals.
- Cargo, instrumenter and JS packaging use one immutable CoreQ source revision; aggregate/diagnostic limits and shared-writer semantics remain those above.

## Store qualification

- Qualification covers acknowledged-write crash/reopen, corrupt/newer store preservation, concurrent writers, transactional rollback, retention/export gap and exact-batch acknowledgment checks.
- These qualify the store, not UI coverage, runtime adapters, global ingestion or host rollout.
