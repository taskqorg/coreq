# coreq contract

coreq is the shared Rust crate of taskqorg tools: install layout, self-update, launcher and rollback, OS service units, `status.json`, selftest harness, incident reports, board token, profiling, telemetry and bounded process execution. It is a library linked into each tool, not a separate service.
This file holds every decision; README and code follow it. [taskq](https://github.com/taskqorg/taskq) is the reference consumer; its `taskq.md` § 4 and § 8 define the behavior extracted here.
Rules carry stable IDs (`R<n>`) and are mandatory. Each section starts with its rules; unnumbered lines that follow are binding detail.

## Rules
- **R1 Shared rules.** [specq](https://github.com/taskqorg/specq) binds every task here, [spec first](https://github.com/taskqorg/specq/blob/main/engineering/spec-first.md) included; this file holds only coreq decisions.
- **R2 No constants for a consumer.** Tool name, title, release repository, version and install folder are fields of `Tool`; a second consumer passes its own and needs no fork.
- **R3 No behavior change on extraction.** With `Tool::new("taskq", "TaskQ", "taskqorg/taskq", VERSION)` every path, unit, message and check matches taskq.md § 8.
- **R4 Dependencies.** `ring`, `serde_json`, `tracing`/`tracing-subscriber`, bundled `rusqlite` for the shared durable local telemetry database and portable `flate2` for lossless bounded event blocks. No HTTP client: HTTP goes through the OS `curl`.
- **R5 Checks.** `python3 scripts/check.py` selects the merge gate: documentation checks for prose-only changes, otherwise `cargo test --locked` for this shared crate. Unknown impact uses the full suite.
- `--full` validates selection on workflow dispatch and before dependency-pin adoption. A failed selector is repaired before reuse.
- Every gate runs the [specq contract lint](https://github.com/taskqorg/specq/blob/main/engineering/contracts.md#lint) on this file at the specq commit pinned in `scripts/check.py`.
- **R6 CI.** CI (`.github/workflows/tests.yml`) runs affected Rust checks on Linux, macOS and Windows; job `tests` is the required status.
- Elapsed budget: 900 s per GitHub-hosted `ubuntu-latest`, `macos-latest` or `windows-latest` runner, including toolchain setup, build and execution. The runner prints elapsed time and fails on exhaustion, without retries.
- **R7 Workflow permissions.** Workflows grant `contents: read` only; a publishing job adds just what it writes. Vulnerability reports: SECURITY.md.

## Tool identity (`Tool`)
- **R8 Compiled-in release repository.** `repo` is the GitHub `owner/name` of releases. It is built into the consumer binary, never read from project config: project files are written by agents, so config must not choose the code hosts run.
- **R9 Resolved installation root.** No cwd, home traversal, sibling or disk discovery. Resolve before capture/bootstrap and keep the resolved root for the process lifetime.
- A recognized but invalid or inaccessible installed layout is unknown/refused; never silently redirect it to another installation.
- `Tool { name, title, repo, version, install, fault }`. `name` names the binary (`<name>`, Windows `<name>.exe`), files and env vars; `ENV` below is `name` in upper case. `title` names the Windows Scheduled Task.
- `install` uses explicit env `<ENV>_INSTALL` first. Without it, an installed launcher or immutable release resolves its own installation from the exact executable location and the existing install layout; recognized launcher `.old` variants retain that root.
- A standalone/source executable uses `~/.local/share/<name>`, Windows `%LOCALAPPDATA%\<name>`.
- Native service starts and their child entrypoints use this same rule; this changes no worker grant or filesystem permission.

## Install layout
- **R10 Exact recognition.** `install_dir(name, title, aliases)` resolves the process root before bootstrap; `Tool::new` uses it without aliases. Recognition uses only the executable's exact launcher/release/bundle layout, bounded current selection and the primary release file; no search.
- A consumer names any existing auxiliary executable exactly, before its first resolution. Aliases do not grant permissions. Explicit `Tool.install` values stay caller-owned.
- Launcher and update-smoke children receive the selected `<ENV>_INSTALL`, including first-install smoke before current exists.
- `releases/<ver>/<name>`: immutable release folders; `releases/<ver>.tmp/` is a download in progress.
- `current`, `previous`: one version each, written atomically (temp file, then rename).
- `releases/<ver>/starts/<project>`, `releases/<ver>/host-starts`, `releases/<ver>/healthy`: launcher counters ([launcher](#launcher-launcherlaunch)).
- `<name>`: the launcher, a copy of the binary that last updated or installed; `<name>.tmp` (Unix) and `<name>.old`, `<name>.old.<n>` (Windows) are leftovers of replacing it.
- `<name>.log`: host service output (Windows and launchd).
- `rollback-floor`: minimum launcher version, recorded by `launcher::set_rollback_floor` only after the consumer verifies host-unit ownership. An unreadable or invalid floor refuses service launch; an absent floor keeps legacy behavior.

## Release artifacts
- **R11 Release trust.** Trust is HTTPS to `repo` and who can push its tags. `ponytail:` no signature; add one when releases are served elsewhere.
- A `v*` tag builds `<name>-<ver>-<os>-<arch><exe suffix>` per OS (`os`: `macos`, `windows`, `linux`; `arch`: Rust's), `SHA256SUMS` and `latest.json` `{"version", "files": {"<os>-<arch>": "<file>"}}` under `https://github.com/<repo>/releases/latest/download/`.
- Release CI follows the [specq release contract](https://github.com/taskqorg/specq/blob/main/engineering/release-contract.md).

## Update (`update::update`)
**R12 Update steps.** `update::update` runs these steps in order:

1. Prune: delete leftover `releases/*.tmp` and, when `current` is set, every `releases/<ver>/` that is neither `current` nor `previous`, its exe first. One that fails (Windows: exe in use) is reported on stderr and kept for the next update. Read `latest.json`. Not newer than `current` (else `version`), or already present: no update.
2. Refuse a version with characters outside `[A-Za-z0-9.-]` or `..`, or a file other than `<name>-<ver>-<os>-<arch><exe suffix>`.
3. `check` stops here and reports the version.
4. Download the file and `SHA256SUMS` into `releases/<ver>.tmp/`; the file's sha256 must be listed with its name. Rename it to `<name>`, mode 0755.
5. Smoke: `<name> selftest` with `<ENV>_SELFTEST_OFFLINE=1`. Failure: delete the folder, report, keep current.
6. Rename to `releases/<ver>/`, write `previous` = old `current`, then `current` = `<ver>`. Replace the launcher with the new binary. Prune again, as in step 1. The consumer re-execs.

Versions compare by numeric dot parts; a `-pre` suffix sorts before its release (`4.1.0` > `4.0.12` > `4.0.0-dev`).

### Binary replacement
- **R13 Never write a running binary in place.** Replacing a binary on Unix, and the release binary of `install_self` everywhere: copy to `<target>.tmp-<pid>`, set the old file's mode, rename it over `<target>` (macOS kills a rewritten running binary). A failed copy deletes the temp file and leaves the old binary intact.
- Extended attributes (macOS quarantine) follow the source file, not the old one; Linux copies none.
- Replacing the launcher on Windows renames the running `<name>` to `<name>.old`, or to the first `<name>.old.<n>` (n = 1, 2, ...) that is free or removable when an earlier one is still in use, then copies the new file. A failed copy renames it back.
- No live swap: a running launcher keeps its process until the service restarts.
- **R14 Never a downgrade.** `update::install_self` copies the running binary to `releases/<version>/` and replaces the launcher with it; it makes it `current` (old one `previous`) only when newer.

### Transactional install
- **R15 Prepare, then activate.** Transactional consumers use `update::prepare_install(t, verified_bytes)` then `activate_install(t)`. The consumer serializes installation with its existing update/lifecycle locks, verifies/signs and freezes the exact bytes first and retains durable originals before activation.
- Preparation syncs an immutable release without changing launcher, current/previous or units. Identical bytes reuse it; different bytes at the same version refuse and preserve it.
- Activation requires that regular prepared file, refuses unknown/current downgrade before effects, replaces the launcher from the prepared release and durably writes current/previous.
- A later activation error is uncertain, not a rollback: the consumer owns native observation and restart-safe restore. These primitives do not install/start a unit or prove health.
- The combined `install_self` remains an identified compatibility caller until consumers migrate.

## Launcher (`launcher::launch`)
- **R16 Rollback after three unhealthy starts.** Only `service run` counts starts of a version without `healthy`. At the 3rd counted start the launcher writes `previous` into `current` and runs it, only when `previous` is not below `rollback-floor`; otherwise current stays.
- Runs `releases/$(current)/<name>` with the same arguments. No `current`: exit 1 with `run \`<name> update\``.
- Project mode counts per project in `releases/<ver>/starts/<project>`, `<project>` the name of the working directory (the project root); starts of other projects do not count. A `starts` file of an older launcher is replaced.
- Host mode (`service run` with `<install>` as working directory) counts in `releases/<ver>/host-starts`, independent of project roots.
- `healthy` stays install-wide: the consumer writes it after `rollback_after`.
- The floor setter accepts only valid versions, raises the floor atomically and never lowers it; it refuses a floor newer than current.
- The consumer serializes floor recording with topology installation. Recording the floor is separate from unit loading: only the consumer can verify host ownership.
- Unix: exec. Windows `service run`: the launcher supervises. It frees its console, runs the child without a window and appends output to `<name>.log`; exit 75 restarts at once, any other exit after 60 s. Other Windows commands: run, exit with the child's code.
- Messages go to stderr as `<name>: <message>`. Windows `service run` has no console, so its messages, including the reason it fails to pick a release before it exits, go to `<name>.log`: a dead launcher leaves one line.

### Launcher fault hook
- **R17 A hook never changes the launcher.** A hook that panics is caught: launcher behavior and exit code stay unchanged.
- `Tool::fault` (default none) receives a `Fault` with `board`, `runtime`, `revision`, `contract` and `observed`/`inferred` empty; the consumer fills them and reports.
- The rollback gives `update.rollback` (site `launcher:rollback`, fingerprint field and evidence `failed:<ver>`). On Windows a `service run` exit 101 gives `dispatch.panic` (site `launcher:exit`, evidence `exit:101`). No `previous` to restore: no fault.

## Host service units (`service::host_unit`)
- **R18 Verify before replacing.** Before updating an existing Windows task, verify its principal, executable, arguments and physical installation directory by volume serial plus a 128-bit file ID; foreign scope refuses replacement.
- **R19 No automatic ACL repair.** Runtime operations never repair existing task ACLs automatically; an unqualified existing task requires explicit installation repair. Worker/project grants do not change.
- `host_unit(t)` writes and loads one unit running `<install>/<name> service run` with `<install>` as its working directory and the caller's PATH.
- Names: launchd `com.<name>.service`, Scheduled Task `<title>`, systemd user `<name>.service`. launchd output goes to `<install>/<name>.log`; other settings match the [legacy generators](#legacy-service-units-serviceunit).
- The consumer calls it only when installation or reload is safe; it does not drain children or migrate project configuration.
- On Windows, explicit host installation registers management rights for the exact current-user SID and SYSTEM, including an elevated installer: `D:P(A;;GA;;;<current SID>)(A;;GA;;;SY)`.
- The principal stays InteractiveToken/LeastPrivilege and the logon trigger stays scoped to that user.
- Directory identity accepts distinct canonical spellings of the same existing local path without prefix stripping or case folding.
- Read back the native management rights before starting or acknowledging installation.

### Legacy unit removal (`service::legacy_unit`)
- **R20 Unknown readback is an error.** `legacy_unit(t, project)` unloads and deletes the legacy per-project unit and reads back unit absence from the native manager and filesystem. Unknown readback is an error, never success.
- The consumer must first verify that the legacy service and its children have exited; run removal from an idle helper when unloading would kill the caller.
- No other unit is removed. The legacy `unit` generator stays compatible. Legacy per-project starts keep their existing counters and also honor the floor.

## Legacy service units (`service::unit`)
One unit keeps `<install>/<name> service run` running in the project root with the caller's `PATH`, restarting on failure.

| OS | Unit | Load | Unload | Stop (unit retained) |
|---|---|---|---|---|
| macOS | `~/Library/LaunchAgents/com.<name>.<project>.plist`, `KeepAlive`, output `<install>/<project>.log` | `launchctl bootstrap gui/<uid>` | `launchctl bootout gui/<uid>/<label>` | same as unload |
| Windows | Scheduled Task `<title>-<project>` from `<install>/<title>-<project>.xml` (UTF-16 with BOM), triggers below | `schtasks /Create /XML /F`, `/Run` | `/End`, `/Delete /F` | `/End` |
| Linux | `~/.config/systemd/user/<name>-<project>.service`, `Restart=always`, journal | `daemon-reload`, `enable --now` | `disable --now` | `stop` |

- Windows triggers: this user's logon (`whoami`) plus a time trigger repeating every 5 minutes, `MultipleInstancesPolicy` `IgnoreNew`, no admin, no time limit.
- Windows `RestartOnFailure` (every minute) covers only a failed start, so the repeating trigger is the restart source for any launcher exit, within 5 minutes.
- **R21 State the real restart condition.** Consumers state the actual restart condition instead of promising every login restarts a stopped service. No login watcher is added.
- Stop retains the autostart definition. launchd and Task Scheduler keep their logon trigger.
- The Task Scheduler repeating trigger starts a stopped launcher again within 5 minutes, so a lasting Windows stop is an unload.
- systemd user units start with the user manager or an explicit native start. With linger, a new login does not restart that manager.
- `schtasks /End` ends only the launcher: the consumer also ends the service process tree. Commands run with captured output and no console window.

## Acknowledged state files
- **R22 Success only after durable replacement.** `write_durable` accepts text or bytes and is for acknowledged files whose acknowledgment requires persisted replacement. It creates the temporary exclusively, writes and syncs it, then replaces the destination; Unix also syncs its directory, Windows uses write-through replacement. Only then does it return success.
- The caller owns a lock covering the destination and its reserved `<filename>.pending` temporary file.
- A failure after replacement is uncertain, never proof that the old value remains.
- An existing temporary file is preserved and refused for explicit recovery; this helper never guesses whether an interrupted write should become current. Errors remove only the temporary created by this call.
- Consumers retain their own corruption/schema checks and decide recovery without replaying business effects.
- `write_atomic` remains rename-only; it cannot establish an acknowledged power-loss durability boundary.
- Qualification uses process termination after acknowledgment, not simulated hardware power loss.

## status.json (`status`)
- **R23 Stale is unknown.** A reader treats a file older than its freshness limit (taskq: 60 s) or unreadable as unknown, never as zero or success.
- **R24 Publication is not health.** A successful write proves publication only, not service health or command application. Consumers report failed publication without abandoning owned children and cannot qualify health from an unpublished pass.
- Each service pass writes `<root>/.<name>/status.json` atomically. Common keys: `host`, `version`, `pid`, `at` (unix seconds), `project`, `error` (null when the pass succeeded), `update` (a newer version found and not installed, else null). Consumers add their own keys.
- Publication runs through the shared execution boundary: a failed directory/write/rename records `status-write` even when a caller handles it. The original error returns to the caller; paths and status contents never enter capture.

## Selftest harness
- **R25 No install without selftest.** Update smoke (step 5) and release CI run the consumer's `selftest` on each artifact; a version whose selftest fails is not published or installed.
- The consumer's `selftest` command loads config, runs its built-in fixtures and, unless `<ENV>_SELFTEST_OFFLINE` is set, reads the board. It writes nothing.

## Universal telemetry (`telemetry`)
- **R26 One local-first implementation.** One shared implementation serves all consumers, local first and always available without a connection; no product forks the storage or synchronization bookkeeping. A storage API alone does not qualify coverage.
- **R27 Metadata only.** Observations hold fixed codes and finite numbers only; no task text, arguments, prompts, output, keys or paths. Unknown values are null with an explicit reason.
- **R28 Acknowledged means committed.** Enqueue is not a durable acknowledgment. Store writes, IPC batches and required receipts are acknowledged only after SQLite FULL commit; acknowledged writes survive process restart, unacknowledged writes may be lost on crash.
- **R29 Preserve, never reset.** Corrupt, newer-schema or unknown-schema stores are preserved and refused, never reset. No source store or historical row is silently discarded.
- **R30 Loss reads unknown.** A full queue, failed batch, or absent or empty collector reports unknown, never a passing qualification. Collection failure never changes product exit codes or prevents image replacement.
- **R31 Bounded retention.** Keep the newest observations within 30 days and the physical database cap, whichever is smaller, pruning oldest whole observations. Pruning is reported; it is not evidence that an operation never occurred.
- **R32 Telemetry grants nothing.** Endpoint state grants no SQL/read/delete/command access and is never logged. Neither an unreachable writer nor a sandbox denial widens a grant; failed store access never widens permissions.
- **R33 Normal mode stays cheap.** Generated method sites are disabled by default. Normal mode does no per-method timing, state allocation or persistence; diagnostic selection is bounded (at most 60 s and 256 new spans, one session).
- Store: SQLite WAL, `synchronous=FULL`, schema version 6, at a consumer-selected stable path below the installation; one writer connection per `Store` ([store and schema](docs/design/telemetry.md#store-and-schema)).
- Budget: consumers pass `telemetry::INSTALLATION_BYTES` (1 GiB) and `telemetry::PEER_BYTES` (64 MiB): database 384 MiB, WAL+SHM 64 MiB, peers 64 MiB, reserve 512 MiB. A share over its limit is a degraded diagnostic, never a caller error ([budget](docs/design/telemetry.md#shared-physical-budget)).
- [docs/design/telemetry.md](docs/design/telemetry.md) holds the binding limits, formats and procedures: schema upgrades, budget shares, payload blocks, checkpoints, the local writer transport, the capture collector, read-only analysis, import, sync, repeating roots, polling, instrumentation, causal context and receipts, the JS wrapper, store qualification and the consumer integration matrix.

- **R34 Matrix rows are the claim.** The [consumer integration matrix](docs/design/telemetry.md#consumer-integration-acceptance) names implemented collection modes, not equivalent coverage claims.
- A request for continuous Rust method aggregates changes the matrix row for generated internal Rust methods and its implementation; diagnostic capability alone does not satisfy it. No claim that this matrix fulfills a stronger owner requirement.

## Bounded command execution (`process`)
- **R35 Bounded and private.** `process::output` and `process::exchange` own a direct subprocess with a fixed operation code and a positive deadline up to 1 h. No command, arguments, environment, input or output enters telemetry; input never enters argv or temporary files.
- **R36 Identity grants nothing.** Process identity reads grant no ownership or termination permission; termination requires the caller's existing authority over that recorded execution.
- `process::output` uses null stdin and both captured output pipes; stdout+stderr payload is at most 1 MiB. At most 16 executions, including unfinished cleanup, own capacity at once. The caller retains the real output/OS error.
- `process::exchange` uses the same owner for bounded stdin bytes (at most 1 MiB) and a combined response of at most 8 MiB.
- The shared execution boundary records spawn, exit, wait/read/write error, timeout, input/output limit or capacity failure before a caller handles the result. A nonzero exit returns its Output with the failure.
- Success requires complete input, child exit and both pipe EOFs before the deadline; a descendant holding a pipe makes completion unknown/timeout.
- This helper does not own arbitrary descendants or replace long-lived worker supervision. It does not widen permissions, change process groups, retry commands or turn a successful spawn into completion. Consumers own per-operation deadlines and domain-specific interpretation of exit status.
- [docs/design/process.md](docs/design/process.md) holds cleanup ownership, `process::identity` and `process::terminate`, adapter deadlines and the per-OS identity and termination mechanisms.

## Incident reports (`incident`)
- **R37 Opt-in only.** Env `<ENV>_INCIDENT_REPORTS=off` or config `false`: `disabled`, nothing written or run. Config absent (the owner has not chosen yet): `local`, reason `not acknowledged`. Only `true` sends.
- **R38 Codes, no free text.** A report holds only the listed keys and code values: no free text, link or mention. Any other key, value or size: `local`, reason `unsafe`, no file.
- **R39 Library stays silent.** coreq builds, checks, routes and stores the report; it never prints, panics out, exits or touches consumer state.
- Implements the [taskq incidents design](https://github.com/taskqorg/taskq/blob/main/docs/design/incidents.md) for any tool. The consumer owns its fault catalog: it detects a fault at a fixed point and passes a `Fault` (code, fingerprint fields, board, runtime, site, revision, contract, evidence, observed, inferred).
- The launcher detects `update.rollback` and Windows `dispatch.panic` for the consumer through `Tool::fault` ([launcher fault hook](#launcher-fault-hook)).
- **Disclosure.** `incident::disclosure` is the text setup and the first update show before any send: destination (the public `repo`), posting account, payload keys, fallback, opt-out.
- **Report.** Schema `<name>-incident/1`, one JSON object of at most 4 KiB with keys only `schema`, `fault`, `fingerprint`, `report_id` (random UUID), `release`, `revision`, `contract`, `board`, `runtime`, `os` (`<os>-<arch>`), `site`, `evidence` (at most 8), `observed`, `inferred`, `profile` ([incident link](docs/design/profile.md#incident-link)).
- **Values.** Codes of `[A-Za-z0-9._:/-]`, at most 64 characters, no leading `/` or `//`. `fingerprint` = first 16 hex of sha256 over the schema major, fault, its fingerprint fields, board, runtime and os; never the release.
- **Login.** Only a github.com token: the board token when the board is github.com, else the `gh` login for github.com. None: `unsupported-auth`. HTTP goes to `api.github.com` through `curl`; the token goes on stdin, never in arguments.
- [docs/design/incidents.md](docs/design/incidents.md) holds search, routing, claim and outcome rules.

## Board token (`token`)
- **R40 Token stays with the board.** Never in config, never sent anywhere but the board API.
- Env `<ENV>_TOKEN`, else `GITHUB_TOKEN`/`GITLAB_TOKEN`, else the board kind's file (0600): GitHub `<install>/token`, GitLab `<install>/gitlab-token`; a GitLab board never reads `<install>/token`.
- Else `gh auth token --hostname <host>` / `glab config get token --host <host>`: only the board host's login. A missing token names that kind's file.

## Legacy profiling (`profile`)
- **R41 Compatibility view.** The aggregate profile is the compatibility view. New universal observations use `telemetry`; consumers migrate without deleting retained legacy observations.
- **R42 Never zero for unknown.** An operation without samples, a corrupt record or a missing or unreadable store reports `unknown` with a reason, never zero. coreq never retries.
- Follows [specq profiling](https://github.com/taskqorg/specq/blob/main/engineering/profiling.md). coreq measures its own calls against the budgets in the linked detail; a consumer records its operations with the same `profile::record` and passes their budgets to `profile::report`.
- [docs/design/profile.md](docs/design/profile.md) holds the operation budgets, measured adapter boundary, counter classes, store bounds, trace switch, report ranking and incident-link format.

## Product connections (`enroll`)
- **R43 Linked client, not a service.** The accepted target is the [shared connection client](docs/design/enrollment.md), following [shared rules](https://github.com/taskqorg/specq/blob/main/engineering/product-connections.md). Connection behavior must pass the linked conformance and platform checks before release.
- The consumer-specific baseline above remains. Pull-first dashboard research ([dashboard contract](docs/design/dashboard-contract.md)) does not apply to this scope.
- `enroll` holds wire v1 checks, the pairing client with protected, crash-safe connection state, bounded delivery with accepted/projected receipts, revoke-only disconnect recovery and capability-scoped operation pull with server-set actor/workspace/profile authority and immutable results (`agentq.configure`, § Client there).

## Local socket connection (`local_socket`)
- **R44 Connect only.** `local_socket::connect(path, until)` does not create a listener, authenticate a peer, read/write messages, retry product work or grant authority. Callers own endpoint protection, protocol validation and bounded I/O.
- It reuses the nonblocking Unix connector from telemetry for native adapters. It returns one close-on-exec blocking stream or an explicit failure before the caller's deadline; a saturated accept queue refuses without waiting.
- Existing telemetry keeps its one-second connection budget.

## Advisory file locks (`file_lock`)
- **R45 Unlock before close.** The guard explicitly unlocks before closing its descriptor: an inherited child descriptor must not keep the parent operation locked after guard retirement. Release errors are reported, not silently discarded.
- `try_lock(path)` opens an owned lock file and returns a nonblocking exclusive guard or the native lock/open error.
- `try_lock_file` and `try_lock_shared_file` take an already opened file; `Lock::file` borrows that same protected handle for bounded I/O.
- Initialization and update callers keep their existing paths and wait policies; this API neither unlinks lock files nor changes ownership.

## Linked detail
- [docs/design/telemetry.md](docs/design/telemetry.md): binding telemetry limits, formats and procedures under § Universal telemetry.
- [docs/design/process.md](docs/design/process.md): process cleanup ownership, adapter deadlines, identity and termination under § Bounded command execution.
- [docs/design/incidents.md](docs/design/incidents.md): incident search, routing, claim and outcome under § Incident reports.
- [docs/design/profile.md](docs/design/profile.md): legacy profile counters, store, traces, report and incident link under § Legacy profiling.
- [docs/design/enrollment.md](docs/design/enrollment.md): shared connection client, wire v1 and conformance traces under § Product connections.
- [docs/design/dashboard-contract.md](docs/design/dashboard-contract.md): research on a shared dashboard source and action contract; not adopted.
