# coreq contract
coreq is the shared Rust crate of taskqorg tools: install layout, self-update, launcher and rollback, OS service units, `status.json`, selftest harness, board token, profiling. This file holds every decision; README and code follow it. [taskq](https://github.com/taskqorg/taskq) is the reference consumer; its `taskq.md` § 4 and § 8 define the behavior extracted here.

## Rules
- **Shared rules.** [specq](https://github.com/taskqorg/specq) binds every task here, [spec first](https://github.com/taskqorg/specq/blob/main/engineering/spec-first.md) included; this file holds only coreq decisions.
- **No constants for a consumer.** Tool name, title, release repository, version and install folder are fields of `Tool`; a second consumer passes its own and needs no fork.
- **No behavior change on extraction.** With `Tool::new("taskq", "TaskQ", "taskqorg/taskq", VERSION)` every path, unit, message and check matches taskq.md § 8.
- **Dependencies.** Only crates the consumer already uses: `ring`, `serde_json`. No HTTP client: HTTP goes through the OS `curl`.
- **Checks.** `cargo test --locked`. CI (`.github/workflows/tests.yml`) runs it on Linux, macOS and Windows for pushes to `main` and `taskq-*` and for PRs; job `tests` is the required status. Workflows grant `contents: read` only; a publishing job adds just what it writes. Vulnerability reports: SECURITY.md.

## Tool
`Tool { name, title, repo, version, install, fault }`. `name` names the binary (`<name>`, Windows `<name>.exe`), files and env vars; `ENV` below is `name` in upper case. `title` names the Windows Scheduled Task. `repo` is the GitHub `owner/name` of releases; it is built into the consumer binary, never read from project config: project files are written by agents, so config must not choose the code hosts run. `install` defaults to env `<ENV>_INSTALL`, else `~/.local/share/<name>`, Windows `%LOCALAPPDATA%\<name>`.

## Install layout
- `releases/<ver>/<name>`: immutable release folders; `releases/<ver>.tmp/` is a download in progress.
- `current`, `previous`: one version each, written atomically (temp file, then rename).
- `releases/<ver>/starts/<project>`, `releases/<ver>/healthy`: launcher counters (below).
- `<name>`: the launcher, a copy of the binary that last updated or installed; `<name>.tmp` (Unix) and `<name>.old`, `<name>.old.<n>` (Windows) are leftovers of replacing it.
- `<name>.log`: host service output (Windows and launchd).
- `rollback-floor`: minimum launcher version, recorded by `launcher::set_rollback_floor` only after the consumer verifies host-unit ownership. An unreadable or invalid floor refuses service launch; an absent floor keeps legacy behavior.

## Release
A `v*` tag builds `<name>-<ver>-<os>-<arch><exe suffix>` per OS (`os`: `macos`, `windows`, `linux`; `arch`: Rust's), `SHA256SUMS` and `latest.json` `{"version", "files": {"<os>-<arch>": "<file>"}}` under `https://github.com/<repo>/releases/latest/download/`. Trust is HTTPS to `repo` and who can push its tags; `ponytail:` no signature, add one when releases are served elsewhere. Release CI follows the [specq release contract](https://github.com/taskqorg/specq/blob/main/engineering/release-contract.md).

## Update (`update::update`)
1. Prune: delete leftover `releases/*.tmp` and, when `current` is set, every `releases/<ver>/` that is neither `current` nor `previous`, its exe first; one that fails (Windows: exe in use) is reported on stderr and kept for the next update. Read `latest.json`. Not newer than `current` (else `version`), or already present: no update.
2. Refuse a version with characters outside `[A-Za-z0-9.-]` or `..`, or a file other than `<name>-<ver>-<os>-<arch><exe suffix>`.
3. `check` stops here and reports the version.
4. Download the file and `SHA256SUMS` into `releases/<ver>.tmp/`; the file's sha256 must be listed with its name. Rename it to `<name>`, mode 0755.
5. Smoke: `<name> selftest` with `<ENV>_SELFTEST_OFFLINE=1`. Failure: delete the folder, report, keep current.
6. Rename to `releases/<ver>/`, write `previous` = old `current`, then `current` = `<ver>`. Replace the launcher with the new binary. Prune again, as in step 1. The consumer re-execs.

Versions compare by numeric dot parts; a `-pre` suffix sorts before its release (`4.1.0` > `4.0.12` > `4.0.0-dev`).
Replacing a binary on Unix, and the release binary of `install_self` everywhere: copy to `<target>.tmp-<pid>`, set the old file's mode, rename it over `<target>`; never write a binary in place (macOS kills a rewritten running binary); a failed copy deletes the temp file and leaves the old binary intact. Extended attributes (macOS quarantine) follow the source file, not the old one; Linux copies none. Replacing the launcher on Windows: renames the running `<name>` to `<name>.old`, or to the first `<name>.old.<n>` (n = 1, 2, ...) that is free or removable when an earlier one is still in use (a failed copy renames it back) and copies the new file. No live swap: a running launcher keeps its process until the service restarts.
`update::install_self`: copy the running binary to `releases/<version>/` and replace the launcher with it; make it `current` (old one `previous`) only when newer: never a downgrade.

## Launcher (`launcher::launch`)
- Runs `releases/$(current)/<name>` with the same arguments. No `current`: exit 1 with `run \`<name> update\``.
- Only `service run` counts starts of a version without `healthy`, per project: the counter is `releases/<ver>/starts/<project>`, `<project>` the name of the working directory (the project root). At a project's 3rd it writes `previous` into `current` and runs that only when it is not below `rollback-floor` (host mode below); starts of other projects do not count. `healthy` stays install-wide: the consumer writes it after `rollback_after`. A `starts` file of an older launcher is replaced.
- Unix: exec. Windows `service run`: the launcher supervises: frees its console, runs the child without a window, appends output to `<name>.log`; exit 75 restarts at once, any other exit after 60 s. Other Windows commands: run, exit with the child's code.
- Messages go to stderr as `<name>: <message>`.
- **Fault hook.** `Tool::fault` (default none) receives a `Fault` with `board`, `runtime`, `revision`, `contract` and `observed`/`inferred` empty; the consumer fills them and reports. Two points call it: the rollback above gives `update.rollback` (site `launcher:rollback`, fingerprint field and evidence `failed:<ver>`); on Windows a `service run` exit 101 gives `dispatch.panic` (site `launcher:exit`, evidence `exit:101`). No `previous` to restore: no fault. A hook that panics is caught: launcher behavior and exit code stay unchanged.

## Host service units (`service::host_unit`, `service::legacy_unit`)
`host_unit(t)` writes and loads one unit running `<install>/<name> service run` with `<install>` as its working directory and the caller's PATH. Names: launchd `com.<name>.service`, Scheduled Task `<title>`, systemd user `<name>.service`. launchd output goes to `<install>/<name>.log`; other settings match the legacy generators below. The consumer calls it only when installation or reload is safe; it does not drain children or migrate project configuration.

`legacy_unit(t, project)` unloads and deletes the legacy per-project unit and reads back unit absence from the native manager and filesystem. Unknown readback is an error, never success. The consumer must first verify that the legacy service and its children have exited; run removal from an idle helper when unloading would kill the caller. No other unit is removed. The legacy `unit` generator stays compatible.

For `service run` with `<install>` as working directory, unhealthy starts use `releases/<ver>/host-starts`, independent of project roots. The third start rolls back only if `previous` is at least `rollback-floor`; otherwise current stays. The floor setter accepts only valid versions, raises the floor atomically and never lowers it; it refuses a floor newer than current. The consumer serializes floor recording with topology installation. Recording the floor is separate from unit loading: only the consumer can verify host ownership. Legacy per-project starts keep their existing counters and also honor the floor.

## Legacy service units (`service::unit`)
One unit keeps `<install>/<name> service run` running in the project root with the caller's `PATH`, restarting on failure.

| OS | Unit | Load | Unload | Stop (until next logon) |
|---|---|---|---|---|
| macOS | `~/Library/LaunchAgents/com.<name>.<project>.plist`, `KeepAlive`, output `<install>/<project>.log` | `launchctl bootstrap gui/<uid>` | `launchctl bootout gui/<uid>/<label>` | same as unload |
| Windows | Scheduled Task `<title>-<project>` from `<install>/<title>-<project>.xml` (UTF-16 with BOM): this user's logon (`whoami`), no admin, no time limit, restart every minute | `schtasks /Create /XML /F`, `/Run` | `/End`, `/Delete /F` | `/End` |
| Linux | `~/.config/systemd/user/<name>-<project>.service`, `Restart=always`, journal | `daemon-reload`, `enable --now` | `disable --now` | `stop` |

`schtasks /End` ends only the launcher: the consumer also ends the service process tree. Commands run with captured output and no console window.

## status.json (`status`)
Each service pass writes `<root>/.<name>/status.json` atomically. Common keys: `host`, `version`, `pid`, `at` (unix seconds), `project`, `error` (null when the pass succeeded), `update` (a newer version found and not installed, else null). Consumers add their own keys. A reader treats a file older than its freshness limit (taskq: 60 s) or unreadable as unknown, never as zero or success.

## Selftest harness
The consumer's `selftest` command loads config, runs its built-in fixtures and, unless `<ENV>_SELFTEST_OFFLINE` is set, reads the board. It writes nothing. Update smoke (step 5) and release CI run it on each artifact; a version whose selftest fails is not published or installed.

## Incident reports (`incident`)
Implements the [taskq incidents design](https://github.com/taskqorg/taskq/blob/main/docs/design/incidents.md) for any tool. The consumer owns its fault catalog: it detects a fault at a fixed point (the launcher detects `update.rollback` and Windows `dispatch.panic` for it through `Tool::fault`, § Launcher) and passes a `Fault` (code, fingerprint fields, board, runtime, site, revision, contract, evidence, observed, inferred). coreq builds, checks, routes and stores the report; it never prints, panics out, exits or touches consumer state.

- **Switch.** Env `<ENV>_INCIDENT_REPORTS=off` or config `false`: `disabled`, nothing written or run. Config absent (an existing project before the owner chose): `local`, reason `not acknowledged`. Only `true` sends. `incident::disclosure` is the text setup and the first update show before any send: destination (the public `repo`), posting account, payload keys, fallback, opt-out.
- **Report.** Schema `<name>-incident/1`, one JSON object of at most 4 KiB with keys only `schema`, `fault`, `fingerprint`, `report_id` (random UUID), `release`, `revision`, `contract`, `board`, `runtime`, `os` (`<os>-<arch>`), `site`, `evidence` (at most 8), `observed`, `inferred`, `profile` (`profile::window`, § Profiling). Values are codes of `[A-Za-z0-9._:/-]`, at most 64 characters, no leading `/` or `//`: no free text, link or mention. `fingerprint` = first 16 hex of sha256 over the schema major, fault, its fingerprint fields, board, runtime and os; never the release. Any other key, value or size: `local`, reason `unsafe`, no file.
- **Login.** Only a github.com token: the board token when the board is github.com, else the `gh` login for github.com. None: `unsupported-auth`. HTTP goes to `api.github.com` through `curl`; the token goes on stdin, never in arguments.
- **Route.** Search the `repo` issues whose title holds the fingerprint: at most 2 pages of 100 and 5 s, then the exact marker `<!-- <schema> <fingerprint> -->` in the body; incomplete, failed or over budget: `local`, reason `search incomplete`. Open match: one `confirmed` comment unless this account already confirmed this release (at most 2 comment pages). Closed match: this release seen: no write; newer release: one `regression` comment; never reopen. No match: create the issue `<name> incident <fault> <fingerprint>` with label `incident`. At most one write per report; a failed write is looked up by `report_id` read-only, else `local`, reason `unknown delivery`. One report per fingerprint per process; no retry store.
- **Claim.** Before the search a report claims the fingerprint with an atomic create of `<install>/incidents/<fingerprint>`, writes the issue link into it once known and removes it when delivery fails. A sibling that finds the claim never creates an issue: with a link it routes as above but takes the link when the search misses the issue; without a link it skips, `local`, reason `claimed`, unless the claim is older than 10 minutes, then it reclaims.
- **Outcome.** `sent` (link), `local` or `unsupported-auth` (reason, `<root>/.<name>/incidents/<fingerprint>.json` with the sanitized JSON for manual submission), `disabled`. `Outcome::json` is the `status.json` `incident` value `{state, fault, at, path, url, reason}`. A report never reports itself: a nested or panicking report ends `local` without a write. The first report installs a panic hook that stays silent while a report runs and otherwise delegates to the previous hook, so a caught panic prints nothing; a hook the consumer sets later replaces it.

## Board token (`token`)
Env `<ENV>_TOKEN`, else `GITHUB_TOKEN`/`GITLAB_TOKEN`, else `<install>/token` (0600), else `gh auth token --hostname <host>` / `glab config get token --host <host>`: only the board host's login. Never in config, never sent anywhere but the board API.

## Profiling (`profile`)
Follows [specq profiling](https://github.com/taskqorg/specq/blob/main/engineering/profiling.md). coreq measures its own calls below; a consumer records its operations with the same `profile::record` and passes their budgets to `profile::report`.

| Operation | Call | p95 budget | Error budget |
|---|---|---|---|
| `forge.read` | GitHub API `GET` (incident login, search, comments) | 2 s | 5% |
| `forge.write` | GitHub API `POST` (incident issue, comment) | 5 s | 5% |
| `release.read` | `update::download` (`latest.json`, `SHA256SUMS`, binary) | 60 s | 5% |
| `token.read` | `Tool::token` and the `gh` login of `incident::github_token` | 1 s | 1% |

- **Counters.** Always on, in process memory: per operation count, outcomes `ok`, `failed`, `cancelled`, `retried`, max and the last 64 durations (p50, p95), error classes. Classes: `rate_limit` (HTTP 429, or 403 naming a rate limit), `auth` (401, other 403), `http`, `timeout` (curl exit 28), `network`, `json`, `spawn`, `missing` (no token). coreq never retries; a consumer that succeeds only after a retry records `retried`, not `ok`.
- **Store.** `profile::flush` merges memory into `<root>/.<name>/profile.json` atomically; the consumer calls it once per service pass. Bounds: 32 operations (least recently seen pruned), 64 durations and 8 classes (the rest `other`, also for a stored record) per operation, 256 trace spans. Names and classes are codes of `[a-z0-9._-]`, at most 32 characters: an operation with another name is dropped, another class becomes `other`. No secrets, content or paths. A failed write keeps the measurements in memory for the next flush. An unreadable store is replaced; a corrupt operation record (zero count, outcome sum that disagrees or overflows, a sample above `max` or more samples than calls) is dropped on flush; counters saturate, never wrap.
- **Traces.** Env `<ENV>_PROFILE=trace` at flush writes the spans `{op, ms, outcome, class, at}` recorded since the last flush; otherwise the store holds none. The store's `trace` key reports the switch.
- **Report.** `profile::report` lists every budgeted or stored operation: count, window (`since`, `at`), p50, p95, max, failure and retried rates, spread (p95/p50), classes, budget and state `ok`, `over` or `unbudgeted`. `slow` ranks measured operations by p95 over their budget (unbudgeted after, by p95), `failing` by failure rate, `unstable` by retried rate, then spread. Budget states and ranks compare unrounded values; the report rounds rates to 3 places and spread to 2 for display only. An operation without samples reads `unknown`, reason `no samples`; a corrupt record `unknown`, reason `corrupt`; never zero. No store: state `unknown`, reason `no samples`; unreadable store: `unknown`, reason `corrupt store`.
- **Incident link.** Every incident report carries `profile`, from `profile::window` over the store as of the last flush: `window:<since>-<at>` (else `window:unknown:<reason>`), then at most 7 operations over coreq's budgets as `<op>:over`, slowest first. The issue body's JSON and each `confirmed` or `regression` comment (line `profile <codes>`) carry it. Codes only, so the report's 4 KiB and code checks hold.

## Product connections
Accepted target: [shared connection client](docs/design/enrollment.md), following [shared rules](https://github.com/taskqorg/specq/blob/main/engineering/product-connections.md). This adds a linked client, not a service. The consumer-specific baseline above remains; connection behavior must pass the linked conformance and platform checks before release. Earlier pull-first dashboard research is superseded for this scope. `enroll` holds wire v1 checks, the pairing client with protected, crash-safe connection state, bounded delivery with accepted/projected receipts, revoke-only disconnect recovery and capability-scoped operation pull with server-set actor/workspace/profile authority and immutable results (`agentq.configure`, § Client there).
