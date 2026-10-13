# coreq contract
coreq is the shared Rust crate of taskqorg tools: install layout, self-update, launcher and rollback, OS service units, `status.json`, selftest harness, board token. This file holds every decision; README and code follow it. [taskq](https://github.com/taskqorg/taskq) is the reference consumer; its `taskq.md` § 4 and § 8 define the behavior extracted here.

## Rules
- **Spec first.** A change that alters a decision edits this file first; code and README follow in the same deliverable.
- **No constants for a consumer.** Tool name, title, release repository, version and install folder are fields of `Tool`; a second consumer passes its own and needs no fork.
- **No behavior change on extraction.** With `Tool::new("taskq", "TaskQ", "alexkirs/taskq", VERSION)` every path, unit, message and check matches taskq.md § 8.
- **Dependencies.** Only crates the consumer already uses: `ring`, `serde_json`. No HTTP client: HTTP goes through the OS `curl`.
- **Checks.** `cargo test --locked`. CI (`.github/workflows/tests.yml`) runs it on Linux, macOS and Windows for pushes to `main` and `taskq-*` and for PRs; job `tests` is the required status.

## Tool
`Tool { name, title, repo, version, install }`. `name` names the binary (`<name>`, Windows `<name>.exe`), files and env vars; `ENV` below is `name` in upper case. `title` names the Windows Scheduled Task. `repo` is the GitHub `owner/name` of releases; it is built into the consumer binary, never read from project config: project files are written by agents, so config must not choose the code hosts run. `install` defaults to env `<ENV>_INSTALL`, else `~/.local/share/<name>`, Windows `%LOCALAPPDATA%\<name>`.

## Install layout
- `releases/<ver>/<name>`: immutable release folders; `releases/<ver>.tmp/` is a download in progress.
- `current`, `previous`: one version each, written atomically (temp file, then rename).
- `releases/<ver>/starts`, `releases/<ver>/healthy`: launcher counters (below).
- `<name>`: the launcher, a copy of the binary that first installed.
- `<name>.log`: Windows service output.

## Release
A `v*` tag builds `<name>-<ver>-<os>-<arch><exe suffix>` per OS (`os`: `macos`, `windows`, `linux`; `arch`: Rust's), `SHA256SUMS` and `latest.json` `{"version", "files": {"<os>-<arch>": "<file>"}}` under `https://github.com/<repo>/releases/latest/download/`. Trust is HTTPS to `repo` and who can push its tags; `ponytail:` no signature, add one when releases are served elsewhere. Release CI follows the [specq release contract](https://github.com/taskqorg/specq/blob/main/engineering/release-contract.md).

## Update (`update::update`)
1. Delete leftover `releases/*.tmp`. Read `latest.json`. Not newer than `current` (else `version`), or already present: no update.
2. Refuse a version with characters outside `[A-Za-z0-9.-]` or `..`, or a file other than `<name>-<ver>-<os>-<arch><exe suffix>`.
3. `check` stops here and reports the version.
4. Download the file and `SHA256SUMS` into `releases/<ver>.tmp/`; the file's sha256 must be listed with its name. Rename it to `<name>`, mode 0755.
5. Smoke: `<name> selftest` with `<ENV>_SELFTEST_OFFLINE=1`. Failure: delete the folder, report, keep current.
6. Rename to `releases/<ver>/`, write `previous` = old `current`, then `current` = `<ver>`. The consumer re-execs.

Versions compare by numeric dot parts; a `-pre` suffix sorts before its release (`4.1.0` > `4.0.12` > `4.0.0-dev`).
`update::install_self`: copy the running binary to `releases/<version>/` and to the launcher; make it `current` (old one `previous`) only when newer: never a downgrade.

## Launcher (`launcher::launch`)
- Runs `releases/$(current)/<name>` with the same arguments. No `current`: exit 1 with `run \`<name> update\``.
- Only `service run` counts starts of a version without `healthy`; at the 3rd it writes `previous` into `current` and runs that. The consumer writes `healthy` after `rollback_after`.
- Unix: exec. Windows `service run`: the launcher supervises: frees its console, runs the child without a window, appends output to `<name>.log`; exit 75 restarts at once, any other exit after 60 s. Other Windows commands: run, exit with the child's code.
- Messages go to stderr as `<name>: <message>`.

## Service units (`service::unit`)
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
Implements the [taskq incidents design](https://github.com/taskqorg/taskq/blob/main/docs/design/incidents.md) for any tool. The consumer owns its fault catalog: it detects a fault at a fixed point and passes a `Fault` (code, fingerprint fields, board, runtime, site, revision, contract, evidence, observed, inferred). coreq builds, checks, routes and stores the report; it never prints, panics out, exits or touches consumer state.

- **Switch.** Env `<ENV>_INCIDENT_REPORTS=off` or config `false`: `disabled`, nothing written or run. Config absent (an existing project before the owner chose): `local`, reason `not acknowledged`. Only `true` sends. `incident::disclosure` is the text setup and the first update show before any send: destination (the public `repo`), posting account, payload keys, fallback, opt-out.
- **Report.** Schema `<name>-incident/1`, one JSON object of at most 4 KiB with keys only `schema`, `fault`, `fingerprint`, `report_id` (random UUID), `release`, `revision`, `contract`, `board`, `runtime`, `os` (`<os>-<arch>`), `site`, `evidence` (at most 8), `observed`, `inferred`. Values are codes of `[A-Za-z0-9._:/-]`, at most 64 characters, no leading `/` or `//`: no free text, link or mention. `fingerprint` = first 16 hex of sha256 over the schema major, fault, its fingerprint fields, board, runtime and os; never the release. Any other key, value or size: `local`, reason `unsafe`, no file.
- **Login.** Only a github.com token: the board token when the board is github.com, else the `gh` login for github.com. None: `unsupported-auth`. HTTP goes to `api.github.com` through `curl`; the token goes on stdin, never in arguments.
- **Route.** Search the `repo` issues whose title holds the fingerprint: at most 2 pages of 100 and 5 s, then the exact marker `<!-- <schema> <fingerprint> -->` in the body; incomplete, failed or over budget: `local`, reason `search incomplete`. Open match: one `confirmed` comment unless this account already confirmed this release (at most 2 comment pages). Closed match: this release seen: no write; newer release: one `regression` comment; never reopen. No match: create the issue `<name> incident <fault> <fingerprint>` with label `incident`. At most one write per report; a failed write is looked up by `report_id` read-only, else `local`, reason `unknown delivery`. One report per fingerprint per process; no retry store.
- **Outcome.** `sent` (link), `local` or `unsupported-auth` (reason, `<root>/.<name>/incidents/<fingerprint>.json` with the sanitized JSON for manual submission), `disabled`. `Outcome::json` is the `status.json` `incident` value `{state, fault, at, path, url, reason}`. A report never reports itself: a nested or panicking report ends `local` without a write.

## Board token (`token`)
Env `<ENV>_TOKEN`, else `GITHUB_TOKEN`/`GITLAB_TOKEN`, else `<install>/token` (0600), else `gh auth token --hostname <host>` / `glab config get token --host <host>`: only the board host's login. Never in config, never sent anywhere but the board API.
