# Agent runtime detail

This page holds the binding detail of [coreq.md § Agent runtimes](../../coreq.md#agent-runtimes-agent). The rules R47-R51 live there. The behavior is TaskQ's CLI adapter (`src/runtime.rs` and `src/runtime/accounts.rs` at TaskQ v4.6.57), moved unchanged.

## Session model

- One native session belongs to one conversation or one task attempt.
- A new start passes a caller-chosen session ID to Claude (`agent::new_session` gives a UUID v4). Codex names its thread in the first `thread.started` event; `new_session` gives `-` until then.
- Each turn is one client process. The process reads the turn text from stdin and exits when the turn ends. The next turn resumes the same session ID.
- The client compacts its own context. For Claude, the caller passes `agent::claude_settings(Some(hook), env)` in `--settings`: a `SessionStart` hook on `compact` runs the caller's command, which prints the rules again. The settings `env` survives resume.
- Output of every turn appends to one caller log per session. A turn reads only the bytes it appended.

## Arguments

The caller builds the base command: the client executable from `agent::executable`, or a sandbox wrapper around it. `agent::command` appends the arguments, sets the project root as working directory, removes `CLAUDE_CODE_SESSION_ID` and `CODEX_THREAD_ID`, adds the caller's environment and sets the account home.

| Client | Start | Resume |
|---|---|---|
| Claude | `-p --output-format stream-json --verbose <args> --session-id <id>` | `-p --output-format stream-json --verbose <args> --resume <id>` |
| Codex | `exec --json <args> -C <root> -` | `exec --json <args> resume <thread> -` |

- `<args>` are the caller's client arguments in this position: TaskQ passes `--permission-mode <mode> --settings <json>` to Claude and its sandbox arguments to Codex.
- `agent::which` resolves a name on `PATH`; on Windows it also tries `<name>.exe` and the npm shim `<name>.cmd`. `agent::user_bin_path` appends `~/.local/bin` when `PATH` lacks it.
- `agent::codex_native` replaces an npm `codex.js` launcher with its native executor on macOS and Linux, so a kill reaches the client. It never scans other installations.

## End of a turn

- A turn ends at a Claude `result` event or a Codex `turn.completed` or `turn.failed` event.
- A process that is still running after its turn-end event is stopped: Codex on Windows kept running after its turn ended.
- `Turn::wait(deadline)` stops the process at the deadline; the turn then reads `cancelled` with error `timeout`.
- `Turn::poll` serves a caller loop that does not block.

## Outcome

`Turn::report` classifies the turn in this order; `Report::read` alone has no deadline or exit code:

| Condition | Outcome | Error |
|---|---|---|
| Deadline passed | `cancelled` | `timeout` |
| Limit reached before work started | `refused` | `usage-limit` |
| Codex login rejected (refresh token or 401) | `failed` | `auth-rejected` |
| Codex `turn.failed` naming model capacity | `failed` | `model-capacity` |
| Codex `turn.failed` | `failed` | `turn-failed` |
| Claude `result` with `is_error: true` | `failed` | `client-error` |
| Turn-end event | `ok` | `none` |
| No turn-end event, nonzero exit | `failed` | `exit` |
| No turn-end event, exit 0 | `unknown` | `no-end` |

A reached limit returns `Report::reached`: the reported reset in unix seconds, or none. Claude reports it on the first `assistant` event (`apiError: usage_limit_reached` or `quotaLimits.status: rejected`, reset `quotaLimits.resetsAt` or `apiErrorParams.rate_limit_info.resetsAt`). Codex reports it as `error` or `turn.failed` text containing `usage limit`, without a reset. A limit after work started is not a start refusal.

## Usage

- Claude: each `assistant` message ID counts once, because one message spans several lines. Uncached input is `input_tokens`, cache read `cache_read_input_tokens`, cache write `cache_creation_input_tokens`, output `output_tokens`. The model is the one of most calls.
- Codex: each `turn.completed` counts. Its `input_tokens` include `cached_input_tokens`, so uncached input is the difference. Codex reports no cache writes (0), no call count and no model in `exec --json` output.
- A counted call without a value makes that sum unknown, never smaller. No counted call: every count is null with reason `no-usage`.
- A model ID is a code of at most 64 characters from `[A-Za-z0-9._:-]`; any other value is not reported.

## Limit snapshot

| Client | Source | Used percent | Reset | Window |
|---|---|---|---|---|
| Claude | last `rate_limit_event` of the turn | `rate_limit_info.utilization` (0-1) × 100 | `rate_limit_info.resetsAt` | `rate_limit_info.rateLimitType` (`five_hour`, `seven_day`) |
| Codex | last `rate_limits` of the turn output, else the last `token_count` in the thread rollout `<home>/sessions/<yyyy>/<mm>/<dd>/rollout-*-<thread>.jsonl` | `primary`/`secondary` `used_percent` (0-100) of the window with higher use | that window's `resets_at` (or now + `resets_in_seconds`) | that window's `window_minutes` as `<n>m` |

- Claude also sends `unifiedWindows` with each window; the top-level window is the binding one, so only it is recorded.
- Codex `exec --json` output carries no limit snapshot today; the rollout of the account home does. A rollout snapshot is the latest one of the thread, which can be from an earlier turn.
- Percent is rounded to a whole number.
- Reasons: `not-reported` (no snapshot), `no-rollout` (Codex rollout not found), `partial-snapshot` (percent or reset missing).

## Observation

`agent::run` appends one observation per turn; `Report::observation` builds it for a caller that runs turns itself.

- Keys: `product`, `version`, `operation` and `kind` = `agent.turn`, `started_ms`, `duration_us`, `outcome`, `error`, `project`, `runtime`, `account`, `task`, `attempt`, `route`, `model`, `input_tokens`, `cache_read_tokens`, `cache_write_tokens`, `output_tokens`, `limit_used_percent`, `limit_resets_at`, `limit_window`, `unknown`.
- `trace` and `span` are null with reason `not-traced`. A task turn has `route` null with reason `task-turn`; a route turn has `task` and `attempt` null with reason `no-task`.
- A failed append is kept in `Report::recorded`; it never changes the turn's result.

## Accounts

- `Accounts { list, limits }` holds one client's ordered accounts and their recorded limits. The caller stores both.
- An empty list is one enabled `default` account. A label is 1-32 of `[a-z0-9-]`, not starting with `-`.
- A limit ends at its reported reset, else `PROBE` (1800 s) after it was seen.
- `pick`: the first enabled account whose limit has ended, else the first enabled account; none when all are disabled. `spare`: the account a limit moves to, only with more than one account. `hold`: the earliest live limit end of an enabled account and whether it was reported.
- Homes: `default` keeps the caller's own home (`CLAUDE_CONFIG_DIR` / `CODEX_HOME`, else `~/.claude` / `~/.codex`; a Windows service uses `USERPROFILE`). Any other account is `<home>/<tool>-accounts/<label>`, with `<tool>` the consumer's `Tool.name`. `command` sets the home variable for the child.

## Not covered

- Claude background (`claude --bg`) sessions and Codex app turns keep a process after the turn and need native job recovery. They stay with the consumer.
- Rule text, sandbox policy, permission mode and attempt logs are consumer decisions; the caller passes them in.
