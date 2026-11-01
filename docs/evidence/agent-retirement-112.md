# CoreQ agent retirement receipt

CoreQ v0.19.0 removes `coreq::agent`; AgentQ owns the runtime. This implements
[task #112](https://github.com/taskqorg/coreq/issues/112) and its amended A2 under
the [approved architecture](https://github.com/taskqorg/assetq/blob/0b9af20891d769ed989c7aa7ca9b8b7e9ab79930/docs/architecture.md).
The owner instruction and acceptance are preserved at that task, including the
instruction to proceed after accepted adoption while carrying native gaps in Open.

## Contract and source boundary

R47-R51 and [agent detail](../design/agent.md) were updated before implementation
removal. AgentQ owns adapters, native sessions/recovery, account selection and
usage/limits. CoreQ retains process execution/identity, local sockets, locks,
telemetry, update, launcher/rollback and service primitives. No upward dependency
or runtime shim is introduced (R47).

Before: v0.18.0 exported `pub mod agent` and contained `src/agent.rs`,
`src/agent/{native,app}.rs` and `tests/agent.rs`. The retirement boundary audit
therefore reported the expected FAIL. After: v0.19.0 removes that export, those
four files and only their explicit fake-client Cargo test target. The same audit
checks their absence, no AgentQ dependency, and unchanged infrastructure and
remaining test sources. This is source-boundary evidence, not a native runtime
failure reproduction or live qualification.

## First-party inventory

`gh repo list taskqorg --limit 100` and the organization repositories REST endpoint
both enumerated these eight repositories. Immutable source archives were read
without executing downloaded code. Manifests, lockfiles and Rust/JS/Python source
were searched for CoreQ dependencies, legacy agent imports and AgentQ imports.
This inventories source/dependency consumers, not installed hosts or private
session data. Revisions below freeze the observation; mutable branches are not pins.

| Repository / source revision | Dependency and source readback | Compatibility |
|---|---|---|
| [TaskQ](https://github.com/taskqorg/taskq/tree/aebc4f167bed39228dab30a8c4e0a7358808ccdb) | `Cargo.toml:10-11`, `Cargo.lock:14,265`: AgentQ runtime revision below and CoreQ v0.18.0 (`e84761cdab43723b871a42b3021e9c39afcd9484`). `src/runtime.rs`, `src/runtime/owner.rs` use `agentq_runtime::agent`; no legacy agent import remains. `package.json:13` and `package-lock.json:11,488-490` pin `@taskq/coreq` to the same full `e84761cdab43723b871a42b3021e9c39afcd9484` codeload tarball; `src/ui-runtime.js:3` uses only its `shared`/`connect` telemetry helpers. | Worker, reviewer and owner/operator paths adopted; existing CoreQ pin retained. `coreq-instrument` (`Cargo.toml:67`, `Cargo.lock:280`) uses that same immutable CoreQ SHA. |
| [PilotQ accepted adoption](https://github.com/taskqorg/pilotq/tree/226a340381b16471e114011188863b5f8efb8a17), [sampled main](https://github.com/taskqorg/pilotq/tree/233a12b2b9c6363f449acb451b5a57faafe7b090) | `Cargo.toml:9-10`, `Cargo.lock:25,264`: same AgentQ runtime and CoreQ v0.18.0 pins. `src/{manager,claude,host,main,slots,lib}.rs` use AgentQ. Accepted and sampled `src/lib.rs` match. | Production adopted. `src/lib.rs:128,130` deliberately still uses old `coreq::agent` inside `sessions_294_preserve_protocol_recovery_and_usage`, a rollback test protected by its old pin. Migrate this fixture before a future CoreQ v0.19.0 pin upgrade. |
| [AgentQ](https://github.com/taskqorg/agentq/tree/b2887ec28f258ac31466024d1065dc41ad59c344) | Root `Cargo.toml:12-13` uses CoreQ v0.15.0 (`cce23bdfd1854d287e5e8a453e689370ece90d5b`) and the workspace runtime. `runtime/Cargo.toml:13` pins CoreQ v0.18.0 (`e84761c` above); `runtime/src/lib.rs:8` uses `process`, `quiet`, `telemetry`, never legacy agent. | `agentq/main.rs` dispatches the runtime CLI. `control.rs`/`compression.rs` use runtime optimization policy, not the retired adapter. Python Hermes/provider integrations invoke `agentq compress` and forward optimization intent; they do not import CoreQ. |
| [AgentQ external example](https://github.com/taskqorg/agentq/tree/b2887ec28f258ac31466024d1065dc41ad59c344/examples/runtime-consumer) | `Cargo.toml:9`, `Cargo.lock:8,40`: immutable runtime revision below and CoreQ v0.18.0; `src/main.rs:2-3` imports runtime agent/optimization. | No path patch; accepted upstream receipt ran both deterministic clients from the remote pin. |
| [DashQ](https://github.com/taskqorg/dashq/tree/b781279c1df83003e6899f26012f2852a7ddb9ee) | `Cargo.toml:8`, `Cargo.lock:42`: CoreQ v0.11.0 (`ac29532e63f7a661c9091ad166aaee4f1f4e4941`). | Infrastructure only; no legacy agent source import or AgentQ runtime dependency. `assets/taskq/project-blocks.js:12-14` also bundles CoreQ telemetry runtime from this immutable DashQ snapshot; no separate npm runtime dependency. |
| [ServerQ](https://github.com/taskqorg/serverq/tree/c29a2b248ff4f251491abb7493511d7fe8f01ef4) | `Cargo.toml:8`, `Cargo.lock:24`: same CoreQ v0.11.0 revision as DashQ. | Infrastructure only; no legacy agent source import or AgentQ runtime dependency. |
| [AssetQ](https://github.com/taskqorg/assetq/tree/0b9af20891d769ed989c7aa7ca9b8b7e9ab79930) | No CoreQ or AgentQ runtime dependency/import in sampled manifests/sources. | Architecture/intake consumer; no adapter to migrate. |
| [SpecQ](https://github.com/taskqorg/specq/tree/537db47c9f01c17d8cf6259a3698d8bd15435d16) | No CoreQ or AgentQ runtime dependency/import in sampled manifests/sources. | Shared rules only. |
| [CoreQ baseline](https://github.com/taskqorg/coreq/tree/e84761cdab43723b871a42b3021e9c39afcd9484) | `src/lib.rs` exported the module; `tests/agent.rs:3` imported it. No other in-crate caller. | Export, implementation and migrated checks retired by this candidate. |

The immutable runtime source is
[`513193c8a96f3de1c71a86291a679230817ce0b2`](https://github.com/taskqorg/agentq/tree/513193c8a96f3de1c71a86291a679230817ce0b2/runtime),
`agentq-runtime` 0.1.0. Its eight runtime files match accepted AgentQ merge
`b2887ec28f258ac31466024d1065dc41ad59c344`. Its own CoreQ v0.18.0 dependency
uses infrastructure only and need not change to retire the newer CoreQ source.

## Accepted prerequisite receipts

All three cross-repository dependencies were already recorded on #112 before work.
Their issues read back closed/completed, with these immutable merge receipts:

- [AgentQ #182 close](https://github.com/taskqorg/agentq/issues/182#issuecomment-6556308410):
  PR #183 merged as `b2887ec28f258ac31466024d1065dc41ad59c344`, merge-result CI green.
  [Accepted review](https://github.com/taskqorg/agentq/issues/182#issuecomment-6556239784)
  identifies the published runtime pin, deterministic external consumer and
  exact-result Linux/macOS/Windows checks.
- [TaskQ #2111 close](https://github.com/taskqorg/taskq/issues/2111#issuecomment-6558251430):
  PR #2130 merged as `aebc4f167bed39228dab30a8c4e0a7358808ccdb`, merge-result CI green.
  [Accepted review](https://github.com/taskqorg/taskq/issues/2111#issuecomment-6558171020)
  covers candidate `387645ddbbf021bd2457151055feabb9f5868264`, Windows selected gate,
  all agent paths, recovery, optional optimization and the immutable pin.
- [PilotQ #294 close](https://github.com/taskqorg/pilotq/issues/294#issuecomment-6558006676):
  PR #319 merged as `226a340381b16471e114011188863b5f8efb8a17`, merge-result CI green.
  [Accepted review](https://github.com/taskqorg/pilotq/issues/294#issuecomment-6557932081)
  covers candidate `983ac1589a86ca0b3f0b1908e67adbd451df3ce7`, protocol/tools/drafts/
  catalogue, journal schema 13, checkpoint compatibility and optional optimization.

## Retained risk protection

The retired checks protect runtime behavior now owned by AgentQ, rather than
infrastructure behavior removed from CoreQ. Source comparison against the
published runtime pin found all six original `#[test]` cases retained in
`runtime/src/agent.rs`: account order/disable/failover, start/resume argument order,
message deduplication/unknown usage, snapshots/reasons, limit/login/capacity
classification and valid metadata observations.

All nine `tests/agent.rs` harness cases remain runnable in AgentQ's
`runtime/tests/agent.rs`: Claude start/resume, Codex thread/account home, lingering
process stop, deadline, background native mode, input/terminal/recovery binding,
app-server session/policy/accounting, readiness/history ownership and native
tools/drafts/catalogue. Its manifest retains `harness = false`; three additional
traces cover plain JSON turns, optimization off and the external consumer.
The upstream accepted runtime receipt reports 11 unit and 12 fake-client cases
passing; this task does not claim to have rerun consumer or paid client tests.

CoreQ's existing process, durable-file, telemetry privacy/schema/history/fixture,
service, update, launcher, enrollment and instrumentation protection is retained.
No generic telemetry fields, schema, fixtures or historical rows are removed
(R27-R29, R46, R49-R50). No replacement framework or dependency is added.

## Checks and preservation

The R5 selected merge gate is `python3 scripts/check.py`; Rust changes select
`cargo +stable test --locked`, plus selector/tag regressions and pinned contract
lint. R6 requires Linux/macOS/Windows CI within 900 seconds per runner. The task
result identifies the frozen candidate and actual local command outcomes.
GitHub publication requires green `merge_group` CI; a branch run or this receipt
does not prove that result. The worker opens one PR and hands it to review without
waiting for CI (TaskQ §7).

Only source and documentation in the task worktree changed. No native client was
started, no installation/prune/restart/permission operation ran, and no session or
recovery data was read or rewritten (A4, R13, R47-R51). Existing immutable CoreQ
tags and consumer pins remain the rollback boundary. Forward adoption of this
CoreQ version is a separate consumer pin decision; no existing build must move.

## Open

- TaskQ: live Mac/win Q1/Q6/Q15, Claude (limit 0), live usage baseline and live
  forward/rollback remain unverified. Native Claude overlay/Codex desktop
  optimizer effect remains unverified.
- PilotQ: pds18 Codex, Mac Claude (limit 0), exact-merge A40/cleanup and live
  restore remain PM/consumer host checks; no production cutover is claimed.
- AgentQ's local macOS legacy fixture/process inventory was sandbox-blocked;
  its accepted CI and deterministic runtime results do not replace native proof.
- This candidate's Linux/macOS/Windows merge-result CI remains the publication
  gate. No live host qualification, deployment or active-work retirement is claimed.
