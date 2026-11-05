# Agent runtime boundary

[CoreQ R47-R51](../../coreq.md#agent-runtime-ownership) assign agent execution to
[AgentQ's runtime contract](https://github.com/taskqorg/agentq/blob/main/docs/spec/RUNTIME.md).
Consumers use `agentq_runtime::agent` for CLI, Claude background, Codex desktop and
app-server turns, native recovery, account selection, usage and limits. Dynamic
tools, private drafts and model catalogue requests use that same runtime.
Callers retain instructions, UI, durable execution ownership, input files,
process-group supervision, credentials and permission policy.

## Compatibility sessions

CoreQ v0.19.0 removed `coreq::agent`, its native/app implementations and migrated
runtime tests. There is no re-export, shim or dependency from CoreQ to AgentQ.
The runtime's deterministic client, recovery, terminal-correlation and once-only
usage protection lives in AgentQ's `runtime/src/agent.rs` and
`runtime/tests/agent.rs`; CoreQ's infrastructure checks remain here.

Old immutable CoreQ releases, including v0.18.0, retain their original API.
Existing consumer builds and AgentQ's published runtime keep their original
CoreQ pins; retirement does not require repinning them. A consumer using
`coreq::agent` must adopt an immutable AgentQ runtime before adopting this CoreQ
source boundary. The [retirement receipt](../evidence/agent-retirement-112.md)
records the first-party inventory, accepted pins and remaining qualification gaps.

## Preserved infrastructure and rollback

CoreQ still supplies generic bounded process execution/identity, local sockets,
locks, account-independent telemetry validation/storage/transport, update,
launcher/rollback and service units. Agent metadata and historical telemetry
remain readable and writable under the existing schema and privacy rules.

Retirement performs no installation, pruning, host restart or active-session
retirement. Native session homes, execution checkpoints, replay boundaries,
permissions and old builds remain untouched. Rollback selects the previous
immutable consumer build with its original pins; it never replays paid input or
changes accounts, model, effort, provider or grants. Live forward/rollback and
native client qualification remain consumer-owned.
