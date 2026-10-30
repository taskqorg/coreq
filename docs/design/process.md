# Bounded command execution detail

Binding detail of [coreq.md § Bounded command execution](../../coreq.md#bounded-command-execution-process). The rules there take precedence; this file holds cleanup ownership, adapter deadlines and the native identity and termination mechanisms.

## Cleanup ownership

- Nonblocking pipe reads use no detached reader threads. A stdin exchange has at most one owned writer thread, sharing the execution deadline and capacity.
- Cleanup retains its handle and permit until the child is reaped and the writer finishes; inherited stdin may retain capacity but cannot block the caller or create unbounded threads.
- On error or unwind, terminate the owned direct child. A single bounded reaper owns unfinished cleanup and its observation until try_wait confirms exit and input completion; do not block the caller on wait or lose the child handle.
- Unreaped children retain capacity and unknown cleanup status. Unexpected cleanup-owner failure reports failure and retains its child handle/capacity until process exit.
- Kernel spawn/I/O calls and OS scheduling are not hard real-time guarantees.

## Adapter deadlines

- HTTP exchange adapters preserve credentials on stdin and their HTTP parsing/error rules. Enrollment uses 15 s around curl's 10 s; incidents use their existing remaining deadline plus 5 s.
- HTTP protocol success requires a successful process and a complete response; timeout or failed input never makes a partial response successful.
- Shared adapters use 30 s for native service commands/readback/identity, Windows storage identity/ACL helpers and update candidate selftest; downloads use 305 s around curl's existing 300 s limit.
- A timed-out selftest never promotes a candidate. Ignored native nonzero exits retain their captured failure; spawn/read/timeout failures are never ignored.
- CLI credential lookup uses 10 s and the same bounded executor. A failed lookup is unavailable, not evidence that no credential exists; the legacy token counter uses `unknown`, while the executor retains the specific failure. Captured output remains private to the token adapter.
- Windows storage protection keeps the same owner ACL and refuses failed identity, empty identity, nonzero exit, timeout and output overflow.
- Collector bootstrap/maintenance disables tracing around its own native helpers to avoid recursive collection; connection callers retain helper failures.

## Process identity

- `process::identity(pid)` reads a PID plus its native creation identity: Linux boot ID/start ticks, macOS creation seconds/microseconds, Windows process creation FILETIME. Missing PID returns None.
- On Windows, a signaled process handle also returns None even while another handle keeps the exited process object accessible; zero-time native wait errors remain unknown.
- Invalid PID, denied access, an existing process whose creation identity is unavailable (including hidden/zombie cases), malformed native data and unsupported OS return an error.
- Equality identifies one native process, not its descendants or external effects. Consumers persist only identities needed for their existing recovery records and must use owned handles/native exit evidence for control.
- Telemetry records the operation/outcome, never raw identity values. TaskQ is the first consumer; existing command APIs remain unchanged.
- Target OS qualification is required before relying on the read for recovery.

## Process termination

- `process::terminate(&Identity)` requests termination only through a native mechanism that binds the signal to the verified process incarnation. Success confirms the request, never exit or descendant termination.
- macOS reads the native PID version, verifies the saved creation identity, and uses the kernel audit-token signal check.
- Linux opens a pidfd before checking the saved boot/start identity, signals through that descriptor, and closes it on every outcome.
- Windows opens a handle with query, synchronization and termination rights, compares the saved creation FILETIME and live state through that same handle, then calls TerminateProcess on it; PID reuse cannot retarget the request.
- Denied, missing, changed or unsupported native capabilities return an error without a PID-only fallback. Other targets currently return unsupported.
- Consumers must still read native exit before releasing capacity. Operation telemetry excludes identity and token payloads.
- Target-platform checks qualify each mechanism separately.
