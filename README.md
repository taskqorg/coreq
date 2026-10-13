# coreq

Shared Rust crate of taskqorg tools: release and update, launcher and rollback, service (launchd, Scheduled Task, systemd), `status.json`, selftest harness, incident reports. Extracted from [taskq](https://github.com/taskqorg/taskq) v4; the contract is [specq release-contract](https://github.com/taskqorg/specq/blob/main/engineering/release-contract.md). Decisions live in coreq.md. taskq is the reference consumer; pin the crate by git tag (`v0.2.0`).
