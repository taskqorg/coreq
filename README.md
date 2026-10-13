# coreq

Shared Rust crate of taskqorg tools: release and update, launcher and rollback, service (launchd, Scheduled Task, systemd), `status.json`, selftest harness, incident reports. Extracted from [taskq](https://github.com/taskqorg/taskq) v4. Decisions live in coreq.md; releases follow the [specq release contract](https://github.com/taskqorg/specq/blob/main/engineering/release-contract.md). taskq is the reference consumer; pin the crate by the latest git tag in CHANGELOG.md.
