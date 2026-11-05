# Compatibility API retirement

[CoreQ #126](https://github.com/taskqorg/coreq/issues/126) authorizes removal of
the combined installation and legacy unit removal APIs, with the accepted
[A1 scope answer](https://github.com/taskqorg/coreq/issues/126#issuecomment-6660245987)
and [clarification](https://github.com/taskqorg/coreq/issues/126#issuecomment-6660250011).
R13-R15 and R20 describe the remaining installation and service boundary;
R29 and R41 retain telemetry upgrades and profiling.

## Consumer source inventory

Each main was fetched again at continuation start on 2026-11-05. Searches used
`git grep` over tracked source at the fetched commit, including imports, aliases
and every occurrence of `install_self`, `legacy_unit`, `prepare_install` and
`activate_install`. None of the six products calls either retired CoreQ API.

| Product | Main SHA | Installation source |
|---|---|---|
| TaskQ | `96064118e637ea8d7347edc4ca50c5c5da186757` | `src/platform.rs` preparation/activation wrappers; ordered host installation in `src/host_install.rs` |
| AgentQ | `3669a53ddc547c659f014239dc592f737bb09ae9` | `agentq/main.rs:634,680` calls its own combined installer, not CoreQ's; migration is filed separately on the AgentQ board |
| DashQ | `5d42c599d07943cdf9ea50f42159b7eaf6534f53` | `src/main.rs:898-899` prepares then activates |
| PilotQ | `79f962e9b42a3deb5d63741b54a3a0eedf446c07` | `src/main.rs:740` prepares then activates |
| ServerQ | `29dc7b9eeeef2595b24168235baf3d4b5941abf1` | `src/main.rs:415-416` prepares then activates |
| AssetQ | `87c7e8242c283340f358076c35cce3fdcee6625b` | `docs/deploy.md` delegates deployment to a host task; no self-update or CoreQ installation call, so A1 does not apply |

For each product, reproduce against the SHA above:

```sh
git grep -n -E 'install_self|legacy_unit|prepare_install|activate_install' <sha> -- '*.rs' '*.py' '*.js' '*.ts'
```

This is source compatibility evidence, not a claim that AgentQ or AssetQ adopted
transactional installation, or that any running consumer upgraded.

## Before and after

At baseline `f8ce4cb612d413c4559d32501d691137f38cd246`, source-absence assertions
failed for `update::install_self` and `service::legacy_unit`/`remove_legacy`.
After deletion, the same one-time assertions pass; preparation, activation,
`service::unit` and the profile export remain present. No runtime defect is
claimed: this fixes the obsolete public source surface and documentation.

`legacy_removal_requires_readback` is deleted because the entire removal operation
is retired by A2/R20. Retained checks protect the surviving risks:
`prepared_install_preserves_selection_and_existing_release`,
`activation_refuses_missing_preparation_and_unsafe_selection`,
`launcher_is_replaced_by_each_install`,
`copy_new_renames_and_keeps_the_old_file_on_failure`, `host_units_per_os` and
`units_per_os_name_the_tool`. No new persistent test infrastructure is needed
for deleting unused APIs.

AgentQ still imports CoreQ profiling in `agentq/profile.rs:3`, calling
`profile::summarize` at line 126 and `profile::rank` at line 212. CoreQ also uses
profiling in `src/lib.rs`, `src/incident.rs` and `src/update.rs`. Profile code,
telemetry schema upgrades and their checks are unchanged. No inventory proves
all live stores reached schema 6, so all upgrade paths remain.

The R5 candidate gate and its platform outcome are recorded in the task result
and PR. Native service installation/restart, live-store access, consumer builds
and release publication are outside this cleanup's source verification.
