# Changelog

## Unreleased
- Update prunes `releases/<ver>/` folders that are neither `current` nor `previous`, before and after installing; a release whose exe is in use on Windows is reported on stderr and kept (#20).

## v0.5.0
- Incident reports: an atomic claim `<install>/incidents/<fingerprint>` lets racing processes of one install open one issue per fingerprint; a sibling comments or skips (#14).
- Launcher counts unhealthy `service run` starts per project in `releases/<ver>/starts/<project>`; `healthy` stays install-wide. taskq 4.1.9 must bump its coreq pin from `v0.4.0` to `v0.5.0`.
