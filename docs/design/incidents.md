# Incident routing detail

Binding detail of [coreq.md § Incident reports](../../coreq.md#incident-reports-incident). The switch, report schema, value rules and login there take precedence; this file holds search, routing, claim and outcome rules.

## Search

- Search the `repo` issues whose title holds the fingerprint: at most 2 pages of 100 and 5 s, then the exact marker `<!-- <schema> <fingerprint> -->` in the body.
- Incomplete, failed or over budget: `local`, reason `search incomplete`.

## Route

- Open match: one `confirmed` comment unless this account already confirmed this release (at most 2 comment pages).
- Closed match: this release seen: no write; newer release: one `regression` comment; never reopen.
- No match: create the issue `<name> incident <fault> <fingerprint>` with label `incident`.
- At most one write per report; a failed write is looked up by `report_id` read-only, else `local`, reason `unknown delivery`.
- One report per fingerprint per process; no retry store.

## Claim

- Before the search a report claims the fingerprint with an atomic create of `<install>/incidents/<fingerprint>`, writes the issue link into it once known and removes it when delivery fails.
- A sibling that finds the claim never creates an issue. With a link it routes as above but takes the link when the search misses the issue.
- Without a link it skips, `local`, reason `claimed`, unless the claim is older than 10 minutes, then it reclaims.

## Outcome

- `sent` (link), `local` or `unsupported-auth` (reason, `<root>/.<name>/incidents/<fingerprint>.json` with the sanitized JSON for manual submission), `disabled`.
- `Outcome::json` is the `status.json` `incident` value `{state, fault, at, path, url, reason}`.
- A report never reports itself: a nested or panicking report ends `local` without a write.
- The first report installs a panic hook that stays silent while a report runs and otherwise delegates to the previous hook, so a caught panic prints nothing; a hook the consumer sets later replaces it.
