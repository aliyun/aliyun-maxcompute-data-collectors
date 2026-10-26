# Acceptance evidence: engine SQL matrix on Metabase 0.63.18

- Date: 2026-10-23
- Artifact: a JAR built from this branch with `./scripts/build-driver.sh`
- ODPS JDBC: 3.10.14, bundled
- Artifact build baseline: Metabase 0.63.16. The branch is verified against
the current stable minor; the published 0.1.1 baseline itself is unchanged.
- Metabase under test: an existing Metabase 0.63.18 instance (OSS). Instance
  identifier, host and credentials omitted.
- MaxCompute: an existing real project; project id, table identifiers and
  access keys omitted. Only read-only statements were issued. Row counts and cell
  values are deliberately not reproduced here.

## Five-check acceptance suite (`CONTRIBUTING.md`)

| # | Check | Result |
| --- | --- | --- |
| 1 | Plugin loads without initialization errors | Pass — the driver answers queries on the instance |
| 2 | A real MaxCompute database can be created | Pass — connection is registered and queryable |
| 3 | Full schema and field sync completes | Pass — schema listed, 19 fields synced on the sampled table |
| 4 | `SELECT 1 AS e2e_value` completes | Pass — returns `1` |
| 5 | An MBQL query against a synced real table completes | Pass — see the matrix below |

## MBQL and SQL generation matrix

Every row was compiled by the driver and executed, read-only, against the project
above. "Pass" means the statement the driver generated was accepted by the engine
and returned a well-formed result.

| Shape | Result |
| --- | --- |
| Browse (`SELECT` over all synced fields) | Pass |
| `:count` / `:sum` / `:avg` / `:min` / `:max` / `:distinct` | Pass |
| `:stddev` / `:var` / `:median` / `:share` | Pass |
| `:percentile` on a field, and on an expression | Pass |
| `:sum-where` / `:count-where` / `:distinct-where` / `:cum-sum` / `:cum-count` | Pass |
| Expression breakouts: string, math, conditional (`:case`), regex, date | Pass |
| `:binning` with an explicit bin width | Pass |
| `:extract` year / quarter / month / week / day / hour / minute | Pass |
| `:datetime-add` / `:datetime-subtract` in day / week / month / year / hour | Pass |
| `:datetime-diff` in day / month / year / hour | Pass |
| Filter: relative-datetime, absolute-datetime, time-interval week/month/quarter | Pass |
| Filter: `:is-null`, `:contains`, `:starts-with`, `:ends-with` (case-sensitive and not) | Pass |
| `:convert-timezone` on an instant column | Pass |
| Nested query / aggregate over a source query | Pass |
| `LEFT` / `INNER` / `RIGHT` / `FULL` join | Pass |
| Ordering, `:limit`, sorting by aggregation | Pass |
| Native query with a text parameter | Pass |
| Native query with a field-filter parameter | Pass |
| SQL shown by "view the SQL" / the query log | Pass — valid, copy-pastable backticks |

## Engine defects these changes fix

- `:absolute-datetime` filters previously rendered `TIMESTAMP(datetime"...Z")`.
  A `datetime` literal never accepts a zone suffix; instants are now inlined as
  `timestamp"<UTC wall clock>Z"`.
- Time-of-day literals are anchored to the epoch date; `datetime"hh:mm:ss"` alone
  is not a valid literal.
- `LIKE` filters previously carried `ESCAPE '\'`, which the engine rejects. The
  engine has a built-in backslash escape, so the clause is now omitted.
- The SQL Metabase displays is now formatted with the MySQL dialect. The default
  fallback dialect rewrote backtick identifiers as `` ` tbl `.` col ` ``, which
  the engine cannot resolve.
- `:percentile` / `:median` with a renamed aggregation, and column qualification
  inside source queries (see `CHANGELOG.md`, Unreleased).

## Known limitations

- `:nested-queries`, `:fingerprint`, `:set-timezone` and `:persist-models` remain
  disabled: they were not verified end to end against this engine.
- `:binning` with the automatic strategy needs a field fingerprint, so an explicit
  bin width is required while `:fingerprint` is disabled.
- Type coercion is Metabase metadata, not driver behaviour: a text column mapped
  with a temporal coercion strategy is only usable as that temporal type if the
  stored values match the strategy.

## Sanitization

No access keys, project credentials, session tokens, instance identifiers or full
server logs are included. The checks were executed against a live project with
read-only statements; temporary fixtures were not created and no customer data is
reproduced.
