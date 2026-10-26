# Changelog

All notable changes to the MaxCompute Metabase Driver are recorded here.

## [Unreleased]

- Fix SQL shapes this engine rejects, all verified against a live project:
  `TO_DATE(x, 'yyyymmddhhmiss')` instead of `PARSE_DATETIME`, two-argument
  `DATETRUNC(x, 'hh')` instead of the three-argument form, `EXTRACT` without a
  timezone argument, composite `quarter` / `day-of-week` / `week` truncation,
  `DATEDIFF`-based `datetime-diff`, `GETDATE()` for "now", and `DOUBLE` for
  `->float`.
- Inline instants as `timestamp"<UTC wall clock>Z"`; a `datetime` literal never
  accepts a zone suffix, so absolute-datetime filters previously failed with
  `ODPS-0130161: invalid DATETIME format`. Time-of-day literals are anchored to
  the epoch date for the same reason.
- Drop the `ESCAPE '\'` clause that core adds to `LIKE` filters. The engine has
  a built-in backslash escape and rejects the SQL-standard `ESCAPE` clause, so
  every `:contains` / `:starts-with` / `:ends-with` filter failed.
- Enable the capability flags the engine was verified to support:
  `:percentile-aggregations` (`PERCENTILE(x, p)` and `MEDIAN(x)`),
  `:standard-deviation-aggregations`, `:advanced-math-expressions`, `:regex`,
  `:binning`, `:full-join` and `:convert-timezone`.
- Fix `:percentile` and `:median` when the aggregation was renamed in the GUI:
  MBQL then carries a clause metadata map, which was passed to `->honeysql`.
  The same guard now applies to `:convert-timezone`, `:regex-match-first`,
  `:datetime-diff` and `:log`.
- Qualify three-or-more-component field identifiers with the project id. The
  engine only resolves a column qualifier when the full
  `<project>.<schema>.<table>` is quoted as one identifier, so cards that became
  an aggregate over a source query failed with `ODPS-0130071`.
- Escape inline string literals with a single backslash; the doubled-backslash
  form breaks the literal on this engine. Escape aliases collision-safely: a
  lossy fold of a non-ASCII alias now carries a CRC32 suffix so distinct joins
  cannot fold onto the same identifier.
- Format the SQL that Metabase displays and logs with the MySQL dialect. The
  default fallback dialect does not understand backtick identifiers and rewrote
  `` `tbl`.`col` `` as `` ` tbl `.` col ` ``, which the engine cannot resolve.
- Remove the legacy `unprepare`/`unprepare-value` implementations, which are
  superseded by `sql.qp/inline-value`.
- Add a unit test suite covering the compilation paths above, and run it in CI.
- Move the build baseline and the CI pin to Metabase 0.63.16, the first stable
  minor of the supported window. The published 0.1.1 JAR is still compiled
  against 0.51.14; the new unit test suite exercises MBQL that the 0.51 schema
  rejects, so the branch needs the baseline inside the window. This is the
  baseline move already listed above; the driver runtime already supports
  pre-0.63 Metabase through the optional-method shims.

- Correct the 0.1.1 compatibility status to verification-pending. Keep the
  artifact-specific ARRAY observations, but require full schema sync, native
  SELECT 1 and MBQL evidence before declaring the target window supported.

- Declare the supported scope as the current Metabase stable minor, in
  `compatibility.yaml` and `README.md`. Metabase minors that are no longer
  current are outside the window and fixes are not backported to them; the
  per-version observations for 0.1.1 are recorded in
  `docs/e2e/v0.1.1-metabase-supported-window.md`.
- Move the build baseline and the CI pin inside the supported window. The
  published 0.1.1 JAR is still compiled against 0.51.14, and the runtime
  fallback that resolves pre-0.56 Metabase driver APIs becomes dead code once
  the baseline moves.

## [0.1.1] - 2026-09-22

- Upgrade the bundled ODPS JDBC from 3.10.11 to 3.10.14.
- Append `legacyArrayGetObject=false` to the JDBC URL so ARRAY columns are
  returned as `java.sql.Array` from `ResultSet.getObject()`, matching the
  declared `Types.ARRAY` mapping Metabase relies on; the previous
  `ClassCastException` on ARRAY columns (and the `TO_JSON` workaround) is no
  longer needed on this artifact inside the supported Metabase window, which is
  `>=0.63.0, <0.64.0`. Outside that window the unwrapped ARRAY read is not
  guaranteed: on 0.51.x the value reaches the UI as an unrendered object
  reference. Requires ODPS JDBC >= 3.10.14 bundled.

## [0.1.0] - 2026-07-29

- Add compatibility with the Metabase 0.63 driver APIs.
- Pin the verified Metabase baseline to 0.63.1.12.
- Upgrade ODPS JDBC from 3.6.0 to 3.10.11 and bundle it in the driver JAR.
- Use the Metabase driver API for secret values and settings access.
- Migrate query processor extension points for float types, inline values, and
  field-filter dispatch.
- Remove the external JDBC class dependency declaration because the JDBC driver
  is packaged inside the plugin.
- Add a versioned compatibility matrix, reproducible build, checksum, SBOM,
  real MaxCompute E2E gate, and a reproducible release pipeline.

