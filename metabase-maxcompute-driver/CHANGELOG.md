# Changelog

All notable changes to the MaxCompute Metabase Driver are recorded here.

## [Unreleased]

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

