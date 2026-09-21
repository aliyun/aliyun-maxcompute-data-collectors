# Changelog

All notable changes to the MaxCompute Metabase Driver are recorded here.

## [Unreleased]

- Model compatibility as one immutable driver JAR tested across a Metabase
  version range, with exact versions retained as verification anchors.
- Compile against the minimum range baseline and resolve the pre/post-0.56
  Metabase driver APIs at runtime.

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

