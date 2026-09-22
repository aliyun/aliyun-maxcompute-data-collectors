# MaxCompute Metabase Driver

Community driver for connecting [Metabase](https://www.metabase.com/) to
[Alibaba Cloud MaxCompute](https://www.alibabacloud.com/product/maxcompute).

This directory contains the MaxCompute driver for Metabase and is part of the
[`aliyun-maxcompute-data-collectors`](https://github.com/aliyun/aliyun-maxcompute-data-collectors)
project. Compatibility claims and build provenance for the driver are maintained
here, with the exact machine-readable matrix in
[`compatibility.yaml`](compatibility.yaml). The driver is maintained by the
Alibaba Cloud MaxCompute team. It is not bundled with Metabase and is not
supported on Metabase Cloud.

## Compatibility

| Driver | Target Metabase range | Verified anchors | ODPS JDBC | Status |
| --- | --- | --- | --- | --- |
| 0.1.0 | `>=0.51.14, <0.64.0` | 0.51.14, 0.56.25.1, and 0.60.15 load smoke; 0.63.1.12 full E2E | 3.10.11, bundled | Range verification |
| 0.0.5 | `>=0.50.0, <0.51.0` | 0.50.21 | External JDBC | Legacy |

The exact, machine-readable matrix is in
[`compatibility.yaml`](compatibility.yaml). A release contains one driver JAR,
not one JAR per Metabase version. Every compatibility cell must test the same
artifact SHA-256.

The target range is promoted to supported only after the same JAR passes plugin
load smoke tests on the latest patch of every included Metabase minor and full
real-MaxCompute E2E at the production version, range boundaries, and selected
API-transition anchors. The matrix distinguishes target, tested, and supported
claims.

The release JAR is compiled against the minimum verified Metabase version and
then tested forward. Run a source-level plugin load check for an exact version
without MaxCompute credentials:

```bash
METABASE_SOURCE_DIR=/path/to/metabase-v0.51.14 \
  ./scripts/run-load-smoke.sh
```

## Install

1. Obtain the driver JAR (`maxcompute-metabase-driver-<driver-version>.jar`)
   from the project release assets, or build it from source (see Build).
2. Verify it with the published `SHA256SUMS`.
3. Copy the JAR into the Metabase plugins directory.
4. Remove older MaxCompute driver JARs and separately installed old ODPS JDBC
   JARs from that directory.
5. Restart Metabase and add a database using the `MaxCompute` driver.

The 0.1.1 artifact already bundles ODPS JDBC 3.10.14; 0.1.0 bundles 3.10.11. Do not install another
ODPS JDBC JAR beside it.

## Build

Prerequisites:

- Git
- Clojure CLI
- JDK 21 or newer for source builds
- JDK 25 for parity with the Metabase 0.63 official runtime

Build against the pinned Metabase source:

```bash
./scripts/build-driver.sh
```

To build against an existing checkout:

```bash
METABASE_SOURCE_DIR=/path/to/metabase ./scripts/build-driver.sh
```

Artifacts are written to `dist/`:

- versioned driver JAR
- `SHA256SUMS`
- CycloneDX SBOM
- build provenance JSON

## Real E2E

The real E2E starts a clean Metabase instance, loads the built plugin, creates
a real MaxCompute database, waits for full schema sync, runs `SELECT 1`, and
runs an MBQL query against a synced table.

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID=...
export ALIBABA_CLOUD_ACCESS_KEY_SECRET=...
export MAXCOMPUTE_ENDPOINT=...
export MAXCOMPUTE_PROJECT=...

./scripts/run-real-e2e.sh
```

Or run the exact official Metabase container image:

```bash
./scripts/run-real-e2e-docker.sh
```

Credentials are only passed to the temporary Metabase process. They are not
written to the E2E evidence file. Release and scheduled pipelines should use a
dedicated, read-only, low-cost MaxCompute project.

## Versioning and releases

Driver releases use semantic versions independently of Metabase. One immutable
artifact is tested across the declared Metabase range:

```text
maxcompute-metabase-driver-0.1.0.jar
```

Every change must pass `scripts/check-repository.sh` and a clean build, and
changes that affect the compatibility matrix must additionally pass the real
MaxCompute E2E suite; `docs/e2e/` records sanitized evidence per release.
Release tagging and asset publishing are performed by the maintainers.

## License

The driver in this directory is licensed under the
GNU Affero General Public License v3.0; see [`LICENSE`](LICENSE) in this
directory. Other modules of the parent `aliyun-maxcompute-data-collectors`
repository remain under the Apache License 2.0. The AGPL-3.0 terms in this
directory govern the driver source, its build outputs, and derived JARs.
