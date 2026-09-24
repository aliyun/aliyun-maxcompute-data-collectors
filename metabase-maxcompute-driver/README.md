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

**Supported window: the current Metabase stable minor.** This driver tracks
Metabase's latest stable line. Metabase minors that are no longer current are
outside the supported window: they are not tested for, and fixes are not
backported to them. As of this writing the supported line is **0.63.x**.

| Driver | Supported Metabase range | Verified with this artifact | ODPS JDBC | Status |
| --- | --- | --- | --- | --- |
| 0.1.1 | `>=0.63.0, <0.64.0` | 0.63.18 reads `ARRAY` columns as real arrays; 0.56.25.1 and 0.60.15 also pass but are outside the window | 3.10.14, bundled | Supported on 0.63.x (ARRAY read verified; full-range E2E still open) |
| 0.1.0 | `>=0.51.14, <0.64.0` (historical) | 0.51.14, 0.56.25.1, 0.60.15 load smoke; 0.63.1.12 full E2E | 3.10.11, bundled | Superseded by 0.1.1 |
| 0.0.5 | `>=0.50.0, <0.51.0` | 0.50.21 | External JDBC | Legacy |

Two gaps are worth stating plainly, both on driver 0.1.1 and both outside the
supported window. Neither is being backported; the fix is to move Metabase to
the current stable minor.

- On **0.51.x** an `ARRAY` column reaches the UI as an unrendered object
  reference (`com.aliyun.odps.jdbc.data.OdpsArray@…`) instead of a list.
- On **0.52.x – 0.55.x** the driver loads but creating the database fails,
  because the Metabase secret-value function this driver resolves at runtime
  was renamed in 0.52 and moved to another namespace in 0.55.

The per-version observations, pinned to the published artifact's SHA-256, are
in
[`docs/e2e/v0.1.1-metabase-supported-window.md`](docs/e2e/v0.1.1-metabase-supported-window.md).

A Metabase release newer than the upper bound of the window is untested, not
implicitly supported: check the matrix before upgrading past it.

The exact, machine-readable matrix is in
[`compatibility.yaml`](compatibility.yaml). A release contains one driver JAR,
not one JAR per Metabase version, and every compatibility cell must test the
same artifact SHA-256.

The 0.1.1 JAR is still compiled against 0.51.14 as its build baseline and was
verified forward from there. The next driver release moves the build baseline
and the CI pin into the supported window, so the artifact people install is
built against the Metabase line it claims to support.

Run a source-level plugin load check for an exact version without MaxCompute
credentials:

```bash
METABASE_SOURCE_DIR=/path/to/metabase-v0.63.18 \
  ./scripts/run-load-smoke.sh
```

## Installation

### Obtain the Driver

#### Precompiled Releases
Each driver JAR already bundles its ODPS JDBC driver (3.10.14 for 0.1.1, 3.10.11
for 0.1.0). Do not install another ODPS JDBC JAR beside it.

- [MaxCompute Metabase Driver 0.1.1](https://github.com/aliyun/aliyun-maxcompute-data-collectors/releases/download/metabase-0.1.1/maxcompute-metabase-driver-0.1.1.jar)
  &mdash; supported on Metabase `>=0.63.0, <0.64.0`; verify against the
  [SHA256SUMS](https://github.com/aliyun/aliyun-maxcompute-data-collectors/releases/download/metabase-0.1.1/SHA256SUMS)
  published beside it.
- [MaxCompute Metabase Driver 0.1.0](https://github.com/aliyun/aliyun-maxcompute-data-collectors/releases/download/metabase-0.1.0/maxcompute-metabase-driver-0.1.0.jar)
  &mdash; superseded by 0.1.1. Its historical target range
  `>=0.51.14, <0.64.0` predates the supported-window policy and is not a
  support claim for Metabase 0.51.x – 0.55.x; verify against the
  [SHA256SUMS](https://github.com/aliyun/aliyun-maxcompute-data-collectors/releases/download/metabase-0.1.0/SHA256SUMS)
  published beside it.

Driver releases use a `metabase-<driver-version>` tag, in the same shape as the
`presto-<version>` release this project already publishes, and the assets are
produced by the `Metabase Driver` workflow (see Continuous integration below).

#### Build from Source

```bash
./scripts/build-driver.sh
```

Prerequisites: Git, a JDK (21 or newer for source builds, 25 for parity with
the Metabase 0.63 official runtime), and the Clojure CLI pinned by
`scripts/install-clojure-cli.sh`. The build checks out the Metabase source
named in `.metabase-version`; pass `METABASE_SOURCE_DIR=/path/to/metabase` to
build against an existing checkout.

Artifacts are written to `dist/`: the versioned driver JAR, `SHA256SUMS`, a
CycloneDX SBOM, and a build provenance JSON.

### Install into Metabase

1. Verify the JAR against `SHA256SUMS`.
2. Copy the JAR into the Metabase plugins directory.
3. Remove older MaxCompute driver JARs and separately installed old ODPS JDBC
   JARs from that directory.
4. Restart Metabase and add a database using the `MaxCompute` driver.

The plugin JAR already bundles the ODPS JDBC driver, so a separately installed
ODPS JDBC JAR in the same plugins directory is a conflict, not a fallback.

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
maxcompute-metabase-driver-0.1.1.jar
```

Every change must pass `scripts/check-repository.sh` and a clean build, and
changes that affect the compatibility matrix must additionally pass the real
MaxCompute E2E suite; `docs/e2e/` records sanitized evidence per release.
Maintainers cut a release by pushing a `metabase-<driver-version>` tag, and the
`Metabase Driver` workflow attaches the JAR, `SHA256SUMS`, SBOM and provenance
to that GitHub Release.

## Continuous integration

The `Metabase Driver` workflow
([`.github/workflows/metabase-driver.yml`](../.github/workflows/metabase-driver.yml))
runs on every pull request and push that touch this directory. It installs the
Clojure CLI pinned by `scripts/install-clojure-cli.sh`, executes
`scripts/check-repository.sh`, `scripts/build-driver.sh`, and
`scripts/verify-artifact.sh` against the Metabase source pinned in
`.metabase-version`, finishes with the credential-free plugin load smoke
(`scripts/run-load-smoke.sh`), and keeps `dist/` as a workflow artifact.

Pushing a `metabase-<driver-version>` tag that matches `VERSION` adds a second
job that publishes the JAR, `SHA256SUMS`, the CycloneDX SBOM and the build
provenance JSON as the assets of that GitHub Release; that release is what the
Precompiled Releases list above points at. The real MaxCompute E2E suite is
intentionally outside CI because it needs MaxCompute credentials; release owners
run it before promoting a Metabase range to `supported` in
`compatibility.yaml`.

## License

The driver in this directory is licensed under the
GNU Affero General Public License v3.0; see [`LICENSE`](LICENSE) in this
directory. Other modules of the parent `aliyun-maxcompute-data-collectors`
repository remain under the Apache License 2.0. The AGPL-3.0 terms in this
directory govern the driver source, its build outputs, and derived JARs.
