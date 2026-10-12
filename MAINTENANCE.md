# Module maintenance status

This repository is 17 projects that were added at different times, against different engine
versions, with different toolchains and no single release process. That makes "is X still
supported?" unanswerable from the top level, and it is how issues end up sitting for years:
people ask a question about a directory that has not changed since 2024, and nobody can say so
without reading every `pom.xml`.

This file answers it per directory with evidence a reader can re-check: what the module declares
as its target, when the directory last changed, how many test sources it tracks, whether any
artifact exists today, and the status we propose for it.

Reference point: `master` at `59550835b`, read on 2026-10-12. The commands that produce every
number below are at the end — re-run them instead of trusting this table, and update the table in
the same commit when you do.

## Statuses

| Status | What it means |
| --- | --- |
| **Supported** | The directory changed in the last 12 months, it declares the engine version it targets, and there is a verification channel that actually runs (a CI job, a release with checksums, or a test suite). Bug reports get acted on. Moving the target version is a design discussion, not a drive-by. |
| **Maintenance-limited** | Usable at the declared version. Fixes need a reproducer, and only the declared version is verified — anything else is untested, which is not the same as supported. Binaries, where they exist, come from GitHub Releases, not a repository. |
| **Legacy, frozen** | No change for 24 months or more, no test suite that runs here, and no published artifact. Bug reports get an answer and a pointer to a maintained path; they do not get a fix unless someone sends a pull request that one of us can verify. Sources stay so that people pinned to those engine versions can still build. |
| **Internal snapshot** | The coordinates are `*-SNAPSHOT` and were never published; the directory exists so that other directories in this repository compile. It is not a dependency you can resolve. |

## Matrix

| Directory | Declared target | Last change | Test sources it tracks | What a user can get today | Proposed status |
| --- | --- | --- | --- | --- | --- |
| `metabase-maxcompute-driver` | Metabase current stable minor (`0.63.x` for driver 0.1.1), JDK 21+; policy in `compatibility.yaml` | 2026-09-30 | none as unit tests; `scripts/check-repository.sh`, `build-driver.sh`, `verify-artifact.sh` and a plugin-load smoke run in CI | GitHub Release `metabase-0.1.1`: driver jar + `SHA256SUMS` + CycloneDX SBOM, built by CI at the tag | **Supported** |
| `google-sheets-plugin` | Google Sheets add-on (Apps Script), Node.js >= 20 | 2026-09-21 | 8 `*.test.js` plus a Apps Script harness helper, run by `npm test` | `npm run release:local` bundles to `dist/apps-script/`; you deploy it with clasp — no hosted binary | **Supported** |
| `spark-connector` | Spark 3.3.1 / Scala 2.12.10, MaxCompute SDK 0.61.2-public; the Spark Connector page states 3.2.x-3.5.x | 2026-09-18 | 11 Scala suites (they need a live project and credentials) | not published; `mvn clean package` in the directory — the official page says to clone and build this repo | **Supported** |
| `presto-connector` | PrestoDB 0.287 (`presto.version`, and the GAV is `com.facebook.presto:presto-maxcompute:0.287`), JDK 8 | 2025-04-03 | 3 | GitHub Release `presto-0.287` (one jar); nothing on Maven Central | **Maintenance-limited** |
| `trino-connector` | Trino 470 (`dep.trino.version`, and `<parent>` is `io.trino:trino-root:470`); the README also links a 422 build | 2025-04-03 | 2 | GitHub Release tag `trino`: `trino-maxcompute-422.zip`, `trino-maxcompute-470.zip` | **Maintenance-limited** |
| `flink-connector-odps` | Flink 1.14.3 / Scala 2.11.12, SDK 0.36.7-public | 2024-07-04 | 19 | the dependency the README prints (`com.aliyun.odps:flink-connector-odps:1.14-SNAPSHOT`) is not published; build from source | **Legacy, frozen** |
| `odps-sqoop` | Sqoop extension, "tested on Hadoop 2.6.1" per its README; `ivy.xml` pins `odps-sdk-core` 0.17.3, source level 1.6 | 2024-07-04 | 193, inherited from Apache Sqoop; nothing here runs them | not published; Apache Sqoop upstream is retired | **Legacy, frozen** |
| `kettle-plugin` | README asks for Kettle 7.0; `pom.xml` compiles against `kettle-core` 5.4.0.1-130, source level 1.7 | 2024-07-04 | none | not published | **Legacy, frozen** |
| `flume-plugin` | DataHub sink for Flume; `pom.xml` pins `flume-ng-core` 1.6.0 while the README recommends Flume-NG 1.9 | 2024-07-04 | none | not published (`com.aliyun.datahub:aliyun-datahub-flume-plugin` is not on Maven Central) | **Legacy, frozen** |
| `ogg-plugin` | Oracle GoldenGate handler writing to DataHub; `goldengate.version` 19.1.0.0.1.003 | 2024-07-04 | 5 | not published | **Legacy, frozen** |
| `hive_data_transfer_udtf` | Hive UDTF, `hive-exec` 0.13.0 / `hadoop-common` 2.4.0, source level 1.6 | 2024-07-04 | none | not published (`aliyun:odps_data_dumper:2.0.5`) | **Legacy, frozen** |
| `odps-data-carrier` | five subprojects for Hive-to-MaxCompute data and metadata transfer; `hive-exec` 0.13.0, SDK 0.30.9-public | 2024-07-04 | none | `com.aliyun.odps:data-carrier:1.0-SNAPSHOT`, never published; `./build.sh` | **Legacy, frozen** |
| `spark-datasource-v2.3` | Spark 2.3.0 / Scala 2.11, jars vendored under `libs/` | 2024-07-04 | 6 Scala suites | `com.aliyun.maxcompute:spark-datasource:1.0-SNAPSHOT`, not published | **Legacy, frozen** |
| `spark-datasource-v3.1` | Spark 3.1.2 / Scala 2.12, jars vendored under `libs/` | 2024-07-04 | 6 Scala suites | same coordinates as `spark-datasource-v2.3` — two directories, one artifact name | **Legacy, frozen** |
| `cupid-table-api` | Table API used by the two `spark-datasource-*` directories; SDK 0.36.7-public, Arrow 1.0.0 | 2024-07-04 | none | `com.aliyun.odps:cupid-table-api:1.1.6-SNAPSHOT`, not published | **Internal snapshot** |
| `table-api-tunnel-impl` | Tunnel-backed implementation of `cupid-table-api` | 2024-07-04 | 1 | `com.aliyun.odps.cupid:table-api-tunnel-impl:1.1.6-SNAPSHOT`, not published | **Internal snapshot** |
| `common` | code shared by `flume-plugin`, `kettle-plugin`, `ogg-plugin`, `odps-sqoop`; source level 1.6 | 2024-07-04 | none | `com.aliyun:maxcompute.data.collectors.common:2.0.5`, not published | **Internal snapshot** |

Three things this table says out loud that the README does not:

- **There are three Spark paths and two Trino/Presto builds.** `spark-connector` (Spark 3.2.x-3.5.x,
  DataSource V2, read and write) is the one being changed. `spark-datasource-v2.3` and
  `spark-datasource-v3.1` are DataSource V2 experiments against Spark 2.3 and 3.1 that stopped
  changing in 2024 and share one unpublished artifact name.
- **Nothing here is on Maven Central.** Every module of this repository that we checked returns 404
  under its own group on Central (see the commands below). The only binaries users can download are
  the three GitHub Releases: Metabase driver, Presto 0.287, Trino 422/470. So "add the dependency"
  instructions in module READMEs are only true if the user builds and installs locally.
- **The 1.6 / 1.7 source levels are not just old, they are unbuildable on current JDKs.** `javac`
  from JDK 17 onwards rejects `-source 6`, and JDK 21 onwards rejects `-source 7`, so the frozen
  transfer plugins cannot be rebuilt on a modern toolchain even by the people still running them.

## What we checked, and what we did not

Checked here: the last commit per directory, the tracked test sources, the version each module
declares as its target, the Java source levels, the published artifacts on Maven Central and GitHub
Releases, and whether the third-party parents and engine artifacts a source build needs are
resolvable (for example `io.trino:trino-root:470` is published on Central — HTTP 200 on 2026-10-12,
which matters because the `<relativePath>` in `trino-connector/pom.xml` points at `../../pom.xml`,
a path that does not exist in a checkout of this repository).

Not checked here: whether each module still compiles. There is no CI for that — the only workflow in
this repository is `.github/workflows/metabase-driver.yml`, and its triggers are scoped to
`metabase-maxcompute-driver/**`, so a green check on a pull request says nothing about the Maven
modules. Before this file is merged, the two Supported rows that users build from source
(`spark-connector`, and the two query-engine rows marked Maintenance-limited) should be built once
per stated JDK and have the result recorded in the row; until then those rows describe what the
module declares and what is published, not a build we have run.

## If you need one of the frozen ones today

| Instead of | Use |
| --- | --- |
| `odps-sqoop` for database-to-MaxCompute bulk transfer | DataWorks Data Integration, which is the maintained managed path, or a JDBC/Spark job you own |
| `flink-connector-odps` 1.14 for new Flink work | the MaxCompute connector shipped with Realtime Compute for Apache Flink, which is versioned and documented with that product |
| `spark-datasource-v2.3` / `-v3.1` | `spark-connector` (Spark 3.2.x-3.5.x) |
| `flume-plugin` / `ogg-plugin` | these write to DataHub topics, not to MaxCompute tables; for DataHub ingestion use the DataHub SDK/connectors for the version you run |

## How a status changes

- A frozen directory moves up when someone lands a change in it that CI or a stated verification
  procedure proves, and the row is edited in the same pull request.
- A supported directory moves down when it stops being verified, not merely when it stops being
  touched: `metabase-maxcompute-driver` is the model, with `compatibility.yaml` holding the machine-
  readable window and per-version observations pinned to the SHA-256 of the released jar.
- Claiming a version you have not run is the failure mode this file exists to prevent. Keep claims
  at the level of the evidence, the way the driver's README does: observed behaviour versus
  accepted support, stated separately.

## Decisions we are asking maintainers to make

1. Accept or adjust the four statuses row by row. Everything below assumes this table is what we tell users.
2. Whether the frozen transfer plugins (`odps-sqoop`, `kettle-plugin`, `flume-plugin`, `ogg-plugin`,
   `hive_data_transfer_udtf`, `odps-data-carrier`) stay open for issues with a stated response policy,
   or get a `legacy/` subtree or archived upstream pointers. Repository settings (archive, labels,
   issue templates) are not something a pull request can do, so those need an owner.
3. Whether `spark-connector`, `presto-connector` and `trino-connector` get published artifacts
   (Maven Central, or Release assets the way Presto/Trino already do) instead of "clone and build".
4. Whether the duplicated `spark-datasource-v2.3` / `-v3.1` coordinates should be renamed to avoid
   two directories producing the same artifact name.

## Refresh commands

Run from a clean checkout of `master`. These are the commands of each shape; run them and diff the
output against the table. The release and Maven Central columns are plain HTTP, so they are checkable
without a checkout.

```bash
git rev-parse --short=9 HEAD

# last change per directory
for d in */; do d=${d%/}; printf '%-30s %s\n' "$d" "$(git log -1 --format=%cs -- "$d")"; done

# test sources tracked per module (paths containing /test or /tests)
for d in */; do d=${d%/}; printf '%-30s %s\n' "$d" \
  "$(git ls-files "$d" | grep -E '\.(java|scala|clj|cljc|cljs|js|jsx|ts|gs)$' | grep -Ei '/(test|tests)/' | wc -l)"; done

# the engine version each module declares as its target (pom properties)
grep -rhoE '<(presto|dep\.trino|flink|spark|pentaho-kettle|goldengate)[^>]*>[^<]*' */pom.xml

# versions pinned as dependencies instead of properties
grep -rn -A2 -E '<artifactId>(flume-ng-core|hive-exec|hadoop-common|kettle-core|trino-spi|presto-spi)</artifactId>' \
  */pom.xml | grep -E 'artifactId>|version>'

# the MaxCompute SDK each module builds against
grep -rn 'odps\.sdk\.version\|odps-sdk-core' */pom.xml odps-sqoop/ivy.xml | grep -E '[0-9]+\.[0-9]+\.[0-9]+'

# Java source levels, which decide which JDK can build a module at all
grep -rhoE '<(source|target|java\.version|maven\.compiler\.source)>[^<]*' */pom.xml pom.xml | sort -u

# published artifacts: release tags and assets, and the Maven Central answer
gh api repos/aliyun/aliyun-maxcompute-data-collectors/releases --jq '.[] | .tag_name + " " + (.assets | map(.name) | join(","))'
curl -s -o /dev/null -w '%{http_code}\n' https://repo1.maven.org/maven2/com/aliyun/odps/spark-connector/
```
