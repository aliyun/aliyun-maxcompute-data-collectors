# Aliyun MaxCompute Data Collectors

A collection of integrations between Alibaba Cloud MaxCompute and other data engines, ETL
tools, and BI tools. Each top-level directory is a separate project with its own toolchain,
engine version, and JDK baseline. This is not one artifact you build once at the root, so
start from the list below and build the directory you need.

## Modules

### Connectors for query engines, data engines, and BI tools

| Directory | Integrates with | Toolchain | Build in that directory | Details |
| --- | --- | --- | --- | --- |
| `presto-connector` | PrestoDB 0.287 | Maven | `mvn clean package` | [README](presto-connector/README.md) · [中文](presto-connector/README-cn.md) |
| `trino-connector` | Trino 422 and 470 | Maven | `mvn clean package` | [README](trino-connector/README.md) · [中文](trino-connector/README-cn.md) |
| `spark-connector` | Spark 3.3.1 (Scala 2.12) | Maven, aggregates `common`, `datasource`, `hive` | `mvn clean package` | — |
| `spark-datasource-v2.3` | Spark 2.3 DataSource V2 | Maven, uses the jars vendored in `libs/` | `mvn clean package jar:test-jar` | [README](spark-datasource-v2.3/README.md) · [English](spark-datasource-v2.3/README-en.md) |
| `spark-datasource-v3.1` | Spark 3.1.2 DataSource V2 | Maven, uses the jars vendored in `libs/` | `mvn clean package jar:test-jar` | [README](spark-datasource-v3.1/README.md) · [English](spark-datasource-v3.1/README-en.md) |
| `flink-connector-odps` | Flink 1.14.3 | Maven | `mvn clean package` | [README](flink-connector-odps/README.md) |
| `metabase-maxcompute-driver` | Metabase (community driver) | Clojure CLI (`deps.edn`) | `./scripts/build-driver.sh`, artifacts land in `dist/` | [README](metabase-maxcompute-driver/README.md) |
| `google-sheets-plugin` | Google Sheets (Apps Script add-on) | Node.js | `npm run release:local`, bundle lands in `dist/apps-script/` | [README](google-sheets-plugin/README.md) |

### Transfer plugins for other ingest paths

| Directory | Integrates with | Toolchain | Build in that directory | Details |
| --- | --- | --- | --- | --- |
| `flume-plugin` | Apache Flume sink that writes to DataHub topics | Maven | `mvn clean package` | [README](flume-plugin/README.md) |
| `ogg-plugin` | Oracle GoldenGate handler that writes to DataHub | Maven | `mvn clean package` | [README](ogg-plugin/README.md) |
| `kettle-plugin` | Kettle/Pentaho input and output steps over Tunnel | Maven | `mvn clean package` | [README](kettle-plugin/README.md) |
| `odps-sqoop` | Sqoop import/export (tested against Hadoop 2.6.1) | Maven | `mvn clean package` | [README](odps-sqoop/README.md) · [COMPILING](odps-sqoop/COMPILING.txt) |
| `hive_data_transfer_udtf` | Hive UDTF (`MaxComputeDataTransferUDTF`) | Maven, not in the root reactor | `mvn clean package` | — |
| `odps-data-carrier` | Hive and MaxCompute data and metadata transfer, five subprojects | Maven aggregator with `build.sh` | `./build.sh`, output zipped as `odps-data-carrier.zip` | — |

### Shared libraries

| Directory | Provides |
| --- | --- |
| `common` | Code shared by the transfer plugins |
| `cupid-table-api` | Table API that hides in-cluster versus Tunnel access differences; used by the two `spark-datasource-*` modules |
| `table-api-tunnel-impl` | Tunnel-backed implementation of `cupid-table-api` |

## Which directories the root build covers

`pom.xml` at the repository root declares 11 of the 17 module directories:
`common`, `cupid-table-api`, `flink-connector-odps`, `flume-plugin`, `kettle-plugin`,
`odps-sqoop`, `ogg-plugin`, `presto-connector`, `spark-connector`, `table-api-tunnel-impl`,
`trino-connector`.

Six are not in that list and are built in their own directory:
`google-sheets-plugin`, `hive_data_transfer_udtf`, `metabase-maxcompute-driver`,
`odps-data-carrier`, `spark-datasource-v2.3`, `spark-datasource-v3.1`.

## Requirements

- Apache Maven 3.x for the Maven modules — except `trino-connector`, whose parent enforces Maven
  3.9.8 or newer — plus the engine you integrate with (Flink, Spark, Presto, Trino, Hive, or
  Kettle; each module pins its own version in `pom.xml`).
- Node.js 20 or newer for `google-sheets-plugin`.
- A JDK matched to what you build. The modules do not share one Java baseline, so no single
  JDK builds this whole repository:

| Java level a module builds at | Modules | JDKs that accept it |
| --- | --- | --- |
| 1.6 | `common`, `odps-sqoop`, `hive_data_transfer_udtf` | 8 and 11 only; JDK 17 and later stop with `Source option 6 is no longer supported` |
| 1.7 | `kettle-plugin` | 8, 11, and 17; JDK 21 and later stop with `Source option 7 is no longer supported` |
| 1.8 | `flume-plugin`, `ogg-plugin`, `presto-connector`, `table-api-tunnel-impl`, `cupid-table-api`, `spark-connector`, `flink-connector-odps` | 8 through 25 |
| 23 | `trino-connector` | it inherits `io.trino:trino-root:470`, whose enforcer rules require a JDK in `[23.0.0,)`, a Temurin or Oracle JDK, and Maven in `[3.9.8,)` — anything else fails before compilation |
| 21 or newer | `metabase-maxcompute-driver` | see its [README](metabase-maxcompute-driver/README.md) |

The 1.6, 1.7, and 1.8 levels are the levels pinned in the root `pom.xml` and the module poms;
the acceptance column is what `javac` of each JDK reports for those `-source`/`-target` flags.
`mvn -v` prints the JDK that Maven is actually using.

## Building the Sources

Clone the project from GitHub:

```
$ git clone https://github.com/aliyun/aliyun-maxcompute-data-collectors.git
```

Build the module you need, with the JDK from the table above:

```
$ cd aliyun-maxcompute-data-collectors
$ mvn -pl flink-connector-odps -am clean package   # -am also builds the reactor modules it needs
$ mvn -pl presto-connector clean package
```

The Maven directories outside the root reactor, and the two non-Maven modules, are built in
place:

```
$ cd spark-datasource-v3.1      && mvn clean package jar:test-jar
$ cd hive_data_transfer_udtf    && mvn clean package
$ cd odps-data-carrier          && ./build.sh
$ cd metabase-maxcompute-driver && ./scripts/build-driver.sh
$ cd google-sheets-plugin       && npm run release:local
```

Maven modules put their packages under each module's `target` directory; the Metabase driver
and the Google Sheets add-on write to their own `dist/` directories, as their READMEs describe.

A whole-reactor build (`mvn clean package` at the repository root) asks one JDK to satisfy both
the 1.6-level modules and the Trino module, which today no JDK does. Prefer the per-module
commands above.

## Continuous integration

`.github/workflows/metabase-driver.yml` is the only workflow in this repository, and its
triggers are scoped to `metabase-maxcompute-driver/**`. It builds, verifies, and releases the
Metabase driver; it does not compile any Maven module, so a green run is not evidence that the
rest of the repository builds.

## Usages

Please refer to the **[Wiki](https://github.com/aliyun/aliyun-maxcompute-data-collectors/wiki)**
for basic usage, and to the module READMEs linked above for engine-specific configuration.

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
