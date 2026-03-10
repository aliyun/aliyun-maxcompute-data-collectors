---
sidebar_position: 1
title: Overview
---

# Spark Connector for MaxCompute

The Spark Connector enables reading from and writing to Alibaba Cloud MaxCompute tables using Apache Spark.

## Features

- Read MaxCompute tables as Spark DataFrames
- Write Spark DataFrames to MaxCompute tables
- Support for partitioned tables
- Compatible with Spark SQL

## Modules

This connector consists of multiple modules:

| Module | Description |
|--------|-------------|
| `spark-connector/common` | Shared utilities and common code |
| `spark-connector/datasource` | Core DataSource implementation |
| `spark-connector/hive` | Hive-compatible integration |
| `spark-connector/e2e-test` | End-to-end tests |

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>spark-connector</artifactId>
    <version>2.0.5</version>
</dependency>
```

### Basic Usage

```scala
val spark = SparkSession.builder()
  .appName("MaxCompute Spark Example")
  .getOrCreate()

// Read from MaxCompute
val df = spark.read
  .format("odps")
  .option("project", "your_project")
  .option("table", "your_table")
  .load()

df.show()
```

## Requirements

- Apache Spark 2.x or 3.x
- JDK 1.8 or later
- Alibaba Cloud MaxCompute account

## Source Code

- [spark-connector on GitHub](https://github.com/aliyun/aliyun-maxcompute-data-collectors/tree/master/spark-connector)
