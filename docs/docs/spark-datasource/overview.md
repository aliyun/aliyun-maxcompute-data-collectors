---
sidebar_position: 1
title: Overview
---

# Spark DataSource V2 for MaxCompute

:::caution Archived
This connector is no longer actively maintained. Consider using the [Spark Connector](/docs/spark-connector/overview) instead.
:::

Spark DataSource V2 API implementation for Alibaba Cloud MaxCompute, providing optimized data access for Spark 2.3 and Spark 3.1.

## Versions

| Module | Spark Version | Description |
|--------|--------------|-------------|
| `spark-datasource-v2.3` | Spark 2.3.x | DataSource V2 API for Spark 2.3 |
| `spark-datasource-v3.1` | Spark 3.1.x | DataSource V2 API for Spark 3.1 |

## Features

- Spark DataSource V2 API compliance
- Optimized read/write paths
- Partition pruning support
- Column pruning support

## Requirements

- Apache Spark 2.3.x or 3.1.x
- JDK 1.8 or later
- Alibaba Cloud MaxCompute account

## Source Code

- [spark-datasource-v2.3 on GitHub](https://github.com/aliyun/aliyun-maxcompute-data-collectors/tree/master/spark-datasource-v2.3)
- [spark-datasource-v3.1 on GitHub](https://github.com/aliyun/aliyun-maxcompute-data-collectors/tree/master/spark-datasource-v3.1)
