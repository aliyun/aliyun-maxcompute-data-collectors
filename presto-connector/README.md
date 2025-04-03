# MaxCompute Connector

The MaxCompute connector enables direct querying and processing of data stored in the MaxCompute data warehouse. This connector is useful for implementing data
integration and analysis between MaxCompute and other systems, such as Hive.

## MaxCompute Connector Setup

### Prerequisites

Before using the MaxCompute connector, ensure the following conditions are met:

- Access permissions: You must have valid access rights to a MaxCompute project and the necessary authentication credentials for that project.
- Network setup: For stable data transmission, the use of Aliyun VPC network is recommended. See
  the [MaxCompute Network Configuration Guide](https://help.aliyun.com/zh/maxcompute/user-guide/network-connection-process) for more information.
- Resource group configuration: Possession of an exclusive Tunnel resource group is required. Note: MaxCompute currently allows access to shared resource groups, but this could
  change at any time. See [Exclusive resource group](https://help.aliyun.com/zh/maxcompute/user-guide/purchase-and-use-exclusive-resource-groups-for-dts).
- (Optional) Schema activation: Enable Schema mode to use different directories in Presto. See
  [How to Enable Schema Model in MaxCompute](https://help.aliyun.com/zh/maxcompute/user-guide/schema-related-operations) for more information.

### Obtain the Connector JAR

Obtain the JAR file by building the repository. Use the Maven command `mvn clean package` to build the project and generate the
JAR. Once built, place the JAR file into the `plugin/maxcompute` directory within your Presto installation.

### Configuration

Before configuring the MaxCompute connector, have ready all the necessary information associated with your MaxCompute project, including project name, access
ID, access key, and resource group name.

1. Create a configuration file (for example, `maxcompute.properties`) in Presto's `etc/catalog` directory:
    ```properties
    # MaxCompute Connector
    connector.name=maxcompute
    # MaxCompute project name, connections support only this project
    odps.project.name=<Your MaxCompute Project Name>
    # Alibaba Cloud authentication parameter
    odps.access.id=<Your Alibaba Cloud Access ID>
    # Alibaba Cloud authentication parameter
    odps.access.key=<Your Alibaba Cloud Access Key>
    # MaxCompute Endpoint
    odps.end.point=<Your MaxCompute Endpoint>
    # MaxCompute exclusive resource group name
    odps.quota.name=<Your MaxCompute Exclusive Resource Group Name>
    ```

2. Replace `<Your MaxCompute Project Name>`, `<Your Alibaba Cloud Access ID>`, `<Your Alibaba Cloud Access Key>`, `<Your MaxCompute Service Endpoint>`,
and `<Your MaxCompute Exclusive Resource Group Name>` with the values for your MaxCompute environment.

### Accessing Multiple MaxCompute Projects

To access multiple MaxCompute projects simultaneously, create separate configuration files for each project. For example, if you have two projects, one for "sales" (`sales`)
and another for "analysis" (`analytics`), then you can create `sales.properties` and `analytics.properties` configuration files. Specify a different `odps.project.name` in each
file:

- `sales.properties` for the sales project.
- `analytics.properties` for the analysis project.

  With this setup, `sales` and `analytics` catalogs will be created in Presto respectively.

### Data Type Mapping

When using the MaxCompute connector, MaxCompute's data types are mapped to Presto's data types. The table below shows this mapping relationship and its corresponding Presto
types:

| MaxCompute Data Type | Presto Data Type |
|----------------------|------------------|
| BOOLEAN              | BOOLEAN          |
| TINYINT              | TINYINT          |
| SMALLINT             | SMALLINT         |
| INT                  | INTEGER          |
| BIGINT               | BIGINT           |
| FLOAT                | REAL             |
| DOUBLE               | DOUBLE           |
| DECIMAL              | DECIMAL          |
| STRING               | VARCHAR          |
| VARCHAR              | VARCHAR          |
| JSON                 | VARCHAR          |
| CHAR                 | CHAR             |
| BINARY               | VARBINARY        |
| DATE                 | DATE             |
| DATETIME             | TIMESTAMP        |
| TIMESTAMP            | TIMESTAMP        |
| TIMESTAMP_NTZ        | TIMESTAMP        |
| DECIMAL              | DECIMAL          |
| ARRAY                | ARRAY            |

Note: Some MaxCompute data types (such as `MAP`, `STRUCT`) are not fully supported in the current version of the connector.

### Notices

This project is under active development, with plans to introduce more features in subsequent versions, such as support for DDL operations, data writing, and expanded data
type support. We welcome and encourage community members to participate and contribute.

### Contributors

- [Jason Zhang](https://github.com/dingxin-tech)
