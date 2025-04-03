# MaxCompute Connector

The MaxCompute Connector enables direct querying and processing of data stored in MaxCompute data warehouses. It is particularly suited for integrating and analyzing data between MaxCompute and systems like Hive.

## Prerequisites

Before using the connector, ensure the following requirements are met:

- **Access Permissions**  
  Valid access to a MaxCompute project with authentication credentials (AccessKey ID/Secret).

- **Network Configuration**  
  Use Alibaba Cloud VPC for stable data transmission.  
  🔗 [MaxCompute Network Configuration Guide](https://help.aliyun.com/zh/maxcompute/user-guide/network-connection-process)

- **Resource Groups**  
  Requires an 🔗 [exclusive resource group](https://help.aliyun.com/zh/maxcompute/user-guide/purchase-and-use-exclusive-resource-groups-for-dts) or enabled 🔗 [Open Storage (Pay-As-You-Go)](https://help.aliyun.com/zh/maxcompute/product-overview/open-storage-pay-as-you-go).

- **Schema Mode (Optional)**  
  To use Namespace Schema in Trino, enable 🔗 [Schema Mode](https://help.aliyun.com/zh/maxcompute/user-guide/schema-related-operations) in MaxCompute.

---

## Installation and Configuration

### Obtain the Connector

#### Precompiled Releases (Recommended)
- Trino 470
- Trino 422

#### Build from Source
```bash
# Build with Maven
mvn clean package
# Extract to Trino plugin directory
unzip target/trino-maxcompute-*.zip -d $TRINO_HOME/plugin/
```

### Configuration File Example

Create `maxcompute.properties` in `etc/catalog/`:
```properties
connector.name=maxcompute
odps.project.name=your_project_name
odps.access.id=your_access_key_id
odps.access.key=your_access_key_secret
odps.end.point=http://service.cn-hangzhou.maxcompute.aliyun.com/api
odps.quota.name=your_quota_name
```

⚠️ **Important Configuration Notes**
- Set environment variables when starting Trino:
  ```bash
  export _JAVA_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
  ```
  🔗 [Apache Arrow Configuration Guide](https://arrow.apache.org/docs/java/install.html)

---

## Multi-Project Access

Access multiple projects by creating separate configuration files:
```
etc/catalog/
├── sales.properties     # Set odps.project.name=sales
└── analytics.properties # Set odps.project.name=analytics
```
Trino will automatically create corresponding `sales` and `analytics` catalogs.

---

## Data Type Mapping

| MaxCompute Type | Trino Type | Notes |
|----------------|------------|-------|
| BOOLEAN        | BOOLEAN    | -     |
| TINYINT        | TINYINT    | -     |
| SMALLINT       | SMALLINT   | -     |
| INT            | INTEGER    | -     |
| BIGINT         | BIGINT     | -     |
| FLOAT          | REAL       | -     |
| DOUBLE         | DOUBLE     | -     |
| DECIMAL        | DECIMAL    | -     |
| STRING         | VARCHAR    | -     |
| VARCHAR        | VARCHAR    | -     |
| JSON           | VARCHAR    | -     |
| CHAR           | CHAR       | -     |
| BINARY         | VARBINARY  | -     |
| DATE           | DATE       | -     |
| DATETIME       | TIMESTAMP  | -     |
| TIMESTAMP      | TIMESTAMP  | -     |
| TIMESTAMP_NTZ  | TIMESTAMP  | -     |

> ⚠️ **Limitations**  
> Complex types (MAP/STRUCT/ARRAY) are not yet supported but will be added in future releases.

---

## Development Roadmap

- ✅ Basic type read support
- 🚧 DDL operations (In progress)
- 📅 Write support & complex types (Planned)

Contributions via PRs or Issues are welcome!  
👤 Maintainer: [Jason Zhang](https://github.com/dingxin-tech)
