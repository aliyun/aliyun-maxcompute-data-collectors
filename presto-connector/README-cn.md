# MaxCompute 连接器

MaxCompute 连接器允许您直接查询和处理存储在 MaxCompute 数据仓库中的数据。此连接器特别适用于实现 MaxCompute 与其他系统（如 Hive）之间的数据集成与分析。

## MaxCompute 连接器设置

### 先决条件

使用 MaxCompute 连接器之前，请确保满足以下条件：

- 访问权限：确保您拥有有效的 MaxCompute 项目访问权限，并已获取必要的身份验证凭证。
- 网络设置：为确保数据传输的稳定性，推荐使用阿里云 VPC 网络。查看[MaxCompute网络配置指南](https://help.aliyun.com/zh/maxcompute/user-guide/network-connection-process)了解更多信息。
- 资源组配置：拥有独享 Tunnel 资源组（尽管目前 MaxCompute
  还不禁用共享资源组访问，这一情况随时可能变化。）[独享资源组](https://help.aliyun.com/zh/maxcompute/user-guide/purchase-and-use-exclusive-resource-groups-for-dts)。
- （可选）Schema 启用：开启 Schema 模式以在 Presto 中使用不同的目录。参考[如何在MaxCompute中启用Schema模型](https://help.aliyun.com/zh/maxcompute/user-guide/schema-related-operations)
  获取更多信息。

### 获取连接器 JAR 文件

要使用 MaxCompute 连接器，首先需要通过构建存储库来获取 JAR 文件。使用Maven命令`mvn clean package`构建项目并生成
JAR 文件。构建完成后，将 JAR 文件放入 Presto 安装中的 `plugin/maxcompute` 目录中。

### 配置

在配置 MaxCompute 连接器之前，请先确定您已准备好了与 MaxCompute 项目关联的所有必要信息，包括项目名称、访问ID、访问密钥和资源组名称。
按照以下步骤在 Presto 的`etc/catalog`目录下创建配置文件（例如`maxcompute.properties`）：

```properties
# MaxCompute 连接器
connector.name=maxcompute
# MaxCompute 项目名称，连接时仅支持该项目
odps.project.name=<您的 MaxCompute 项目名称>
# 阿里云身份验证参数
odps.access.id=<您的阿里云访问ID>
# 阿里云身份验证参数
odps.access.key=<您的阿里云访问密钥>
# MaxCompute Endpoint
odps.end.point=<您的 MaxCompute Endpoint>
# MaxCompute 独享资源组名
odps.quota.name=<您的 MaxCompute 独享资源组名>
```

将 <您的 MaxCompute 项目名称>、<您的阿里云访问ID>、<您的阿里云访问密钥>、<您的 MaxCompute 服务端点> 以及 <您的 MaxCompute 独享资源组名> 替换为您的 MaxCompute 环境的实际值。

### 访问多个MaxCompute项目

若要同时访问多个 MaxCompute 项目，请为每个项目创建独立的配置文件。例如，如果您有两个项目，一个用于“销售”(`sales`)，另一个用于“分析”(`analytics`)，则可创建`sales.properties`
和`analytics.properties`两个配置文件。在每个文件中指定不同的`odps.project.name`：

- `sales.properties`针对销售项目。
- `analytics.properties`针对分析项目。

这样设置后，在 Presto 中分别对应创建了`sales`和`analytics`两个目录。

### 数据类型映射

在使用 MaxCompute 连接器时，MaxCompute 的数据类型会映射到 Presto 的数据类型。下表展示了这种映射关系及其对应的 Presto 类型：

| MaxCompute数据类型 | Presto数据类型 |
|----------------|------------|
| BOOLEAN        | BOOLEAN    |
| TINYINT        | TINYINT    |
| SMALLINT       | SMALLINT   |
| INT            | INTEGER    |
| BIGINT         | BIGINT     |
| FLOAT          | REAL       |
| DOUBLE         | DOUBLE     |
| DECIMAL        | DECIMAL    |
| STRING         | VARCHAR    |
| VARCHAR        | VARCHAR    |
| JSON           | VARCHAR    |
| CHAR           | CHAR       |
| BINARY         | VARBINARY  |
| DATE           | DATE       |
| DATETIME       | TIMESTAMP  |
| TIMESTAMP      | TIMESTAMP  |
| TIMESTAMP_NTZ  | TIMESTAMP  |
| DECIMAL        | DECIMAL    |
| ARRAY          | ARRAY      |

请注意，部分 MaxCompute 数据类型（如`MAP`、`STRUCT`）在当前版本的连接器中尚未完全支持。未来版本中，我们将继续完善对这些数据类型的支持。

### 其他

本项目仍在积极开发中，并计划在后续版本中引入更多功能，如支持DDL操作、数据写入和更丰富的数据类型支持。我们欢迎并鼓励社区成员的参与贡献。

### 贡献者

- [Jason Zhang](https://github.com/dingxin-tech)