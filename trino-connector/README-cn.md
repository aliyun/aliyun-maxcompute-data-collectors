# MaxCompute 连接器

MaxCompute 连接器支持直接查询和处理存储于 MaxCompute 数据仓库中的数据，特别适用于实现 MaxCompute 与 Hive 等系统间的数据集成与分析。

## 前置条件

使用前请确认以下条件已满足：

- **访问权限**  
  拥有有效的 MaxCompute 项目访问权限及身份验证凭证（AccessKey ID/Secret）。

- **网络配置**  
  推荐使用阿里云 VPC 网络以确保数据传输稳定性。🔗 [MaxCompute 网络配置指南](https://help.aliyun.com/zh/maxcompute/user-guide/network-connection-process)

- **资源组**  
  需使用🔗 [独享资源组](https://help.aliyun.com/zh/maxcompute/user-guide/purchase-and-use-exclusive-resource-groups-for-dts)或开启 🔗 [开放存储（按量计费）](https://help.aliyun.com/zh/maxcompute/product-overview/open-storage-pay-as-you-go)。  

- **Schema 模式（可选）**  
  如需在 Trino 中使用 Namespace Schema 功能，需在 MaxCompute 中启用 🔗 [Schema 模式](https://help.aliyun.com/zh/maxcompute/user-guide/schema-related-operations)。  

---

## 安装与配置

### 获取连接器

#### 预编译版本（推荐）
- Trino 470
- Trino 422

#### 自行构建
```bash
# 使用 Maven 构建
mvn clean package
# 解压生成的 ZIP 至 Trino 插件目录
unzip target/trino-maxcompute-*.zip -d $TRINO_HOME/plugin/
```

### 配置文件示例

在 `etc/catalog` 目录下创建 `maxcompute.properties`：
```properties
connector.name=maxcompute
odps.project.name=your_project_name
odps.access.id=your_access_key_id
odps.access.key=your_access_key_secret
odps.end.point=http://service.cn-hangzhou.maxcompute.aliyun.com/api
odps.quota.name=your_quota_name
```

⚠️ **重要配置说明**
- 环境变量要求：启动 Trino 时需设置
```bash
  export _JAVA_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
>   ```
🔗 由于 MaxCompute 使用 Apache Arrow 进行数据传输，详见[Apache Arrow 配置文档](https://arrow.apache.org/docs/java/install.html)

---

## 多项目访问

通过创建多个配置文件实现跨项目访问：
```
etc/catalog/
├── sales.properties     # 配置 odps.project.name=sales
└── analytics.properties # 配置 odps.project.name=analytics
```
Trino 将自动创建对应的 `sales` 和 `analytics` 目录。

---

## 数据类型映射表

| MaxCompute 类型 | Trino 类型 | 说明 |
|----------------|------------|------|
| BOOLEAN        | BOOLEAN    | -    |
| TINYINT        | TINYINT    | -    |
| SMALLINT       | SMALLINT   | -    |
| INT            | INTEGER    | -    |
| BIGINT         | BIGINT     | -    |
| FLOAT          | REAL       | -    |
| DOUBLE         | DOUBLE     | -    |
| DECIMAL        | DECIMAL    | -    |
| STRING         | VARCHAR    | -    |
| VARCHAR        | VARCHAR    | -    |
| JSON           | VARCHAR    | -    |
| CHAR           | CHAR       | -    |
| BINARY         | VARBINARY  | -    |
| DATE           | DATE       | -    |
| DATETIME       | TIMESTAMP  | -    |
| TIMESTAMP      | TIMESTAMP  | -    |
| TIMESTAMP_NTZ  | TIMESTAMP  | -    |

> ⚠️ **限制说明**  
> 当前版本暂不支持复杂类型（MAP/STRUCT/ARRAY），后续版本将逐步完善。

---

## 开发计划

- ✅ 支持基本类型读取
- 🚧 开发中：DDL 操作支持
- 📅 规划中：数据写入功能、复杂类型支持

欢迎通过 PR 或 Issue 参与贡献！  
👤 主要维护者：[Jason Zhang](https://github.com/dingxin-tech)