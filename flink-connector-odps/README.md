## 概述
Flink connector odps 为开源Flink引擎读写odps表提供支持，主要包含以下功能：

- 提供OdpsInputFormat和OdpsOutputFormat接口，支持批读写odps表
- 提供OdpsSinkFunction接口，支持流式写入odps表
- 提供OdpsDynamicTableFactory，支持通过Table Api和SQL读写Odps Table
- 提供OdpsCatalog，支持直接创建/删除/查看Odps Table，以及通过SQL读写Odps Table
- 写入模式的支持如下表，批模式下数据在所有Task完成后可见，流模式下数据会定时Append到Table中

| 模式 | 动态分区 | 写入模式 |
| --- | --- | --- |
| 批模式 | 不支持动态分区 | 支持Overwrite和Append |
| 流模式 | 支持动态分区 | 只支持Append |



## 搭建开发环境
### Maven依赖
需要包含的依赖
```
        <dependency>
            <groupId>com.aliyun.odps</groupId>
            <artifactId>flink-connector-odps</artifactId>
            <version>1.14-SNAPSHOT</version>
        </dependency>
```

## OdpsTable
### 介绍 
Odps table 可以按如下定义：
```
-- 在 Flink SQL 中注册一张已经存在的Odps表 'users'
CREATE TABLE MyUserTable (
  id BIGINT,
  name STRING,
  age INT,
  status BOOLEAN
) WITH (
   'connector' = 'odps',
   'table-name' = 'users',
   'odps.access.id'='xxx',
   'odps.access.key'='xxx',
   'odps.end.point'='xxx',
   'odps.project.name'='xxx'
);

-- 在 Flink SQL 中注册一张已经存在的Odps分区表 'ptTable'
CREATE TABLE MyPtTable (
  x STRING, 
  p1 INT, 
  p2 STRING
) partitioned by (p1, p2) WITH (
   'connector' = 'odps',
   'table-name' = 'ptTable',
   'odps.access.id'='xxx',
   'odps.access.key'='xxx',
   'odps.end.point'='xxx',
   'odps.project.name'='xxx'
);


-- 从另一张表 "T" 将数据写入到 ODPS 表中
INSERT INTO MyUserTable
SELECT id, name, age, status FROM T;


-- Insert with static partition
INSERT INTO MyPtTable 
PARTITION (p1=1, p2='2019-08-08') SELECT 'Tom'

-- Insert with dynamic partition
INSERT INTO MyPtTable SELECT 'Tom', 1, '2019-08-08';

-- Insert with static(p1) and dynamic(p2) partition
INSERT INTO MyPtTable PARTITION (p1=2) SELECT 'Tom', '2019-08-08';

-- 查看 ODPS 表中的数据
SELECT id, name, age, status FROM MyUserTable;

-- ODPS 表在时态表关联中作为维表
SELECT * FROM myTopic
LEFT JOIN MyUserTable FOR SYSTEM_TIME AS OF myTopic.proctime
ON myTopic.key = MyUserTable.id;
```

- 注意：创建表时需要按照odps table schema，且列顺序保持一致



### 数据类型映射

- Flink类型与Odps类型的映射如下表

| Flink Data Type | Odps Data Type |
| --- | --- |
| CHAR(p) | CHAR(p) |
| VARCHAR(p) | VARCHAR(p) |
| STRING | STRING |
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INT |
| BIGINT | LONG |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL(p, s) | DECIMAL(p, s) |
| DATE | DATE |
| TIMESTAMP(9) [WITHOUT TIMEZONE] | TIMESTAMP |
| TIMESTAMP(3)  [WITHOUT TIMEZONE] | DATETIME |
| BYTES | BINARY |
| ARRAY<T> | LIST<T> |
| MAP<K, V> | MAP<K, V> |
| ROW | STRUCT |



- 注意：通过OdpsCatalog创建Odps table时
   - 如果TIMESTAMP精度小于等于3，对应的Odps Table数据类型为DATETIME
   - 如果TIMESTAMP精度大于3小于等于9，对应的Odps Table数据类型为TIMESTAMP
   - CHAR最大长度为255
   - VARCHAR最大长度为65535
### Connector参数
| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| connector| connector类型，需要设置为odps | 无默认值 |
| odps.project.name | ODPS Project名称 | 无默认值 |
| odps.access.id | ODPS Access Id | 无默认值 |
| odps.access.key | ODPS Access Key | 无默认值 |
| odps.end.point | ODPS Endpoint | 无默认值 |
| table-name | ODPS Table名称，格式为[project.]table，若project与odps.project.name不一致，则project不可省略 | 无默认值 |
| odps.input.split.size | 读表时每个split大小，默认为256MB | 256 |
| odps.cupid.writer.buffer.enable | 批式写入参数，写表时是否使用Buffered Writer | true |
| odps.cupid.writer.buffer.size | 批式写入参数，Buffered Writer缓存的最大数据 | 64mb |
| lookup.cache.ttl | Lookup cache 中表记录的最大存活时间，若超过该时间，则会重新加载表 | 10min |
| lookup.max-retrie | Lookup 读取数据最大重试次数 | 3 |
| sink.buffer-flush.max-size | 流式写入参数，flush 前缓存记录的最大值，可以设置为 '0' 来禁用它 | 16mb |
| sink.buffer-flush.max-rows | 流式写入参数，flush 前缓存记录的最大行数，可以设置为 '0' 来禁用它 | 1000 |
| sink.buffer-flush.interval | 流式写入参数，flush 间隔时间，超过该时间后异步线程将 flush 数据。可以设置为 '0' 来禁用它。注意, 为了完全异步地处理缓存的 flush 事件，可以将 'sink.buffer-flush.max-rows' 和'sink.buffer-flush.max-size'设置为 '0' 并配置适当的 flush 时间间隔 | 300s |
| sink.dynamic-partition.limit | 动态分区写入时，单个Task可同时写的分区数量 | 20 |
| sink.parallelism | 写入的并行度，如果不设置，则默认使用上游数据并行度 | 无默认值 |
| sink.max-retries | 写入记录到ODPS失败后的最大重试次数 | 3 |

## OdpsCatalog
### 介绍

- OdpsCatalog 提供了Odps元数据信息，通过OdpsCatalog可以创建/删除/查看Odps表。需要注意：
   - OdpsCatalog不支持临时表，通过OdpsCatalog创建的表均为Odps物理表
   - 不支持表属性
- Flink Catalog 和 Odps 之间的映射如下：

| Flink Catalog Metaspace Structure | Odps Metaspace Structure |
| --- | --- |
| catalog name (defined in Flink only) | N/A |
| database name | project name |
| table name | table name |

### 连接到Odps
#### 使用编程方式
```
 // 注册OdpsCatalog后，用户可以通过Table API和SQL操作Odps表
 OdpsConf odpsConf = OdpsUtils.getOdpsConf()
 OdpsCatalog catalog = new OdpsCatalog(catalogName, projectName, odpsConf);
 EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
 TableEnvironment tableEnv = TableEnvironment.create(settings);
 tableEnv.registerCatalog(catalogName, catalog);
```
#### 使用SQL Cli

- step 1:  配置odps.conf
   - 在flink conf目录下创建odps.conf文件，填写以下project以及ak信息
```
odps.project.name=xxxxx
odps.access.id=xxxxx
odps.access.key=xxxxxx
odps.end.point=xxxxxx
```

- step 2: 添加依赖，配置SQL CLI 
   - 将jar包添加到lib/目录下
   - 配置conf/sql-client-defaults.yaml
```
execution:
    planner: blink
    type: streaming
    current-catalog: myodps
    current-database: cupid_test_release
    
catalogs:
   - name: myodps
     type: odps
     odps-conf-dir: /home/peter.wsj/flink/flink-1.13.2/conf/  # contains odps.conf
```

   - 或者使用SQL创建catalog
```
-- Define available catalogs

CREATE CATALOG MyCatalog
  WITH (
    'type' = 'odps',
    'odps-conf-dir' = '/home/peter.wsj/flink/flink-1.13.2/conf/'
  );
```

- step 3: 执行Flink SQL
```
-- create odps tables

create table MyCatalog.[projectname].mc_test_table (
 id STRING,
 value INT
);

-- select odps tables

select * from MyCatalog.[projectname].mc_test_table;

```
### DDL
#### DATABASE ｜View ｜Function

- Flink中的database对应于Odps中的project，OdpsCatalog不支持对database进行ddl操作
- OdpsCatalog不支持视图和函数
#### Table

- Create
```
CREATE TABLE [IF NOT EXISTS] table_name
  [(col_name data_type [COMMENT col_comment], ... )]
  [COMMENT table_comment]
  [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
```

- Alter
```
ALTER TABLE table_name RENAME TO new_table_name;
ALTER TABLE table_name ADD [IF NOT EXISTS] (PARTITION partition_spec);
ALTER TABLE table_name DROP [IF EXISTS] PARTITION partition_spec
```

- Drop
```
DROP TABLE [IF EXISTS] table_name;
```
### DML

- Insert 
```
// partition_spec可以是完整或部分分区。如果partition_spec只是部分，则为动态分区
INSERT (INTO|OVERWRITE) [TABLE] table_name [PARTITION partition_spec] SELECT ...;

// examples
//  ------ Insert with static partition ------ 
INSERT OVERWRITE myparttable PARTITION (my_type='type_1', my_date='2019-08-08') SELECT 'Tom', 25;
// ------ Insert with dynamic partition ------ 
INSERT OVERWRITE myparttable SELECT 'Tom', 25, 'type_1', '2019-08-08';
// ------ Insert with static(my_type) and dynamic(my_date) partition ------ 
INSERT OVERWRITE myparttable PARTITION (my_type='type_1') SELECT 'Tom', 25, '2019-08-08';
```
### DQL

- Flink使用[Apache Calcite](https://calcite.apache.org/docs/reference.html)解析SQL，具体语法参考[文档](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/batch/connectors.html)
- 查询优化
   - 分区裁剪：支持分区裁剪以限制Flink查询Odps表时读取的文件和分区的数量。对数据进行分区后，当查询与某些过滤条件匹配时，Flink仅读取Odps表中的分区的子集。
   - 列裁剪：Flink利用投影下推功能，通过从表扫描中删除不必要的字段来最大程度地减少Flink和Odps表之间的数据传输。



## 数据读取

- 数据读取的核心类是OdpsInputFormat
```
  // columns, partitions决定了要读取的列和分区
  public OdpsInputFormat(OdpsConf odpsConf, String project, String table, String[] columns, String[] partitions, int splitSize);
```

- 注意columns和partitions数组传入为null时，默认读取全部列和全部分区，如果传入为空数组，代表读取空列和空分区
- OdpsInputFormat支持读取的数据类型包括：
   - **Java元组：最多支持25个字段，根据位置进行映射**
   - **Java POJO：根据列名匹配POJO的属性名称**
   - **Row：通过SQL读取的类型，根据位置进行映射**
### DataSetAPI
```
// pojo
OdpsInputFormat<WordWithCount> inputFormat = new OdpsInputFormat<WordWithCount>(odpsConf, odpsConf.getProject(), inputTableName, 32).asPojos(WordWithCount.class);
final DataSet<WordWithCount> counts = env.createInput(inputFormat).groupBy(new KeySelector<WordWithCount, String>() {
      @Override
      public String getKey(WordWithCount w) {
                return w.key;
      }
}).reduce(new WordCounter());
counts.print();

// tuple
OdpsInputFormat<Tuple2<String,Long>> inputFormat = new OdpsInputFormat<Tuple2<String,Long>>(odpsConf, odpsConf.getProject(), inputTableName, 32).
      asFlinkTuples();
final DataSet<Tuple2<String,Long>> counts = env.createInput(inputFormat).groupBy(0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
     @Override
     public Tuple2<String,Long> reduce(Tuple2<String,Long> t1, Tuple2<String,Long> t2) throws Exception {
          t1.setField( (long)t1.getField(1)+(long)t2.getField(1),1);
          return t1;
     }
});
counts.print();

// row
OdpsInputFormat<Row> inputFormat = new OdpsInputFormat<>(odpsConf, odpsConf.getProject(), inputTableName, 32);
final DataSet<Row> counts = env.createInput(inputFormat).groupBy(0).reduce(new ReduceFunction<Row>() {
	  @Override
	  public Row reduce(Row row, Row t1) throws Exception {
         row.setField(1, (long)row.getField(1)+(long)t1.getField(1));
         return row;
    }
});
counts.print();
```
### DataStreamAPI
```
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<String> text;
text = env.createInput(new OdpsInputFormat<Row>(projectName, inputTableName)).flatMap(
			new RichFlatMapFunction<Row, String>() {
				@Override
				public void flatMap(Row record, Collector<String> collector) throws Exception {
					Thread.sleep(sleepTime * 100);
					collector.collect((String) record.getField(0));
				}
			});
DataStream<Tuple2<String, Integer>> counts =
			text.flatMap(new Tokenizer())
					.keyBy(0).sum(1);
```
### Table API/SQL
```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
TableEnvironment tEnv = StreamTableEnvironment.create(env);
tEnv.executeSql(
                "CREATE TABLE MyUserTable (
                  id BIGINT,
                  name STRING,
                  age INT,
                  status BOOLEAN
                ) WITH (
                   'connector' = 'odps',
                   'table-name' = 'users',
                   'odps.access.id'='xxx',
                   'odps.access.key'='xxx',
                   'odps.end.point'='xxx',
                   'odps.project.name'='xxx'
                )");

Iterator<Row> collected = tEnv.executeSql("SELECT * FROM MyUserTable").collect();
```
## 数据写入

- 数据写入的核心类是OdpsOutputFormat和OdpsSinkFunction
- 如果是批模式，使用OdpsOutputFormat，如果是流模式，使用OdpsSinkFunction
### DataSetAPI
```
OdpsInputFormat<Row> inputFormat = new OdpsInputFormat<>(odpsConf, odpsConf.getProject(), inputTableName, 32);
final DataSet<Row> counts = env.createInput(inputFormat).groupBy(0).reduce(new ReduceFunction<Row>() {
      @Override
      public Row reduce(Row row, Row t1) throws Exception {
           row.setField(1, (long)row.getField(1)+(long)t1.getField(1));
           return row;
      }
 });
 counts.output(new OdpsOutputFormat<Row>(odpsConf.getProject(), outTaputTableName, false));
```
### DataStreamAPI
```
 String hostname = "localhost";
 int port = 9999;

DataStream<String> text = env.socketTextStream(hostname, port, "\n");

 DataStream<WordWithCount> windowCounts = text
         .flatMap(new FlatMapFunction<String, WordWithCount>() {
             @Override
             public void flatMap(String value, Collector<WordWithCount> out) {
                 for (String word : value.split("\\s")) {
                     out.collect(new WordWithCount(word, 1L));
                 }
             }
         })
         .keyBy("key")
         .timeWindow(Time.seconds(5))
         .reduce(new ReduceFunction<WordWithCount>() {
             @Override
             public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                 return new WordWithCount(a.key, a.cnt + b.cnt);
             }
         });

 OdpsSinkFunction.OdpsSinkBuilder<Tuple2<String, Long>> builder =
         new OdpsSinkFunction.OdpsSinkBuilder<>(odpsConf, odpsConf.getProject(), output);
 windowCounts.map(new RichMapFunction<WordWithCount, Tuple2<String, Long>>() {
     @Override
     public Tuple2<String, Long> map(WordWithCount wordWithCount) throws Exception {
         return new Tuple2<>(wordWithCount.key, wordWithCount.cnt);
     }
 }).addSink(builder.build());
 env.execute("Socket Window WordCount");
```
### Table API/SQL
```
-- streaming sql, insert into odps table
INSERT INTO TABLE odps_table SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH') FROM kafka_table;
```
### ​
