Presto-ODPS Connector作为Presto的一个Plugin，可以支持通过Presto SQL来访问Odps表的数据，并且可以结合其他Catalog的数据进行联邦查询，下面将介绍下这个Plugin的部署和使用。

## 部署步骤
Presto的部署可以参考官方文档：https://prestodb.io/docs/current/installation/deployment.html
为了简单起见，下面以部署一个单节点Presto Cluster为例来看下如何在Presto中加入Odps Connector。

1、下载0.229的Presto版本并解压（当前只在该版本验证过，相近版本应该也是可以支持的）：

```shell
# 下载 presto 0.229
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.229/presto-server-0.229.tar.gz
# 解压
tar zxvf presto-server-0.229.tar.gz
```

2、编译并部署Presto-ODPS Connector的Plugin jar包

```shell
# 进入presto-connector项目根目录
cd ${workspace}/presto-connector

# 打包presto-ODPS connector
mvn clean package

# 部署connector
mkdir ${presto-server-0.229}/plugin/odps
cp ${workspace}/presto-connector/target/presto-connector-${version}.jar ${presto-server-0.229}/plugin/odps/
cp ${workspace}/presto-connector/libs/*.jar ${presto-server-0.229}/plugin/odps/
``` 

3、配置Presto
在解压后的presto-server-0.229目录下创建etc目录，etc目录里面分别需要创建config.properties，jvm.config，log.properties，node.properties以及catalog目录。
```text
${presto-server-0.299}
  |
  +-- config.properties
  |
  +-- jvm.config
  |
  +-- log.properties
  |
  +-- node.properties
  |
  +-- catalog
       |
       +-- odps.properties

```

- config.properties 内容如下
```text
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=5GB
query.max-memory-per-node=1GB
query.max-total-memory-per-node=2GB
discovery-server.enabled=true
discovery.uri=http://127.0.0.1:8080
```

- jvm.config 内容如下

```text
-server
-Xmx16G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
```

- log.properties 内容如下
```text
com.facebook.presto=INFO
```

- node.properties 内容如下

```text
node.environment=production
node.id=ffffffff-ffff-ffff-ffff-fffffffffffe
node.data-dir=/path/to/presto-server-0.229/presto/data
```

- odps.properties 内容如下：
```text
connector.name=odps 
odps.project.name=XXXXXX # maxcompute 项目名字
odps.access.id=XXXXXX # 阿里云鉴权参数
odps.access.key=XXXXXX # 阿里云鉴权参数
odps.end.point=XXXXXX # maxcompute 服务endpoint
odps.tunnel.end.point=xxx # maxcompute tunnel endpoint
odps.input.split.size=64 # 切分表的split大小

```

> 注意：
> - odps.end.point和odps.tunnel.end.point的参数配置具体可以参考文档：https://help.aliyun.com/document_detail/34951.html ，根据project所在的region以及presto运行所在的网络环境来配置。
> - 如果需要同时访问多个project，通过etc/catalog/odps.properties新增的参数来配置，odps.project.name.extra.list=cupid_test,xxx,aaaas  这样逗号分隔，配置多个。

4、启动Presto

上面这些配置都完成后，就可以启动Presto了。
- bin/launcher start 这样会把Presto放到后台运行
- bin/launcher run 这样是在前台运行

## 使用方式
下面介绍通过Presto CLI的方式来连接使用Odps Catalog.

1、下载Presto CLI
```shell
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.229/presto-cli-0.229-executable.jar
```

下载后将presto-cli-0.229-executable.jar重命名为presto，并且加上可执行的权限。
```shell
mv presto-cli-0.229-executable.jar presto
chmod +x presto
```

2、运行Presto CLI

通过如下命令即可连接上Odps的Catalog进行使用。
```shell
./presto --server 127.0.0.1:8080 --catalog odps --schema project_name
```
在Presto CLI成功连接后，可以使用Presto的SQL对Odps表数据进行查询，例如：

```sql
show tables;
desc tablename;
select * from xxx limit 10;
select count(*) from xxx;
```

限制条件
- 不支持读取array,map,struct等复杂类型的表数据
- 不支持将数据写入odps表