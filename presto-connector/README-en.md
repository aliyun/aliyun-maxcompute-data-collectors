As a Plugin of Presto, Presto-ODPS Connector can support access to Odps table data through Presto SQL, and can combine data from other catalogs for federated query. The following will introduce the deployment and usage of this Plugin.

## Deploying Presto
For the deployment of Presto, refer to the official documentation: https://prestodb.io/docs/current/installation/deployment.html. For simplicity, let's take the deployment of a single-node Presto Cluster as an example to see how to add Odps Connector to Presto.

1、Download the Presto version 0.229 and unzip it (currently it has only been verified in this version, and similar versions should also be supported):

```shell
# download presto 0.229
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.229/presto-server-0.229.tar.gz
# unzip
tar zxvf presto-server-0.229.tar.gz
```

2、Compile and deploying the Plugin jar package

```shell
# Enter the presto-connector project root directory
cd ${workspace}/presto-connector

# Package the presto-ODPS connector
mvn clean package

# deploy connector
mkdir ${presto-server-0.229}/plugin/odps
cp ${workspace}/presto-connector/target/presto-connector-${version}.jar ${presto-server-0.229}/plugin/odps/
cp ${workspace}/presto-connector/libs/*.jar ${presto-server-0.229}/plugin/odps/
``` 

3、Configure Presto 

Create the sub-directory etc in the decompressed presto-server-0.229 directory. In the etc directory, you need to create the config.properties, jvm.config, log.properties, node.properties and catalog directories respectively.

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

- The content of config.properties
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

- The content of jvm.config 

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

- The content of log.properties
```text
com.facebook.presto=INFO
```

- The content of node.properties

```text
node.environment=production
node.id=ffffffff-ffff-ffff-ffff-fffffffffffe
node.data-dir=/path/to/presto-server-0.229/presto/data
```

- The content of odps.properties
```text
connector.name=odps 
odps.project.name=XXXXXX # maxcompute project name
odps.access.id=XXXXXX # Alibaba Cloud authentication parameters
odps.access.key=XXXXXX # Alibaba Cloud authentication parameters
odps.end.point=XXXXXX # maxcompute service endpoint
odps.tunnel.end.point=xxx # maxcompute tunnel endpoint
odps.input.split.size=64 # split size while reading table

```

> Notice：
> - For details of configuration items both odps.end.point and odps.tunnel.end.point, please refer to the document:https://help.aliyun.com/document_detail/34951.html. 
> - If you need to access multiple projects at the same time, configure them through the newly added parameters in etc/catalog/odps.properties, such as odps.project.name.extra.list=cupid_test,xxx,aaaas separated by commas.

4、Start Presto

After the above configuration is completed, you can start Presto.
- `bin/launcher start` start Presto as daemon service
- `bin/launcher run` start presto as foreground process

## Usage
The following describes how to connect and use Odps Catalog through Presto CLI.

1、Download Presto CLI
```shell
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.229/presto-cli-0.229-executable.jar
```

After downloading, rename presto-cli-0.229-executable.jar to presto and add executable permissions.
```shell
mv presto-cli-0.229-executable.jar presto
chmod +x presto
```

2、Run the Presto CLI

Use the following command to connect to the Odps Catalog for use.
```shell
./presto --server 127.0.0.1:8080 --catalog odps --schema project_name
```

After the Presto CLI is successfully connected, you can use the Presto SQL to query the Odps table data, for example:
```sql
show tables;
desc tablename;
select * from xxx limit 10;
select count(*) from xxx;
```

Limitation
- It does not support reading table data of complex types such as array, map, struct, etc.
- Writing data to odps table is not supported