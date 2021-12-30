spark-datasource-v2.3是符合[spark datasource v2](https://issues.apache.org/jira/browse/SPARK-15689)规范的一个实现，开发者可以使用此datasource访问阿里云MaxCompute表。支持操作如下:


|表类型|操作方式|支持情况|
|:----|:----|:----|
|内部非分区表| 读 | 支持 |
|内部非分区表| 写 | 支持
|内部分区表 | 分区裁剪读 | 支持 |
|内部分区表 | 全分区读 | 支持 |
|内部分区表 | 指定分区append写 | 支持 |
|内部分区表 | 指定分区overwrite写 | 支持 |
|内部分区表 | 动态分区append写 | 支持 |
|内部分区表 | 动态分区overwrite写 | 不支持 |

## 1 spark-datasource-v2.3简介


![spark-datasource-description](doc-images/spark_datasource_description.jpg)

模块包含的内容如上图所示，spark-datasource-v2.3仅包含红色背景模块，绿色背景模块需要使用者准备。模块间的依赖关系是：上方模块会依赖下方模块。各个模块的介绍如下。

- spark Application 以代码形式存在，位于${project.dir}/spark-datasource-v2.3/src/test/scala目录下。既是项目的自测代码，也是使用spark-datasource的样例代码。
- spark distribution 实际是spark引擎本身，本项目在spark-2.3引擎上测试通过。用户可以下载[spark-2.3引擎](https://archive.apache.org/dist/spark/spark-2.3.0/)试用。
- ODPS spark source 以代码形式存在，位于${project.dir}/spark-datasource-v2.3/src/main/scala目录下。这个部分与spark引擎按照[spark datasource v2](https://issues.apache.org/jira/browse/SPARK-15689)规范交互，利用[MaxCompute官方SDK](https://help.aliyun.com/document_detail/34614.html)访问表元信息，利用[tunnel](https://help.aliyun.com/document_detail/27835.html)访问MaxCompute表数据。
- ODPS SDK 是提交到MAVEN仓库的jar包，已经配置在pom.xml文件依赖中。用户在构建本模块时会自动下载打包。spark datasource 使用SDK获得表的元信息。例如：列名及类型、分区列及类型、表的所有分区等等。点击查看[MaxCompute官方SDK](https://help.aliyun.com/document_detail/34614.html)介绍。
- cupid-table API 以jar包形式存在，位于${project.dir}/spark-datasource-v2.3/libs目录下。cupid-table API封装了环境差异，对引擎提供一套稳定的API。如果引擎运行在MaxCompute集群内，那么是可以直接访问MaxCompute物理数据的。如果引擎运行在MaxCompute集群外，那么需要利用[tunnel](https://help.aliyun.com/document_detail/27835.html)访问MaxCompute表。cupid-table的存在解决了这些环境差异，spark-datasource仅需考虑cupid-table API的特性列表就可以了。
- tunnel-table-impl 以jar包形式存在，位于${project.dir}/spark-datasource-v2.3/libs。这是cupid-table API的一种实现，底层依赖了[阿里云tunnel](https://help.aliyun.com/document_detail/27835.html)。

## 2 构建方法

首先，进入spark-datasource-v2.3目录.
```shell
cd ${project.dir}/spark-datasource-v2.3
```


其次，执行mvn命令构建spark-datasource。
```shell
mvn clean package jar:test-jar
```

最后, 确认构建结果。
```text
# spark-datasource
ls -l ${project.dir}/spark-datasource-v2.3/target/spark-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar

# test-case/example
ls -l ${project.dir}/spark-datasource-v2.3/target/spark-datasource-1.0-SNAPSHOT-tests.jar
``` 

## 3 使用方式

首先，配置SPARK_HOME环境变量指向spark v2.3版本引擎根目录。如下图是测试环境的环境变量。
![spark-home-env](./doc-images/spark_home_env.jpg)

然后，进入SPARK_V2.3 引擎根目录

```shell
cd ${SPARK_HOME}
```

然后，执行读表操作。
```shell
./bin/spark-submit \
    --master local \
    --jars ${project.dir}/spark-datasource-v2.3/target/spark-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar,${project.dir}/spark-datasource-v2.3/libs/cupid-table-api-1.1.5-SNAPSHOT.jar,${project.dir}/spark-datasource-v2.3/libs/table-api-tunnel-impl-1.1.5-SNAPSHOT.jar \
    --class DataReaderTest \
    ${project.dir}/spark-datasource-v2.3/target/spark-datasource-1.0-SNAPSHOT-tests.jar \
    ${maxcompute-project-name} \
    ${aliyun-access-key-id} \
    ${aliyun-access-key-secret} \
    ${maxcompute-table-name}
    
#example
./bin/spark-submit \
    --master local \
    --jars ${project.dir}/spark-datasource-v2.3/target/spark-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar,${project.dir}/spark-datasource-v2.3/libs/cupid-table-api-1.1.5-SNAPSHOT.jar,${project.dir}/spark-datasource-v2.3/libs/table-api-tunnel-impl-1.1.5-SNAPSHOT.jar \
    --class DataReaderTest \
    ${project.dir}/spark-datasource-v2.3/target/spark-datasource-1.0-SNAPSHOT-tests.jar \
    test_project \
    ${aliyun-access-key-id} \
    ${aliyun-access-key-secret} \
    test_o
```


然后，执行写表操作。
```shell
./bin/spark-submit \
    --master local \
    --jars ${project.dir}/spark-datasource-v2.3/target/spark-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar,${project.dir}/spark-datasource-v2.3/libs/cupid-table-api-1.1.5-SNAPSHOT.jar,${project.dir}/spark-datasource-v2.3/libs/table-api-tunnel-impl-1.1.5-SNAPSHOT.jar \
    --class DataWriterTest \
    ${project.dir}/spark-datasource-v2.3/target/spark-datasource-1.0-SNAPSHOT-tests.jar \
    ${maxcompute-project-name} \
    ${aliyun-access-key-id} \
    ${aliyun-access-key-secret} \
    ${maxcompute-table-name} \
    ${partition-descripion} 
    
# example
./bin/spark-submit \
    --master local \
    --jars ${project.dir}/spark-datasource-v2.3/target/spark-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar,${project.dir}/spark-datasource-v2.3/libs/cupid-table-api-1.1.5-SNAPSHOT.jar,${project.dir}/spark-datasource-v2.3/libs/table-api-tunnel-impl-1.1.5-SNAPSHOT.jar \
    --class DataWriterTest \
    ${project.dir}/spark-datasource-v2.3/target/spark-datasource-1.0-SNAPSHOT-tests.jar \
    test_project \
    ${aliyun-access-key-id} \
    ${aliyun-access-key-secret} \
    test_o \
    20211201
```
