# Datahub Handler for Oracle GoldenGate

[中文版](https://datahub.console.aliyun.com/intro/guide/plugins/ogg.html)

The datahub handler processes the operations of OGG trails, and upload the change logs into datahub. It has the following features:

- Support 3 types of operation logs: *INSERT*, *UPDATE*, *DELETE*
- User customize key fields, focus fields, and corresponding datahub field types
- Configurable retry times and retry interval
- Provide mechanism to handle dirty data

## Getting Started

### Requirements

1. JDK 1.6 or later (JDK 1.7 recommended)
2. Apache Maven 3.x
3. OGG Java Adapter

### Build the Package

Build the package with the script file:

```
$ cd ogg-plugin
$ mvn clean package -DskipTests
```

Wait until building success. The datahub handler will be in **target/aliyun-datahub-ogg-plugin-x.x.x.tar.gz**

### Use Datahub Handler

1. Configure datahub handler for OGG extract (conf/javaue.properties).
   
    Set the handler name and type:

    ```
    gg.handlerlist=ggdatahub
    gg.handler.ggdatahub.type=com.aliyun.odps.ogg.handler.datahub.DatahubHandler
    ```

    Set jvm boot options and goldengate classpath:

    ```
    
    gg.handler.ggdatahub.configureFileName={YOUR_HOME}/aliyun-datahub-ogg-plugin/conf/configure.xml
    goldengate.userexit.nochkpt=false
    goldengate.userexit.timestamp=utc
     
    gg.classpath={YOUR_HOME}/aliyun-datahub-ogg-plugin/lib/*
    gg.log.level=debug
        
    jvm.bootoptions=-Xmx512m -Dlog4j.configuration=file:{YOUR_HOME}/aliyun-datahub-ogg-plugin/conf/log4j.properties -Djava.class.path=ggjava/ggjava.jar
    ```


2. Configure other handler parameters (conf/configure.xml).

    ```
    <?xml version="1.0" encoding="UTF-8"?>
<configure>

    <defaultOracleConfigure>
        <!-- oracle sid, required-->
        <sid>100</sid>
        <!-- oracle database name, can overwritten by oracleSchema in mappings-->
        <schema>ogg_test</schema>
        <!-- dateformat, optional, default is yyyy-MM-dd HH:mm:ss-->
        <dateFormat>yyyy-MM-dd HH:mm:ss</dateFormat>
    </defaultOracleConfigure>

    <defalutDatahubConfigure>
        <!-- datahub endpoint, required-->
        <endPoint>YOUR_DATAHUB_ENDPOINT</endPoint>
        <!-- datahub project, can overwritten by datahubProject in mappings-->
        <project>YOUR_DATAHUB_PROJECT</project>
        <!-- datahub accessId, can overwritten by accessId in mappings-->
        <accessId>YOUR_DATAHUB_ACCESS_ID</accessId>
        <!-- datahub accessKey, can overwritten by accessKey in mappings-->
        <accessKey>YOUR_DATAHUB_ACCESS_KEY</accessKey>
        <!-- the column in datahub corresponding to data change type, can overwritten by ctypeColumn in columnMapping-->
        <ctypeColumn></ctypeColumn>
        <!-- the column in datahub corresponding to data change time, can overwritten by ctimeColumn in columnMapping-->
        <ctimeColumn></ctimeColumn>
    </defalutDatahubConfigure>

    <!-- default is not writing dirty data files and will retry forever-->

    <!-- the max size upload to datahub one time, optional, default 1000-->
    <batchSize>1000</batchSize>

    <!-- whether to continue or not when dirty data appears, optional, default false-->
    <dirtyDataContinue>false</dirtyDataContinue>

    <!-- dirty data file path, optional, default datahub_ogg_plugin.dirty-->
    <dirtyDataFile>datahub_ogg_plugin.dirty</dirtyDataFile>

    <!-- max size of dirty file in MB, optional, default 500-->
    <dirtyDataFileMaxSize>500</dirtyDataFileMaxSize>

    <!-- retry times, -1:retry forever 0:no retry n:retry n times, optional, default -1-->
    <retryTimes>-1</retryTimes>

    <!-- retry interval, unit: ms, optional, default 3000-->
    <retryInterval>3000</retryInterval>

    <mappings>
        <mapping>
            <!-- oracle schema-->
            <oracleSchema></oracleSchema>
            <!-- oracle table, required-->
            <oracleTable>t_person</oracleTable>
            <!-- datahub project-->
            <datahubProject></datahubProject>
            <!-- datahub AccessId-->
            <datahubAccessId></datahubAccessId>
            <!-- datahub AccessKey-->
            <datahubAccessKey></datahubAccessKey>
            <!-- datahub topic-->
            <datahubTopic>t_person</datahubTopic>
            <ctypeColumn></ctypeColumn>
            <ctimeColumn></ctimeColumn>
            <columnMapping>
                <!--
                src:oracle column name, required;
                dest:datahub field, required;
                destOld: the datahub field corresponding to data before change , optional;
                isShardColumn: whether as hashkey of shard, optional, default is false, can be overwritten by shardId
                isDateFormat: timestamp column whether using DateFormat to transform, default true. 
                              if false, source data must be long;
                dateFormat: timestamp transform format, optional
                -->
                <column src="id" dest="id" isShardColumn="false"  isDateFormat="true" dateFormat="yyyy-MM-dd HH:mm:ss"/>
                <column src="name" dest="name" isShardColumn="true"/>
                <column src="age" dest="age"/>
                <column src="address" dest="address"/>
                <column src="comments" dest="comments"/>
                <column src="sex" dest="sex"/>
                <column src="temp" dest="temp" destOld="temp1"/>
            </columnMapping>

            <!--specified shard id, prior to take effect, optional-->
            <shardId>1</shardId>
        </mapping>
    </mappings>
</configure>
    
    ```

3. Start OGG extract, then the handler will start uploading data to datahub.

The following are the escape sequences supported:

Alias | Description
---|---
%t|Unix time in milliseconds
%a|locale’s short weekday name (Mon, Tue, ...)
%A|locale’s full weekday name (Monday, Tuesday, ...)
%b|locale’s short month name (Jan, Feb, ...)
%B|locale’s long month name (January, February, ...)
%c|locale’s date and time (Thu Mar 3 23:05:25 2005)
%d|day of month (01)
%D|date; same as %m/%d/%y
%H|hour (00..23)
%I|hour (01..12)
%j|day of year (001..366)
%k|hour ( 0..23)
%m|month (01..12)
%M|minute (00..59)
%p|locale’s equivalent of am or pm
%s|seconds since 1970-01-01 00:00:00 UTC
%S|second (00..59)
%y|last two digits of year (00..99)
%Y|year (2010)
%z|+hhmm numeric timezone (for example, -0400)




## Authors && Contributors
---
- [Ouyang Zhe](https://github.com/oyz)
- [Tian Li](https://github.com/tianliplus)
- [Yang Hongbo](https://github.com/hongbosoftware)

## License
---

licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
