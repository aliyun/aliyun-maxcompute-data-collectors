# Sqoop-ODPS

This is the extension of Apache Sqoop project. Sqoop-ODPS allows imports and exports of data sets between databases and ODPS tables.

Before you can use Sqoop-ODPS, a release of Hadoop must be installed and configured. Sqoop-ODPS is tested on Hadoop version 2.6.1.

This document assumes you are using a Linux or Linux-like environment. If you are using Windows, you may be able to use cygwin to accomplish most of the following tasks. If you are using Mac OS X, you should see few (if any) compatibility errors.

## Requirements

- JDK 1.6 or later 
- Apache Ant 1.7 or above
- Apache Maven 3.x

## Building the Sources

Build the sources using maven:

``` 
$ cd odps-sqoop
$ mvn clean package -DskipTests=true  -Dmaven.javadoc.skip=true
```

Verify the build results using help command:

``` 
$ bin/sqoop help 
15/10/10 10:54:20 INFO sqoop.Sqoop: Running Sqoop version: 1.4.7-SNAPSHOT 
usage: sqoop COMMAND [ARGS]

Available commands: 
codegen Generate code to interact with database records 
create-hive-table Import a table definition into Hive 
eval Evaluate a SQL statement and display the results 
export Export an HDFS directory to a database table 
help List available commands 
import Import a table from a database to HDFS 
import-all-tables Import tables from a database to HDFS 
import-mainframe Import datasets from a mainframe server to HDFS 
job Work with saved jobs 
list-databases List available databases on a server 
list-tables List available tables in a database 
merge Merge results of incremental imports 
metastore Run a standalone Sqoop metastore 
version Display version information 
```

## Using Sqoop-ODPS

Sqoop-ODPS is compatible with most Sqoop functionality. Details about Sqoop features can be found in the [Sqoop User Guide](http://sqoop.apache.org/docs/1.4.6/SqoopUserGuide.html). Following are the introduction to the ODPS related paramters and some basic examples.

### Import data to ODPS tables

If you want to import data to ODPS tables, append the following ODPS import arguments to the Sqoop `import` command. (Note: these arguments can also be applied to `import-all-table` and `import-mainframe` commands.)

Argument | Description 
--- | --- 
--odps-accessid &lt;access ID&gt; | ODPS access ID
--odps-accesskey &lt;access key&gt; | ODPS access key
--odps-project &lt;project&gt; | Set the ODPS project name
--odps-table &lt;table&gt; | ODPS table name
--create-odps-table | If specified, missing ODPS table will be created automatically
--odps-datahub-endpoint &lt;datahub endpoint&gt; | Set the ODPS datahub endpoint if target table is ODPS hub table
--odps-tunnel-endpoint &lt;tunnel endpoint&gt; | Set the ODPS tunnel endpoint if target table is ODPS offline table
--map-column-odps &lt;arg&gt; | Override mapping for specific column to ODPS types
--odps-batch-size &lt;batch size&gt; | Set the upload batch size (default 1000)
--odps-endpoint &lt;endpoint&gt;|Set the ODPS endpoint
--odps-hublifecycle &lt;hub lifecyle&gt; | Set the lifecycle of the hub table (default 7)
--odps-input-dateformat &lt;dateformat&gt; | Set the dateFormat of the input data (i.e., yyyy-mm-dd)
--odps-partition-keys &lt;partition key&gt; | Set the ODPS partition key
--odps-partition-values &lt;partition value&gt; | Set the ODPS partition value
--odps-retry-count &lt;retry count&gt; | Set the batch upload retry count (default 3)
--odps-shard-num &lt;shardNum&gt; | Set the shard number of the hub table (default 1)
--odps-shard-timeout &lt;shard timeout&gt; | Set the load shard timeout in seconds  (default 60)

Some basic examples:

- A basic import of a table named `EMPLOYEES` in the `corp` database:

``` 
$ bin/sqoop import --connect jdbc:mysql://db.foo.com/corp --table EMPLOYEES \ 
--odps-table corp_odps --odps-project project_name --odps-accessid xxxx \ 
--odps-accesskey xxxx --odps-endpoint your_odps_endpoint_url \ 
--odps-tunnel-endpoint your_odps_tunnel_endpoint_url 
```

- A basic import requiring a login:

``` 
$ bin/sqoop import --connect jdbc:mysql://db.foo.com/corp --table EMPLOYEES \ 
    --username SomeUser -P \ 
    --odps-table corp_odps --odps-project project_name --odps-accessid xxxx \ 
    --odps-accesskey xxxx --odps-endpoint your_odps_endpoint_url \ 
    --odps-tunnel-endpoint your_odps_tunnel_endpoint_url 
```

- Import to a specific partition `pt='p1'`:

``` 
$ bin/sqoop import --connect jdbc:mysql://db.foo.com/corp --table EMPLOYEES \ 
    --odps-table corp_odps --odps-project project_name --odps-accessid xxxx \ 
    --odps-accesskey xxxx --odps-endpoint your_odps_endpoint_url \ 
    --odps-tunnel-endpoint your_odps_tunnel_endpoint_url \ 
    --odps-partition-keys pt --odps-partition-values p1 
```

- Import to the dynamic partition that takes value from specific column `pt='p1',region=%{col_name}`:

``` 
$ bin/sqoop import --connect jdbc:mysql://db.foo.com/corp --table EMPLOYEES \ 
    --odps-table corp_odps --odps-project project_name --odps-accessid xxxx \ 
    --odps-accesskey xxxx --odps-endpoint your_odps_endpoint_url \ 
    --odps-tunnel-endpoint your_odps_tunnel_endpoint_url \ 
    --odps-partition-keys pt,region --odps-partition-values p1,%{col_name} 
```

Sqoop automatically supports several databases, including MySQL. Connect strings beginning with jdbc:mysql:// are handled automatically in Sqoop. (A full list of databases with built-in support is provided in the "Supported Databases" section of the [Sqoop User Guide](http://sqoop.apache.org/docs/1.4.6/SqoopUserGuide.html).)

You should download the appropriate JDBC driver for the type of database you want to import, and install the `.jar` file in the `$SQOOP_HOME/lib` directory on your client machine. Each driver `.jar` file also has a specific driver class which defines the entry-point to the driver. For example, MySQL’s Connector/J library has a driver class of `com.mysql.jdbc.Driver`. Refer to your database vendor-specific documentation to determine the main driver class. This class must be provided as an argument to Sqoop-ODPS with `--driver`.

### Export data from ODPS tables

If you want to export data from ODPS tables, append the following ODPS export arguments to the Sqoop `export` command.

Argument | Description
--- | ---
--odps-accessid &lt;access ID&gt;|ODPS access ID
--odps-accesskey &lt;access key&gt;|ODPS access key
--odps-endpoint &lt;endpoint&gt;|Set the ODPS endpoint
--odps-partition-spec &lt;partitionSpec&gt;|Set the ODPS table partitionSpec
--odps-project &lt;project&gt;|Set the ODPS project name
--odps-table &lt;table&gt;|Export &lt;table&gt; in ODPS

Some basic examples:

- A basic export from ODPS table `odps_bar` to populate a table named `bar`:

``` 
$ sqoop export --connect jdbc:mysql://db.example.com/foo --table bar \ 
    --odps-table odps_bar --odps-project project_name --odps-accessid xxxx \ 
    --odps-accesskey xxxx --odps-endpoint your_odps_endpoint_url 
```

- Export using a intermediate staging table `my_stage`:

``` 
$ sqoop export --connect jdbc:mysql://db.example.com/foo --table bar \ 
    --odps-table odps_bar --odps-project project_name --odps-accessid xxxx \ 
    --odps-accesskey xxxx --odps-endpoint your_odps_endpoint_url \ 
    --staging-table my_stage --clear-staging-table 
```

### Import data from HDFS to ODPS tables

If you want to import HDFS data into ODPS tables, append the option `--hdfs-to-odps` to the `import` command.

Basic example:

- Import a HIVE table from HDFS to ODPS:

```
$ sqoop import --hdfs-to-odps --export-dir /user/hive/warehouse/hive_table -m 1 \
    --odps-table hive_odps --columns id,city,name --odps-project odpstest \
    --odps-accessid xxxx --odps-accesskey xxxx \
    --odps-endpoint your_odps_endpoint_url \
    --odps-tunnel-endpoint your_odps_tunnel_endpoint_url \
    --create-odps-table --fields-terminated-by '\001' \
    --odps-partition-keys=dt,pt --odps-partition-values=20151031,p1
```
