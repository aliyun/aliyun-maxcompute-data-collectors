package org.apache.spark.sql.execution.datasources.v2.odps

import com.aliyun.odps.{Column, Odps, OdpsType, TableSchema}
import com.aliyun.odps.account.AliyunAccount
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import org.scalatest.funsuite.AnyFunSuite

import java.util

class SQLQuerySuite extends AnyFunSuite with Logging {

  private val project: String = ""
  private val accessId: String = ""
  private val accessKey: String = ""
  private val endPoint: String = ""

  private val table: Identifier = Identifier.of(Array(project), "testTable")

  private def sparkSession: SparkSession = SparkSession.builder()
    .master("local[2]")
    .config("spark.hadoop.odps.access.id", accessId)
    .config("spark.hadoop.odps.access.key", accessKey)
    .config("spark.hadoop.odps.end.point", endPoint)
    .config("spark.hadoop.odps.project.name", project)
    .config("spark.sql.catalog.odps", "org.apache.spark.sql.execution.datasources.v2.odps.OdpsTableCatalog")
    .config("spark.sql.extensions", "org.apache.spark.sql.execution.datasources.v2.odps.extension.OdpsExtensions")
    .config("spark.sql.defaultCatalog", "odps")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.catalog.odps.enhanceWriteCheck", "true")
    .getOrCreate()

  private def odps: Odps = {
    val odps = new Odps(new AliyunAccount(accessId, accessKey))
    odps.setDefaultProject(project)
    odps.setEndpoint(endPoint)
    odps
  }

  test("filterPushDownColumnNames") {
    sparkSession.conf.set("spark.sql.catalog.odps.enableFilterPushDown", true)

    val tableSchema = new TableSchema
    val columns = new util.ArrayList[Column]
    columns.add(new Column("c0", OdpsType.BIGINT))
    columns.add(new Column("c1", OdpsType.BIGINT))
    columns.add(new Column("列2", OdpsType.BIGINT))
    columns.add(new Column("列3", OdpsType.BIGINT))
    columns.add(new Column("44", OdpsType.BIGINT))
    columns.add(new Column("5列", OdpsType.BIGINT))
    columns.add(new Column("列六", OdpsType.BIGINT))
    columns.add(new Column("'列七'", OdpsType.BIGINT))

    tableSchema.setColumns(columns)
    createTable(table.name(), tableSchema)

    sparkSession.sql(s"insert overwrite table ${table.name()} values (0,1,2,3,4,5,6,7), (1,2,3,4,5,6,7,8)")
    val result = sparkSession.sql("select * from testTable where c0 = 0 " +
      "and `列2` = 2 and `44` = 4 and `列六` = 6 and `'列七'` = 7").collect()

    assert(result.length == 1)
    assert(result(0).get(0) == 0)
    assert(result(0).get(1) == 1)
    assert(result(0).get(2) == 2)
    assert(result(0).get(3) == 3)
    assert(result(0).get(4) == 4)
    assert(result(0).get(5) == 5)
    assert(result(0).get(6) == 6)
    assert(result(0).get(7) == 7)
  }

  test("testSqlInsertTable") {
    sparkSession.sql("DROP TABLE IF EXISTS spark_sql_test_table")
    sparkSession.sql("CREATE TABLE spark_sql_test_table(name STRING, num BIGINT)")
    sparkSession.sql("INSERT INTO spark_sql_test_table SELECT 'abc', 100000")
    val data1 = sparkSession.sql("SELECT * FROM spark_sql_test_table").collect
    assert(data1.length == 1)
    assert(data1(0).get(0) == "abc")
    assert(data1(0).get(1) == 100000)

    val count1 = sparkSession.sql("SELECT COUNT(*) FROM spark_sql_test_table").collect
    assert(count1(0).get(0) == 1)

    sparkSession.sql("INSERT OVERWRITE TABLE spark_sql_test_table SELECT 'abcd', 200000")
    val data2 = sparkSession.sql("SELECT * FROM spark_sql_test_table").collect
    assert(data2.length == 1)
    assert(data2(0).get(0) == "abcd")
    assert(data2(0).get(1) == 200000)

    val count2 = sparkSession.sql("SELECT COUNT(*) FROM spark_sql_test_table").collect
    assert(count2(0).get(0) == 1)

    sparkSession.sql("INSERT INTO TABLE spark_sql_test_table SELECT 'aaaa', 140000")
    sparkSession.sql("INSERT INTO TABLE spark_sql_test_table SELECT 'bbbb', 160000")
    val data3 = sparkSession.sql("SELECT * FROM spark_sql_test_table order by num").collect
    assert(data3.length == 3)
    assert(data3(0).get(0) == "aaaa")
    assert(data3(0).get(1) == 140000)
    assert(data3(1).get(0) == "bbbb")
    assert(data3(1).get(1) == 160000)
    assert(data3(2).get(0) == "abcd")
    assert(data3(2).get(1) == 200000)

    sparkSession.sql("DROP TABLE IF EXISTS spark_sql_test_table_null")
    sparkSession.sql("CREATE TABLE spark_sql_test_table_null(name string, num bigint)")
    sparkSession.sql("INSERT INTO spark_sql_test_table_null SELECT null, 100000")
    val data4 = sparkSession.sql("SELECT * FROM spark_sql_test_table_null").collect
    assert(data4.length == 1)
    assert(data4(0).isNullAt(0))
    assert(data4(0).get(1) == 100000)

    val count3 = sparkSession.sql("SELECT COUNT(*) FROM spark_sql_test_table_null").collect
    assert(count3(0).get(0) == 1)

    sparkSession.sql("INSERT OVERWRITE TABLE spark_sql_test_table_null SELECT null, 200000")
    val data5 = sparkSession.sql("SELECT * FROM spark_sql_test_table_null").collect
    assert(data5.length == 1)
    assert(data5(0).isNullAt(0))
    assert(data5(0).get(1) == 200000)

    val count4 = sparkSession.sql("SELECT COUNT(*) FROM spark_sql_test_table_null").collect
    assert(count4(0).get(0) == 1)

    sparkSession.sql("INSERT INTO TABLE spark_sql_test_table_null SELECT 'aaaa', null")
    sparkSession.sql("INSERT INTO TABLE spark_sql_test_table_null SELECT null, null")
    val data6 = sparkSession.sql("SELECT * FROM spark_sql_test_table_null order by num,name").collect
    assert(data6.length == 3)
    assert(data6(0).isNullAt(0))
    assert(data6(0).isNullAt(1))
    assert(data6(1).get(0) == "aaaa")
    assert(data6(1).isNullAt(1))
    assert(data6(2).isNullAt(0))
    assert(data6(2).get(1) == 200000)
  }

  test("testSqlInsertPartitionTable") {
    sparkSession.sql("DROP TABLE IF EXISTS spark_sql_test_partition_table")
    sparkSession.sql(s"CREATE TABLE spark_sql_test_partition_table(name STRING, num BIGINT) PARTITIONED BY (p1 STRING, p2 STRING)")

    sparkSession.sql("INSERT INTO spark_sql_test_partition_table PARTITION (p1='2017',p2='hangzhou') SELECT 'hz', 100")
    sparkSession.sql("INSERT OVERWRITE TABLE spark_sql_test_partition_table PARTITION (p1='2017',p2='hangzhou') SELECT 'hz', 160")
    sparkSession.sql("INSERT INTO spark_sql_test_partition_table PARTITION (p1='2017',p2='shanghai') SELECT 'sh', 200")
    sparkSession.sql("INSERT INTO spark_sql_test_partition_table PARTITION (p1='2017',p2='shanghai') SELECT 'sh', 300")
    sparkSession.sql("INSERT INTO spark_sql_test_partition_table PARTITION (p1='2017',p2='hangzhou') SELECT 'hz', 400")
    sparkSession.sql("INSERT INTO spark_sql_test_partition_table PARTITION (p1='2017',p2='shanghai') SELECT 'sh', 500")
    sparkSession.sql("INSERT INTO spark_sql_test_partition_table PARTITION (p1='2017',p2='hangzhou') SELECT 'hz', 600")

    val count = sparkSession.sql("SELECT COUNT(*) FROM spark_sql_test_partition_table").collect
    assert(count(0).get(0) == 6)

    val data = sparkSession.sql("SELECT * FROM spark_sql_test_partition_table order by num").collect()
    assert(data.length == 6)
    assert(data(0).get(0)=="hz")
    assert(data(0).get(1)==160)
    assert(data(0).get(2)=="2017")
    assert(data(0).get(3)=="hangzhou")
    assert(data(3).get(0)=="hz")
    assert(data(3).get(1)==400)
    assert(data(3).get(2)=="2017")
    assert(data(3).get(3)=="hangzhou")

    val pData1 = sparkSession.sql("SELECT p1 FROM spark_sql_test_partition_table").collect()
    assert(pData1.length == 6)
    assert(pData1(5).get(0)=="2017")

    val pData2 = sparkSession.sql("SELECT p2 FROM spark_sql_test_partition_table order by p2").collect()
    assert(pData2.length == 6)
    assert(pData2(0).get(0)=="hangzhou")

    val pData3 = sparkSession.sql("SELECT p1,p2 FROM spark_sql_test_partition_table order by p2").collect()
    assert(pData3.length == 6)
    assert(pData3(0).get(0)=="2017")
    assert(pData3(0).get(1)=="hangzhou")

    val pData4 = sparkSession.sql("SELECT p1,p2 FROM spark_sql_test_partition_table where p2='shanghai'").collect()
    assert(pData4.length == 3)
    assert(pData4(0).get(0)=="2017")
    assert(pData4(0).get(1)=="shanghai")
  }

  private def createTable(taleName: String, tableSchema: TableSchema): Unit = {
    if (odps.tables().exists(taleName)) {
      odps.tables().delete(taleName)
    }
    odps.tables().create(taleName, tableSchema)
  }
}
