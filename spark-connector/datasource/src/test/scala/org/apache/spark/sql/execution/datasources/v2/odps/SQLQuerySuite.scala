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
    .config( "spark.sql.sources.partitionOverwriteMode", "dynamic")
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

  private def createTable(taleName: String, tableSchema: TableSchema): Unit = {
    if (odps.tables().exists(taleName)) {
      odps.tables().delete(taleName)
    }
    odps.tables().create(taleName, tableSchema)
  }
}
