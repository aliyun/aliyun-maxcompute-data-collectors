/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.odps

import com.aliyun.odps.`type`.TypeInfoFactory
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.task.SQLTask
import com.aliyun.odps.{Column, Odps, OdpsType, TableSchema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, sources}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsPartitionManagement}
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, NamedReference, Transform}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

import java.util

class OdpsTableSuite extends AnyFunSuite with Logging {

  private val project: String = ""
  private val accessId: String = ""
  private val accessKey: String = ""
  private val endPoint: String = ""

  private val partTable: Identifier = Identifier.of(Array(project), "partTable")
  private val multiPartTable: Identifier = Identifier.of(Array(project), "multiPartTable")
  private val allTypePartTable: Identifier = Identifier.of(Array(project), "allTypePartTable")

  private def sparkSession: SparkSession = SparkSession.builder()
    .master("local[2]")
    .config("spark.hadoop.odps.access.id", accessId)
    .config("spark.hadoop.odps.access.key", accessKey)
    .config("spark.hadoop.odps.end.point", endPoint)
    .config("spark.hadoop.odps.project.name", project)
    .getOrCreate()

  private def odps: Odps = {
    val odps = new Odps(new AliyunAccount(accessId, accessKey))
    odps.setDefaultProject(project)
    odps.setEndpoint(endPoint)
    odps
  }

  private val catalog: OdpsTableCatalog = {
    SparkSession.setDefaultSession(sparkSession)

    val newCatalog = new OdpsTableCatalog
    newCatalog.initialize("odps", CaseInsensitiveStringMap.empty())

    newCatalog
  }

  test("listPartitionIdentifiers") {
    val partTable = createPartTable()

    assert(!hasPartitions(partTable))

    val partIdent = InternalRow.apply(UTF8String.fromString("3"))
    partTable.createPartition(partIdent, new util.HashMap[String, String]())
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 1)

    val partIdent1 = InternalRow.apply(UTF8String.fromString("4"))
    partTable.createPartition(partIdent1, new util.HashMap[String, String]())
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 2)
    assert(partTable.listPartitionIdentifiers(Array("dt"), partIdent1).length == 1)

    partTable.dropPartition(partIdent)
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 1)
    partTable.dropPartition(partIdent1)
    assert(!hasPartitions(partTable))
  }

  test("listPartitionsByFilter") {
    createMultiPartTable()

    Seq(
       Array[Filter]() -> Set(
         Map("part0" -> "0", "part1" -> "abc"),
         Map("part0" -> "0", "part1" -> "def"),
         Map("part0" -> "1", "part1" -> "abc")),

      Array[Filter](sources.EqualTo("part0", 0)) -> Set(
        Map("part0" -> "0", "part1" -> "abc"),
        Map("part0" -> "0", "part1" -> "def")),

      Array[Filter](sources.EqualTo("part1", "abc")) -> Set(
        Map("part0" -> "0", "part1" -> "abc"),
        Map("part0" -> "1", "part1" -> "abc")),

      Array[Filter](sources.And(
            sources.LessThanOrEqual("part0", 1),
            sources.GreaterThanOrEqual("part0", 0))) -> Set(
        Map("part0" -> "0", "part1" -> "abc"),
        Map("part0" -> "0", "part1" -> "def"),
        Map("part0" -> "1", "part1" -> "abc")),

      Array[Filter](sources.GreaterThanOrEqual("part0", 1)) -> Set(
        Map("part0" -> "1", "part1" -> "abc")),

      Array[Filter](sources.GreaterThanOrEqual("part0", 2)) -> Set(),

    ).foreach { case (filters, expected) =>
      assert(catalog.listPartitionsByFilter(multiPartTable, filters).toSet === expected)
    }

    createPartTableAllType()
    Seq(
      Array[Filter]() -> Set(
        Map("p1" -> "11",     "p2" -> "22",    "p3" -> "3", "p4" -> "4", "p5" -> "5", "p6" -> "6"),
        Map("p1" -> "11   ",  "p2" -> "22   ", "p3" -> "3", "p4" -> "4", "p5" -> "5", "p6" -> "6"),
        Map("p1" -> "111   ", "p2" -> "222   ","p3" -> "3", "p4" -> "4", "p5" -> "5", "p6" -> "6")),

      Array[Filter](sources.StringStartsWith("p1", "11")) -> Set(
        Map("p1" -> "11",     "p2" -> "22",    "p3" -> "3", "p4" -> "4", "p5" -> "5", "p6" -> "6"),
        Map("p1" -> "11   ",  "p2" -> "22   ", "p3" -> "3", "p4" -> "4", "p5" -> "5", "p6" -> "6"),
        Map("p1" -> "111   ", "p2" -> "222   ","p3" -> "3", "p4" -> "4", "p5" -> "5", "p6" -> "6")),

      Array[Filter](sources.StringEndsWith("p1", " ")) -> Set(
        Map("p1" -> "11   ",  "p2" -> "22   ", "p3" -> "3", "p4" -> "4", "p5" -> "5", "p6" -> "6"),
        Map("p1" -> "111   ", "p2" -> "222   ","p3" -> "3", "p4" -> "4", "p5" -> "5", "p6" -> "6")),
    ).foreach { case (filters, expected) =>
      assert(catalog.listPartitionsByFilter(allTypePartTable, filters).toSet === expected)
    }
  }

  test("partitionExists") {
    val partTable = createMultiPartTable()

    assert(partTable.partitionExists(InternalRow(0, UTF8String.fromString("def"))))
    assert(!partTable.partitionExists(InternalRow(-1, UTF8String.fromString("def"))))

    val errMsg = intercept[ClassCastException] {
      partTable.partitionExists(InternalRow(UTF8String.fromString("abc"), UTF8String.fromString("def")))
    }.getMessage
    assert(errMsg.contains("org.apache.spark.unsafe.types.UTF8String cannot be cast to java.lang.Integer"))

    val errMsg2 = intercept[IllegalArgumentException] {
      partTable.partitionExists(InternalRow(0))
    }.getMessage
    assert(errMsg2.contains("The identifier might not refer to one partition"))
  }

  test("listPartitionByNames") {
    val partTable = createMultiPartTable()

    Seq(
      (Array("part0", "part1"), InternalRow(0, UTF8String.fromString("abc"))) -> Set(InternalRow(0, UTF8String.fromString("abc"))),

      (Array("part0"), InternalRow(0)) -> Set(InternalRow(0, UTF8String.fromString("abc")),
        InternalRow(0, UTF8String.fromString("def"))),

      (Array("part1"), InternalRow(UTF8String.fromString("abc"))) -> Set(InternalRow(0, UTF8String.fromString("abc")),
        InternalRow(1, UTF8String.fromString("abc"))),

      (Array.empty[String], InternalRow.empty) -> Set(InternalRow(0, UTF8String.fromString("abc")),
        InternalRow(0, UTF8String.fromString("def")), InternalRow(1, UTF8String.fromString("abc"))),

      (Array("part0", "part1"), InternalRow(3, UTF8String.fromString("xyz"))) -> Set(),

      (Array("part1"), InternalRow(UTF8String.fromString("xyz"))) -> Set()
    ).foreach { case ((names, idents), expected) =>
      assert(partTable.listPartitionIdentifiers(names, idents).toSet === expected)
    }

    // Check invalid parameters
    Seq(
      (Array("part0", "part1"), InternalRow(0))
    ).foreach { case (names, idents) =>
      intercept[ArrayIndexOutOfBoundsException](partTable.listPartitionIdentifiers(names, idents))
    }

    Seq(
      (Array("col0", "part1"), InternalRow(0, 1)),
      (Array("wrong"), InternalRow("invalid"))
    ).foreach { case (names, idents) =>
      intercept[IllegalArgumentException](partTable.listPartitionIdentifiers(names, idents))
    }
  }

  test("listPartitionForAllType") {
    val partTable = createPartTableAllType()

    Seq(
      (Array.empty[String], InternalRow.empty) -> Set(
        InternalRow(UTF8String.fromString("11   "), UTF8String.fromString("22   "), 3,4,5,6),
        InternalRow(UTF8String.fromString("111   "), UTF8String.fromString("222   "), 3,4,5,6),
        InternalRow(UTF8String.fromString("11"), UTF8String.fromString("22"), 3,4,5,6)),

      (Array("p1"), InternalRow(UTF8String.fromString("11"))) -> Set(
        InternalRow(UTF8String.fromString("11"), UTF8String.fromString("22"), 3,4,5,6)),

      (Array("p1"), InternalRow(UTF8String.fromString("11  "))) -> Set(),

      (Array("p1"), InternalRow(UTF8String.fromString("111   "))) -> Set(
        InternalRow(UTF8String.fromString("111   "), UTF8String.fromString("222   "), 3,4,5,6)
      ),

      (Array("p2"), InternalRow(UTF8String.fromString("22   "))) -> Set(
        InternalRow(UTF8String.fromString("11   "), UTF8String.fromString("22   "), 3,4,5,6),
      ),

    ).foreach { case ((names, idents), expected) =>
      assert(partTable.listPartitionIdentifiers(names, idents).toSet === expected)
    }
  }

  private def createPartTable(): OdpsTable = {
    if (catalog.tableExists(partTable)) {
      catalog.dropTable(partTable)
    }

    val odpsTable = catalog.createTable(
      partTable,
      new StructType()
        .add("id", IntegerType)
        .add("data", StringType)
        .add("dt", StringType),
      Array[Transform](LogicalExpressions.identity(ref("dt"))),
      util.Collections.emptyMap[String, String]).asInstanceOf[OdpsTable]
    odpsTable
  }

  private def createMultiPartTable(): OdpsTable = {
    if (catalog.tableExists(multiPartTable)) {
      catalog.dropTable(multiPartTable)
    }

    val odpsTable = catalog.createTable(
      multiPartTable,
      new StructType()
        .add("col0", IntegerType)
        .add("part0", IntegerType)
        .add("part1", StringType),
      Array(LogicalExpressions.identity(ref("part0")), LogicalExpressions.identity(ref("part1"))),
      util.Collections.emptyMap[String, String]).asInstanceOf[OdpsTable]

    Seq(
      InternalRow(0, UTF8String.fromString("abc")),
      InternalRow(0, UTF8String.fromString("def")),
      InternalRow(1, UTF8String.fromString("abc"))).foreach { partIdent =>
      odpsTable.createPartition(partIdent, new util.HashMap[String, String]())
    }
    odpsTable
  }

  private def createPartTableAllType(): OdpsTable = {
    val tableSchema = new TableSchema
    val columns = new util.ArrayList[Column]
    columns.add(new Column("c0", OdpsType.STRING))
    columns.add(new Column("c1", OdpsType.BIGINT))
    tableSchema.setColumns(columns)

    val partitionColumns = new util.ArrayList[Column]
    partitionColumns.add(new Column("p1", TypeInfoFactory.getCharTypeInfo(5)))
    partitionColumns.add(new Column("p2", TypeInfoFactory.getVarcharTypeInfo(5)))
    partitionColumns.add(new Column("p3", OdpsType.SMALLINT))
    partitionColumns.add(new Column("p4", OdpsType.TINYINT))
    partitionColumns.add(new Column("p5", OdpsType.BIGINT))
    partitionColumns.add(new Column("p6", OdpsType.INT))
    tableSchema.setPartitionColumns(partitionColumns)

    if (odps.tables().exists(allTypePartTable.name)) {
      odps.tables().delete(allTypePartTable.name)
    }
    odps.tables().create(allTypePartTable.name, tableSchema)

    createPartitionUseOdpsSQL(allTypePartTable.name, "p1='11',p2='22',p3=3,p4=4,p5=5,p6=6")
    createPartitionUseOdpsSQL(allTypePartTable.name, "p1='11   ',p2='22   ',p3=3,p4=4,p5=5,p6=6")
    createPartitionUseOdpsSQL(allTypePartTable.name, "p1='111   ',p2='222   ',p3=3,p4=4,p5=5,p6=6")

    catalog.loadTable(allTypePartTable).asInstanceOf[OdpsTable]
  }

  private def ref(name: String): NamedReference = LogicalExpressions.parseReference(name)

  private def hasPartitions(table: SupportsPartitionManagement): Boolean = {
    !table.listPartitionIdentifiers(Array.empty, InternalRow.empty).isEmpty
  }

  private def createPartitionUseOdpsSQL(tableName: String, partition: String): Unit = {
    val i = SQLTask.run(odps, odps.getDefaultProject,
      s"ALTER TABLE $project.$tableName ADD PARTITION ($partition);",
      "SQLAddPartitionTask",
      new util.HashMap[String, String](),
      null)
    i.waitForSuccess()
  }
}
