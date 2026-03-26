package org.apache.spark.sql.execution.datasources.v2.odps

import com.aliyun.odps.`type`.{TypeInfo, TypeInfoFactory}
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.task.SQLTask
import com.aliyun.odps.{Column, Odps, TableSchema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.TableChange.{ColumnChange, ColumnPosition, addColumn, deleteColumn, renameColumn, updateColumnComment, updateColumnNullability, updateColumnPosition, updateColumnType}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.collection.mutable

class OdpsTableCatalogSuite extends AnyFunSuite with Logging {

  private val project: String = ""
  private val accessId: String = ""
  private val accessKey: String = ""
  private val endPoint: String = ""

  private val allTypeAlterTable: Identifier = Identifier.of(Array(project), "allTypeAlterTable")
  private val maxPartTable: Identifier = Identifier.of(Array(project), "maxPartTable")

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

  test("alterTableForAllType") {
    createAlterTableAllType()
    insertValueUseOdpsSQL(allTypeAlterTable.name())
    val alterTableCatalog = new OdpsTableCatalog

    // expected (index, name, type, comment, nullable)
    Seq(
      (Array(deleteColumn(Array("c2"), true)), allTypeAlterTable) ->
        Set(Array(Array(None), "c2", StringType, null, true)),

      (Array(addColumn(Array("c2"), StringType, true, null, ColumnPosition.after("c1"), null)), allTypeAlterTable) ->
        Set(Array(Array(2), "c2", StringType, null, true)),

      (Array(addColumn(Array("test"), StructType(Array(StructField("a", LongType)))),
        addColumn(Array("test2"), LongType)), allTypeAlterTable) ->
        Set(Array(Array(18), "test", StructType, null, true), Array(Array(19), "test2", LongType, null, true)),

      (Array(addColumn(Array("test", "b"), StringType)), allTypeAlterTable) ->
        Set(Array(Array(18, 0), "test.b", StringType, null, true)),

      (Array(renameColumn(Array("c1"), "name")), allTypeAlterTable) ->
        Set(Array(Array(1), "name", LongType, null, true)),

      (Array(renameColumn(Array("c17", "name"), "nickName")), allTypeAlterTable) ->
        Set(Array(Array(17), "nickName", StringType, null, true)),

      (Array(updateColumnType(Array("name"), StringType)), allTypeAlterTable) ->
        Set(Array(Array(1), "name", StringType, null, true)),

      (Array(updateColumnNullability(Array("c0"), true)), allTypeAlterTable) ->
        Set(Array(Array(0), "c0", IntegerType, null, true)),

      (Array(updateColumnComment(Array("c0"), "comment test")), allTypeAlterTable) ->
        Set(Array(Array(0), "c0", IntegerType, "comment test", true)),

      (Array(updateColumnPosition(Array("c0"), ColumnPosition.after("test"))), allTypeAlterTable) ->
        Set(Array(Array(18), "c0", IntegerType, "comment test", true)),

      (Array(updateColumnPosition(Array("c0"), ColumnPosition.first())), allTypeAlterTable) ->
        Set(Array(Array(0), "c0", IntegerType, "comment test", true)),

      (Array(updateColumnPosition(Array("test", "a"), ColumnPosition.after("b"))), allTypeAlterTable) ->
        Set(Array(Array(18, 1), "test.a", LongType, null, true)),
    ).foreach { case ((changes, ident), expected) =>
      alterTableCatalog.alterTable(ident, changes: _*)
      val alterTable: OdpsTable = alterTableCatalog.loadTable(ident).asInstanceOf[OdpsTable]
      var resultSet = Set.empty[Array[Any]]
      changes.foreach { change =>
        resultSet += getTableChangeSchema(alterTable.dataSchema, change.asInstanceOf[ColumnChange].fieldNames())
      }
      assert(resultSet === expected)
      odps.tables().delete(allTypeAlterTable.name())
    }
  }

  test("testOdpsDefinedFunctions") {
    createMaxPartTable()
    insertPartitionValueUseOdpsSQL(maxPartTable.name)

    Seq(
      ("odps_max_pt", StructType(Array(StructField("_0", StringType))), InternalRow(UTF8String.fromString(maxPartTable.name()))) ->
        "b"
    ).foreach { case ((name, structType, input), expected) =>
      val funcTableCatalog = new OdpsTableCatalog
      val unboundFunc = funcTableCatalog.loadFunction(Identifier.of(Array(project), name))
      val boundFunc = unboundFunc.bind(structType).asInstanceOf[ScalarFunction[UTF8String]]
      assert(boundFunc.produceResult(input).toString === expected)
    }
    odps.tables().delete(maxPartTable.name)
  }

  private def createAlterTableAllType(): OdpsTable = {
    val tableSchema = new TableSchema

    val columns = new util.ArrayList[Column]
    columns.add(new Column(Column.newBuilder("c0", TypeInfoFactory.INT).notNull()))
    columns.add(new Column("c1", TypeInfoFactory.BIGINT))
    columns.add(new Column("c2", TypeInfoFactory.STRING))
    columns.add(new Column("c3", TypeInfoFactory.getDecimalTypeInfo(6, 3)))
    columns.add(new Column("c4", TypeInfoFactory.TINYINT))
    columns.add(new Column("c5", TypeInfoFactory.SMALLINT))
    columns.add(new Column("c6", TypeInfoFactory.DOUBLE))
    columns.add(new Column("c7", TypeInfoFactory.FLOAT))
    columns.add(new Column("c8", TypeInfoFactory.BOOLEAN))
    columns.add(new Column("c9", TypeInfoFactory.DATE))
    columns.add(new Column("c10", TypeInfoFactory.DATETIME))
    columns.add(new Column("c11", TypeInfoFactory.TIMESTAMP))
    columns.add(new Column("c12", TypeInfoFactory.getCharTypeInfo(2)))
    columns.add(new Column("c13", TypeInfoFactory.getVarcharTypeInfo(4)))
    columns.add(new Column("c14", TypeInfoFactory.BINARY))
    columns.add(new Column("c15", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.INT)))
    columns.add(new Column("c16", TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.BIGINT, TypeInfoFactory.STRING)))
    columns.add(new Column("c17", TypeInfoFactory.getStructTypeInfo(
      new util.ArrayList[String](util.Arrays.asList("name", "age", "parents", "salary", "hobbies")),
      new util.ArrayList[TypeInfo](util.Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.INT,
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.STRING),
        TypeInfoFactory.FLOAT, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING))))))

    tableSchema.setColumns(columns)

    val partitionColumns = new util.ArrayList[Column]
    partitionColumns.add(new Column("dt", TypeInfoFactory.STRING))
    tableSchema.setPartitionColumns(partitionColumns)

    if (odps.tables().exists(allTypeAlterTable.name)) {
      odps.tables().delete(allTypeAlterTable.name)
    }
    odps.tables().create(allTypeAlterTable.name, tableSchema)

    catalog.loadTable(allTypeAlterTable).asInstanceOf[OdpsTable]
  }

  private def createMaxPartTable(): OdpsTable = {
    val tableSchema = new TableSchema

    val columns = new util.ArrayList[Column]
    columns.add(new Column("c1", TypeInfoFactory.BIGINT))
    columns.add(new Column("c2", TypeInfoFactory.STRING))

    tableSchema.setColumns(columns)

    val partitionColumns = new util.ArrayList[Column]
    partitionColumns.add(new Column("dt", TypeInfoFactory.STRING))
    tableSchema.setPartitionColumns(partitionColumns)

    if (odps.tables().exists(maxPartTable.name)) {
      odps.tables().delete(maxPartTable.name)
    }
    odps.tables().create(maxPartTable.name, tableSchema)

    catalog.loadTable(allTypeAlterTable).asInstanceOf[OdpsTable]
  }

  private def insertValueUseOdpsSQL(tableName: String): Unit = {
    val i = SQLTask.run(odps, odps.getDefaultProject,
      s"INSERT INTO TABLE $project.$tableName PARTITION (dt='hangzhou') VALUES (1, BIGINT(9223372036854775807), " +
        s"'abc', 123.456, TINYINT(-128), SMALLINT(-32768), DOUBLE(-1.7976931348623157e308), FLOAT(-3.402823e+38), " +
        s"false, DATE('2021-11-11'), DATETIME('2025-08-21 11:19:19'), TIMESTAMP('2025-08-21 11:19:19.161'), 'ab', " +
        s"'abcd', BINARY('abcd'), ARRAY(1,2,3), MAP(BIGINT(-9223372036854775807),'Long.MinValue + 1', BIGINT(0),'zero'), " +
        s"STRUCT('tom', 20, MAP('father','Jack', 'mather','Rose'), FLOAT(100.1), ARRAY('a', 'b'));",
      "SQLInsertValueTask",
      new util.HashMap[String, String](),
      null)
    i.waitForSuccess()
  }

  private def insertPartitionValueUseOdpsSQL(tableName: String): Unit = {
    val i1 = SQLTask.run(odps, odps.getDefaultProject,
      s"INSERT INTO TABLE $project.$tableName PARTITION (dt='a') VALUES (1, 'test-1'), (2, 'test2');",
      "SQLInsertPartValueTask1",
      new util.HashMap[String, String](),
      null)
    i1.waitForSuccess()
    val i2 = SQLTask.run(odps, odps.getDefaultProject,
      s"INSERT INTO TABLE $project.$tableName PARTITION (dt='b') VALUES (3, 'test-3');",
      "SQLInsertPartValueTask2",
      new util.HashMap[String, String](),
      null)
    i2.waitForSuccess()
  }

  private def getTableChangeSchema(dataSchema: StructType, fieldNames: Array[String]): Array[Any] = {
    val indexSeq = mutable.ArrayBuilder.make[Int]
    val parentIndex = dataSchema.getFieldIndex(fieldNames.head)
    var field = dataSchema.fields(parentIndex.get)
    indexSeq += parentIndex.getOrElse(throw new IllegalArgumentException("fieldName not found"))
    // get nested field index
    if (fieldNames.length > 1) {
      indexSeq += field.dataType.asInstanceOf[StructType]
        .getFieldIndex(fieldNames(1)).getOrElse(throw new IllegalArgumentException("fieldName not found"))
      field = field.dataType.asInstanceOf[StructType].fields(indexSeq.result()(1))
    }

    val index = indexSeq.result()
    val name = fieldNames.mkString(".")
    val dataType = field.dataType
    val comment = field.getComment()
    val nullable = field.nullable
    Array(index, name, dataType, comment, nullable)
  }
}
