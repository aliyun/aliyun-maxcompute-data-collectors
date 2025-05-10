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

import java.util
import java.util.OptionalLong

import scala.collection.mutable
import scala.collection.JavaConverters._
import com.aliyun.odps.task.SQLTask
import com.aliyun.odps.{Column, OdpsException, PartitionSpec, TableSchema, Table => SdkTable}
import com.aliyun.odps.`type`.TypeInfoParser
import com.aliyun.odps.utils.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.odps.OdpsUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, sources}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper, StructFilters}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{BasePredicate, BoundReference, Cast, Literal}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.catalyst.TableIdentifier

class OdpsTableCatalog extends TableCatalog with SupportsNamespaces with SQLConfHelper
  with Logging {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import OdpsMetaClient._
  import OdpsTableCatalog._

  private var catalogName: String = _
  private var metaClient: OdpsMetaClient = _

  var odpsOptions: OdpsOptions = _
  var schemaEnable: Boolean = _
  var defaultSchema: String = _

  override def name(): String = {
    require(catalogName != null, "The ODPS table catalog is not initialed")
    catalogName
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    assert(catalogName == null, "The ODPS table catalog is already initialed")
    catalogName = name

    val map = options.asCaseSensitiveMap().asScala.toMap
    odpsOptions = new OdpsOptions(map)

    metaClient = new OdpsMetaClient(odpsOptions)
    metaClient.initialize()

    schemaEnable = odpsOptions.enableNamespaceSchema
    defaultSchema = odpsOptions.defaultSchema
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    checkNamespace(namespace)
    withClient {
      val odpsClone = odps.clone()
      val (project, schema) = getProjectSchema(namespace)
      odpsClone.setDefaultProject(project)
      odpsClone.setCurrentSchema(schema)
      odpsClone.tables().asScala.map(t => Identifier.of(namespace, t.getName)).toArray
    }
  }

  override def loadTable(ident: Identifier): Table = {
    checkNamespace(ident.namespace())
    withClient {
      getTableUsingSdk(ident)
    }
  }

  override def invalidateTable(ident: Identifier): Unit = {
    checkNamespace(ident.namespace())
    val (project, odpsSchema) = getProjectSchema(ident.namespace())
    metaClient.invalidateTableCache(project, odpsSchema, ident.name())
  }

  override def tableExists(ident: Identifier): Boolean = {
    checkNamespace(ident.namespace())
    val (project, odpsSchema) = getProjectSchema(ident.namespace())
    withClient {
      metaClient.getSdkTableOption(project, odpsSchema, ident.name(), refresh = true).isDefined
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    checkNamespace(ident.namespace())
    val (project, odpsSchema) = getProjectSchema(ident.namespace())
    val table = ident.name()
    metaClient.invalidateTableCache(project, odpsSchema, ident.name())

    val (partitionColumns, maybeBucketSpec) = convertTransforms(partitions)

    val partitionSchema = schema.filter(p => partitionColumns.contains(p.name))
    val tableSchema = new TableSchema
    partitionSchema.foreach(
      f => tableSchema.addPartitionColumn(
        new Column(f.name, TypeInfoParser.getTypeInfoFromTypeString(
          typeToName(f.dataType).replaceAll("`", "")), f.getComment().orNull))
    )
    schema.filter(f => !partitionColumns.contains(f.name)).foreach { f =>
      tableSchema.addColumn(
        new Column(f.name, TypeInfoParser.getTypeInfoFromTypeString(
          typeToName(f.dataType).replaceAll("`", "")), f.getComment().orNull))
    }

    val provider = Option(properties.get(TableCatalog.PROP_PROVIDER))
    val tableProperties = properties.asScala
    val location = Option(properties.get(TableCatalog.PROP_LOCATION))
    val storage = DataSource.buildStorageFormatFromOptions(toOptions(tableProperties.toMap))
      .copy(locationUri = location.map(CatalogUtils.stringToURI))
    val tableType = if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED

    val tableDesc = CatalogTable(
      identifier = TableIdentifier(table, Some(odpsSchema)),
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = provider,
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = tableProperties.toMap,
      tracksPartitionsInCatalog = true,
      comment = Option(properties.get(TableCatalog.PROP_COMMENT)))

    withClient {
      SQLTask.run(odps, getSQLString(project, odpsSchema, table, tableSchema, ifNotExists = false, tableDesc))
        .waitForSuccess()
    }

    loadTable(ident)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new AnalysisException("alter table not supported")
  }

  override def dropTable(ident: Identifier): Boolean = {
    checkNamespace(ident.namespace())
    val (project, odpsSchema) = getProjectSchema(ident.namespace())
    val table = ident.name()

    try {
      withClient {
        val sdkTable = metaClient.getSdkTable(project, odpsSchema, table)
        metaClient.invalidateTableCache(project, odpsSchema, table)
        val sb = new StringBuilder
        if (sdkTable.isVirtualView) {
          sb.append("DROP VIEW ")
        } else {
          sb.append("DROP TABLE ")
        }
        sb.append(project)
        if (!StringUtils.isNullOrEmpty(odpsSchema)) sb.append(".").append(odpsSchema)
        sb.append(".").append(table).append(";")
        SQLTask.run(odps, sb.toString).waitForSuccess()
        metaClient.dropTableInCache(project, odpsSchema, table)
        true
      }
    } catch {
      case _: NoSuchTableException => false
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    checkNamespace(oldIdent.namespace())
    checkNamespace(newIdent.namespace())

    if (tableExists(newIdent)) {
      throw new TableAlreadyExistsException(newIdent)
    }

    val (oldProject, oldOdpsSchema) = getProjectSchema(oldIdent.namespace())
    val oldTable = oldIdent.name()
    val (newProject, newOdpsSchema) = getProjectSchema(newIdent.namespace())
    val newTable = newIdent.name()

    if (!conf.resolver(oldProject, newProject)) {
      throw new AnalysisException("rename table to different project not supported")
    }

    val sdkTable = metaClient.getSdkTable(oldProject, oldOdpsSchema, oldTable)
    metaClient.invalidateTableCache(oldProject, oldOdpsSchema, oldTable)
    metaClient.invalidateTableCache(newProject, newOdpsSchema, newTable)
    val sb = new StringBuilder
    if (sdkTable.isVirtualView) {
      sb.append("ALTER VIEW ")
    } else {
      sb.append("ALTER TABLE ")
    }
    sb.append(oldProject)
    if (!StringUtils.isNullOrEmpty(oldOdpsSchema)) sb.append(".").append(oldOdpsSchema)
    sb.append(".`").append(oldTable).append("` ")
    sb.append("RENAME TO ")
    if (!StringUtils.isNullOrEmpty(newOdpsSchema)) sb.append(newOdpsSchema).append(".")
    sb.append("`").append(newTable).append("`;")
    SQLTask.run(odps, sb.toString).waitForSuccess()

    metaClient.dropTableInCache(oldProject, oldOdpsSchema, oldTable)
  }

  def truncateTable(ident: Identifier): Unit = {
    checkNamespace(ident.namespace())
    val (project, odpsSchema) = getProjectSchema(ident.namespace())
    val table = ident.name()
    withClient {
      val sdkTable = metaClient.getSdkTable(project, odpsSchema, table)
      sdkTable.truncate()
      metaClient.invalidateTableCache(project, odpsSchema, table)
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    checkNamespace(namespace)
    val (project, odpsSchema) = getProjectSchema(namespace)
    if (schemaEnable) {
      metaClient.getSchemaOption(project, odpsSchema, refresh = true).isDefined
    } else {
      metaClient.getProjectOption(project, refresh = true).isDefined
    }
  }

  override def listNamespaces(): Array[Array[String]] = {
    if (schemaEnable) {
      getSchemas(defaultNamespace().head)
    } else {
      Array(defaultNamespace())
    }
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() => listNamespaces()
      case Array(db) => if (schemaEnable) {
        getSchemas(db)
      } else {
        if (namespaceExists(namespace)) {
          Array()
        } else {
          throw QueryCompilationErrors.noSuchNamespaceError(namespace)
        }
      }
      case Array(_, _) if namespaceExists(namespace) => Array()
      case _ => throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    if (!namespaceExists(namespace)) {
      throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
    namespace match {
      case Array(db) =>
        withClient {
          val metadata = mutable.HashMap[String, String]()
          val projectName = if (schemaEnable) defaultNamespace().head else db
          val project = metaClient.getProject(projectName)
          project.getProperties.asScala.foreach {
            case (key, value) => metadata.put(key, value)
          }
          if (schemaEnable) {
            metadata.put(SupportsNamespaces.PROP_LOCATION, "odps:///" + projectName + "/" + db)
          } else {
            metadata.put(SupportsNamespaces.PROP_LOCATION, "odps:///" + projectName)
          }
          metadata.put(SupportsNamespaces.PROP_COMMENT, project.getComment)
          metadata.asJava
        }
    case Array(db, schema) =>
        withClient {
          val project = metaClient.getProject(db)
          val metadata = mutable.HashMap[String, String]()
          project.getProperties.asScala.foreach {
            case (key, value) => metadata.put(key, value)
          }
          metadata.put(SupportsNamespaces.PROP_LOCATION, "odps:///" + db + "/" + schema)
          metadata.put(SupportsNamespaces.PROP_COMMENT, project.getComment)
          metadata.asJava
        }
      case _ => throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit = {
    if (namespaceExists(namespace)) {
      throw new NamespaceAlreadyExistsException(namespace)
    }
    namespace match {
      case Array(db) =>
        if (schemaEnable) {
          createSchema(defaultNamespace().head, db)
        } else {
          throw new AnalysisException("create project not supported")
        }
      case Array(project, schema) =>
        createSchema(project, schema)
      case _ =>
        throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
    }
  }

  override def dropNamespace(namespace: Array[String]): Boolean =  {
    checkNamespace(namespace)
    namespace match {
      case Array(db) =>
        if (schemaEnable) {
          dropSchema(defaultNamespace().head, db)
        } else {
          throw new AnalysisException("drop project not supported")
        }
      case Array(project, schema) =>
        dropSchema(project, schema)
      case _ =>
        throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
    }
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new AnalysisException("alter namespace not supported")
  }

  override def defaultNamespace(): Array[String] = synchronized {
    if (schemaEnable) {
      Array(odps.getDefaultProject, defaultSchema)
    } else {
      Array(odps.getDefaultProject)
    }
  }

  def hasPartition(tableIdent: Identifier, partitionSpec: TablePartitionSpec): Boolean = {
    checkNamespace(tableIdent.namespace())
    val (project, odpsSchema) = getProjectSchema(tableIdent.namespace())
    val table = tableIdent.name()
    withClient {
      val sdkTable = metaClient.getSdkTable(project, odpsSchema, table)
      val sdkPartitionSpec = new PartitionSpec()
      partitionSpec.foreach {
        case (name, value) => sdkPartitionSpec.set(name, value)
      }
      sdkTable.hasPartition(sdkPartitionSpec)
    }
  }

  def listPartitions(tableIdent: Identifier): Array[TablePartitionSpec] =
    listPartitionsByFilter(tableIdent, Array.empty)

  def listPartitionsByFilter(
      tableIdent: Identifier,
      filters: Array[Filter]): Array[TablePartitionSpec] = {
    checkNamespace(tableIdent.namespace())
    val (project, odpsSchema) = getProjectSchema(tableIdent.namespace())
    val table = tableIdent.name()
    withClient {
      val sdkTable = metaClient.getSdkTable(project, odpsSchema, table)
      val partitionSchema = getPartitionSchema(sdkTable)
      val partitionSpecs =
        sdkTable.getPartitionSpecs.asScala.map(p => convertToTablePartitionSpec(p))

      val prunedPartitions = if (filters.nonEmpty) {
        val predicate = new PartitionFilters(filters, partitionSchema).toPredicate
        partitionSpecs.filter(p => predicate.eval(convertToPartIdent(p, partitionSchema)))
      } else {
        partitionSpecs
      }

      prunedPartitions.toArray
    }
  }

  def createPartition(tableIdent: Identifier, ident: InternalRow): Unit = {
    checkNamespace(tableIdent.namespace())
    val (project, odpsSchema) = getProjectSchema(tableIdent.namespace())
    val table = tableIdent.name()
    withClient {
      val sdkTable = metaClient.getSdkTable(project, odpsSchema, table)
      sdkTable.createPartition(convertToSdkPartitionSpec(ident, getPartitionSchema(sdkTable)))
    }
  }

  def dropPartition(tableIdent: Identifier, ident: InternalRow): Boolean = {
    checkNamespace(tableIdent.namespace())
    val (project, odpsSchema) = getProjectSchema(tableIdent.namespace())
    val table = tableIdent.name()
    withClient {
      val sdkTable = metaClient.getSdkTable(project, odpsSchema, table)
      try {
        sdkTable.deletePartition(convertToSdkPartitionSpec(ident, getPartitionSchema(sdkTable)))
        true
      } catch {
        case _: OdpsException => false
      }
    }
  }

  private def getTableUsingSdk(ident: Identifier): OdpsTable = {
    val (project, odpsSchema) = getProjectSchema(ident.namespace())
    val table = ident.name()
    val newIdent = if (schemaEnable) {
      Identifier.of(Array(project, odpsSchema), table)
    } else {
      ident
    }
    val sdkTable = metaClient.getSdkTable(project, odpsSchema, table)
    val tableType = getTableType(sdkTable)
    val dataSchema = getDataSchema(sdkTable)
    val partitionSchema = getPartitionSchema(sdkTable)
    val stats = if (sdkTable.getSize <= 0) OdpsStatistics(OptionalLong.empty(), OptionalLong.empty())
      else OdpsStatistics(OptionalLong.of(sdkTable.getSize), OptionalLong.empty())
    val bucketSpec = getBucketSpec(sdkTable)
    val viewText = if (sdkTable.isVirtualView) Some(sdkTable.getViewText) else None

    OdpsTable(this, newIdent, tableType, dataSchema, partitionSchema, stats, bucketSpec, viewText)
  }

  def checkNamespace(namespace: Array[String]): Unit = {
    if (schemaEnable) {
      if (namespace.length > 2) {
        throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
      }
    } else if (namespace.length > 1) {
      throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
    }
  }

  def getProjectSchema(namespace: Array[String]): (String, String) = namespace match {
    case Array(db) =>
      if (schemaEnable) {
        (defaultNamespace().head, db)
      } else {
        (db, "")
      }
    case Array(db, schema) =>
      (db, schema)
    case _ =>
      throw QueryExecutionErrors.invalidNamespaceNameError(namespace)
  }

  def createSchema(project: String, schema: String): Unit = {
    val schemaName = project + "." + schema
    withClient {
      SQLTask.run(odps, "create schema " + schemaName + ";")
        .waitForSuccess()
      metaClient.putSchemaInCache(project, schema)
    }
  }

  def dropSchema(project: String, schema: String): Boolean = {
    val schemaName = project + "." + schema
    val sb = new StringBuilder()
    sb.append("DROP SCHEMA ")
    sb.append(schemaName)
    sb.append(" RESTRICT;")
    try {
      withClient {
        SQLTask.run(odps, sb.toString).waitForSuccess()
      }
      metaClient.dropSchemaInCache(project, schema)
      true
    } catch {
      case e: Exception => throw e
    }
  }

  def getSchemas(project: String): Array[Array[String]] = {
    metaClient.getSchemas(project, refresh = true)
      .map(schema => Array(project, schema))
      .toArray
  }
}

object OdpsTableCatalog {

  def getTableType(sdkTable: SdkTable): OdpsTableType = {
    if (sdkTable.isVirtualView) {
      OdpsTableType.VIRTUAL_VIEW
    } else if (sdkTable.isExternalTable) {
      OdpsTableType.EXTERNAL_TABLE
    } else {
      OdpsTableType.MANAGED_TABLE
    }
  }

  def getDataSchema(sdkTable: SdkTable): StructType = {
    StructType(sdkTable.getSchema.getColumns.asScala.map(
      c => StructField(c.getName, typeInfo2Type(c.getTypeInfo))))
  }

  def getPartitionSchema(sdkTable: SdkTable): StructType = {
    StructType(sdkTable.getSchema.getPartitionColumns.asScala.map(
      c => StructField(c.getName, typeInfo2Type(c.getTypeInfo))))
  }

  def getBucketSpec(sdkTable: SdkTable): Option[OdpsBucketSpec] = {
    val clusterInfo = sdkTable.getClusterInfo
    if (clusterInfo != null && clusterInfo.getClusterCols.size() > 0) {
      Some(OdpsBucketSpec(
        clusterInfo.getClusterType.toLowerCase,
        clusterInfo.getBucketNum.toInt,
        clusterInfo.getClusterCols.asScala,
        clusterInfo.getSortCols.asScala.map(s => SortColumn(s.getName, s.getOrder))))
    } else {
      None
    }
  }

  def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col

      case BucketTransform(numBuckets, FieldReference(Seq(col))) =>
        bucketSpec = Some(BucketSpec(numBuckets, col :: Nil, Nil))

      case transform =>
        throw new UnsupportedOperationException(
          s"SessionCatalog does not support partition transform: $transform")
    }

    (identityCols.toSeq, bucketSpec)
  }

  def toOptions(properties: Map[String, String]): Map[String, String] = {
    properties.filterKeys(_.startsWith(TableCatalog.OPTION_PREFIX)).map {
      case (key, value) => key.drop(TableCatalog.OPTION_PREFIX.length) -> value
    }.toMap
  }

  def getSQLString(
      projectName: String,
      odpsSchema: String,
      tableName: String,
      schema: TableSchema,
      ifNotExists: Boolean,
      tableDefinition: CatalogTable): String = {
    val sb = new StringBuilder()
    if (tableDefinition.tableType != CatalogTableType.EXTERNAL) {
      sb.append("CREATE TABLE ")
    } else {
      sb.append("CREATE EXTERNAL TABLE ")
    }
    if (ifNotExists) {
      sb.append(" IF NOT EXISTS ")
    }
    sb.append(projectName)
    if (!StringUtils.isNullOrEmpty(odpsSchema)) sb.append(".").append(odpsSchema)
    sb.append(".`").append(tableName).append("` (")
    val columns = schema.getColumns
    var pColumns = 0
    while (pColumns < columns.size) {
      {
        val i = columns.get(pColumns)
        sb.append("`").append(i.getName).append("` ").append(i.getTypeInfo.getTypeName)
        if (i.getComment != null) sb.append(" COMMENT \'").append(i.getComment).append("\'")
        if (pColumns + 1 < columns.size) sb.append(',')
      }
      {
        pColumns += 1
      }
    }
    sb.append(')')
    tableDefinition.comment map (comment => sb.append(" COMMENT \'" + comment + "\' "))
    val partCols = schema.getPartitionColumns

    // partitioned by
    if (partCols.size > 0) {
      sb.append(" PARTITIONED BY (")
      var index = 0
      while (index < partCols.size) {
        val c = partCols.get(index)
        sb.append(c.getName).append(" ").append(c.getTypeInfo.getTypeName)
        if (c.getComment != null) {
          sb.append(" COMMENT \'").append(c.getComment).append("\'")
        }
        if (index + 1 < partCols.size) {
          sb.append(',')
        }
        index += 1
      }
      sb.append(')')
    }

    // clustered by
    tableDefinition.bucketSpec.map(bucketSpec => {
      sb.append(" CLUSTERED BY ")
      val bucketCols = bucketSpec.bucketColumnNames.mkString("(", ",", ")")
      sb.append(bucketCols)
      val sortCols = bucketSpec.sortColumnNames.mkString("(", ",", ")")
      if (sortCols.nonEmpty) {
        sb.append(" SORTED BY ").append(sortCols)
      }
      sb.append(" INTO ").append(bucketSpec.numBuckets).append(" BUCKETS")
    })

    // storage
    if (tableDefinition.tableType == CatalogTableType.EXTERNAL) {
      // external table
      require(tableDefinition.storage.locationUri.isDefined)
      val outputFormat = tableDefinition.storage.outputFormat.get
      val formatList = Set("PARQUET", "TEXTFILE", "ORC", "RCFILE", "AVRO", "SEQUENCEFILE")

      val outputFormatClause = if (formatList.contains(outputFormat.toUpperCase)) {
        s" STORED AS $outputFormat"
      } else {
        s" STORED BY '$outputFormat'"
      }
      sb.append(outputFormatClause)
      if (tableDefinition.storage.properties.nonEmpty) {
        val properties = tableDefinition.storage.properties
          .mkString(" WITH SERDEPROPERTIES (", ",", ")")
        sb.append(properties)
      }
      sb.append(s" LOCATION '${tableDefinition.storage.locationUri.get.toString}'")
    } else {
      // non-external table
      tableDefinition.storage.outputFormat foreach (format => sb.append(s" STORED AS $format"))
    }

    // table properties
    if (tableDefinition.properties.nonEmpty) {
      val props = tableDefinition.properties.map(x => {
        s"'${x._1}'='${x._2}'".stripMargin
      }) mkString("(", ",", ")")
      sb.append(" TBLPROPERTIES ").append(props)
    }

    sb.append(';')
    sb.toString
  }

  def typeToName(dataType: DataType): String = {
    dataType match {
      case FloatType => "FLOAT"
      case DoubleType => "DOUBLE"
      case BooleanType => "BOOLEAN"
      case DateType => "DATE"
      case TimestampType => "TIMESTAMP"
      case ByteType => "TINYINT"
      case ShortType => "SMALLINT"
      case IntegerType => "INT"
      case LongType => "BIGINT"
      case StringType => "STRING"
      case BinaryType => "BINARY"
      case d: DecimalType => d.sql
      case a: ArrayType => a.sql
      case m: MapType => m.sql
      case s: StructType => s.sql
      case _ =>
        throw new AnalysisException("Spark data type:" + dataType + " not supported!")
    }
  }

  def convertToPartIdent(
      partitionSpec: TablePartitionSpec,
      partitionSchema: StructType): InternalRow = {
    InternalRow.fromSeq(partitionSchema.map { field =>
      Cast(Literal(partitionSpec(field.name)), field.dataType, None).eval()
    })
  }

  def convertToTablePartitionSpec(partitionSpec: PartitionSpec): TablePartitionSpec = {
    val partitionMap = new mutable.LinkedHashMap[String, String]
    partitionSpec.keys().asScala.foreach(key => {
      partitionMap.put(key, partitionSpec.get(key))
    })
    partitionMap.toMap
  }

  def convertToSdkPartitionSpec(ident: InternalRow, partitionSchema: StructType): PartitionSpec = {
    val partitionSpec = new PartitionSpec()
    partitionSchema.zipWithIndex.foreach { case (field, index) =>
      val value = Cast(
        BoundReference(index, field.dataType, nullable = false),
        StringType).eval(ident).toString
      partitionSpec.set(field.name, value)
    }
    partitionSpec
  }
}

class PartitionFilters(filters: Seq[sources.Filter], requiredSchema: StructType)
  extends StructFilters(filters, requiredSchema) {
  override def skipRow(row: InternalRow, index: Int): Boolean = false
  override def reset(): Unit = {}

  def toPredicate: BasePredicate = {
    toPredicate(filters)
  }
}
