/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import java.lang.reflect.InvocationTargetException
import java.net.URI
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.aliyun.odps.`type`.TypeInfoParser
import com.aliyun.odps.task.SQLTask
import com.aliyun.odps.{Column, NoSuchObjectException, OdpsException, Partition, PartitionSpec, Project, ReloadException, Table, TableSchema}
import com.google.common.cache.{CacheBuilder, CacheLoader}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.DDL_TIME
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT
import org.apache.thrift.TException
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchPartitionException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.types.{AnsiIntervalType, ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.odps.OdpsClient
import org.apache.spark.sql.odps.OdpsUtils

/**
 * A persistent implementation of the system catalog using Hive.
 * All public methods must be synchronized for thread-safety.
 */
private[spark] class HiveExternalCatalog(conf: SparkConf, hadoopConf: Configuration)
  extends ExternalCatalog with Logging {

  import CatalogTypes.TablePartitionSpec
  import HiveExternalCatalog._
  import CatalogTableType._

  // SPARK-32256: Make sure `VersionInfo` is initialized before touching the isolated classloader.
  // This is to ensure Hive can get the Hadoop version when using the isolated classloader.
  org.apache.hadoop.util.VersionInfo.getVersion()

  /**
   * A Hive client used to interact with the metastore.
   */
  lazy val client: HiveClient = {
    HiveUtils.newClientForMetadata(conf, hadoopConf)
  }

  // Exceptions thrown by the hive client that we would like to wrap
  private val clientExceptions = Set(
    classOf[HiveException].getCanonicalName,
    classOf[TException].getCanonicalName,
    classOf[InvocationTargetException].getCanonicalName)

  /**
   * Whether this is an exception thrown by the hive client that should be wrapped.
   *
   * Due to classloader isolation issues, pattern matching won't work here so we need
   * to compare the canonical names of the exceptions, which we assume to be stable.
   */
  private def isClientException(e: Throwable): Boolean = {
    var temp: Class[_] = e.getClass
    var found = false
    while (temp != null && !found) {
      found = clientExceptions.contains(temp.getCanonicalName)
      temp = temp.getSuperclass
    }
    found
  }

  /**
   * Run some code involving `client` in a [[synchronized]] block and wrap certain
   * exceptions thrown in the process in [[AnalysisException]].
   */
  private def withClient[T](body: => T): T = synchronized {
    try {
      body
    } catch {
      case NonFatal(exception) if isClientException(exception) =>
        val e = exception match {
          // Since we are using shim, the exceptions thrown by the underlying method of
          // Method.invoke() are wrapped by InvocationTargetException
          case i: InvocationTargetException => i.getCause
          case o => o
        }
        throw new AnalysisException(
          e.getClass.getCanonicalName + ": " + e.getMessage, cause = Some(e))
    }
  }

  // --------------------------------------------------------------------------
  // ODPS Client
  // --------------------------------------------------------------------------

  import CatalogTableType._
  import CatalogTypes.TablePartitionSpec

  {
    val hints = mutable.Map[String, String]()
    hints.put("odps.sql.preparse.odps2", "lot")
    hints.put("odps.sql.planner.parser.odps2", "true")
    hints.put("odps.sql.planner.mode", "lot")
    hints.put("odps.compiler.output.format", "lot,pot")
    hints.put("odps.compiler.playback", "true")
    hints.put("odps.compiler.warning.disable", "false")
    hints.put("odps.sql.ddl.odps2", "true")
    hints.put("odps.sql.runtime.mode", "EXECUTIONENGINE")
    hints.put("odps.sql.sqltask.new", "true")
    hints.put("odps.sql.hive.compatible", "true")
    hints.put("odps.compiler.verify", "true")
    hints.put("odps.sql.decimal.odps2", "true")
    SQLTask.setDefaultHints(hints.asJava)
  }

  private def odps = OdpsClient.builder()
    .config(SparkSession.active.sessionState.newHadoopConf())
    // TODO: Load defaults in cluster mode
    .getOrCreate()
    .odps()

  private val cacheSize = conf.get(OdpsOptions.ODPS_META_CACHE_SIZE)
  private val cacheExpireTime = conf.get(OdpsOptions.ODPS_META_CACHE_EXPIRE_TIME)

  private val enableExternalProject = conf.get(OdpsOptions.ODPS_EXT_PROJECT_ENABLE)
  private val enableExternalTable = conf.get(OdpsOptions.ODPS_EXT_TABLE_ENABLE)

  // TODO statsLevel expects `none`, `size`, `rowCount` and `colStats`,
  //      higher levels imply including all lower level stats
  private val statsLevel = conf.get(OdpsOptions.ODPS_META_STATS_LEVEL)
  private val statsSizeEnable = statsLevel.equalsIgnoreCase("size")

  private def createCacheBuilder(): CacheBuilder[AnyRef, AnyRef] = {
    CacheBuilder.newBuilder()
      .maximumSize(cacheSize)
      .expireAfterWrite(cacheExpireTime, TimeUnit.SECONDS)
  }

  private val projectLoader = new CacheLoader[String, Option[Project]] {
    override def load(key: String): Option[Project] = {
      try {
        val project = odps.projects().get(key)
        project.reload()
        Some(project)
      } catch {
        case _: NoSuchObjectException => None
        case ex: Exception =>
          logWarning("load odps project failed: " + ex.getMessage)
          None
      }
    }
  }
  private val projectCache = createCacheBuilder().build(projectLoader)

  private val odpsTableLoader = new CacheLoader[QualifiedTableName, Option[Table]] {
    override def load(key: QualifiedTableName): Option[Table] = {
      try {
        val table = odps.tables().get(key.database, key.name)
        table.reload()
        if (table.isExternalTable) {
          if (!enableExternalTable) {
            throw new Exception("Odps external table is not enabled!")
          }
          val project = getProject(key.database)
          if (project.getType.equals(Project.ProjectType.external) && !enableExternalProject) {
            throw new Exception("Odps external project is not enabled!")
          }
        }
        Some(table)
      } catch {
        case _: NoSuchObjectException => None
        case ex: Exception =>
          logWarning("load odps table failed: " + ex.getMessage)
          None
      }
    }
  }
  private val odpsTableCache = createCacheBuilder().build(odpsTableLoader)

  private def getProjectOption(name: String, refresh: Boolean = false): Option[Project] = {
    if (refresh) {
      projectCache.invalidate(name)
    }
    projectCache.get(name)
  }

  private def getProject(name: String, refresh: Boolean = false): Project = {
    getProjectOption(name, refresh)
      .getOrElse(throw new NoSuchDatabaseException(name))
  }

  private def getOdpsTableOption(
                                  db: String, table: String, refresh: Boolean = false): Option[Table] = {
    val key = QualifiedTableName(db, table)
    if (refresh) {
      odpsTableCache.invalidate(key)
    }
    odpsTableCache.get(key)
  }

  private def getOdpsTable(db: String, table: String, refresh: Boolean = false): Table = {
    getOdpsTableOption(db, table, refresh)
      .getOrElse(throw new NoSuchTableException(db, table))
  }

  private def invalidateTableCache(db: String, table: String): Unit = {
    val key = QualifiedTableName(db, table)
    odpsTableCache.invalidate(key)
  }

  private def dropTableInCache(db: String, table: String): Unit = {
    val key = QualifiedTableName(db, table)
    odpsTableCache.put(key, None)
  }

  private def isOdpsClientException(e: Throwable): Boolean = e match {
    case _: OdpsException => true
    case _: ReloadException => true
    case _ => false
  }

  private def withOdpsClient[T](body: => T): T = {
    try {
      body
    } catch {
      case NonFatal(e) if isOdpsClientException(e) =>
        throw new AnalysisException(
          e.getClass.getCanonicalName + ": " + e.getMessage, cause = Some(e))
    }
  }

  private def setDefaultProject(name: String): Unit = synchronized {
    odps.setDefaultProject(name)
  }

  private def getDefaultProject(): String = synchronized {
    odps.getDefaultProject()
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(
      dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = withClient {
    if (dbDefinition.name != SessionCatalog.DEFAULT_DATABASE) {
      throw new AnalysisException("create database not supported")
    }
  }

  override def dropDatabase(
      db: String,
      ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = withClient {
    throw new AnalysisException("drop database not supported")
  }

  /**
   * Alter a database whose name matches the one specified in `dbDefinition`,
   * assuming the database exists.
   *
   * Note: As of now, this only supports altering database properties!
   */
  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = withClient {
    throw new AnalysisException("alter database not supported")
  }

  override def getDatabase(db: String): CatalogDatabase = withClient {
    val project = getProject(db)
    CatalogDatabase(db, project.getComment, new URI("file:///__DUMMY_DATABASE_LOCATION__"),
      project.getProperties.asScala.toMap)
  }

  override def databaseExists(db: String): Boolean = withClient {
    getProjectOption(db, refresh = true).isDefined
  }

  override def listDatabases(): Seq[String] = withClient {
    getDefaultProject() :: Nil
  }

  override def listDatabases(pattern: String): Seq[String] = withClient {
    val project = getDefaultProject()
    project match {
      case `pattern` => project :: Nil
      case _ => Nil
    }
  }

  override def setCurrentDatabase(db: String): Unit = withClient {
    if (databaseExists(db)) {
      setDefaultProject(db)
    }
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override def createTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = withOdpsClient {
    val db = tableDefinition.identifier.database.getOrElse(getDefaultProject())
    val table = tableDefinition.identifier.table
    invalidateTableCache(db, table)
    if (tableDefinition.tableType.equals(VIEW)) {
      SQLTask.run(odps, s"CREATE VIEW $db.`$table` AS ${tableDefinition.viewText.get};")
        .waitForSuccess()
    } else {
      val tableSchema = new TableSchema
      tableDefinition.partitionSchema.foreach(
        f => tableSchema.addPartitionColumn(
          new Column(f.name, TypeInfoParser.getTypeInfoFromTypeString(
            typeToName(f.dataType).replaceAll("`", "")), f.getComment().orNull))
      )
      tableDefinition.schema
        .filter(f => !tableDefinition.partitionColumnNames.contains(f.name))
        .foreach(f => tableSchema.addColumn(
          new Column(f.name, TypeInfoParser.getTypeInfoFromTypeString(
            typeToName(f.dataType).replaceAll("`", "")), f.getComment().orNull)
        ))
      SQLTask.run(odps, getSQLString(
        db,
        table,
        tableSchema,
        ignoreIfExists,
        tableDefinition)
      ).waitForSuccess()
    }
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = withOdpsClient {
    val odpsTable = getOdpsTable(db, table)
    invalidateTableCache(db, table)
    if (odpsTable.isVirtualView) {
      SQLTask.run(odps, s"DROP VIEW $db.`$table`;").waitForSuccess()
    } else {
      val dropSql = new StringBuilder
      dropSql.append("DROP TABLE")
      if (ignoreIfNotExists) {
        dropSql.append(" IF EXISTS")
      }

      dropSql.append(s" $db.`$table`;")
      SQLTask.run(odps, dropSql.toString()).waitForSuccess()
    }
    dropTableInCache(db, table)
  }

  override def renameTable(
      db: String,
      oldName: String,
      newName: String): Unit = withOdpsClient {
    val odpsTable = getOdpsTable(db, oldName)
    invalidateTableCache(db, oldName)
    invalidateTableCache(db, newName)
    val sql = if (odpsTable.isVirtualView) {
      s"ALTER VIEW $db.`$oldName` RENAME TO `$newName`;"
    } else {
      s"ALTER TABLE $db.`$oldName` RENAME TO `$newName`;"
    }
    SQLTask.run(odps, sql).waitForSuccess()
    dropTableInCache(db, oldName)
  }

  /**
   * Alter a table whose name that matches the one specified in `tableDefinition`,
   * assuming the table exists. This method does not change the properties for data source and
   * statistics.
   *
   * Note: As of now, this doesn't support altering table schema, partition column names and bucket
   * specification. We will ignore them even if users do specify different values for these fields.
   */
  override def alterTable(tableDefinition: CatalogTable): Unit = withClient {
    throw new AnalysisException("alter table not supported")
  }

  /**
   * Alter the data schema of a table identified by the provided database and table name. The new
   * data schema should not have conflict column names with the existing partition columns, and
   * should still contain all the existing data columns.
   */
  override def alterTableDataSchema(
      db: String,
      table: String,
      newDataSchema: StructType): Unit = withClient {
    throw new AnalysisException("alter table data schema not supported")
  }

  /** Alter the statistics of a table. If `stats` is None, then remove all existing statistics. */
  override def alterTableStats(
      db: String,
      table: String,
      stats: Option[CatalogStatistics]): Unit = withClient {
    logWarning("alter table stats not supported")
  }

  override def getTable(db: String, table: String): CatalogTable = withOdpsClient {
    val odpsTable = getOdpsTable(db, table)
    val fields = odpsTable.getSchema.getColumns.asScala.map(c =>
      StructField(c.getName, OdpsUtils.typeInfo2Type(c.getTypeInfo), true).withComment(c.getComment)).toList
    val partitions = odpsTable.getSchema.getPartitionColumns.asScala.map(c =>
      StructField(c.getName, OdpsUtils.typeInfo2Type(c.getTypeInfo), true).withComment(c.getComment)).toList
    val tableType = if (odpsTable.isExternalTable) {
      EXTERNAL
    } else if (odpsTable.isVirtualView) {
      VIEW
    } else {
      MANAGED
    }
    val bucketSpec = if (odpsTable.getClusterInfo != null
      && odpsTable.getClusterInfo.getClusterType.toLowerCase.equals("hash")) {
      val sortColNames = if (odpsTable.getClusterInfo.getSortCols == null) Nil else
        odpsTable.getClusterInfo.getSortCols.asScala.map(_.getName)
      Some(BucketSpec(
        odpsTable.getClusterInfo.getBucketNum.asInstanceOf[Int],
        odpsTable.getClusterInfo.getClusterCols.asScala,
        sortColNames))
    } else {
      None
    }

    val viewText = if (odpsTable.isVirtualView) {
      Some(odpsTable.getViewText)
    } else {
      None
    }

    /**
     * cases to set stats to Long.MaxValue:
     * 1. partitioned table
     * 2. parameter doesn't contains "size"
     * 3. size <= 0
     * 4. record_num < 0
     * otherwise set to size in parameters
     * */
    val stats = if (odpsTable.getSize <= 0) {
      Some(CatalogStatistics(BigInt(Long.MaxValue)))
    } else {
      Some(CatalogStatistics(BigInt(odpsTable.getSize)))
    }

    new CatalogTable(
      TableIdentifier(table, Option(db)),
      tableType,
      CatalogStorageFormat.empty,
      StructType(fields ::: partitions),
      Option("hive"),
      odpsTable.getSchema.getPartitionColumns.asScala.map(_.getName),
      bucketSpec = bucketSpec,
      viewText = viewText,
      stats = stats)
  }

  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = withClient {
    throw new AnalysisException("get tables by name not supported")
  }

  override def tableExists(db: String, table: String): Boolean = withOdpsClient {
    getOdpsTableOption(db, table, refresh = true).isDefined
  }

  override def listTables(db: String): Seq[String] = withOdpsClient {
    val odpsClone = odps.clone()
    odpsClone.setDefaultProject(db)
    odpsClone.tables().asScala.map(t => t.getName).toSeq
  }

  override def listTables(db: String, pattern: String): Seq[String] = withOdpsClient {
    StringUtils.filterPattern(listTables(db), pattern)
  }

  override def listViews(db: String, pattern: String): Seq[String] = withOdpsClient {
    throw new AnalysisException("list Views not supported")
  }

  override def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = withClient {
    throw new AnalysisException("load table not supported")
  }

  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = withClient {
    throw new AnalysisException("load partition not supported")
  }

  override def loadDynamicPartitions(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int): Unit = withClient {
    throw new AnalysisException("load dynamic partition not supported")
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  private def createPartitionSpec(table: Table, part: TablePartitionSpec): PartitionSpec = {
    val partCols = table.getSchema.getPartitionColumns.asScala
    new PartitionSpec(partCols.map(c => s"${c.getName}=${part(c.getName)}").mkString(","))
  }

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = withOdpsClient {
    val odpsTable = getOdpsTable(db, table)
    parts.foreach(p =>
      odpsTable.createPartition(createPartitionSpec(odpsTable, p.spec), ignoreIfExists))
  }

  override def dropPartitions(
      db: String,
      table: String,
      parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = withOdpsClient {
    val odpsTable = getOdpsTable(db, table)
    parts.foreach(p =>
      odpsTable.deletePartition(createPartitionSpec(odpsTable, p), ignoreIfNotExists))
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = withOdpsClient {
    val odpsTable = getOdpsTable(db, table)
    val partCols = odpsTable.getSchema.getPartitionColumns.asScala.map(_.getName)
    specs.zip(newSpecs).foreach(pair => {
      val oldPart = partCols.map(c => s"$c='${pair._1(c)}'").mkString(",")
      val newPart = partCols.map(c => s"$c='${pair._2(c)}'").mkString(",")
      val sql = s"ALTER TABLE $db.`$table` PARTITION($oldPart) RENAME TO PARTITION ($newPart);"
      SQLTask.run(odps, sql).waitForSuccess()
    })
  }

  override def alterPartitions(
      db: String,
      table: String,
      newParts: Seq[CatalogTablePartition]): Unit = withClient {
    throw new AnalysisException("alter partitions not supported")
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = withOdpsClient {
    getPartitionOption(db, table, spec)
      .getOrElse(throw new NoSuchPartitionException(db, table, spec))
  }

  /**
   * Returns the specified partition or None if it does not exist.
   */
  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = withOdpsClient {
    val odpsTable = getOdpsTable(db, table)
    val partitionSpec = createPartitionSpec(odpsTable, spec)
    val odpsPartition = odpsTable.getPartition(partitionSpec)
    try {
      odpsPartition.reload()
      Some(transformPartition(odpsPartition, true))
    } catch {
      case _: NoSuchObjectException => None
    }
  }

  private def transformPartition(
                                  odpsPartition: Partition, withSize: Boolean): CatalogTablePartition = {
    val partSpec = odpsPartition.getPartitionSpec()
    val partitionMap = new mutable.LinkedHashMap[String, String]
    partSpec.keys().asScala.foreach(key => {
      partitionMap.put(key, partSpec.get(key))
    })
    if (withSize) {
      val partSize = odpsPartition.getSize()
      val stats = Some(CatalogStatistics(BigInt(partSize)))
      CatalogTablePartition(partitionMap.toMap, CatalogStorageFormat.empty, stats = stats)
    } else {
      CatalogTablePartition(partitionMap.toMap, CatalogStorageFormat.empty)
    }
  }

  private def listOdpsPartitions(
                                  db: String,
                                  table: String,
                                  partialSpec: Option[TablePartitionSpec]): Seq[Partition] = withOdpsClient {
    val odpsTable = getOdpsTable(db, table)
    odpsTable.getPartitions.asScala.filter(x =>
      partialSpec match {
        case Some(spec) =>
          spec.forall { case (key, value) =>
            x.getPartitionSpec().get(key) == value
          }
        case None => true
      }
    )
  }

  /**
   * Returns the partition names from hive metastore for a given table in a database.
   */
  override def listPartitionNames(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = withOdpsClient {
    listOdpsPartitions(db, table, partialSpec).map { p =>
      val spec = p.getPartitionSpec()
      spec.keys().asScala.map(key => key + "=" + spec.get(key)).mkString("/")
    }
  }

  /**
   * Returns the partitions from hive metastore for a given table in a database.
   */
  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = withClient {
    listOdpsPartitions(db, table, partialSpec).map(transformPartition(_, statsSizeEnable))
  }

  override def listPartitionsByFilter(
      db: String,
      table: String,
      predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    val catalogTable = getTable(db, table)
    val partitionColumnNames = catalogTable.partitionColumnNames
    val nonPartitionPruningPredicates = predicates.filterNot {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames.toSet)
    }

    if (nonPartitionPruningPredicates.nonEmpty) {
      throw new AnalysisException(
        s"Expected only partition pruning predicates: ${predicates.reduceLeft(And)}")
    }

    val odpsPartitions = listOdpsPartitions(db, table, None)
    val filtered = if (predicates.nonEmpty) {
      val partitionSchema = catalogTable.partitionSchema
      val boundPredicate =
        Predicate.createInterpreted(predicates.reduce(And).transform {
          case att: AttributeReference =>
            val index = partitionSchema.indexWhere(_.name == att.name)
            BoundReference(index, partitionSchema(index).dataType, nullable = true)
        })
      odpsPartitions.filter { p =>
        boundPredicate.eval(transformPartition(p, false).toRow(partitionSchema, defaultTimeZoneId))
      }
    } else {
      odpsPartitions
    }

    filtered.map(p => transformPartition(p, statsSizeEnable))
  }

  def getOdpsBucketSpec(db: String, table: String): Option[OdpsBucketSpec] = {
    val sdkTable = getOdpsTable(db, table)
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

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(
      db: String,
      funcDefinition: CatalogFunction): Unit = withClient {
    requireDbExists(db)
    // Hive's metastore is case insensitive. However, Hive's createFunction does
    // not normalize the function name (unlike the getFunction part). So,
    // we are normalizing the function name.
    val functionName = funcDefinition.identifier.funcName.toLowerCase(Locale.ROOT)
    requireFunctionNotExists(db, functionName)
    val functionIdentifier = funcDefinition.identifier.copy(funcName = functionName)
    client.createFunction(db, funcDefinition.copy(identifier = functionIdentifier))
  }

  override def dropFunction(db: String, name: String): Unit = withClient {
    requireFunctionExists(db, name)
    client.dropFunction(db, name)
  }

  override def alterFunction(
      db: String, funcDefinition: CatalogFunction): Unit = withClient {
    requireDbExists(db)
    val functionName = funcDefinition.identifier.funcName.toLowerCase(Locale.ROOT)
    requireFunctionExists(db, functionName)
    val functionIdentifier = funcDefinition.identifier.copy(funcName = functionName)
    client.alterFunction(db, funcDefinition.copy(identifier = functionIdentifier))
  }

  override def renameFunction(
      db: String,
      oldName: String,
      newName: String): Unit = withClient {
    requireFunctionExists(db, oldName)
    requireFunctionNotExists(db, newName)
    client.renameFunction(db, oldName, newName)
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = withClient {
    requireFunctionExists(db, funcName)
    client.getFunction(db, funcName)
  }

  override def functionExists(db: String, funcName: String): Boolean = withClient {
    requireDbExists(db)
    client.functionExists(db, funcName)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = withClient {
    requireDbExists(db)
    client.listFunctions(db, pattern)
  }
}

case class SortColumn(name: String, order: String)
object SortColumn {
  def apply(name: String, orderTag: Int): SortColumn = {
    val order = orderTag match {
      case 0 => "asc"
      case 1 => "desc"
    }
    new SortColumn(name, order)
  }
}

case class OdpsBucketSpec(clusterType: String,
                          numBuckets: Int,
                          bucketColumnNames: Seq[String],
                          sortColumns: Seq[SortColumn]) {
  override def toString: String = {
    val typeString = s"cluster type: $clusterType"
    val bucketString = s"bucket columns: [${bucketColumnNames.mkString(", ")}]"
    val sortString = if (sortColumns.nonEmpty) {
      s", sort columns: [${sortColumns.mkString(", ")}]"
    } else {
      ""
    }
    s"$numBuckets buckets, $typeString, $bucketString$sortString"
  }
}

object HiveExternalCatalog {
  val SPARK_SQL_PREFIX = "spark.sql."

  val DATASOURCE_PREFIX = SPARK_SQL_PREFIX + "sources."
  val DATASOURCE_PROVIDER = DATASOURCE_PREFIX + "provider"
  val DATASOURCE_SCHEMA = DATASOURCE_PREFIX + "schema"
  val DATASOURCE_SCHEMA_PREFIX = DATASOURCE_SCHEMA + "."
  val DATASOURCE_SCHEMA_NUMPARTCOLS = DATASOURCE_SCHEMA_PREFIX + "numPartCols"
  val DATASOURCE_SCHEMA_NUMSORTCOLS = DATASOURCE_SCHEMA_PREFIX + "numSortCols"
  val DATASOURCE_SCHEMA_NUMBUCKETS = DATASOURCE_SCHEMA_PREFIX + "numBuckets"
  val DATASOURCE_SCHEMA_NUMBUCKETCOLS = DATASOURCE_SCHEMA_PREFIX + "numBucketCols"
  val DATASOURCE_SCHEMA_PART_PREFIX = DATASOURCE_SCHEMA_PREFIX + "part."
  val DATASOURCE_SCHEMA_PARTCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "partCol."
  val DATASOURCE_SCHEMA_BUCKETCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "bucketCol."
  val DATASOURCE_SCHEMA_SORTCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "sortCol."

  val STATISTICS_PREFIX = SPARK_SQL_PREFIX + "statistics."
  val STATISTICS_TOTAL_SIZE = STATISTICS_PREFIX + "totalSize"
  val STATISTICS_NUM_ROWS = STATISTICS_PREFIX + "numRows"
  val STATISTICS_COL_STATS_PREFIX = STATISTICS_PREFIX + "colStats."

  val TABLE_PARTITION_PROVIDER = SPARK_SQL_PREFIX + "partitionProvider"
  val TABLE_PARTITION_PROVIDER_CATALOG = "catalog"
  val TABLE_PARTITION_PROVIDER_FILESYSTEM = "filesystem"

  val CREATED_SPARK_VERSION = SPARK_SQL_PREFIX + "create.version"

  val HIVE_GENERATED_TABLE_PROPERTIES = Set(DDL_TIME)
  val HIVE_GENERATED_STORAGE_PROPERTIES = Set(SERIALIZATION_FORMAT)

  // When storing data source tables in hive metastore, we need to set data schema to empty if the
  // schema is hive-incompatible. However we need a hack to preserve existing behavior. Before
  // Spark 2.0, we do not set a default serde here (this was done in Hive), and so if the user
  // provides an empty schema Hive would automatically populate the schema with a single field
  // "col". However, after SPARK-14388, we set the default serde to LazySimpleSerde so this
  // implicit behavior no longer happens. Therefore, we need to do it in Spark ourselves.
  val EMPTY_DATA_SCHEMA = new StructType()
    .add("col", "array<string>", nullable = true, comment = "from deserializer")

  /**
   * Detects a data source table. This checks both the table provider and the table properties,
   * unlike DDLUtils which just checks the former.
   */
  private[spark] def isDatasourceTable(table: CatalogTable): Boolean = {
    val provider = table.provider.orElse(table.properties.get(DATASOURCE_PROVIDER))
    provider.isDefined && provider != Some(DDLUtils.HIVE_PROVIDER)
  }

  private def getColumnNamesByType(
      props: Map[String, String],
      colType: String,
      typeName: String): Seq[String] = {
    for {
      numCols <- props.get(s"spark.sql.sources.schema.num${colType.capitalize}Cols").toSeq
      index <- 0 until numCols.toInt
    } yield props.getOrElse(
      s"$DATASOURCE_SCHEMA_PREFIX${colType}Col.$index",
      throw new AnalysisException(
        s"Corrupted $typeName in catalog: $numCols parts expected, but part $index is missing."
      )
    )
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  private def getSQLString(
                            projectName: String,
                            tableName: String,
                            schema: TableSchema,
                            ifNotExists: Boolean,
                            tableDefinition: CatalogTable): String = {
    val sb = new StringBuilder
    if (tableDefinition.tableType != CatalogTableType.EXTERNAL) {
      sb.append("CREATE TABLE ")
    } else {
      sb.append("CREATE EXTERNAL TABLE ")
    }
    if (ifNotExists) {
      sb.append(" IF NOT EXISTS ")
    }
    sb.append(projectName).append(".`").append(tableName).append("` (")
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
        pColumns
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
      if (!sortCols.isEmpty) {
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
      // TODO: support aliort
      // tableDefinition.storage.outputFormat foreach (format => sb.append(s" STORED AS $format"))
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

  private def typeToName(dataType: DataType): String = {
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
}
