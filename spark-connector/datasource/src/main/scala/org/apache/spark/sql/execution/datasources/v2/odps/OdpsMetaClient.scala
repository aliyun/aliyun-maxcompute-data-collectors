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

import java.util.concurrent.TimeUnit

import com.aliyun.odps.table.TableIdentifier

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import com.aliyun.odps.{NoSuchObjectException, Odps, OdpsException, Project, ReloadException, Table => SdkTable}
import com.aliyun.odps.task.SQLTask
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.odps.OdpsClient
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.Identifier

private[odps] class OdpsMetaClient(odpsOptions: OdpsOptions) extends Logging {

  import OdpsMetaClient._

  private val cacheSize = odpsOptions.metaCacheSize
  private val cacheExpireTime = odpsOptions.metaCacheExpireSeconds
  private val viewCacheSize = odpsOptions.metaCacheSize
  private val viewExpireTime = odpsOptions.viewCacheExpireSeconds

  private val enableExternalProject = odpsOptions.enableExternalProject
  private val enableExternalTable = odpsOptions.enableExternalTable

  // TODO partStatsLevel expects `none`, `size`, `rowCount` and `colStats`,
  //      higher levels imply including all lower level stats
  private val partStatsLevel = odpsOptions.metaStatsLevel
  private val partStatsSizeEnable = partStatsLevel.equalsIgnoreCase("size")

  private def createCacheBuilder(): CacheBuilder[AnyRef, AnyRef] = {
    CacheBuilder.newBuilder()
      .maximumSize(cacheSize)
      .expireAfterWrite(cacheExpireTime, TimeUnit.SECONDS)
  }

  private def createViewCacheBuilder(): CacheBuilder[AnyRef, AnyRef] = {
    CacheBuilder.newBuilder()
      .maximumSize(viewCacheSize)
      .expireAfterWrite(viewExpireTime, TimeUnit.SECONDS)
  }

  private val projectLoader = new CacheLoader[String, Option[Project]] {
    override def load(key: String): Option[Project] = {
      try {
        val project = odps.projects().get(key)
        project.reload()
        if (project.getType.equals(Project.ProjectType.external) && !enableExternalProject) {
          throw new Exception("Odps external project is not enable!")
        }
        Some(project)
      } catch {
        case _: NoSuchObjectException => None
        case ex: Exception =>
          logWarning("load odps project failed: " + ex.getMessage)
          None
      }
    }
  }
  private val projectCache: LoadingCache[String, Option[Project]] =
    createCacheBuilder().build(projectLoader)

  private val sdkTableLoader = new CacheLoader[TableIdentifier, Option[SdkTable]] {
    override def load(key: TableIdentifier): Option[SdkTable] = {
      try {
        val table = odps.tables().get(key.getProject, key.getSchema, key.getTable)
        table.reload()
        if (table.isExternalTable && !enableExternalTable) {
          throw new Exception("Odps external table is not enable!")
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
  private val sdkTableCache: LoadingCache[TableIdentifier, Option[SdkTable]] =
    createCacheBuilder().build(sdkTableLoader)

  private val schemaLoader = new CacheLoader[OdpsSchema, Option[String]] {
    override def load(odpsSchema: OdpsSchema): Option[String] = {
      try {
        val exist = odps.schemas().exists(odpsSchema.project, odpsSchema.schemaName)
        if (exist) {
          Some(odpsSchema.schemaName)
        } else {
          None
        }
      } catch {
        case _: NoSuchObjectException => None
        case ex: Exception =>
          logWarning("load odps project failed: " + ex.getMessage)
          None
      }
    }
  }
  private val schemaCache: LoadingCache[OdpsSchema, Option[String]] =
    createCacheBuilder().build(schemaLoader)

  private val viewCacheTableLoader = new CacheLoader[String, Option[SdkTable]] {
    override def load(query: String): Option[SdkTable] = {
      withClient {
        val tableName =
          s"odps_spark_materialize_odps_view_${System.currentTimeMillis()}_${query.hashCode() & Int.MaxValue}"
        val inst = SQLTask.run(odps, s"CREATE TABLE IF NOT EXISTS $tableName LIFECYCLE 1 AS $query;")
        logInfo(s"Materialize odps view instance: ${inst.getId}, query: $query")
        inst.waitForSuccess()
        val sdkTable = odps.tables().get(odps.getDefaultProject, tableName)
        sdkTable.reload()
        Some(sdkTable)
      }
    }
  }

  private val viewTableCache: LoadingCache[String, Option[SdkTable]] =
    createViewCacheBuilder().build(viewCacheTableLoader)

  def initialize(): Unit = {
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
    hints.put("odps.namespace.schema", odpsOptions.enableNamespaceSchema.toString)
    hints.put("odps.sql.allow.namespace.schema", odpsOptions.enableNamespaceSchema.toString)
    SQLTask.setDefaultHints(hints.asJava)
  }

  def getProjectOption(name: String, refresh: Boolean = false): Option[Project] = {
    if (refresh) {
      projectCache.invalidate(name)
    }
    projectCache.get(name)
  }

  def getProject(name: String, refresh: Boolean = false): Project = {
    getProjectOption(name, refresh)
      .getOrElse(throw new NoSuchNamespaceException(name))
  }

  def getSdkTableOption(project: String, schema: String, table: String,
                        refresh: Boolean = false): Option[SdkTable] = {
    val key = TableIdentifier.of(project, schema, table)
    if (refresh) {
      sdkTableCache.invalidate(key)
    }
    sdkTableCache.get(key)
  }

  def getSdkTable(project: String, schema: String, table: String,
                  refresh: Boolean = false): SdkTable = {
    getSdkTableOption(project, schema, table, refresh)
      .getOrElse(throw new NoSuchTableException(Identifier.of(Array(project, schema), table)))
  }

  def invalidateTableCache(project: String, schema: String, table: String): Unit = {
    val key = TableIdentifier.of(project, schema, table)
    sdkTableCache.invalidate(key)
  }

  def dropTableInCache(project: String, schema: String, table: String): Unit = {
    val key = TableIdentifier.of(project, schema, table)
    sdkTableCache.put(key, None)
  }

  def getSchemaOption(project: String, schemaName: String, refresh: Boolean = false): Option[String] = {
    val key = OdpsSchema(project, schemaName)
    if (refresh) {
      schemaCache.invalidate(key)
    }
    schemaCache.get(key)
  }

  def getSchema(project: String, schemaName: String, refresh: Boolean = false): String = {
    getSchemaOption(project, schemaName, refresh)
      .getOrElse(throw new NoSuchNamespaceException(Array(project, schemaName)))
  }

  def getSchemas(project: String, refresh: Boolean = false): Seq[String] = {
    val iterator = odps.schemas().iterator
    var outputSchemaNames = Seq.empty[String]
    while (iterator.hasNext) {
      val schema = iterator.next
      if (refresh) {
        schemaCache.put(OdpsSchema(project, schema.getName), Some(schema.getName))
      }
      outputSchemaNames = outputSchemaNames :+ schema.getName
    }
    outputSchemaNames
  }

  def putSchemaInCache(project: String, schema: String): Unit = {
    schemaCache.put(OdpsSchema(project, schema), Some(schema))
  }

  def dropSchemaInCache(project: String, schema: String): Unit = {
    schemaCache.put(OdpsSchema(project, schema), None)
  }

  def getSdkViewTable(query: String): SdkTable = {
    viewTableCache.get(query)
      .getOrElse(throw new NoSuchTableException(query))
  }
}

case class OdpsSchema(project: String, schemaName: String)

private object OdpsMetaClient {

  private def isClientException(e: Throwable): Boolean = e match {
    case _: OdpsException => true
    case _: ReloadException => true
    case _ => false
  }

  def odps: Odps = OdpsClient.builder()
    .config(SparkSession.active.sessionState.newHadoopConf())
    // TODO: Load defaults in cluster mode
    .getOrCreate()
    .odps()

  def withClient[T](body: => T): T = {
    try {
      body
    } catch {
      case NonFatal(e) if isClientException(e) =>
        throw new AnalysisException(
          e.getClass.getCanonicalName + ": " + e.getMessage, cause = Some(e))
      // todo NoSuchTableException
    }
  }
}
