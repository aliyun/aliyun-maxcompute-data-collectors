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
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}

private[odps] class OdpsMetaClient(odpsOptions: OdpsOptions) extends Logging {

  import OdpsMetaClient._

  private val cacheSize = odpsOptions.metaCacheSize
  private val cacheExpireTime = odpsOptions.metaCacheExpireSeconds

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

  private val sdkTableLoader = new CacheLoader[QualifiedTableName, Option[SdkTable]] {
    override def load(key: QualifiedTableName): Option[SdkTable] = {
      try {
        val table = odps.tables().get(key.database, key.name)
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
  private val sdkTableCache: LoadingCache[QualifiedTableName, Option[SdkTable]] =
    createCacheBuilder().build(sdkTableLoader)

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

  def getSdkTableOption(
      db: String,
      table: String,
      refresh: Boolean = false): Option[SdkTable] = {
    val key = QualifiedTableName(db, table)
    if (refresh) {
      sdkTableCache.invalidate(key)
    }
    sdkTableCache.get(key)
  }

  def getSdkTable(db: String, table: String, refresh: Boolean = false): SdkTable = {
    getSdkTableOption(db, table, refresh)
      .getOrElse(throw new NoSuchTableException(db, table))
  }

  def invalidateTableCache(db: String, table: String): Unit = {
    val key = QualifiedTableName(db, table)
    sdkTableCache.invalidate(key)
  }

  def dropTableInCache(db: String, table: String): Unit = {
    val key = QualifiedTableName(db, table)
    sdkTableCache.put(key, None)
  }
}

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
