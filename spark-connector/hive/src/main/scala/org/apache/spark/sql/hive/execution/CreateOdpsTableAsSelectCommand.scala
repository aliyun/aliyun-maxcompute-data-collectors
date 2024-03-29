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

package org.apache.spark.sql.hive.execution

import scala.util.control.NonFatal

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{DataWritingCommand, DDLUtils}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.HiveSessionCatalog
import org.apache.spark.util.Utils

trait CreateOdpsTableAsSelectBase extends DataWritingCommand {
  val tableDesc: CatalogTable
  val query: LogicalPlan
  val outputColumnNames: Seq[String]
  val mode: SaveMode

  protected val tableIdentifier = tableDesc.identifier

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val tableExists = catalog.tableExists(tableIdentifier)

    if (tableExists) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableIdentifier has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw QueryCompilationErrors.tableIdentifierExistsError(tableIdentifier)
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      val command = getWritingCommand(catalog, tableDesc, tableExists = true)
      command.run(sparkSession, child)
      DataWritingCommand.propogateMetrics(sparkSession.sparkContext, command, metrics)
    } else {
        tableDesc.storage.locationUri.foreach { p =>
          DataWritingCommand.assertEmptyRootPath(p, mode, sparkSession.sessionState.newHadoopConf)
        }
      // TODO ideally, we should get the output data ready first and then
      // add the relation into catalog, just in case of failure occurs while data
      // processing.
      val tableSchema = CharVarcharUtils.getRawSchema(
        outputColumns.toStructType, sparkSession.sessionState.conf)
      assert(tableDesc.schema.isEmpty)
      catalog.createTable(
        tableDesc.copy(schema = tableSchema), ignoreIfExists = false)

      try {
        // Read back the metadata of the table which was created just now.
        val createdTableMeta = catalog.getTableMetadata(tableDesc.identifier)
        val command = getWritingCommand(catalog, createdTableMeta, tableExists = false)
        command.run(sparkSession, child)
        DataWritingCommand.propogateMetrics(sparkSession.sparkContext, command, metrics)
      } catch {
        case NonFatal(e) =>
          // drop the created table.
          catalog.dropTable(tableIdentifier, ignoreIfNotExists = true, purge = false)
          throw e
      }
    }

    Seq.empty[Row]
  }

  // Returns `DataWritingCommand` which actually writes data into the table.
  def getWritingCommand(
    catalog: SessionCatalog,
    tableDesc: CatalogTable,
    tableExists: Boolean): DataWritingCommand

  // A subclass should override this with the Class name of the concrete type expected to be
  // returned from `getWritingCommand`.
  def writingCommandClassName: String

  override def argString(maxFields: Int): String = {
    s"[Database: ${tableDesc.database}, " +
    s"TableName: ${tableDesc.identifier.table}, " +
    s"${writingCommandClassName}]"
  }
}

/**
 * Create table and insert the query result into it.
 *
 * @param tableDesc the table description, which may contain serde, storage handler etc.
 * @param query the query whose result will be insert into the new relation
 * @param mode SaveMode
 */
case class CreateOdpsTableAsSelectCommand(
    tableDesc: CatalogTable,
    query: LogicalPlan,
    outputColumnNames: Seq[String],
    mode: SaveMode)
  extends CreateOdpsTableAsSelectBase {

  override def getWritingCommand(
      catalog: SessionCatalog,
      tableDesc: CatalogTable,
      tableExists: Boolean): DataWritingCommand = {
    // For CTAS, there is no static partition values to insert.
    val partition = tableDesc.partitionColumnNames.map(_ -> None).toMap
    InsertIntoOdpsTable(
      tableDesc,
      partition,
      query,
      overwrite = if (tableExists) false else true,
      ifPartitionNotExists = false,
      outputColumnNames = outputColumnNames)
  }

  override def writingCommandClassName: String =
    Utils.getSimpleName(classOf[InsertIntoOdpsTable])

  override protected def withNewChildInternal(
    newChild: LogicalPlan): CreateOdpsTableAsSelectCommand = copy(query = newChild)
}
