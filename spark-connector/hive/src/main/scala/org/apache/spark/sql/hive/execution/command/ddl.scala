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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{LeafRunnableCommand, RunnableCommand}
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.odps.OdpsClient

trait AlterCommandHelper {
  def getDbTable(sparkSession: SparkSession,
    table: TableIdentifier): (HiveExternalCatalog, String, String) = {
    val catalog = sparkSession.sessionState.catalog
    val odpsCatalog = catalog.externalCatalog.asInstanceOf[HiveExternalCatalog]
    val db = table.database.getOrElse(catalog.getCurrentDatabase)
    (odpsCatalog, db, table.table)
  }
}

case class OdpsTruncateTableCommand(
  tableName: TableIdentifier,
  partitionSpec: Option[TablePartitionSpec]) extends LeafRunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    val catalog = spark.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)

    OdpsClient.builder().getOrCreate().odps().tables().get(table.database, tableName.identifier).truncate()
    return Seq.empty[Row]
  }
}
