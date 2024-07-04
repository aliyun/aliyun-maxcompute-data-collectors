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

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class OdpsScanBuilder(
    catalog: OdpsTableCatalog,
    table: OdpsTable,
    tableIdent: Identifier,
    dataSchema: StructType,
    partitionSchema: StructType,
    stats: OdpsStatistics,
    options: CaseInsensitiveStringMap)
  extends ScanBuilder with SupportsPushDownRequiredColumns with SupportsPushDownFilters {

  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    SparkSession.active.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  private val isCaseSensitive = SparkSession.active.sessionState.conf.caseSensitiveAnalysis

  private var requiredSchema = StructType(dataSchema.fields ++ partitionSchema.fields)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // [SPARK-30107] While `requiredSchema` might have pruned nested columns,
    // the actual data schema of this scan is determined in `readDataSchema`.
    // File formats that don't support nested schema pruning,
    // use `requiredSchema` as a reference and prune only top-level columns.
    this.requiredSchema = requiredSchema
  }

  private var _partitionFilters = Array.empty[Filter]
  private var _dataFilters = Array.empty[Filter]

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (nestedFilters, normalFilters) = filters.partition(_.containsNestedColumn)

    val partitionSet = partitionSchema.map(_.name).toSet
    val (partitionFilters, nonPartitionFilters) =
      normalFilters.partition(_.references.toSet.subsetOf(partitionSet))
    _partitionFilters = partitionFilters
    _dataFilters = nonPartitionFilters.filter(_.references.toSet.intersect(partitionSet).isEmpty)

    nestedFilters ++ nonPartitionFilters
  }

  override def pushedFilters(): Array[Filter] = _partitionFilters ++ _dataFilters

  override def build(): Scan = {
    OdpsScan(SparkSession.active, hadoopConf, catalog, table, tableIdent, dataSchema, partitionSchema,
      readDataSchema(), readPartitionSchema(), _partitionFilters, _dataFilters, stats)
  }

  protected def readDataSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val fields = dataSchema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName) && !partitionNameSet.contains(colName)
    }
    StructType(fields)
  }

  protected def readPartitionSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val fields = partitionSchema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName)
    }
    StructType(fields)
  }

  private def createRequiredNameSet(): Set[String] =
    requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet

  private val partitionNameSet: Set[String] =
    partitionSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet
}
