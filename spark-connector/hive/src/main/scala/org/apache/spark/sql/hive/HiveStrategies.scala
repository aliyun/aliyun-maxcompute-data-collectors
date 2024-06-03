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

import java.io.IOException

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{CreateTableCommand, DDLUtils}
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSourceStrategy}
import org.apache.spark.sql.hive.execution._


/**
 * Determine the database, serde/format and schema of the Hive serde table, according to the storage
 * properties.
 */
class ResolveOdpsTable(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case c @ CreateTable(t, _, query) if DDLUtils.isHiveTable(t) =>
      // Finds the database name if the name does not exist.
      val dbName = t.identifier.database.getOrElse(session.catalog.currentDatabase)
      val withDatabase = t.copy(identifier = t.identifier.copy(database = Some(dbName)))
      c.copy(tableDesc = withDatabase)
  }
}

class DetermineTableStats(session: SparkSession) extends Rule[LogicalPlan] {
  private def hiveTableWithStats(relation: HiveTableRelation): HiveTableRelation = {
    val table = relation.tableMeta
    val partitionCols = relation.partitionCols
    // For partitioned tables, the partition directory may be outside of the table directory.
    // Which is expensive to get table size. Please see how we implemented it in the AnalyzeTable.
    val sizeInBytes = if (conf.fallBackToHdfsForStatsEnabled && partitionCols.isEmpty) {
      try {
        val hadoopConf = session.sessionState.newHadoopConf()
        val tablePath = new Path(table.location)
        val fs: FileSystem = tablePath.getFileSystem(hadoopConf)
        fs.getContentSummary(tablePath).getLength
      } catch {
        case e: IOException =>
          logWarning("Failed to get table size from HDFS.", e)
          conf.defaultSizeInBytes
      }
    } else {
      conf.defaultSizeInBytes
    }

    val stats = Some(Statistics(sizeInBytes = BigInt(sizeInBytes)))
    relation.copy(tableStats = stats)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case relation: HiveTableRelation
      if DDLUtils.isHiveTable(relation.tableMeta) && relation.tableMeta.stats.isEmpty =>
      hiveTableWithStats(relation)

    // handles InsertIntoStatement specially as the table in InsertIntoStatement is not added in its
    // children, hence not matched directly by previous HiveTableRelation case.
    case i @ InsertIntoStatement(relation: HiveTableRelation, _, _, _, _, _, false)
      if DDLUtils.isHiveTable(relation.tableMeta) && relation.tableMeta.stats.isEmpty =>
      i.copy(table = hiveTableWithStats(relation))
  }
}

/**
 * Replaces generic operations with specific variants that are designed to work with Hive.
 *
 * Note that, this rule must be run after `PreprocessTableCreation` and
 * `PreprocessTableInsertion`.
 */
object HiveAnalysis extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case InsertIntoStatement(
        r: HiveTableRelation, partSpec, _, query, overwrite, ifPartitionNotExists, false)
        if DDLUtils.isHiveTable(r.tableMeta) =>
      InsertIntoOdpsTable(r.tableMeta, partSpec, query, overwrite,
        ifPartitionNotExists, query.output.map(_.name))

    case CreateTable(tableDesc, mode, None) if DDLUtils.isHiveTable(tableDesc) =>
      CreateTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)

    case CreateTable(tableDesc, mode, Some(query))
        if DDLUtils.isHiveTable(tableDesc) && query.resolved =>
      CreateOdpsTableAsSelectCommand(tableDesc, query, query.output.map(_.name), mode)
  }
}

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  /**
   * Retrieves data using a HiveTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ScanOperation(projects, filters, null, relation: HiveTableRelation) =>
        // Filters on this relation fall into four categories based
        // on where we can use them to avoid reading unneeded data:
        //  - partition keys only - used to prune directories to read
        //  - bucket keys only - optionally used to prune files to read
        //  - keys stored in the data only - optionally used to skip groups of data in files
        //  - filters that need to be evaluated again after the scan
        val filterSet = ExpressionSet(filters)

        // The attribute name of predicate could be different than the one in schema in case of
        // case insensitive, we should change them to match the one in schema, so we do not need to
        // worry about case sensitivity anymore.
        val normalizedFilters = DataSourceStrategy.normalizeExprs(
          filters.filter(_.deterministic), relation.output)

        val partitionColumns = relation.partitionCols
        val partitionSet = AttributeSet(partitionColumns)

        val partitionKeyFilters = DataSourceStrategy.getPushedDownFilters(partitionColumns,
          normalizedFilters)

        val normalizedFiltersWithoutSubqueries =
          normalizedFilters.filterNot(SubqueryExpression.hasSubquery)

        val dataColumns = relation.dataCols
        // Partition keys are not available in the statistics of the files.
        val dataFilters = normalizedFiltersWithoutSubqueries.filter(_.references.intersect(partitionSet).isEmpty)

        // Predicates with both partition keys and attributes need to be evaluated after the scan.
        val afterScanFilters = filterSet -- partitionKeyFilters.filter(_.references.nonEmpty)
        logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

        val filterAttributes = AttributeSet(afterScanFilters)
        val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
        val requiredAttributes = AttributeSet(requiredExpressions)

        val readDataColumns =
          dataColumns
            .filter(requiredAttributes.contains)
            .filterNot(partitionColumns.contains)
        val outputAttributes = readDataColumns ++ partitionColumns
        val outputSchema = outputAttributes.toStructType
        val dataSchema = dataColumns.toStructType
        val partitionSchema = partitionColumns.toStructType
        val readDataSchema = readDataColumns.toStructType
        val readPartitionSchema = partitionColumns.toStructType

        logInfo(s"Output Data Schema: ${outputSchema.simpleString(5)}")

        // TODO: bucket filters
        val scan =
          OdpsTableScanExec(
            relation,
            outputAttributes,
            outputSchema,
            dataSchema,
            partitionSchema,
            readDataSchema,
            readPartitionSchema,
            partitionKeyFilters.toSeq,
            Seq.empty,
            dataFilters)(sparkSession)

        val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
        val withFilter = afterScanFilter.map(FilterExec(_, scan)).getOrElse(scan)
        val withProjections = if (projects == withFilter.output) {
          withFilter
        } else {
          ProjectExec(projects, withFilter)
        }

        withProjections :: Nil
      case _ =>
        Nil
    }
  }
}
