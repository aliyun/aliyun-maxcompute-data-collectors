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

import com.aliyun.odps.PartitionSpec
import com.aliyun.odps.table.{DataFormat, TableIdentifier}
import com.aliyun.odps.table.configuration.ArrowOptions
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit
import com.aliyun.odps.table.write.{TableBatchWriteSession, TableWriteCapabilities, TableWriteSessionBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.ErrorMsg

import scala.collection.JavaConverters._
import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, Descending, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, PartitioningUtils}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.OdpsBucketSpec
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.OdpsOptions
import org.apache.spark.sql.odps.{OdpsClient, OdpsWriteJobStatsTracker, WriteJobDescription}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable

case class InsertIntoOdpsTable(
    table: CatalogTable,
    partition: Map[String, Option[String]],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean,
    outputColumnNames: Seq[String])
  extends DataWritingCommand {

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
   */
  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val externalCatalog: HiveExternalCatalog =
      sparkSession.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog]

    if (table.tableType == CatalogTableType.EXTERNAL) {
        throw new SparkException(s"Unsupported table type for table write ${table.tableType}")
    }

    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val numStaticPartitions = partition.values.count(_.nonEmpty)
    val odpsPartitionSpec = new PartitionSpec

    val partitionSchema = table.partitionSchema
    val partitionColumnNames = table.partitionColumnNames

    // By this time, the partition map must match the table's partition columns
    if (partitionColumnNames.toSet != partition.keySet) {
      throw QueryExecutionErrors.requestedPartitionsMismatchTablePartitionsError(table, partition)
    }

    val outputPartitionColumns =
      outputColumns.filter(c => partitionSchema.getFieldIndex(c.name).isDefined)
    val outputPartitionSet = AttributeSet(outputPartitionColumns)
    val dataColumns = outputColumns.filterNot(outputPartitionSet.contains)

    if (partitionSchema.nonEmpty) {
      // val partitionSpec = partition.filter(_._2.nonEmpty).map { case (k, v) => k -> v.get }
      val partitionSpec = partition.map {
        // TODO: null partition
        case (key, Some(value)) => key -> value
        case (key, None) => key -> ""
      }

      // Validate partition spec if there exist any dynamic partitions
      if (numDynamicPartitions > 0) {
        // Report error if dynamic partitioning is not enabled
        if (!hadoopConf.get("odps.exec.dynamic.partition", "true").toBoolean) {
          throw new SparkException("Dynamic partition is disabled. " +
            "Either enable it by setting odps.exec.dynamic.partition=true or specify partition column values")
        }

        // Report error if dynamic partition strict mode is on but no static partition is found
        if (numStaticPartitions == 0 &&
          hadoopConf.get("odps.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict")) {
          throw new SparkException("Dynamic partition strict mode requires at least one static partition column. " +
            "To turn this off set odps.exec.dynamic.partition.mode=nonstrict")
        }

        // Report error if any static partition appears after a dynamic partition
        val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
        if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
          throw new AnalysisException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
        }

        val dynamicPartitionSchema = StructType(partitionSchema.takeRight(numDynamicPartitions))
        dynamicPartitionSchema.map(_.dataType).foreach {
          case StringType | LongType | IntegerType | ShortType | ByteType =>
          case dt: DataType =>
            throw new SparkException(s"Unsupported partition column type: ${dt.simpleString}")
        }
      }

      if (numStaticPartitions > 0) {
        var part = 0
        partitionColumnNames.foreach { field =>
          if (part < numStaticPartitions) {
            odpsPartitionSpec.set(field, partitionSpec(field))
            part = part + 1
          }
        }
      }

      val isStaticPartition = numStaticPartitions > 0 && numDynamicPartitions == 0

      if (isStaticPartition && ifPartitionNotExists) {
        // scalastyle:off
        // ifNotExists is only valid with static partition, refer to
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
        // scalastyle:on
        val oldPart =
        sparkSession.sharedState.externalCatalog.getPartitionOption(
          table.database,
          table.identifier.table,
          partitionSpec)
        if (oldPart.isDefined) {
          return Seq.empty[Row]
        }
      }
    }

    val provider = OdpsOptions.odpsTableWriterProvider(conf)
    val settings = OdpsClient.get.getEnvironmentSettings
    val project = table.database
    val tableName = table.identifier.table

    val arrowOptions = ArrowOptions.newBuilder()
      .withDatetimeUnit(TimestampUnit.MILLI)
      .withTimestampUnit(TimestampUnit.MICRO).build()

    val writeCapabilities = TableWriteCapabilities.newBuilder()
      .supportDynamicPartition(true)
      .supportHashBuckets(true)
      .supportRangeBuckets(true)
      .build()

    val odpsBucketSpec = if (table.bucketSpec.isDefined) externalCatalog.getOdpsBucketSpec(project, tableName) else None
    if (odpsBucketSpec.isDefined && !odpsBucketSpec.get.clusterType.toLowerCase.equals("hash")) {
      throw new SparkException(s"Write ${odpsBucketSpec.get.clusterType} bucketed table is not supported")
    }
    val bucketAttributes = odpsBucketSpec match {
      case Some(OdpsBucketSpec(_, _, bucketColumnNames, _)) =>
        bucketColumnNames.map(name => {
          query.resolve(name :: Nil, sparkSession.sessionState.analyzer.resolver).getOrElse(
            throw new AnalysisException(
                  s"Unable to resolve $name given [${query.output.map(_.name).mkString(", ")}]")
          ).asInstanceOf[Attribute]
        })
      case _ => Seq.empty[Attribute]
    }
    val bucketSortOrders = odpsBucketSpec match {
      case Some(OdpsBucketSpec(_, _, _, sortColumns)) =>
        sortColumns.map(col => {
          val attr = query.resolve(col.name :: Nil, sparkSession.sessionState.analyzer.resolver).getOrElse(
            throw new AnalysisException(
                  s"Unable to resolve ${col.name} given [${query.output.map(_.name).mkString(", ")}]")
          ).asInstanceOf[Attribute]
          SortOrder(attr, col.order.toUpperCase() match {
            case "ASC" => Ascending
            case _ => Descending
          })
        })
      case _ => Seq.empty[SortOrder]
    }

    val sinkBuilder = new TableWriteSessionBuilder()
      .identifier(TableIdentifier.of(project, tableName))
      .withArrowOptions(arrowOptions)
      .withCapabilities(writeCapabilities)
      .withSettings(settings)
      .overwrite(overwrite)
      .withSessionProvider(provider)

    if (partitionSchema.nonEmpty) {
      if (numStaticPartitions > 0) {
        sinkBuilder.partition(odpsPartitionSpec)
      }
    }

    val batchSink = sinkBuilder.buildBatchWriteSession
    logInfo(s"Create table sink ${batchSink.getId} for ${batchSink.getTableIdentifier}")

    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val statsTracker = new OdpsWriteJobStatsTracker(metrics)
    val arrowDataFormat = new DataFormat(DataFormat.Type.ARROW, DataFormat.Version.V5)

    val description = new WriteJobDescription(
      serializableHadoopConf = serializableHadoopConf,
      batchSink = batchSink,
      staticPartition = odpsPartitionSpec,
      allColumns = outputColumns,
      dataColumns = dataColumns,
      partitionColumns = outputPartitionColumns,
      dynamicPartitionColumns = outputPartitionColumns,
      maxRecordsPerFile = sparkSession.sessionState.conf.maxRecordsPerFile,
      statsTrackers = Seq(statsTracker),
      writeBatchSize = OdpsOptions.odpsVectorizedWriterBatchSize(sparkSession.sessionState.conf),
      timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone,
      supportArrowWriter = batchSink.supportsDataFormat(arrowDataFormat),
      enableArrowExtension = OdpsOptions.odpsEnableArrowExtension(sparkSession.sessionState.conf),
      compressionCodec = OdpsOptions.odpsTableWriterCompressCodec(sparkSession.sessionState.conf),
      chunkSize = OdpsOptions.odpsWriterChunkSize(sparkSession.sessionState.conf),
      maxRetries = OdpsOptions.odpsWriterMaxRetires(sparkSession.sessionState.conf),
      maxSleepIntervalMs = OdpsOptions.odpsWriterRetrySleepIntervalMs(sparkSession.sessionState.conf),
      maxBlocks = OdpsOptions.odpsWriterMaxBlocks(sparkSession.sessionState.conf)
    )

    OdpsTableWriter.write(
      sparkSession,
      child,
      batchSink,
      description,
      outputColumns,
      table.bucketSpec,
      bucketAttributes,
      bucketSortOrders,
      overwrite)

    // Invalidate the cache.
    sparkSession.sessionState.catalog.refreshTable(table.identifier)

    Seq.empty[Row]
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): InsertIntoOdpsTable =
    copy(query = newChild)
}
