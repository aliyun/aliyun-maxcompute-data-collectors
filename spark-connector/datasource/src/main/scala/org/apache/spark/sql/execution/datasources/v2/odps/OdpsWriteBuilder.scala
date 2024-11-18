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

import com.aliyun.odps.PartitionSpec

import scala.collection.JavaConverters._
import com.aliyun.odps.table.{DataFormat, TableIdentifier}
import com.aliyun.odps.table.configuration.ArrowOptions
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit
import com.aliyun.odps.table.write.{TableBatchWriteSession, TableWriteCapabilities, TableWriteSessionBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{FieldReference, LogicalExpressions, NullOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, RequiresDistributionAndOrdering, SupportsDynamicOverwrite, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, DataSource}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.execution.datasources.v2.odps.OdpsTableType.{EXTERNAL_TABLE, VIRTUAL_VIEW}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.odps.{OdpsClient, OdpsWriteJobStatsTracker, WriteJobDescription}
import org.apache.spark.util.SerializableConfiguration

@Experimental
case class OdpsWriteBuilder(
                             catalog: OdpsTableCatalog,
                             catalogTable: OdpsTable,
                             tableIdent: Identifier,
                             dataSchema: StructType,
                             partitionSchema: StructType,
                             odpsOptions: OdpsOptions,
                             bucketSpec: Option[OdpsBucketSpec],
                             info: LogicalWriteInfo) extends SupportsDynamicOverwrite {
  private val project = tableIdent.namespace().head
  private val table = tableIdent.name()
  private val odpsSchema = if (catalog.odpsOptions.enableNamespaceSchema) tableIdent.namespace().last else "default"

  private val schema = info.schema()
  private val queryId = info.queryId()
  private val options = info.options()

  private var overwrite = false

  override def build(): Write = new Write with RequiresDistributionAndOrdering {

    override def requiredDistribution: Distribution = {
      bucketSpec match {
        case Some(OdpsBucketSpec("range", _, bucketColumnNames, _)) =>
          Distributions.ordered(bucketColumnNames.map(name =>
            LogicalExpressions.sort(FieldReference(name),
              SortDirection.ASCENDING,
              NullOrdering.NULLS_FIRST))
            .toArray)
        case _ => Distributions.unspecified()
      }
    }

    override def requiredOrdering: Array[SortOrder] = {
      bucketSpec match {
        case Some(OdpsBucketSpec(bucketType, _, _, sortColumns)) =>
          if (bucketType.equals("range")) {
            sortColumns.map(sortColumn => LogicalExpressions.sort(
              FieldReference(sortColumn.name),
              toSortDirection(sortColumn.order.toUpperCase),
              SortDirection.ASCENDING.defaultNullOrdering())).toArray
          } else {
            // hash cluster table
            Array.empty
          }
        case _ =>
          if (partitionSchema.nonEmpty) {
            partitionSchema.map(partitionColumn => LogicalExpressions.sort(
              FieldReference(partitionColumn.name),
              SortDirection.ASCENDING,
              SortDirection.ASCENDING.defaultNullOrdering())).toArray
          } else {
            Array.empty
          }
      }
    }

    private def toSortDirection(order: String): SortDirection = order match {
      case "ASC" => SortDirection.ASCENDING
      case "DESC" => SortDirection.DESCENDING
      case _ => throw new IllegalArgumentException(s"Unknown Odps Sort Direction: ${order}")
    }

    override def requiredNumPartitions(): Int = {
      bucketSpec match {
        case Some(OdpsBucketSpec("range", numBuckets, _, _)) => numBuckets
        case _ => 0
      }
    }

    override def toBatch: BatchWrite = {
      val sparkSession = SparkSession.active
      validateInputs(sparkSession.sessionState.conf)
      val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
      // Hadoop Configurations are case sensitive.
      val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
      catalogTable.tableType match {
        case EXTERNAL_TABLE | VIRTUAL_VIEW =>
          throw new AnalysisException(s"Cannot write odps table of ${catalogTable.tableType.name} type")
        case _ =>
      }

      val settings = OdpsClient.get.getEnvironmentSettings
      val provider = catalog.odpsOptions.tableReadProvider

      val arrowOptions = ArrowOptions.newBuilder()
        .withDatetimeUnit(TimestampUnit.MILLI)
        .withTimestampUnit(TimestampUnit.MICRO).build()

      val writeCapabilities = TableWriteCapabilities.newBuilder()
        .supportDynamicPartition(true)
        .supportHashBuckets(true)
        .supportRangeBuckets(true)
        .build()

      val sinkBuilder = new TableWriteSessionBuilder()
        .identifier(TableIdentifier.of(project, odpsSchema, table))
        .withArrowOptions(arrowOptions)
        .withCapabilities(writeCapabilities)
        .withSettings(settings)
        .overwrite(overwrite)
        .withSessionProvider(provider)

      val odpsStaticPartition = new PartitionSpec
      if (partitionSchema.nonEmpty) {
        val partitionSpecValue = options.getOrDefault("writeOdpsStaticPartition", "")
        var isDynamic = partitionSpecValue.isEmpty
        if (!partitionSpecValue.isEmpty) {
          val partitionSpec = partitionSpecValue.split(",")
            .map(_.split("="))
            .filter(_.length == 2)
            .map(kv => kv(0) -> kv(1).replaceAll("'", "").replaceAll("\"", ""))
            .toMap
          val partitionColumnNames = partitionSchema.fields
            .map(PartitioningUtils.getColName(_, caseSensitive = false))
          var numStaticPartitions = 0
          partitionColumnNames.foreach { field =>
            if (partitionSpec.contains(field) && !partitionSpec(field).isEmpty && !isDynamic) {
              odpsStaticPartition.set(field, partitionSpec(field))
              numStaticPartitions = numStaticPartitions + 1
            } else {
              isDynamic = true
            }
          }
          if (numStaticPartitions > 0) {
            sinkBuilder.partition(odpsStaticPartition)
          }
        }
      }

      val batchSink = sinkBuilder.buildBatchWriteSession
      val arrowDataFormat = new DataFormat(DataFormat.Type.ARROW, DataFormat.Version.V5)
      val supportArrowWriter = batchSink.supportsDataFormat(arrowDataFormat)

      val description = createWriteJobDescription(sparkSession,
        hadoopConf,
        batchSink,
        odpsStaticPartition,
        options.asScala.toMap,
        odpsOptions,
        supportArrowWriter)
      new OdpsBatchWrite(catalog, tableIdent, batchSink, description, overwrite)
    }
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    overwrite = true
    this
  }

  private def validateInputs(sqlConf: SQLConf): Unit = {
    val caseSensitiveAnalysis = sqlConf.caseSensitiveAnalysis
    assert(schema != null, "Missing input data schema")
    assert(queryId != null, "Missing query ID")

    SchemaUtils.checkColumnNameDuplication(schema.fields.map(_.name), caseSensitiveAnalysis)
    DataSource.validateSchema(schema, sqlConf)
  }

  private def createWriteJobDescription(sparkSession: SparkSession,
                                        hadoopConf: Configuration,
                                        batchSink: TableBatchWriteSession,
                                        odpsPartitionSpec: PartitionSpec,
                                        options: Map[String, String],
                                        odpsOptions: OdpsOptions,
                                        supportArrowWriter: Boolean): WriteJobDescription = {
    val outputColumns = toAttributes(schema)
    val outputPartitionColumns =
      outputColumns.filter(c => partitionSchema.getFieldIndex(c.name).isDefined)
    val outputPartitionSet = AttributeSet(outputPartitionColumns)
    val dataColumns = outputColumns.filterNot(outputPartitionSet.contains)
    val numDynamicPartitionFields = partitionSchema.length - odpsPartitionSpec.keys().size
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val statsTracker = new OdpsWriteJobStatsTracker(metrics)

    new WriteJobDescription(
      serializableHadoopConf = serializableHadoopConf,
      batchSink = batchSink,
      staticPartition = odpsPartitionSpec,
      allColumns = outputColumns,
      dataColumns = dataColumns,
      partitionColumns = outputPartitionColumns,
      dynamicPartitionColumns = outputPartitionColumns.takeRight(numDynamicPartitionFields),
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      statsTrackers = Seq(statsTracker),
      writeBatchSize = odpsOptions.columnarWriterBatchSize,
      timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone,
      supportArrowWriter = supportArrowWriter,
      enableArrowExtension = odpsOptions.enableArrowExtension,
      compressionCodec = odpsOptions.odpsTableCompressionCodec,
      chunkSize = odpsOptions.writerChunkSize,
      maxRetries = odpsOptions.writerMaxRetires,
      maxSleepIntervalMs = odpsOptions.maxRetrySleepIntervalMs,
      maxBlocks = odpsOptions.writerMaxBlocks
    )
  }

}
