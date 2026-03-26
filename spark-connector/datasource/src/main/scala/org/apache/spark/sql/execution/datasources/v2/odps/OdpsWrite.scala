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
import com.aliyun.odps.table.configuration.{ArrowOptions, DynamicPartitionOptions}
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit
import com.aliyun.odps.table.write.{TableBatchWriteSession, TableWriteCapabilities, TableWriteSessionBuilder}
import com.aliyun.odps.table.{DataFormat, TableIdentifier}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, RequiresDistributionAndOrdering, Write}
import org.apache.spark.sql.execution.datasources.v2.odps.OdpsTableType.{EXTERNAL_TABLE, VIRTUAL_VIEW}
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, DataSource}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.odps.{OdpsAnalysisException, OdpsClient, OdpsUtils, OdpsWriteJobStatsTracker, WriteJobDescription}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

import scala.jdk.CollectionConverters._

case class OdpsSinkInfo(cachedKey: String,
                        sinkBuilder: TableWriteSessionBuilder,
                        odpsStaticPartition: PartitionSpec)

case class OdpsWrite(
                        catalog: OdpsTableCatalog,
                        catalogTable: OdpsTable,
                        tableIdent: Identifier,
                        metaSchema: StructType,
                        dataSchema: StructType,
                        partitionSchema: StructType,
                        odpsOptions: OdpsOptions,
                        bucketSpec: Option[OdpsBucketSpec],
                        info: LogicalWriteInfo,
                        overwrite: Boolean
                    ) extends Write with RequiresDistributionAndOrdering {

  private val project = tableIdent.namespace().head
  private val table = tableIdent.name()
  private val odpsSchema = if (catalog.odpsOptions.enableNamespaceSchema) tableIdent.namespace().last else "default"
  private val queryId = info.queryId()
  private val schema = info.schema()
  private val options = info.options()
  private val sparkSession = SparkSession.active
  private val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options.asCaseSensitiveMap.asScala.toMap)

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
          // For dynamic partition write
          val dynamicPartitionCols =
            options.getOrDefault("writeOdpsDynamicPartitionColumns", "")
          if (dynamicPartitionCols.nonEmpty) {
            val colSet = dynamicPartitionCols.split(",").toSet
            partitionSchema.filter(partitionColumn => colSet.contains(partitionColumn.name))
              .map(partitionColumn => LogicalExpressions.sort(
                FieldReference(partitionColumn.name),
                SortDirection.ASCENDING,
                SortDirection.ASCENDING.defaultNullOrdering())).toArray
          } else {
            Array.empty
          }
        } else {
          Array.empty
        }
    }
  }

  private def toSortDirection(order: String): SortDirection = order match {
    case "ASC" => SortDirection.ASCENDING
    case "DESC" => SortDirection.DESCENDING
    case _ => throw new IllegalArgumentException(s"Unknown Odps Sort Direction: $order")
  }

  override def requiredNumPartitions(): Int = {
    bucketSpec match {
      case Some(OdpsBucketSpec("range", numBuckets, _, _)) => numBuckets
      case _ => 0
    }
  }

  override def toBatch: BatchWrite = {
    val sinkInfo = createOdpsSinkInfo()
    val batchSink = OdpsUtils.retryOnSpecificError(3, "Unexpected end of file from server") {
      () => sinkInfo.sinkBuilder.buildBatchWriteSession
    }
    val description = createWriteJobDescription(sparkSession,
      hadoopConf,
      batchSink,
      sinkInfo.odpsStaticPartition,
      options.asScala.toMap,
      odpsOptions,
      batchSink.supportsDataFormat(new DataFormat(DataFormat.Type.ARROW, DataFormat.Version.V5)))
    OdpsBatchWrite(catalog, tableIdent, batchSink, description, overwrite)
  }

  override def toStreaming: StreamingWrite = {
    val sinkInfo = createOdpsSinkInfo()
    val description = createWriteJobDescription(sparkSession,
      hadoopConf,
      null,
      sinkInfo.odpsStaticPartition,
      options.asScala.toMap,
      odpsOptions,
      supportArrowWriter = true)
    OdpsStreamingWrite(catalog, tableIdent, sinkInfo.cachedKey, sinkInfo.sinkBuilder, description, overwrite)
  }

  private def validateInputs(sqlConf: SQLConf): Unit = {
    val caseSensitiveAnalysis = sqlConf.caseSensitiveAnalysis
    assert(schema != null, "Missing input data schema")
    assert(queryId != null, "Missing query ID")

    SchemaUtils.checkColumnNameDuplication(schema.fields.map(_.name), caseSensitiveAnalysis)
    DataSource.validateSchema("ODPS", schema, sqlConf)
  }

  private def createOdpsSinkInfo(): OdpsSinkInfo = {
    validateInputs(sparkSession.sessionState.conf)
    catalogTable.tableType match {
      case EXTERNAL_TABLE | VIRTUAL_VIEW =>
        throw new OdpsAnalysisException(s"Cannot write odps table of ${catalogTable.tableType.name} type")
      case _ =>
    }

    val settings = OdpsClient.get.getEnvironmentSettings
    val provider = catalog.odpsOptions.tableWriteProvider
    val enhanceWriteCheck = catalog.odpsOptions.enhanceWriteCheck

    val arrowOptions = ArrowOptions.newBuilder()
      .withDatetimeUnit(TimestampUnit.MILLI)
      .withTimestampUnit(TimestampUnit.MICRO).build()

    val dynamicPartitionOptions = DynamicPartitionOptions.newBuilder()
      .withDynamicPartitionLimit(catalog.odpsOptions.dynamicPartitionLimit)
      .build()

    val writeCapabilities = TableWriteCapabilities.newBuilder()
      .supportDynamicPartition(true)
      .supportHashBuckets(true)
      .supportRangeBuckets(true)
      .build()

    val sinkBuilder = new TableWriteSessionBuilder()
      .identifier(TableIdentifier.of(project, odpsSchema, table))
      .withArrowOptions(arrowOptions)
      .withDynamicPartitionOptions(dynamicPartitionOptions)
      .withCapabilities(writeCapabilities)
      .withSettings(settings)
      .overwrite(overwrite)
      .withSessionProvider(provider)
      .withWriteCheck(enhanceWriteCheck)

    val odpsStaticPartition = new PartitionSpec

    if (partitionSchema.nonEmpty) {
      val partitionSpecValue = options.getOrDefault("writeOdpsStaticPartition", "")

      if (partitionSpecValue.nonEmpty) {
        val groups = partitionSpecValue.split(",")
        groups.foreach { group =>
          val kv = group.split("=")
          if (kv.length != 2) {
            throw new IllegalArgumentException("Invalid partition spec.")
          }
          if (kv(0).isEmpty || kv(1).isEmpty) {
            throw new IllegalArgumentException("Invalid partition spec.")
          }
          odpsStaticPartition.set(kv(0), kv(1))
        }
        sinkBuilder.partition(odpsStaticPartition)
      }
    }

    if (catalog.odpsOptions.maxFieldSizeInMB > 0) {
      sinkBuilder.withMaxFieldSize(catalog.odpsOptions.maxFieldSizeInMB * 1024 * 1024L)
    }

    val key = s"$project|$schema|$table|${odpsStaticPartition.toString}"
    OdpsSinkInfo(key, sinkBuilder, odpsStaticPartition)
  }

  private def createWriteJobDescription(sparkSession: SparkSession,
                                        hadoopConf: Configuration,
                                        batchSink: TableBatchWriteSession,
                                        odpsPartitionSpec: PartitionSpec,
                                        options: Map[String, String],
                                        odpsOptions: OdpsOptions,
                                        supportArrowWriter: Boolean): WriteJobDescription = {
    val outputColumns = toAttributes(schema)
    val outputMetaColumns =
      outputColumns.filter(c => metaSchema.getFieldIndex(c.name).isDefined)
    val outputMetaSet = AttributeSet(outputMetaColumns)
    val outputPartitionColumns =
      outputColumns.filter(c => partitionSchema.getFieldIndex(c.name).isDefined)
    val outputPartitionSet = AttributeSet(outputPartitionColumns)
    val dataColumns = outputColumns.filterNot(c => outputMetaSet.contains(c) || outputPartitionSet.contains(c))

    val primaryKeyColumns: Seq[Attribute] = if (catalogTable.isTransactional) {
      val nameToField = outputColumns.map(f => f.name -> f).toMap
      bucketSpec.get.bucketColumnNames.map(name => nameToField.getOrElse(name,
        throw new IllegalArgumentException(
          s"$name does not exist. Available: ${nameToField.keySet.mkString(", ")}")))
    } else {
      Seq.empty
    }

    val numDynamicPartitionFields = partitionSchema.length - odpsPartitionSpec.keys().size
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val statsTracker = new OdpsWriteJobStatsTracker(metrics)

    val dropDuplicates = catalogTable.isTransactional && (overwrite || odpsOptions.forceDeduplicate)
    new WriteJobDescription(
      serializableHadoopConf = serializableHadoopConf,
      batchSink = batchSink,
      staticPartition = odpsPartitionSpec,
      allColumns = outputColumns,
      metaColumns = outputMetaColumns,
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
      maxBlocks = odpsOptions.writerMaxBlocks,
      dropDuplicates = dropDuplicates,
      primaryKeyColumns = primaryKeyColumns
    )
  }
}