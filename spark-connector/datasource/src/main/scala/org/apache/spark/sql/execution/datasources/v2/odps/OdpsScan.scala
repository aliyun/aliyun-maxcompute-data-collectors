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

import java.util.OptionalLong
import com.aliyun.odps.table.TableIdentifier
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit
import com.aliyun.odps.table.configuration.{ArrowOptions, SplitOptions}
import com.aliyun.odps.table.optimizer.predicate.Predicate
import com.aliyun.odps.table.read.split.InputSplit
import com.aliyun.odps.table.read.{TableBatchReadSession, TableReadSessionBuilder}

import com.aliyun.odps.PartitionSpec
import scala.jdk.CollectionConverters._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportStatistics, SupportsRuntimeFiltering}
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DecimalType, IntegerType, StructField, StructType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils, Utils}
import org.apache.spark.sql.odps.bucket.OdpsDefaultHasher
import org.apache.spark.sql.odps.catalyst.expressions.OdpsHashFunction
import org.apache.spark.sql.odps.{ExecutionUtils, OdpsUtils, OdpsAnalysisException, OdpsClient, OdpsEmptyColumnPartition, OdpsPartitionReaderFactory, OdpsScanPartition}
import org.apache.spark.sql.execution.datasources.v2.odps.OdpsTableType.VIRTUAL_VIEW
import org.apache.spark.sql.sources.EqualTo

import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

case class OdpsScan(
                     sparkSession: SparkSession,
                     hadoopConf: Configuration,
                     catalog: OdpsTableCatalog,
                     catalogTable: OdpsTable,
                     tableIdent: Identifier,
                     dataSchema: StructType,
                     partitionSchema: StructType,
                     readDataSchema: StructType,
                     readPartitionSchema: StructType,
                     var dataFilters: Array[Filter],
                     var partitionFilters: Array[Filter],
                     var bucketFilters: Array[Filter],
                     stats: OdpsStatistics)
  extends Scan
    with Batch
    with SupportsReportStatistics
    with SupportsRuntimeFiltering
    with SupportsMetadata
    with Logging {

  override def readSchema(): StructType = {
    val metadataFields = Array.empty[StructField]
    StructType(metadataFields ++ readDataSchema.fields ++ readPartitionSchema.fields)
  }

  override def toBatch: Batch = this

  // RuntimeFiltering needs to execute this multi times to plan new input partitions
  var partitions: Array[InputPartition] = _

  private var runtimeFilter: Boolean = false

  private lazy val driverMetrics: mutable.HashMap[String, Long] = mutable.HashMap.empty

  private def createPartitions(): Array[InputPartition] = {
    var prunedPartitions: Array[TablePartitionSpec] = Array.empty
    if (partitionSchema.nonEmpty) {
      val startTime = System.nanoTime()
      prunedPartitions = catalog.listPartitionSpecByFilter(tableIdent, partitionFilters)
      val timeTakenMs = NANOSECONDS.toMillis(System.nanoTime() - startTime)
      driverMetrics(OdpsMetrics.PRUNING_TIME) = timeTakenMs
      logInfo(s"prunedPartitions: ${seqToString(prunedPartitions)}")
      if (prunedPartitions.isEmpty) {
        return Array.empty
      }
    }

    val emptyColumn = readSchema().isEmpty

    val bucketIds: Seq[Integer] =
      if (bucketFilters.nonEmpty) {
        try {
          val hashVals = bucketFilters.map { predicate =>
            val structField = dataSchema.fields(dataSchema.fieldIndex(predicate.asInstanceOf[EqualTo].attribute))
            val isOdpsDateTime = structField.metadata.contains(OdpsUtils.DATETIME_TYPE_STRING_METADATA_KEY)
            val dataType = structField.dataType
            OdpsHashFunction.hash(predicate.asInstanceOf[EqualTo].value, dataType, 0, isOdpsDateTime).toInt
          }
          val bucketId =
            OdpsDefaultHasher.CombineHashVal(hashVals) % catalogTable.bucketSpec.get.numBuckets
          Seq(bucketId)
        } catch {
          case e: Exception =>
            logInfo("unsupported type, fallback to non-bucketing", e)
            Nil
        }
      } else {
        Nil
      }

    val startTime = System.nanoTime()
    val splits: Array[InputPartition] = if (!emptyColumn) {
      val splitByRowOffset = catalogTable.isAppend2Table

      val predicate = if (catalog.odpsOptions.filterPushDown) {
        val hasUnsupportedType = dataSchema.fields.exists {
          case field if field.dataType.isInstanceOf[DecimalType] =>
            field.dataType.asInstanceOf[DecimalType].scale > 18
          case _ => false
        }
        if (hasUnsupportedType) {
          logWarning(s"Unsupported filter push down for decimal type with scale > 18")
          Predicate.NO_PREDICATE
        } else {
          ExecutionUtils.convertToOdpsPredicate(dataFilters)
        }
      } else {
        Predicate.NO_PREDICATE
      }
      logInfo(s"Try to push down predicate $predicate")

      if (partitionSchema.nonEmpty) {
        if (catalog.odpsOptions.enableSplitSession) {
          val partSplits = collection.mutable.Map[Int, ArrayBuffer[TablePartitionSpec]]()
          val splitPar = catalog.odpsOptions.splitSessionParallelism
          val concurrentNum = Math.min(Math.max(splitPar, prunedPartitions.length / 200), 16)

          prunedPartitions.zipWithIndex.foreach {
            case (x, i) =>
              val key = if (concurrentNum == 1) 1 else i % concurrentNum
              partSplits.getOrElse(key, {
                val pList = ArrayBuffer[TablePartitionSpec]()
                partSplits.put(key, pList)
                pList
              }) += x
          }

          import OdpsScan._

          val future = Future.sequence(partSplits.keys.map(key =>
            Future[Array[InputPartition]] {
              val scan = createTableScan(splitByRowOffset, predicate, bucketIds, partSplits(key).toArray)
              getInputPartitions(scan, splitByRowOffset)
            }(executionContext)
          ))
          val maxWaitTime = catalog.odpsOptions.splitMaxWaitTime
          val futureResults = ThreadUtils.awaitResult(future, Duration(maxWaitTime, MINUTES))
          futureResults.flatten.toArray
        } else {
          val scan = createTableScan(splitByRowOffset, predicate, bucketIds, prunedPartitions)
          getInputPartitions(scan, splitByRowOffset)
        }
      } else {
        val scan = createTableScan(splitByRowOffset, predicate, bucketIds, Array.empty)
        getInputPartitions(scan, splitByRowOffset)
      }
    } else {
      val scan = if (partitionSchema.nonEmpty) {
        createTableScan(splitByRowOffset = true, Predicate.NO_PREDICATE, bucketIds, prunedPartitions)
      } else {
        createTableScan(splitByRowOffset = true, Predicate.NO_PREDICATE, bucketIds, Array.empty)
      }
      Array(OdpsEmptyColumnPartition(scan.getInputSplitAssigner.getTotalRowCount))
    }
    val timeTakenMs = NANOSECONDS.toMillis(System.nanoTime() - startTime)
    driverMetrics(OdpsMetrics.SPLIT_DATA_TIME) = timeTakenMs
    driverMetrics(OdpsMetrics.NUM_SPLITS) = splits.length
    splits
  }

  private def getInputPartitions(scan: TableBatchReadSession,
                                 splitByRowOffset: Boolean): Array[InputPartition] = {
    if (!splitByRowOffset) {
      logInfo(s"AllSplits Length: ${scan.getInputSplitAssigner.getAllSplits.length}")
      scan.getInputSplitAssigner.getAllSplits.grouped(catalog.odpsOptions.splitReaderNum).map(
        split => OdpsScanPartition(split, scan)
      ).toArray
    } else {
      val recordCount: Long = scan.getInputSplitAssigner.getTotalRowCount
      val splits = scala.collection.mutable.ListBuffer[InputSplit]()

      var numRecordPerSplit: Long = recordCount / SparkContext.getActive.get.defaultParallelism
      if (numRecordPerSplit == 0) {
        numRecordPerSplit = 1
      }

      val numSplits: Long = recordCount / numRecordPerSplit
      val remainder: Long = recordCount % numRecordPerSplit
      for (i <- 0L until numSplits) {
        val startIndex: Long = i * numRecordPerSplit
        val split = scan.getInputSplitAssigner.getSplitByRowOffset(startIndex, numRecordPerSplit)
        splits += split
      }
      if (remainder != 0) {
        val startIndex: Long = numSplits * numRecordPerSplit
        val lastSplit = scan.getInputSplitAssigner.getSplitByRowOffset(startIndex, remainder)
        splits += lastSplit
      }
      splits.map(split => OdpsScanPartition(Array(split), scan)).toArray
    }
  }

  private def createTableScan(splitByRowOffset: Boolean,
                              predicate: Predicate,
                              bucketIds: Seq[Integer],
                              selectedPartitions: Array[TablePartitionSpec]): TableBatchReadSession = {
    val provider = catalog.odpsOptions.tableReadProvider
    val project = catalogTable.tableIdent.namespace.head
    val schema: String = if (catalog.odpsOptions.enableNamespaceSchema) catalogTable.tableIdent.namespace.last else "default"
    val table = catalogTable.tableIdent.name

    var tableId: TableIdentifier = TableIdentifier.of(project, schema, table)
    catalogTable.tableType match {
      case VIRTUAL_VIEW =>
        if (!OdpsUtils.odpsMaterializeViewToTableEnabled(
          SparkSession.active.sessionState.conf)) {
          throw new OdpsAnalysisException(s"Cannot read odps table of ${catalogTable.tableType.name} type")
        }
        tableId = materializeViewToTable()
      case _ =>
    }

    val settings = OdpsClient.get.getEnvironmentSettings

    val requiredDataSchema = readDataSchema.map(attr => attr.name).asJava
    val requiredPartitionSchema = readPartitionSchema.map(attr => attr.name).asJava

    val scanBuilder = new TableReadSessionBuilder()
      .identifier(tableId)
      .requiredDataColumns(requiredDataSchema)
      .requiredPartitionColumns(requiredPartitionSchema)
      .withSettings(settings)
      .withSessionProvider(provider)

    if (bucketIds.nonEmpty) {
      scanBuilder.requiredBucketIds(bucketIds.asJava)
    }

    if (partitionSchema.nonEmpty) {
      scanBuilder.requiredPartitions(selectedPartitions.map(partition => {
        val staticPartition = new mutable.LinkedHashMap[String, String]
        partitionSchema.foreach { attr =>
          staticPartition.put(attr.name, partition.getOrElse(attr.name,
            throw new IllegalArgumentException(s"Partition spec is missing a value for column '$attr.name': $partition")))
        }
        val p = new PartitionSpec()
        staticPartition.foreach {
          case (key, value) => p.set(key, value)
        }
        p
      }).toList.asJava)
    }

    val splitOptionsBuilder = if (!splitByRowOffset) {
      if (catalog.odpsOptions.splitParallelism > 0) {
        SplitOptions.newBuilder().SplitByParallelism(catalog.odpsOptions.splitParallelism)
      } else {
        val rawSizePerCore = ((stats.getSizeInBytes / 1024 / 1024) /
          SparkContext.getActive.get.defaultParallelism) + 1
        val sizePerCore = math.max(math.min(rawSizePerCore, Int.MaxValue).toInt, 10)
        val readerNum = math.max(catalog.odpsOptions.splitReaderNum, 1)
        val actualSplitSizeInMB = math.min(catalog.odpsOptions.splitSizeInMB, sizePerCore) / readerNum + (if (readerNum == 1) 0 else 1)
        val splitSizeInBytes = math.max(actualSplitSizeInMB, 10) * 1024L * 1024L
        logInfo(s"Split size: ${splitSizeInBytes / 1024 / 1024}MB, readerNum: $readerNum, totalSize: ${stats.getSizeInBytes / 1024 / 1024}MB")
        driverMetrics(OdpsMetrics.SPLIT_SIZE) = splitSizeInBytes
        SplitOptions.newBuilder().SplitByByteSize(splitSizeInBytes)
      }
    } else {
      SplitOptions.newBuilder().SplitByRowOffset()
    }

    val odpsSplitMaxFileNum = catalog.odpsOptions.splitMaxFileNum
    val splitOptions = if (odpsSplitMaxFileNum > 0) {
      splitOptionsBuilder.withMaxFileNum(odpsSplitMaxFileNum).build()
    } else {
      splitOptionsBuilder.build()
    }

    scanBuilder.withSplitOptions(splitOptions)
      .withArrowOptions(ArrowOptions.newBuilder()
        .withDatetimeUnit(TimestampUnit.MILLI)
        .withTimestampUnit(TimestampUnit.MICRO).build())

    OdpsUtils.retryOnSpecificError(3, "Unexpected end of file from server") {
      () =>
        val s = scanBuilder.withFilterPredicate(predicate).buildBatchReadSession
        logInfo(s"Create table scan ${s.getId} for ${s.getTableIdentifier}")
        s
    }
  }

  override def planInputPartitions(): Array[InputPartition] = {
    if (partitions == null || runtimeFilter) {
      // runtimeFilter must create new partitions
      partitions = createPartitions()
      runtimeFilter = false
    }
    partitions
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    OdpsPartitionReaderFactory(
      broadcastedConf,
      readDataSchema,
      readPartitionSchema,
      catalog.odpsOptions.enableVectorizedReader,
      catalog.odpsOptions.columnarReaderBatchSize,
      catalog.odpsOptions.enableReuseBatch,
      catalog.odpsOptions.odpsTableCompressionCodec,
      catalog.odpsOptions.asyncReadEnable,
      catalog.odpsOptions.asyncReadQueueSize,
      catalog.odpsOptions.asyncReadWaitTime
    )
  }

  override def estimateStatistics(): Statistics = new Statistics {
    override def sizeInBytes(): OptionalLong = if (stats.sizeInBytes.isPresent) {
      val compressionFactor = sparkSession.sessionState.conf.fileCompressionFactor
      val size = (compressionFactor * stats.sizeInBytes.getAsLong /
        (dataSchema.defaultSize + partitionSchema.defaultSize) *
        (readDataSchema.defaultSize + readPartitionSchema.defaultSize)).toLong
      OptionalLong.of(size)
    } else {
      OptionalLong.empty()
    }

    override def numRows(): OptionalLong = stats.numRows
  }

  private val maxMetadataValueLength = sparkSession.sessionState.conf.maxMetadataStringLength

  override def description(): String = {
    val metadataStr = getMetaData().toSeq.sorted.map {
      case (key, value) =>
        val redactedValue =
          Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, value)
        key + ": " + StringUtils.abbreviate(redactedValue, maxMetadataValueLength)
    }.mkString(", ")
    s"${this.getClass.getSimpleName} ${tableIdent.namespace().head}.${tableIdent.name()} $metadataStr"
  }

  override def getMetaData(): Map[String, String] = {
    Map(
      "Format" -> "odps",
      "ReadDataSchema" -> readDataSchema.catalogString,
      "ReadPartitionSchema" -> readPartitionSchema.catalogString,
      "PartitionFilters" -> seqToString(partitionFilters),
      "DataFilters" -> seqToString(dataFilters))
  }

  override def supportedCustomMetrics: Array[CustomMetric] = {
    val scanMetrics: Array[CustomMetric] = Array(
      OdpsSplitDataTimeMetric(),
      OdpsSplitSizeMetric(),
      OdpsNumSplitsMetric(),
      OdpsPruningTimeMetric()
    )
    super.supportedCustomMetrics() ++ scanMetrics
  }

  override def reportDriverMetrics(): Array[CustomTaskMetric] = {
    Array(
      OdpsSplitDataTimeTaskMetric(driverMetrics.getOrElse(OdpsMetrics.SPLIT_DATA_TIME, 0L)),
      OdpsSplitSizeTaskMetric(driverMetrics.getOrElse(OdpsMetrics.SPLIT_SIZE, 0L)),
      OdpsNumSplitsTaskMetric(driverMetrics.getOrElse(OdpsMetrics.NUM_SPLITS, 0L)),
      OdpsPruningTimeTaskMetric(driverMetrics.getOrElse(OdpsMetrics.PRUNING_TIME, 0L)),
    )
  }

  private def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")

  override def filterAttributes(): Array[NamedReference] = {
    if (catalog.odpsOptions.enableRuntimeFilter) {
      (readDataSchema ++ readPartitionSchema).map(field => FieldReference(field.name)).toArray
    } else {
      Array.empty
    }
  }

  override def filter(filters: Array[Filter]): Unit = {
    runtimeFilter = true
    val (_, normalFilters) = filters.partition(_.v2references.exists(_.length > 1))

    val partitionSet = partitionSchema.map(_.name).toSet
    val (_partitionFilters, nonPartitionFilters) =
      normalFilters.partition(_.references.toSet.subsetOf(partitionSet))
    var _dataFilters = nonPartitionFilters.filter(_.references.toSet.intersect(partitionSet).isEmpty)

    if (catalogTable.bucketSpec.isDefined && catalogTable.bucketSpec.get.clusterType.toLowerCase.equals("hash")) {
      val (__bucketFilters, _) = _dataFilters.toSeq.partition {
        case EqualTo(attr, _) =>
          catalogTable.bucketSpec.get.bucketColumnNames.contains(attr)
        case _ => false
      }
      val _bucketFilters = OdpsScanBuilder.alignBucketPredicates(__bucketFilters.asInstanceOf[Seq[EqualTo]],
        catalogTable.bucketSpec).toArray
      bucketFilters = bucketFilters ++ _bucketFilters
      _dataFilters = _dataFilters.filterNot(f => _bucketFilters.contains(f))
    }
    partitionFilters = partitionFilters ++ _partitionFilters
    dataFilters = dataFilters ++ _dataFilters
  }

  override def equals(obj: Any): Boolean = obj match {
    case f: OdpsScan =>
      tableIdent == f.tableIdent && readSchema == f.readSchema && equivalentFilters(partitionFilters, f.partitionFilters)
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  // Returns whether the two given arrays of [[Filter]]s are equivalent.
  def equivalentFilters(a: Array[Filter], b: Array[Filter]): Boolean = {
    a.sortBy(_.hashCode()).sameElements(b.sortBy(_.hashCode()))
  }

  private def materializeViewToTable(): TableIdentifier = {
    val filters = ExecutionUtils.createOdpsSqlFilter(
      readSchema().fields.map(f => (quoteIfNeeded(f.name), f)).toMap, dataFilters,
        SparkSession.active.sessionState.conf.sessionLocalTimeZone)
    val query = createViewToTableSql(catalogTable.tableIdent, readSchema().fieldNames, filters)
    val table = catalog.getViewToTable(query)
    TableIdentifier.of(table.getProject, Option(table.getSchemaName).getOrElse("default"), table.getName)
  }

  private def createViewToTableSql(
                                    identifier: Identifier,
                                    selectedFields: Array[String],
                                    filters: Option[String]): String = {
    val tableName = identifier.asMultipartIdentifier.mkString(".")
    val columns: String = if (selectedFields.isEmpty) "*"
    else selectedFields.map(column => s"`$column`").mkString(",")
    val whereClause = filters.map("WHERE " + _).getOrElse("")
    String.format("SELECT %s FROM `%s` %s", columns, tableName, whereClause)
  }
}

object OdpsScan {
  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  private val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(16,
    namedThreadFactory("odps-scan")))
}
