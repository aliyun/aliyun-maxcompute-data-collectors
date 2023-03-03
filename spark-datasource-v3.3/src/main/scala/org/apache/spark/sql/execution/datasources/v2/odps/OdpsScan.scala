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

import com.aliyun.odps.table.TableIdentifier
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit
import com.aliyun.odps.table.configuration.{ArrowOptions, SplitOptions}
import com.aliyun.odps.table.read.TableReadSessionBuilder

import scala.collection.JavaConverters._
import com.aliyun.odps.{OdpsException, PartitionSpec}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkContext
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.sql.odps.bucket.OdpsDefaultHasher
import org.apache.spark.sql.odps.OdpsClient
import org.apache.spark.sql.execution.datasources.v2.odps.expressions.OdpsHashFunction
import org.apache.spark.sql.execution.datasources.v2.odps.OdpsTableType.VIRTUAL_VIEW
import org.apache.spark.sql.sources.EqualTo

import scala.collection.mutable


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
                     partitionFilters: Array[Filter],
                     bucketFilters: Array[Filter],
                     stats: OdpsStatistics)
  extends Scan
    with Batch
    with SupportsReportStatistics
    with SupportsMetadata
    with Logging {

  override def readSchema(): StructType =
    StructType(readDataSchema.fields ++ readPartitionSchema.fields)

  override def toBatch: Batch = this

  private lazy val partitions = createPartitions()

  private def createPartitions(): Array[InputPartition] = {
    catalogTable.tableType match {
      case VIRTUAL_VIEW =>
        throw new AnalysisException(s"Cannot read odps table of ${catalogTable.tableType.name} type")
      case _ =>
    }
    val project = tableIdent.namespace().head
    val table = tableIdent.name()

    val settings = OdpsClient.get.getEnvironmentSettings
    val provider = catalog.odpsOptions.tableReadProvider

    val requiredDataSchema = readDataSchema.map(attr => attr.name).asJava
    val requiredPartitionSchema = readPartitionSchema.map(attr => attr.name).asJava
    val emptyColumn =
      if (requiredDataSchema.isEmpty && requiredPartitionSchema.isEmpty) true else false

    val scanBuilder = new TableReadSessionBuilder()
      .identifier(TableIdentifier.of(project, table))
      .requiredDataColumns(requiredDataSchema)
      .requiredPartitionColumns(requiredPartitionSchema)
      .withSettings(settings)
      .withSessionProvider(provider)

    val bucketIds: Seq[Integer] =
      if (bucketFilters.nonEmpty) {
        try {
          val hashVals = bucketFilters.map { predicate =>
            val dataType = dataSchema.fields(dataSchema.fieldIndex(predicate.asInstanceOf[EqualTo].attribute)).dataType
            OdpsHashFunction.hash(predicate.asInstanceOf[EqualTo].value, dataType, 0).toInt
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

    if (bucketIds.nonEmpty) {
      scanBuilder.requiredBucketIds(bucketIds.asJava)
    }

    if (partitionSchema.nonEmpty) {
      val allowFullScan = catalog.loadNamespaceMetadata(tableIdent.namespace())
        .getOrDefault("odps.sql.allow.fullscan", "true").toBoolean
      if (!allowFullScan && partitionFilters.isEmpty) {
        throw new OdpsException(s"odps.sql.allow.fullscan is $allowFullScan")
      }

      val prunedPartitions = catalog.listPartitionsByFilter(tableIdent, partitionFilters)
      logInfo(s"prunedPartitions: ${seqToString(prunedPartitions)}")
      if (prunedPartitions.isEmpty) {
        return Array.empty
      }

      scanBuilder.requiredPartitions(prunedPartitions.map(partition => {
        val staticPartition = new mutable.LinkedHashMap[String, String]
        partitionSchema.foreach { attr =>
          staticPartition.put(attr.name, partition.getOrElse(attr.name,
            throw new IllegalArgumentException(s"Partition spec is missing a value for column '$attr.name': $partition")))
        }
        new PartitionSpec(staticPartition.map {
          case (key, value) => key + "=" + value
        }.mkString(","))
      }).toList.asJava)
    }

    val splitOptions = if (!emptyColumn) {
      if (catalog.odpsOptions.splitParallelism > 0) {
        SplitOptions.newBuilder().SplitByParallelism(catalog.odpsOptions.splitParallelism).build()
      } else {
        val rawSizePerCore = ((stats.getSizeInBytes / 1024 / 1024) /
          SparkContext.getActive.get.defaultParallelism) + 1
        val sizePerCore = math.min(rawSizePerCore, Int.MaxValue).toInt
        val splitSizeInMB = math.min(catalog.odpsOptions.splitSizeInMB, sizePerCore)
        SplitOptions.newBuilder().SplitByByteSize(splitSizeInMB * 1024L * 1024L).build()
      }
    } else {
      SplitOptions.newBuilder().SplitByRowOffset().build()
    }

    scanBuilder.withSplitOptions(splitOptions)
      .withArrowOptions(ArrowOptions.newBuilder()
        .withDatetimeUnit(TimestampUnit.MILLI)
        .withTimestampUnit(TimestampUnit.MICRO).build())

    val scan = scanBuilder.buildBatchReadSession

    logInfo(s"Create table scan ${scan.getId} for ${scan.getTableIdentifier}")
    if (!emptyColumn) {
      scan.getInputSplitAssigner.getAllSplits
        .map(split => OdpsScanPartition(split, scan))
    } else {
      Array(OdpsEmptyColumnPartition(scan.getInputSplitAssigner.getTotalRowCount))
    }
  }

  override def planInputPartitions(): Array[InputPartition] = partitions

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    OdpsPartitionReaderFactory(
      broadcastedConf,
      readDataSchema, readPartitionSchema,
      catalog.odpsOptions.enableVectorizedReader,
      catalog.odpsOptions.columnarReaderBatchSize,
      catalog.odpsOptions.enableReuseBatch,
      catalog.odpsOptions.odpsTableReaderCompressionCodec)
  }

  override def estimateStatistics(): Statistics = stats

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
      "DataFilters" -> seqToString(Nil))
  }

  private def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")
}
