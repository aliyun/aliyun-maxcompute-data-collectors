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

package org.apache.spark.sql.odps

import com.aliyun.odps.Column
import com.aliyun.odps.data.ArrayRecord
import com.aliyun.odps.table.configuration.{CompressionCodec, WriterOptions}
import com.aliyun.odps.table.metrics.MetricNames
import com.aliyun.odps.table.metrics.count.{BytesCount, RecordCount}
import com.aliyun.odps.table.write.{BatchWriter, TableBatchWriteSession, WriterAttemptId, WriterCommitMessage => OdpsWriterCommitMessage}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, UnsafeProjection}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats}
import org.apache.spark.sql.odps.execution.vectorized.ArrowBatchWriter
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.{SerializableConfiguration, Utils}

import scala.collection.mutable
import scala.reflect.ClassTag

abstract class OdpsTableDataWriter[T: ClassTag](description: WriteJobDescription)
  extends org.apache.spark.sql.connector.write.DataWriter[InternalRow] {

  protected var commitMessages: mutable.Seq[OdpsWriterCommitMessage] =
    mutable.Seq[OdpsWriterCommitMessage]()

  protected var currentWriter: BatchWriter[T] = _

  protected val settings = OdpsClient.builder.config(description.serializableHadoopConf.value).getOrCreate.getEnvironmentSettings

  /** Trackers for computing various statistics on the data as it's being written out. */
  protected val statsTrackers: Seq[OdpsWriteTaskStatsTracker] =
    description.statsTrackers.map(_.newTaskInstance().asInstanceOf[OdpsWriteTaskStatsTracker])

  protected val writeSchema: Array[Column] = description.batchSink.requiredSchema().getColumns.toArray(Array.empty[Column])

  protected var arrowBatchWriter: ArrowBatchWriter = _

  protected val codec = CompressionCodec.byName(description.compressionCodec).orElse(CompressionCodec.NO_COMPRESSION)

  protected def commitFile(): Unit = {
    if (currentWriter != null) {
      if (arrowBatchWriter != null) {
        arrowBatchWriter.writeBatch(currentWriter.asInstanceOf[BatchWriter[VectorSchemaRoot]])
      }
      currentWriter.close()
      commitMessages :+= currentWriter.commit

      val bytesWritten = currentWriter.currentMetricsValues
        .counter(MetricNames.BYTES_COUNT).orElse(new BytesCount).getCount
      val rowsWritten = currentWriter.currentMetricsValues
        .counter(MetricNames.RECORD_COUNT).orElse(new RecordCount).getCount

      statsTrackers.foreach(_.newFile(bytesWritten, rowsWritten))
      currentWriter = null
    }
  }

  protected def releaseResources(): Unit = {
    if (currentWriter != null) {
      try {
        currentWriter.close()
      } finally {
        currentWriter = null
      }
    }
  }

  protected def createFileWriter(writeId: Long): BatchWriter[T]

  protected def processRow(row: InternalRow): Unit

  /** Writes a row */
  def write(row: InternalRow): Unit

  override def commit(): WriteTaskResult = {
    val (_, taskCommitTime) = Utils.timeTakenMs {
      commitFile()
    }
    releaseResources()
    WriteTaskResult(commitMessages, statsTrackers.map(_.getFinalStats(taskCommitTime)))
  }

  def abort(): Unit = {
    releaseResources()
  }

  override def close(): Unit = {}
}

/** Writes data to a single directory (used for non-dynamic-partition writes). */
class SingleDirectoryArrowWriter(description: WriteJobDescription,
                                 partitionId: Int,
                                 attemptNumber: Int)
  extends OdpsTableDataWriter[VectorSchemaRoot](description) {

  newFileWriter()

  protected def newFileWriter(): Unit = {
    currentWriter = createFileWriter(partitionId)
    arrowBatchWriter = new ArrowBatchWriter(
      writeSchema,
      currentWriter.newElement(),
      description.writeBatchSize)
  }

  override def write(row: InternalRow): Unit = {
    processRow(row)
  }

  override protected def createFileWriter(writeId: Long)
  : BatchWriter[VectorSchemaRoot] = {
    description.batchSink.createArrowWriter(writeId, WriterAttemptId.of(attemptNumber),
      WriterOptions.newBuilder()
        .withBufferedRowCount(description.writeBatchSize.asInstanceOf[Int])
        .withSettings(settings)
        .withCompressionCodec(codec)
        .build())
  }

  override protected def processRow(row: InternalRow): Unit = {
    if (arrowBatchWriter.isFull()) {
      arrowBatchWriter.writeBatch(currentWriter)
    }
    arrowBatchWriter.insertRecord(row)
  }
}

class SingleDirectoryRecordWriter(description: WriteJobDescription,
                                  partitionId: Int,
                                  attemptNumber: Int)
  extends OdpsTableDataWriter[ArrayRecord](description) {

  /** for record writer */
  private var dataTypes: Array[DataType] = _
  private var converters: Array[Object => AnyRef] = _
  private var arrayRecord: ArrayRecord = _

  newFileWriter()

  initArrayRecord(currentWriter)

  protected def initArrayRecord(writer: BatchWriter[ArrayRecord]): Unit = {
    dataTypes = description.dataColumns.map(_.dataType).toArray
    converters = writeSchema.map(c => OdpsUtils.sparkData2OdpsData(c.getTypeInfo))
    arrayRecord = writer.newElement()
  }

  protected final def transform(row: InternalRow): ArrayRecord = {
    var i = 0
    while (i < converters.length) {
      val value = if (row.isNullAt(i)) {
        null
      } else {
        converters(i)(row.get(i, dataTypes(i)))
      }
      arrayRecord.set(i, value)
      i += 1
    }
    arrayRecord
  }

  protected def newFileWriter(): Unit = {
    currentWriter = createFileWriter(partitionId)
  }

  override protected def processRow(row: InternalRow): Unit = {
    currentWriter.write(transform(row))
  }

  override protected def createFileWriter(writeId: Long)
  : BatchWriter[ArrayRecord] = {
    description.batchSink.createRecordWriter(writeId, WriterAttemptId.of(attemptNumber),
      WriterOptions.newBuilder()
        .withBufferedRowCount(description.writeBatchSize.asInstanceOf[Int])
        .withSettings(settings)
        .build())
  }

  /** Writes a row */
  override def write(row: InternalRow): Unit = {
    processRow(row)
  }
}

/**
 * Writes data to using dynamic partition writes, meaning this single function can write to
 * multiple directories (partitions) or files (bucketing).
 */
final class DynamicPartitionArrowWriter(description: WriteJobDescription,
                                        partitionId: Int,
                                        attemptNumber: Int)
  extends SingleDirectoryArrowWriter(description, partitionId, attemptNumber) {

  /** Flag saying whether or not the data to be written out is partitioned. */
  private val isPartitioned = description.dynamicPartitionColumns.nonEmpty

  assert(isPartitioned,
    s"""DynamicPartitionWriteTask should be used for writing out data that's partitioned.
       |In this case neither is true.
       |WriteJobDescription: $description
       """.stripMargin)

  /** Writes a row */
  override def write(row: InternalRow): Unit = {
    // ensureInitialized(row)
    processRow(getOutputRowWithDynamicPartition(row))
  }

  /** Returns the data columns to be written given an input row */
  private val getOutputRowWithDynamicPartition =
    UnsafeProjection.create(description.dataColumns ++ description.dynamicPartitionColumns,
      description.allColumns)
}

/**
 * TODO: DynamicRecordWriter, now mode not support write dynamic partition
 */

/** A shared job description for all the write tasks. */
class WriteJobDescription(
                           val serializableHadoopConf: SerializableConfiguration,
                           val batchSink: TableBatchWriteSession,
                           val staticPartitions: TablePartitionSpec,
                           val allColumns: Seq[Attribute],
                           val dataColumns: Seq[Attribute],
                           val dynamicPartitionColumns: Seq[Attribute],
                           val maxRecordsPerFile: Long,
                           val statsTrackers: Seq[WriteJobStatsTracker],
                           val writeBatchSize: Long,
                           val timeZoneId: String,
                           val supportArrowWriter: Boolean,
                           val enableArrowExtension: Boolean,
                           val compressionCodec: String)
  extends Serializable {

  assert(AttributeSet(allColumns) == AttributeSet(dynamicPartitionColumns ++ dataColumns),
    s"""
       |All columns: ${allColumns.mkString(", ")}
       |Partition columns: ${dynamicPartitionColumns.mkString(", ")}
       |Data columns: ${dataColumns.mkString(", ")}
       """.stripMargin)
}

/** The result of a successful write task. */
case class WriteTaskResult(commitMessage: Seq[OdpsWriterCommitMessage],
                           stats: Seq[WriteTaskStats])
  extends WriterCommitMessage