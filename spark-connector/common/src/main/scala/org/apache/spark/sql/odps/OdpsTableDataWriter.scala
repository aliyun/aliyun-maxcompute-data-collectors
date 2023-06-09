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
import com.aliyun.odps.PartitionSpec
import com.aliyun.odps.data.ArrayRecord
import com.aliyun.odps.table.configuration.{CompressionCodec, WriterOptions}
import com.aliyun.odps.table.metrics.MetricNames
import com.aliyun.odps.table.metrics.count.{BytesCount, RecordCount}
import com.aliyun.odps.table.write.{BatchWriter, TableBatchWriteSession, WriterAttemptId, WriterCommitMessage => OdpsWriterCommitMessage}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, UnsafeProjection}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats}
import org.apache.spark.sql.odps.execution.vectorized.ArrowBatchWriter
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.{SerializableConfiguration, Utils}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

abstract class OdpsTableDataWriter[T: ClassTag](description: WriteJobDescription)
  extends org.apache.spark.sql.connector.write.DataWriter[InternalRow] with Logging {

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

  protected val maxRetries = description.maxRetries

  protected val maxSleepIntervalMs = description.maxSleepIntervalMs

  protected val chunkSize = description.chunkSize

  protected def createBatchWriter(): BatchWriter[T]

  protected def commitFile(): Unit = {
    if (currentWriter != null) {
      try {
        if (arrowBatchWriter != null) {
          arrowBatchWriter.writeBatch(currentWriter.asInstanceOf[BatchWriter[VectorSchemaRoot]], false)
        }
        currentWriter.close()
      } catch {
        case cause: Throwable =>
          currentWriter.abort()

          if (cause.getMessage.contains("FlowExceeded")) {
            if (arrowBatchWriter != null) {
              if (arrowBatchWriter.canResume()) {
                var waitTime = new Random().nextInt(maxSleepIntervalMs - 2000) + 2000
                var retries = 0
                var flushSuccess = false

                while (!flushSuccess && retries < maxRetries) {
                  retries = retries + 1
                  waitTime = waitTime + new Random().nextInt(maxSleepIntervalMs - 2000) + 2000

                  logInfo("Try to recreate batch writer, wait time: " + waitTime)

                  Thread.sleep(waitTime)
                  try {
                    currentWriter = createBatchWriter()
                    arrowBatchWriter.writeBatch(currentWriter.asInstanceOf[BatchWriter[VectorSchemaRoot]], true)
                    currentWriter.close()
                    flushSuccess = true
                  } catch {
                    case cause: Throwable =>
                      currentWriter.abort()
                      if (!cause.getMessage.contains("FlowExceeded")) {
                        throw cause
                      } else if (retries >= maxRetries) {
                        logError(s"Recreate batch writer exceeded the threshold")
                        throw cause
                      }
                  }
                }
              } else {
                logError("Arrow batch writer cannot resume")
                throw cause
              }
            } else {
              throw cause
            }
          } else {
            throw cause
          }
      } finally {
        if (arrowBatchWriter != null) {
          arrowBatchWriter.reset()
          arrowBatchWriter.close()

          arrowBatchWriter = null
        }
      }

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
    if (currentWriter != null) {
      try {
        currentWriter.abort()
      } finally {
        currentWriter = null
      }
    }

    if (arrowBatchWriter != null) {
      try {
        arrowBatchWriter.close()
      } finally {
        arrowBatchWriter = null
      }
    }
  }

  override def close(): Unit = {}
}

/** Writes data to a single directory (used for non-dynamic-partition writes). */
class SingleDirectoryArrowWriter(description: WriteJobDescription,
                                 partitionId: Int,
                                 attemptNumber: Int,
                                 taskId: Long)
  extends OdpsTableDataWriter[VectorSchemaRoot](description) {

  newFileWriter()

  protected def newFileWriter(): Unit = {
    currentWriter = createFileWriter(partitionId)
    arrowBatchWriter = new ArrowBatchWriter(
      writeSchema,
      currentWriter.newElement(),
      description.writeBatchSize)
  }

  override def createBatchWriter(): BatchWriter[VectorSchemaRoot] = {
    createFileWriter(partitionId)
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
        .withChunkSize(chunkSize)
        .build())
  }

  protected var flush = false

  override protected def processRow(row: InternalRow): Unit = {
    if (arrowBatchWriter.isFull()) {
      try {
        arrowBatchWriter.writeBatch(currentWriter, false)

        if (!flush) {
          val bytesWritten = currentWriter.currentMetricsValues
            .counter(MetricNames.BYTES_COUNT).orElse(new BytesCount).getCount
          if (bytesWritten < chunkSize) {
            arrowBatchWriter.addBufferedBatch(currentWriter.newElement())
          } else {
            // No Flow Exceeded
            logInfo(s"Flush success for partition $partitionId (task $taskId, attempt $attemptNumber)")

            arrowBatchWriter.setFlushSuccess()
            flush = true
          }
        }

      } catch {
        case cause: Throwable =>
          if (cause.getMessage.contains("FlowExceeded")) {
            logError(s"Encountered flow exceeded exception " +
              s"for partition $partitionId (task $taskId, attempt $attemptNumber), ${cause.getMessage} ")

            currentWriter.abort()

            if (arrowBatchWriter.canResume()) {
              var waitTime = new Random().nextInt(maxSleepIntervalMs - 2000) + 2000
              var retries = 0
              var flushSuccess = false

              while (!flushSuccess && retries < maxRetries) {
                retries = retries + 1
                waitTime = waitTime + new Random().nextInt(maxSleepIntervalMs - 2000) + 2000

                logInfo(s"Try to recreate batch writer, wait time $waitTime, " +
                  s"partition $partitionId (task $taskId, attempt $attemptNumber)")

                Thread.sleep(waitTime)
                try {
                  currentWriter = createBatchWriter()
                  arrowBatchWriter.writeBatch(currentWriter, true)
                  flushSuccess = true

                  logInfo(s"Retry Flush success for partition $partitionId (task $taskId, attempt $attemptNumber)")

                  arrowBatchWriter.setFlushSuccess()
                  flush = true
                } catch {
                  case cause: Throwable =>
                    currentWriter.abort()
                    if (!cause.getMessage.contains("FlowExceeded")) {
                      throw cause
                    } else if (retries >= maxRetries) {
                      logError(s"Recreate batch writer exceeded the threshold, " +
                        s"partition $partitionId (task $taskId, attempt $attemptNumber)")
                      throw cause
                    }
                }
              }
            } else {
              logError(s"Arrow batch writer cannot resume, " +
                s"partition $partitionId (task $taskId, attempt $attemptNumber)")
              throw cause
            }
          } else {
            throw cause
          }
      }
    }
    arrowBatchWriter.insertRecord(row)
  }
}

class SingleDirectoryRecordWriter(description: WriteJobDescription,
                                  partitionId: Int,
                                  attemptNumber: Int,
                                  taskId: Long)
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

  override def createBatchWriter(): BatchWriter[ArrayRecord] = {
    createFileWriter(partitionId)
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
                                        attemptNumber: Int,
                                        taskId: Long)
  extends SingleDirectoryArrowWriter(description, partitionId, attemptNumber, taskId) {

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
                           val staticPartition: PartitionSpec,
                           val allColumns: Seq[Attribute],
                           val dataColumns: Seq[Attribute],
                           val partitionColumns: Seq[Attribute],
                           val dynamicPartitionColumns: Seq[Attribute],
                           val maxRecordsPerFile: Long,
                           val statsTrackers: Seq[WriteJobStatsTracker],
                           val writeBatchSize: Long,
                           val timeZoneId: String,
                           val supportArrowWriter: Boolean,
                           val enableArrowExtension: Boolean,
                           val compressionCodec: String,
                           val chunkSize: Int,
                           val maxRetries: Int,
                           val maxSleepIntervalMs: Int)
  extends Serializable {

  assert(AttributeSet(allColumns) == AttributeSet(partitionColumns ++ dataColumns),
    s"""
       |All columns: ${allColumns.mkString(", ")}
       |Partition columns: ${partitionColumns.mkString(", ")}
       |Data columns: ${dataColumns.mkString(", ")}
       """.stripMargin)
}

/** The result of a successful write task. */
case class WriteTaskResult(commitMessage: Seq[OdpsWriterCommitMessage],
                           stats: Seq[WriteTaskStats])
  extends WriterCommitMessage