package org.apache.spark.sql.odps.reader.columnar

import com.aliyun.odps.table.DataSchema
import com.aliyun.odps.table.configuration.ReaderOptions
import com.aliyun.odps.table.metrics.MetricNames
import com.aliyun.odps.table.read.SplitReader
import com.aliyun.odps.table.read.split.InputSplitWithIndex
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.odps.OdpsScanPartition
import org.apache.spark.sql.vectorized.ColumnarBatch


class SyncPartitionReader(partition: OdpsScanPartition,
                          readerOptions: ReaderOptions,
                          allNames: Seq[String])
  extends PartitionReader[ColumnarBatch] with Logging {

  private var inputBytes = 0L
  private val wrappedColumnarBatch: WrappedColumnarBatch =
    WrappedColumnarBatch(columnarBatch = null, readerOptions.isReuseBatch)
  private val schema: DataSchema = partition.scan.readSchema
  private var isClosed: Boolean = false
  private val arrowReader: SplitReader[VectorSchemaRoot] = try {
    partition.scan
      .createArrowReader(partition.inputSplits.head, readerOptions)
  } catch {
    case e: Exception =>
      if (e.getMessage != null
        && e.getMessage.contains("Connection refused")) {
        logError("Got data server problem, just suicide and wait for rescheduling!", e)
        System.exit(-1)
      }
      throw e
  }

  override def next(): Boolean = {
    try {
      if (!arrowReader.hasNext) {
        false
      } else {
        wrappedColumnarBatch.updateColumnBatch(arrowReader.get(), allNames, schema)
        true
      }
    } catch {
      case cause: Throwable =>
        val splitIndex = partition.inputSplits.head match {
          case split: InputSplitWithIndex =>
            split.getSplitIndex
          case split: RowRangeInputSplit =>
            split.getRowRange.getStartIndex
          case _ => 0
        }
        val sessionId = partition.inputSplits.head.getSessionId
        logError(s"Sync partition reader split $splitIndex for session $sessionId " +
          s"encountered failure ${cause.getMessage}")
        close()
        throw cause
    }
  }

  override def get(): ColumnarBatch = wrappedColumnarBatch.columnarBatch

  override def close(): Unit = {
    if (!isClosed) {
      if (wrappedColumnarBatch != null) {
        wrappedColumnarBatch.close()
      }

      arrowReader.currentMetricsValues.counter(MetricNames.BYTES_COUNT).ifPresent(c =>
        inputBytes = c.getCount)
      arrowReader.close()

      TaskContext.get().taskMetrics().inputMetrics
        .incBytesRead(inputBytes)

      isClosed = true
    }
  }
}
