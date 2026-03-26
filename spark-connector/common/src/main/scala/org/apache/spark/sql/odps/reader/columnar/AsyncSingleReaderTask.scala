package org.apache.spark.sql.odps.reader.columnar

import com.aliyun.odps.table.DataSchema
import com.aliyun.odps.table.configuration.ReaderOptions
import com.aliyun.odps.table.metrics.MetricNames
import com.aliyun.odps.table.read.SplitReader
import com.aliyun.odps.table.read.split.InputSplitWithIndex
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.internal.Logging
import org.apache.spark.sql.odps.OdpsScanPartition
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.IOException
import java.util.concurrent.{BlockingQueue, Semaphore}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}


case class OdpsSubReaderData(readerId: Int,
                             batch: ColumnarBatch)

case class OdpsSubReaderInput(reuseBatch: Boolean,
                              partition: OdpsScanPartition,
                              schema: DataSchema,
                              inputBytes: AtomicLong,
                              allNames: Seq[String],
                              semaphore: Semaphore)


class AsyncSingleReaderTask(readerId: Int,
                            readerOptions: ReaderOptions,
                            odpsSubReaderInput: OdpsSubReaderInput,
                            asyncQueueForVisit: BlockingQueue[Object],
                            endFlag: AnyRef,
                            readerClose: AtomicBoolean) extends Runnable with Logging {

  private var incBytes = 0L
  private val partition = odpsSubReaderInput.partition
  private val reuseBatch = odpsSubReaderInput.reuseBatch
  private val semaphore = odpsSubReaderInput.semaphore
  private val allNames = odpsSubReaderInput.allNames
  private val schema = odpsSubReaderInput.schema
  private val inputBytes = odpsSubReaderInput.inputBytes
  private val split = partition.inputSplits(readerId)
  private val wrappedColumnarBatch: WrappedColumnarBatch =
    WrappedColumnarBatch(columnarBatch = null, reuseBatch)
  private var currentRoot: VectorSchemaRoot = _
  private val splitIndex = split match {
    case split: InputSplitWithIndex =>
      split.getSplitIndex
    case split: RowRangeInputSplit =>
      split.getRowRange.getStartIndex
    case _ => 0
  }
  private val arrowReader: SplitReader[VectorSchemaRoot] = try {
    partition.scan
      .createArrowReader(split, readerOptions)
  } catch {
    case e: Exception =>
      if (e.getMessage != null
        && e.getMessage.contains("Connection refused")) {
        logError("Got data server problem, just suicide and wait for rescheduling!", e)
        System.exit(-1)
      }
      throw SingleReaderTaskException(e, split.getSessionId, splitIndex)
  }

  override def run(): Unit = {
    try {
      while (!readerClose.get() && hasNext) {
        if (semaphore != null) {
          semaphore.acquire()
        }
      }
    } finally {
      close()
    }
  }

  private def hasNext: Boolean = {
    try {
      if (!arrowReader.hasNext) {
        asyncQueueForVisit.put(endFlag)
        false
      } else {
        currentRoot = arrowReader.get()
        wrappedColumnarBatch.updateColumnBatch(currentRoot, allNames, schema)
        asyncQueueForVisit.put(OdpsSubReaderData(readerId, wrappedColumnarBatch.columnarBatch))
        currentRoot = null
        true
      }
    } catch {
      case ie: InterruptedException =>
        logError("InterruptedException: ", ie)
        if (currentRoot != null) {
          currentRoot.close()
        }
        false
      case cause: Throwable =>
        logError(s"Partition reader $splitIndex for session ${split.getSessionId} " +
          s"encountered failure ${cause.getMessage}", cause)
        val exception = SingleReaderTaskException(cause, split.getSessionId, splitIndex)
        asyncQueueForVisit.put(exception)
        throw exception
    }
  }

  private def close(): Unit = {
    arrowReader.currentMetricsValues.counter(MetricNames.BYTES_COUNT).ifPresent(c =>
      incBytes = c.getCount)
    arrowReader.close()

    inputBytes.addAndGet(incBytes)
  }
}

case class SingleReaderTaskException(cause: Throwable,
                                sessionId: String,
                                splitIndex: Long)
  extends IOException(s"Partition reader $splitIndex for session $sessionId " +
    s"encountered failure ${cause.getMessage}", cause)