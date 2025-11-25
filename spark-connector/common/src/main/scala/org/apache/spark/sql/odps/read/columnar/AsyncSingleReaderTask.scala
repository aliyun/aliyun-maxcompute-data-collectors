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

package org.apache.spark.sql.odps.read.columnar

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
  private val arrowReader: SplitReader[VectorSchemaRoot] = partition.scan
    .createArrowReader(split, readerOptions)

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
        wrappedColumnarBatch.updateColumnBatch(arrowReader.get(), allNames, schema)
        asyncQueueForVisit.put(OdpsSubReaderData(readerId, wrappedColumnarBatch.columnarBatch))
        true
      }
    } catch {
      case ie: InterruptedException =>
        logError("InterruptedException: ", ie)
        false
      case cause: Throwable =>
        val splitIndex = split match {
          case split: InputSplitWithIndex =>
            split.getSplitIndex
          case split: RowRangeInputSplit =>
            split.getRowRange.getStartIndex
          case _ => 0
        }
        val sessionId = split.getSessionId
        logError(s"Partition reader $splitIndex for session $sessionId " +
          s"encountered failure ${cause.getMessage}", cause)
        val exception = SingleReaderTaskException(cause, sessionId, splitIndex)
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