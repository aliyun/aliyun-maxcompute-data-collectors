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

import com.aliyun.odps.table.configuration.ReaderOptions
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.odps.OdpsScanPartition
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, ExecutorService, Executors, Semaphore}


class AsyncPartitionReader(partition: OdpsScanPartition,
                           readerOptions: ReaderOptions,
                           reuseBatch: Boolean,
                           allNames: Seq[String],
                           asyncReaderQueueSize: Int)
  extends PartitionReader[ColumnarBatch] with Logging {

  private val inputBytes = new AtomicLong(0)
  private val schema = partition.scan.readSchema
  private val wrappedColumnarBatch: WrappedColumnarBatch =
    WrappedColumnarBatch(columnarBatch = null, reuseBatch)
  private val multiSplitsExecutor: ExecutorService = Executors.newFixedThreadPool(partition.inputSplits.length)
  private val queueSize = if (reuseBatch) partition.inputSplits.length else
    math.max(asyncReaderQueueSize, partition.inputSplits.length)
  private val asyncQueueForVisit: BlockingQueue[Object] = new ArrayBlockingQueue[Object](queueSize)
  private val DONE_SENTINEL: Object = new Object
  private var doneNum: Int = 0
  private var isClosed: Boolean = false
  private var semaphores: Array[Semaphore] = partition.inputSplits.map(_ => null)
  private var currentReaderIndex: Int = -1
  @volatile private var readerClose: Boolean = false
  init()

  def init(): Unit = {
    if (reuseBatch) {
      semaphores = partition.inputSplits.map { _ => new Semaphore(0) }
    }
    partition.inputSplits.zipWithIndex.foreach { case (_, index) =>
      multiSplitsExecutor.execute(new AsyncSingleReaderTask(index, readerOptions,
        partition, inputBytes, asyncQueueForVisit, semaphores(index), DONE_SENTINEL, readerClose))
    }
  }

  override def next(): Boolean = {
    try {
      if (doneNum == partition.inputSplits.length) {
        false
      } else {
        // reuseBatch mode should release the semaphore here to make sure the batch can be reuse
        if (currentReaderIndex != -1 && semaphores(currentReaderIndex) != null) {
          semaphores(currentReaderIndex).release()
        }

        val nextObject = asyncQueueForVisit.take()
        nextObject match {
          case data: OdpsSubReaderData =>
            wrappedColumnarBatch.updateColumnBatch(data.root, allNames, schema)
            currentReaderIndex = data.readerId
            true
          case DONE_SENTINEL =>
            doneNum += 1
            currentReaderIndex = -1
            next()
          case t: Throwable =>
            close()
            throw t
        }
      }
    } catch {
      case cause: Throwable =>
        logInfo(s"Partition reader encountered failure ${cause.getMessage}", cause)
        throw cause
    }
  }

  override def get(): ColumnarBatch = wrappedColumnarBatch.columnarBatch

  override def close(): Unit = {
    if (!isClosed) {
      if (wrappedColumnarBatch != null) {
        wrappedColumnarBatch.close()
      }

      TaskContext.get().taskMetrics().inputMetrics
        .incBytesRead(inputBytes.get())

      readerClose = true

      for (semaphore <- semaphores) {
        try {
          if (semaphore != null) {
            semaphore.release()
          }
        } catch {
          case e: Exception =>
            logWarning("Failed to clean up exchanger data on close", e)
        }
      }

      if (multiSplitsExecutor != null) {
        multiSplitsExecutor.shutdownNow()

        while (!asyncQueueForVisit.isEmpty) {
          val data = asyncQueueForVisit.take()
          data match {
            case data: OdpsSubReaderData =>
              try {
                data.root.close()
              } catch {
                case e: Exception =>
                  // maybe duplicate close
                  logDebug("Failed to clean up exchanger data on close", e)
              }
            case _ =>
          }
        }
      }
      isClosed = true
    }
  }
}
