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
  private val arrowReader: SplitReader[VectorSchemaRoot] = partition.scan
    .createArrowReader(partition.inputSplits.head, readerOptions)

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
        logError(s"Partition reader $splitIndex for session $sessionId " +
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
