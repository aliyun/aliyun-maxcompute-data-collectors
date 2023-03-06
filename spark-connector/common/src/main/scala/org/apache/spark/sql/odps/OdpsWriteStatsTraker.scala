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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker.TASK_COMMIT_TIME
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.SQLMetric

import scala.collection.mutable

/**
 * Simple [[WriteTaskStatsTracker]] implementation that produces [[BasicWriteTaskStats]].
 */
class OdpsWriteTaskStatsTracker (taskCommitTimeMetric: Option[SQLMetric] = None)
  extends WriteTaskStatsTracker with Logging {

  private[this] val partitions: mutable.ArrayBuffer[InternalRow] = mutable.ArrayBuffer.empty
  private[this] var numFiles: Int = 0
  private[this] var numBytes: Long = 0L
  private[this] var numRows: Long = 0L


  override def newPartition(partitionValues: InternalRow): Unit = {
    partitions.append(partitionValues)
  }

  override def newFile(filePath: String): Unit = {
    // currently unhandled
  }

  def newFile(numBytes: Long, numRows: Long): Unit = {
    numFiles += 1
    this.numBytes += numBytes
    this.numRows += numRows
  }

  override def newRow(filePath : _root_.scala.Predef.String, row: InternalRow): Unit = {
    // currently unhandled
  }

  override def getFinalStats(taskCommitTime : scala.Long): WriteTaskStats = {
    // Reports bytesWritten and recordsWritten to the Spark output metrics.
    Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach { outputMetrics =>
      outputMetrics.setBytesWritten(numBytes)
      outputMetrics.setRecordsWritten(numRows)
    }

    taskCommitTimeMetric.foreach(_ += taskCommitTime)
    BasicWriteTaskStats(partitions.toSeq, numFiles, numBytes, numRows)
  }

  override def closeFile(filePath: String): Unit = {
    // currently unhandled
  }
}

class OdpsWriteJobStatsTracker(metrics: Map[String, SQLMetric])
  extends BasicWriteJobStatsTracker(null,
    metrics - TASK_COMMIT_TIME,
    metrics(TASK_COMMIT_TIME)) {

  override def newTaskInstance(): WriteTaskStatsTracker = {
    new OdpsWriteTaskStatsTracker(Some(metrics(TASK_COMMIT_TIME)))
  }
}
