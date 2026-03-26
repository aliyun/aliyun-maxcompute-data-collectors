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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats}

case class OdpsWriterFactory(description: WriteJobDescription) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    // SPARK-24552: Generate a unique "attempt ID" based on the stage and task attempt numbers.
    // Assumes that there won't be more than Short.MaxValue attempts, at least not concurrently.
    OdpsWriteUtils.createWriter(partitionId, taskId, description)
  }
}

case class OdpsStreamingWriterFactory(description: WriteJobDescription) extends StreamingDataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    // SPARK-24552: Generate a unique "attempt ID" based on the stage and task attempt numbers.
    // Assumes that there won't be more than Short.MaxValue attempts, at least not concurrently.
    OdpsWriteUtils.createWriter(partitionId, taskId, description)
  }
}

object OdpsWriteUtils {

  def createWriter(partitionId: Int, taskId: Long, description: WriteJobDescription): DataWriter[InternalRow] = {
    // SPARK-24552: Generate a unique "attempt ID" based on the stage and task attempt numbers.
    // Assumes that there won't be more than Short.MaxValue attempts, at least not concurrently.
    val attemptNumber = (TaskContext.get.stageAttemptNumber << 16) | TaskContext.get.attemptNumber
    if (description.supportArrowWriter) {
      if (description.dynamicPartitionColumns.isEmpty) {
        new SingleDirectoryArrowWriter(description, partitionId, attemptNumber, taskId)
      } else {
        new DynamicPartitionArrowWriter(description, partitionId, attemptNumber, taskId)
      }
    } else {
      if (description.dynamicPartitionColumns.isEmpty) {
        new SingleDirectoryRecordWriter(description, partitionId, attemptNumber, taskId)
      } else {
        throw new UnsupportedOperationException("Unsupported dynamic writer with record writer")
      }
    }
  }

  /**
   * For every registered [[WriteJobStatsTracker]], call `processStats()` on it, passing it
   * the corresponding [[WriteTaskStats]] from all executors.
   */
  def processStats(
                    statsTrackers: Seq[WriteJobStatsTracker],
                    statsPerTask: Seq[Seq[WriteTaskStats]],
                    jobCommitDuration: Long): Unit = {
    val numStatsTrackers = statsTrackers.length
    assert(statsPerTask.forall(_.length == numStatsTrackers),
      s"""Every WriteTask should have produced one `WriteTaskStats` object for every tracker.
         |There are $numStatsTrackers statsTrackers, but some task returned
         |${statsPerTask.find(_.length != numStatsTrackers).get.length} results instead.
       """.stripMargin)

    val statsPerTracker = if (statsPerTask.nonEmpty) {
      statsPerTask.transpose
    } else {
      statsTrackers.map(_ => Seq.empty)
    }

    statsTrackers.zip(statsPerTracker).foreach {
      case (statsTracker, stats) => statsTracker.processStats(stats, jobCommitDuration)
    }
  }
}
