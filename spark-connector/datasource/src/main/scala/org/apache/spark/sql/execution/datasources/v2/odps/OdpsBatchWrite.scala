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

import com.aliyun.odps.table.write
import com.aliyun.odps.table.write.TableBatchWriteSession
import com.aliyun.odps.task.SQLTask
import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats}
import org.apache.spark.sql.odps.{OdpsClient, OdpsWriterFactory, WriteJobDescription, WriteTaskResult}
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._

@Experimental
class OdpsBatchWrite(
                      catalog: OdpsTableCatalog,
                      tableIdent: Identifier,
                      batchSink: TableBatchWriteSession,
                      description: WriteJobDescription,
                      overwrite: Boolean)
  extends BatchWrite
    with Logging {

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val results = messages.map(_.asInstanceOf[WriteTaskResult])
    val commitMessageList = results.map(_.commitMessage).reduceOption(_ ++ _).getOrElse(Seq.empty).asJava
    try {
      val (_, duration) = Utils.timeTakenMs {
        batchSink.commit(commitMessageList.toArray(Array.empty[write.WriterCommitMessage]))
      }
      logInfo(s"Write Job $tableIdent committed. Elapsed time: $duration ms.")
      processStats(description.statsTrackers, results.map(_.stats), duration)
      logInfo(s"Finished processing stats for write table $tableIdent.")
    } catch {
      case cause: Throwable =>
        if (commitMessageList.stream.filter(_ != null).count == 0) {
          val partition = description.staticPartition.toString
          if (!description.staticPartition.isEmpty &&
            description.dynamicPartitionColumns.isEmpty) {
            val sb = new StringBuilder
            // TODO: schema
            sb.append("ALTER TABLE ")
              .append(tableIdent.namespace().head)
              .append(".")
              .append(tableIdent.name())
              .append(" ADD IF NOT EXISTS PARTITION (")
              .append(partition)
              .append(");")
            val instance = SQLTask.run(OdpsClient.builder.getOrCreate.odps, sb.toString)
            instance.waitForSuccess()
            logInfo(s"Data source write $tableIdent committed empty data. " +
              s"Try to create partition $partition")
          }
          if (overwrite && description.dynamicPartitionColumns.isEmpty) {
            val sb = new StringBuilder
            sb.append("TRUNCATE TABLE ")
              .append(tableIdent.namespace().head)
              .append(".")
              .append(tableIdent.name())
            if (!description.staticPartition.isEmpty) {
              sb.append(" PARTITION (")
              sb.append(partition)
              sb.append(")")
            }
            sb.append(";")
            val instance = SQLTask.run(OdpsClient.builder.getOrCreate.odps, sb.toString)
            instance.waitForSuccess()
            logInfo(s"Data source write $tableIdent committed empty data. Truncate table.")
          }
        } else {
          throw cause
        }
    }
    catalog.invalidateTable(tableIdent)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    batchSink.cleanup()
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    OdpsWriterFactory(description)
  }

  /**
   * For every registered [[WriteJobStatsTracker]], call `processStats()` on it, passing it
   * the corresponding [[WriteTaskStats]] from all executors.
   */
  private def processStats(
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
