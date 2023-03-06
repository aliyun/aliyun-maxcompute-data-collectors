/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import com.aliyun.odps.table.write
import com.aliyun.odps.table.write.TableBatchWriteSession

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.{SQLExecution, SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.connector.write.{DataWriterFactory, PhysicalWriteInfoImpl, WriterCommitMessage}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.v2.DataWritingSparkTask.{logError, logInfo}
import org.apache.spark.sql.execution.datasources.v2.{DataWritingSparkTask, DataWritingSparkTaskResult, StreamWriterCommitProgress}
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.odps.OdpsUtils
import org.apache.spark.sql.odps.catalyst.plans.physical.OdpsHashPartitioning
import org.apache.spark.sql.odps.execution.exchange.OdpsShuffleExchangeExec
import org.apache.spark.sql.odps.OdpsWriterFactory
import org.apache.spark.sql.odps.WriteTaskResult
import org.apache.spark.sql.odps.{OdpsClient, OdpsWriteJobStatsTracker, WriteJobDescription}
import com.aliyun.odps.table.write.{WriterCommitMessage => OdpsWriterCommitMessage}
import org.apache.spark.util.{LongAccumulator, Utils}

import scala.util.control.NonFatal

object OdpsTableWriter extends Logging {

  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      writeSession: TableBatchWriteSession,
      description: WriteJobDescription,
      outputColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      bucketAttributes: Seq[Attribute],
      bucketSortOrders: Seq[SortOrder])
  : Unit = {
    val dynamicPartitionColumns = description.dynamicPartitionColumns
    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    val requiredOrdering = dynamicPartitionColumns
    // the sort order doesn't matter
    val actualOrdering = plan.outputOrdering.map(_.child)
    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    SQLExecution.checkSQLExecutionId(sparkSession)

    val identifier = writeSession.getTableIdentifier

    val tempRdd = bucketSpec match {
      case Some(BucketSpec(numBuckets, _, _)) =>
        val shuffledRdd = new OdpsShuffleExchangeExec(
          OdpsHashPartitioning(bucketAttributes, numBuckets), plan)

        if (bucketSortOrders.nonEmpty || dynamicPartitionColumns.nonEmpty) {
          val orderingExpr = if (dynamicPartitionColumns.nonEmpty) {
            dynamicPartitionColumns.map(SortOrder(_, Ascending)) ++ bucketSortOrders
          } else {
            bucketSortOrders
          }
            .map(BindReferences.bindReference(_, outputColumns))
          SortExec(
            orderingExpr,
            global = false,
            child = shuffledRdd
          ).execute()
        } else {
          shuffledRdd.execute()
        }

      case _ =>
        if (orderingMatched) {
          plan.execute()
        } else {
          val orderingExpr = requiredOrdering
            .map(SortOrder(_, Ascending))
            .map(BindReferences.bindReference(_, outputColumns))
          SortExec(
            orderingExpr,
            global = false,
            child = plan).execute()
        }
    }

    val rdd: RDD[InternalRow] = {
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        sparkSession.sparkContext.parallelize(Array.empty[InternalRow], 1)
      } else {
        tempRdd
      }
    }

    val writerFactory = new OdpsWriterFactory(description)
    val useCommitCoordinator = true
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()
    val customMetrics: Map[String, SQLMetric] = Map.empty

    try {
      sparkSession.sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          DataWritingTask.run(writerFactory, context, iter, useCommitCoordinator,
            customMetrics),
        rdd.partitions.indices,
        (index, result: DataWritingTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
        }
      )

      logInfo(s"Data source write $identifier is committing.")

      val results = messages.map(_.asInstanceOf[WriteTaskResult])
      val commitMessageList = results.map(_.commitMessage).reduceOption(_ ++ _).getOrElse(Seq.empty).asJava
      val (_, duration) = Utils.timeTakenMs {
        writeSession.commit(commitMessageList.toArray(Array.empty[OdpsWriterCommitMessage]))
      }
      processStats(description.statsTrackers, results.map(_.stats), duration)

      logInfo(s"Data source write $identifier committed. Elapsed time: $duration ms.")
    } catch {
      case cause: Throwable =>
        logError(s"Data source write $identifier is aborting.")
        try {
          writeSession.cleanup()
        } catch {
          case t: Throwable =>
            logError(s"Data source write $identifier failed to abort.")
            cause.addSuppressed(t)
            throw QueryExecutionErrors.writingJobFailedError(cause)
        }
        logError(s"Data source write $identifier aborted.")
        cause match {
          // Only wrap non fatal exceptions.
          case NonFatal(e) => throw QueryExecutionErrors.writingJobAbortedError(e)
          case _ => throw cause
        }
    }
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


object DataWritingTask extends Logging {
  def run(
           writerFactory: DataWriterFactory,
           context: TaskContext,
           iter: Iterator[InternalRow],
           useCommitCoordinator: Boolean,
           customMetrics: Map[String, SQLMetric]): DataWritingTaskResult = {
    val stageId = context.stageId()
    val stageAttempt = context.stageAttemptNumber()
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val attemptId = context.attemptNumber()
    val dataWriter = writerFactory.createWriter(partId, taskId)

    var count = 0L
    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        if (count % CustomMetrics.NUM_ROWS_PER_UPDATE == 0) {
          CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)
        }

        // Count is here.
        count += 1
        dataWriter.write(iter.next())
      }

      CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          logInfo(s"Commit authorized for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
          dataWriter.commit()
        } else {
          val commitDeniedException = QueryExecutionErrors.commitDeniedError(
            partId, taskId, attemptId, stageId, stageAttempt)
          logInfo(commitDeniedException.getMessage)
          // throwing CommitDeniedException will trigger the catch block for abort
          throw commitDeniedException
        }

      } else {
        logInfo(s"Writer for partition ${context.partitionId()} is committing.")
        dataWriter.commit()
      }

      logInfo(s"Committed partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")

      DataWritingTaskResult(count, msg)

    })(catchBlock = {
      // If there is an error, abort this writer
      logError(s"Aborting commit for partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")
      dataWriter.abort()
      logError(s"Aborted commit for partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")
    }, finallyBlock = {
      dataWriter.close()
    })
  }
}

case class DataWritingTaskResult(numRows: Long,
                                 writerCommitMessage: WriterCommitMessage)


