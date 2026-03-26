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

import com.aliyun.odps.table.{DataFormat, write}
import com.aliyun.odps.table.write.{TableBatchWriteSession, TableWriteSessionBuilder}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.connector.write.{PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.v2.odps.OdpsOptions.TUNNEL_TABLE_PROVIDER
import org.apache.spark.sql.odps.OdpsWriteUtils.processStats
import org.apache.spark.sql.odps.{OdpsClient, OdpsStreamingWriterFactory, OdpsUtils, WriteJobDescription, WriteTaskResult}
import org.apache.spark.util.Utils

import scala.jdk.CollectionConverters._
import scala.collection.mutable

case class OdpsStreamingWrite(
                          catalog: OdpsTableCatalog,
                          tableIdent: Identifier,
                          cachedKey: String,
                          sinkBuilder: TableWriteSessionBuilder,
                          description: WriteJobDescription,
                          overwrite: Boolean)
  extends StreamingWrite
    with Logging {

  private val committedEpochs = mutable.Map[Long, Boolean]()
  private var batchSink: TableBatchWriteSession = _

  // TODO current commit only supports insert mode using storage api
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    if (isAlreadyCommitted(epochId)) {
      // ensure idempotent
      // TODO current idempotency is based on the memory, not persistence involved
      logInfo(s"Epoch $epochId already committed. Skipping.")
      return
    }
    if (batchSink == null) {
      throw new RuntimeException("Streaming write session is not initialized in createStreamingWriterFactory.")
    }
    val results = messages.map(_.asInstanceOf[WriteTaskResult])
    val commitMessageList = results.map(_.commitMessage).toSeq.asJava
    try {
      val sessionBuilder = new TableWriteSessionBuilder()
        .identifier(batchSink.getTableIdentifier)
        .withSessionId(batchSink.getId)
        .withSettings(OdpsClient.get.getEnvironmentSettings)

      val provider = catalog.odpsOptions.tableWriteProvider
      if (!provider.equals(catalog.odpsOptions.DEFAULT_TABLE_PROVIDER)) {
        sessionBuilder.withSessionProvider(provider)
      }
      // Now reload session for bearer token ttl
      val (_, duration) = Utils.timeTakenMs {
        if (provider.equals(TUNNEL_TABLE_PROVIDER)) {
          batchSink.commit(commitMessageList.toArray(Array.empty[write.WriterCommitMessage]))
        } else {
          OdpsUtils.retryOnSpecificError(3, "Unexpected end of file from server") {
            () => {
              val reloadSession = sessionBuilder.buildBatchWriteSession
              reloadSession.commit(commitMessageList.toArray(Array.empty[write.WriterCommitMessage]))
            }
          }
        }
      }
      committedEpochs.put(epochId, true)
      logInfo(s"Write Job $tableIdent committed. Elapsed time: $duration ms.")
      processStats(description.statsTrackers, results.map(_.stats), duration)
      logInfo(s"Finished processing stats for write table $tableIdent.")
    } catch {
      case cause: Throwable =>
        throw cause
    }
    catalog.invalidateTable(tableIdent)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    committedEpochs.put(epochId, false)
    batchSink.cleanup()
  }

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    // TODO add row count in info to do rate limit
    OdpsStreamingLimiter.getLimiter(catalog, cachedKey).foreach(_.acquire())

    batchSink = OdpsUtils.retryOnSpecificError(3, "Unexpected end of file from server") {
      () => sinkBuilder.buildBatchWriteSession
    }
    description.batchSink = batchSink
    description.supportArrowWriter = batchSink.supportsDataFormat(new DataFormat(DataFormat.Type.ARROW, DataFormat.Version.V5))
    OdpsStreamingWriterFactory(description)
  }

  private def isAlreadyCommitted(epochId: Long): Boolean = {
    committedEpochs.getOrElse(epochId, false)
  }

}
