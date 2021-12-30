/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.odps.writer

import java.util.Objects

import com.aliyun.odps.cupid.table.v1.writer.{TableWriteSessionBuilder, WriteSessionInfo}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}

import scala.collection.JavaConverters._

class OdpsBatchWrite(isDynamicPartition: Boolean, writeSessionInfo: WriteSessionInfo)
  extends BatchWrite {
  val _provider = writeSessionInfo.getProvider

  val _project = writeSessionInfo.getProject

  val _table = writeSessionInfo.getTable

  val _writeSession =
    new TableWriteSessionBuilder(_provider, _project, _table)
      .writeSessionInfo(writeSessionInfo).build()

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val msgArray = messages
      .flatMap(m => m.asInstanceOf[SparkCommitMessage].innerMsgList())
      .filter(m => Objects.nonNull(m))

    if (msgArray.isEmpty) {
      _writeSession.commitTable()
    } else {
      _writeSession.commitTableWithMessage(msgArray.toList.asJava)
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    _writeSession.cleanup()
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new PartitionWriterFactory(isDynamicPartition, writeSessionInfo)
      .asInstanceOf[DataWriterFactory]
  }


}
