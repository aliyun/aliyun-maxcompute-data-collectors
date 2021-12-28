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
