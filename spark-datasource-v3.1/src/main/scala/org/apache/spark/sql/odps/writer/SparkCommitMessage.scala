package org.apache.spark.sql.odps.writer

import org.apache.spark.sql.connector.write.WriterCommitMessage

import scala.collection.mutable.ArrayBuffer

/**
  * @author renxiang
  * @date 2021-12-24
  */
class SparkCommitMessage() extends WriterCommitMessage {
  private var _msgList = new ArrayBuffer[com.aliyun.odps.cupid.table.v1.writer.WriterCommitMessage]

  def innerMsgList(): Array[com.aliyun.odps.cupid.table.v1.writer.WriterCommitMessage] = _msgList.toArray

  def addMsg(odpsCommitMsg: com.aliyun.odps.cupid.table.v1.writer.WriterCommitMessage): Unit = {
    _msgList.append(odpsCommitMsg)
  }

}
