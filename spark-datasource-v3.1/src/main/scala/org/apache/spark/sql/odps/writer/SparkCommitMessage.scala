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
