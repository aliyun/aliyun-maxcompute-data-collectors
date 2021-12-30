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

import com.aliyun.odps.Column
import com.aliyun.odps.cupid.table.v1.writer.{FileWriter, FileWriterBuilder, WriteSessionInfo}
import com.aliyun.odps.data.ArrayRecord
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}

/**
  * @author renxiang
  * @date 2021-12-23
  */
class SinglePartitionWriter(partitionId: Int,
                            attemptNumber: Int,
                            converters: List[Any => AnyRef],
                            writeSessionInfo: WriteSessionInfo,
                            columns: java.util.List[Column]) extends DataWriter[Row] {

  private val _MAX_RECORD_SIZE = 80*1024*1024*1024
  private val _MIN_FILE_LINE = 10000
  private val _MAX_FILE_LOCATION = 1000

  private var _statsRecordCount: Long = 0
  private var _averageSizeOneRow: Long = 0
  private var _currentFileIndex = 0

  private var _currentWriter: Option[FileWriter[ArrayRecord]] = None

  private val _arrayRecord: ArrayRecord = {
    val columnArray = columns.toArray(new Array[Column](0))
    new ArrayRecord(columnArray)
  }

  override def write(row: Row): Unit = {
    val writer = newWriterIfNeeded()
    writer.write(transform(row))
    _statsRecordCount += 1
  }

  override def commit(): WriterCommitMessage = {
    if (_currentWriter.isDefined) {
      commitAndCloseWriter(_currentWriter.get)
    }

    new SparkCommitMessage()
  }

  override def abort(): Unit = {
    if (_currentWriter.isDefined) {
      _currentWriter.get.close()
    }
  }

  private def transform(row: Row): ArrayRecord = {
    var i = 0
    while (i < converters.length) {
      val value = if (row.isNullAt(i)) {
        null
      } else {
        converters(i)(row.get(i))
      }
      _arrayRecord.set(i, value)
      i += 1
    }
    _arrayRecord
  }

  private def newWriterIfNeeded(): FileWriter[ArrayRecord] = {
    if (_currentWriter.isEmpty) {
      _currentWriter = Option(newWriter())
    } else if (_statsRecordCount > _MIN_FILE_LINE) {
      if (_averageSizeOneRow == 0) {
        _averageSizeOneRow = _currentWriter.get.getBytesWritten / _currentWriter.get.getRowsWritten
      }

      val totalSize = _averageSizeOneRow * _statsRecordCount
      if (totalSize >= _MAX_RECORD_SIZE) {
        _currentWriter = Option(newWriter())
      }
    }

    _currentWriter.get
  }

  private def newWriter(): FileWriter[ArrayRecord] = {
    if (_currentWriter.isDefined) {
      commitAndCloseWriter(_currentWriter.get)
    }

    val fileIndex = partitionId * _MAX_FILE_LOCATION + _currentFileIndex
    _statsRecordCount = 0
    _currentFileIndex += 1

    new FileWriterBuilder(writeSessionInfo, fileIndex).buildRecordWriter()
  }

  private def commitAndCloseWriter(writer: FileWriter[ArrayRecord]): Unit = {
    writer.commitWithResult()
    writer.close()
  }
}
