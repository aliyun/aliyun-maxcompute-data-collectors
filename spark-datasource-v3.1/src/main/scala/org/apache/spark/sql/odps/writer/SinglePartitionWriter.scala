package org.apache.spark.sql.odps.writer

import com.aliyun.odps.Column
import com.aliyun.odps.cupid.table.v1.writer.{FileWriter, FileWriterBuilder, WriteSessionInfo}
import com.aliyun.odps.data.ArrayRecord
import org.apache.spark.sql.odps.converter.TypesConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

/**
  * @author renxiang
  * @date 2021-12-23
  */
class SinglePartitionWriter(partitionId: Int,
                            converters: List[Object => AnyRef],
                            writeSessionInfo: WriteSessionInfo,
                            columns: java.util.List[Column]) extends DataWriter[InternalRow] {

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

  override def write(row: InternalRow): Unit = {
    val writer = newWriterIfNeeded()
    writer.write(transform(row))
    _statsRecordCount += 1
  }

  override def close(): Unit = {

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

  private def transform(row: InternalRow): ArrayRecord = {
    var i = 0
    while (i < converters.length) {
      val value = if (row.isNullAt(i)) {
        null
      } else {
        val sparkType = TypesConverter.odpsType2SparkType(columns.get(i).getTypeInfo)
        converters(i)(row.get(i, sparkType))
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
