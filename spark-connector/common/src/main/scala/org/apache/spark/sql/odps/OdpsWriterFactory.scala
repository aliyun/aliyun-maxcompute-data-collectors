package org.apache.spark.sql.odps

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}

case class OdpsWriterFactory(
                              description: WriteJobDescription) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val attemptNumber = TaskContext.get.attemptNumber()
    if (description.supportArrowWriter) {
      if (description.dynamicPartitionColumns.isEmpty) {
        new SingleDirectoryArrowWriter(description, partitionId, attemptNumber)
      } else {
        new DynamicPartitionArrowWriter(description, partitionId, attemptNumber)
      }
    } else {
      if (description.dynamicPartitionColumns.isEmpty) {
        new SingleDirectoryRecordWriter(description, partitionId, attemptNumber);
      } else {
        throw new UnsupportedOperationException("Unsupported dynamic writer with record writer")
      }
    }
  }
}
