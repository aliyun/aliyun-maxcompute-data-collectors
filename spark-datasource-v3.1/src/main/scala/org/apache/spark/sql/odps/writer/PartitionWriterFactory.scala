package org.apache.spark.sql.odps.writer

import com.aliyun.odps.cupid.table.v1.writer.{TableWriteSessionBuilder, WriteSessionInfo}
import org.apache.spark.sql.odps.converter.TypesConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}

import scala.collection.JavaConverters._

/**
  * @author renxiang
  * @date 2021-12-21
  */
class PartitionWriterFactory(dynamicInsert: Boolean, writeSessionInfo: WriteSessionInfo) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val writeSession =
      new TableWriteSessionBuilder(writeSessionInfo.getProvider, writeSessionInfo.getProject, writeSessionInfo.getTable)
        .writeSessionInfo(writeSessionInfo)
        .build()

    val columns = writeSession.getTableSchema.getColumns
    val partitions = writeSession.getTableSchema.getPartitionColumns
    val converters = columns.asScala.map(c => TypesConverter.sparkData2OdpsData(c.getTypeInfo)).toList

    if (dynamicInsert) {
      new DynamicPartitionWriter(partitionId, converters, writeSessionInfo, columns, partitions)
    } else {
      new SinglePartitionWriter(partitionId, converters, writeSessionInfo, columns)
    }
  }
}
