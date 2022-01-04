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

import com.aliyun.odps.cupid.table.v1.writer.{TableWriteSessionBuilder, WriteSessionInfo}
import org.apache.spark.sql.odps.converter.TypesConverter
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}

import scala.collection.JavaConverters._

/**
  * @author renxiang
  * @date 2021-12-21
  */
class PartitionWriterFactory(dynamicInsert: Boolean, writeSessionInfo: WriteSessionInfo) extends DataWriterFactory[Row] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    val writeSession =
      new TableWriteSessionBuilder(writeSessionInfo.getProvider, writeSessionInfo.getProject, writeSessionInfo.getTable)
      .writeSessionInfo(writeSessionInfo)
      .build()

    val columns = writeSession.getTableSchema.getColumns

    val partitions = writeSession.getTableSchema.getPartitionColumns

    val converters = columns.asScala.map(c => TypesConverter.sparkData2OdpsData(c.getTypeInfo)).toList

    if (dynamicInsert) {
      new DynamicPartitionWriter(partitionId, attemptNumber, converters, writeSessionInfo, columns, partitions)
    } else {
      new SinglePartitionWriter(partitionId, attemptNumber, converters, writeSessionInfo, columns)
    }
  }


}
