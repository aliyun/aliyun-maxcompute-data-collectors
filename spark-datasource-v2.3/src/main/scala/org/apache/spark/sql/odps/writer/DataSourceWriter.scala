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

import com.aliyun.odps.PartitionSpec
import com.aliyun.odps.cupid.table.v1.writer.{TableWriteSessionBuilder, WriteSessionInfo}
import org.apache.spark.sql.odps.datasource.{OdpsBaseSource, OdpsSourceOptions}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.JavaConverters._

/**
  * @author renxiang
  * @date 2021-12-21
  */
class DataSourceWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions)
  extends OdpsBaseSource(options) with org.apache.spark.sql.sources.v2.writer.DataSourceWriter {

  schemaValid()

  private val _partitionList = partitionSpec(options)

  private val _isSinglePartition: Boolean = isSinglePartition(_partitionList)

  private val _isDynamicPartition: Boolean = _odpsTablePartitions.nonEmpty && !_isSinglePartition

  private val _writeSession = {
    val partitionSpec = if (_odpsTablePartitions.isEmpty) {
      //非分区表
      null
    } else if (_isSinglePartition) {
      val odpsPartitionSpec = new PartitionSpec(options.get(OdpsSourceOptions.ODPS_PARTITION_SPEC).get())
      _odpsTable.createPartition(odpsPartitionSpec, true)
      _partitionList.toMap.asJava
    } else {
      //check dynamic partition prerequisite
      val dynamicPartitionEnabled = options.getBoolean(OdpsSourceOptions.ODPS_DYNAMIC_PARTITION_ENABLED, false)
      val dynamicPartitionSupportedMode = options.get(OdpsSourceOptions.ODPS_DYNAMIC_PARTITION_MODE).orElse("append")

      if (!dynamicPartitionEnabled || !dynamicPartitionSupportedMode.contains(mode.name().toLowerCase)) {
        throw new Exception("can't support dynamic partition insert")
      }
      _partitionList.toMap.asJava
    }

    new TableWriteSessionBuilder(_tableProvider, _odpsProject, _odpsTableName)
      .overwrite(mode == SaveMode.Overwrite)
      .options(_options)
      .partitionSpec(partitionSpec)
      .build()
  }

  private lazy val _writeSessionInfo: WriteSessionInfo = _writeSession.getOrCreateSessionInfo()

  override def createWriterFactory(): DataWriterFactory[Row] = {
    new PartitionWriterFactory(_isDynamicPartition, _writeSessionInfo)
      .asInstanceOf[DataWriterFactory[Row]]
  }

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    val msgArray = writerCommitMessages
      .flatMap(m => m.asInstanceOf[SparkCommitMessage].innerMsgList())
      .filter(m => Objects.nonNull(m))

    if (msgArray.isEmpty) {
      _writeSession.commitTable()
    } else {
      _writeSession.commitTableWithMessage(msgArray.toList.asJava)
    }
  }

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    _writeSession.cleanup()
  }

  private def isSinglePartition(specList: List[(String, String)]): Boolean = {
    _odpsTablePartitions.nonEmpty && specList.size == _odpsTablePartitions.size && specList.forall(_._2 != null)
  }

  private def partitionSpec(options: DataSourceOptions): List[(String, String)] = {
    val optionalSpec = options.get(OdpsSourceOptions.ODPS_PARTITION_SPEC)
    if (optionalSpec == null || !optionalSpec.isPresent) {
      _odpsTablePartitions.map(f => (f.name, null))
    } else {
      optionalSpec.get.split(",").map(keyEqValue => {
        val pair = keyEqValue.split('=')
        if (pair.length == 2) {
          (pair(0), pair(1))
        } else {
          (pair(0), null)
        }
      }).toList
    }
  }

  private def schemaValid(): Unit = {

    val errorItems = _odpsTableSchema.getColumns.asScala.zipWithIndex
      .map{ case(c, index) =>
        val inputRddField = schema.fields(index)
        if (inputRddField.name != c.getName) {
          (c.getName, inputRddField.name, index)
          val item = s"rdd[$index] actual name is $inputRddField.name, expected $c.getName"
          item
        } else {
          None
        }
      }
      .filter(_ != None)

    if (errorItems.nonEmpty) {
      throw new RuntimeException(s"input rdd schema not same to odps table")
    }
  }
}
