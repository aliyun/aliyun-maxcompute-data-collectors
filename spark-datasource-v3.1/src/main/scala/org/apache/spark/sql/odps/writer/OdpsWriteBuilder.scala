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

import java.util.Collections

import com.aliyun.odps.PartitionSpec
import com.aliyun.odps.cupid.table.v1.util.Options
import com.aliyun.odps.cupid.table.v1.writer.TableWriteSessionBuilder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.odps.datasource.OdpsSourceOptions
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, SupportsOverwrite, WriteBuilder}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class OdpsWriteBuilder(
    provider: String,
    odpsTable: com.aliyun.odps.Table,
    dataSchema: StructType,
    partitionSchema: StructType,
    odpsOptions: Options,
    info: LogicalWriteInfo) extends SupportsOverwrite {

  private val _project = odpsTable.getProject
  private val _name = odpsTable.getName()
  private val _partitions = partitionSchema.fields

  private val _schema = info.schema()
  private val _queryId = info.queryId()
  private val _options = info.options()

  private var _overwrite = false

  private val _partitionList = partitionSpec(_options)

  private val _isSinglePartition: Boolean = isSinglePartition(_partitionList)

  private val _isDynamicPartition: Boolean = _partitions.nonEmpty && !_isSinglePartition

  override def buildForBatch(): BatchWrite = {
    val partitionSpec = if (_partitions.isEmpty) {
      //非分区表
      Collections.emptyMap[String, String]()
    } else if (_isSinglePartition) {
      val optionalSpec = _options.get(OdpsSourceOptions.ODPS_PARTITION_SPEC)
      val odpsPartitionSpec = new PartitionSpec(optionalSpec)
      odpsTable.createPartition(odpsPartitionSpec, true)
      _partitionList.toMap.asJava
    } else {
      //check dynamic partition prerequisite
      val dynamicPartitionEnabled = _options.getBoolean(OdpsSourceOptions.ODPS_DYNAMIC_PARTITION_ENABLED, false)
      var dynamicPartitionSupportedMode = _options.get(OdpsSourceOptions.ODPS_DYNAMIC_PARTITION_MODE)

      if (dynamicPartitionSupportedMode == null) {
        dynamicPartitionSupportedMode = "append"
      }

      if (!dynamicPartitionEnabled ) {
        throw new Exception("can't support dynamic partition insert")
      }

      if (_overwrite && !dynamicPartitionSupportedMode.contains(SaveMode.Overwrite.name().toLowerCase)) {
        throw new Exception("can't support dynamic partition insert")
      }

      _partitionList.toMap.asJava
    }

    val writeSession = new TableWriteSessionBuilder(provider, _project, _name)
        .overwrite(_overwrite)
        .options(odpsOptions)
        .partitionSpec(partitionSpec)
        .build()

    val writeSessionInfo = writeSession.getOrCreateSessionInfo()
    new OdpsBatchWrite(_isDynamicPartition, writeSessionInfo)
  }

  def overwrite(var1: Array[Filter]): WriteBuilder =  {
    _overwrite = true
    this
  }

  private def isSinglePartition(specList: List[(String, String)]): Boolean = {
    _partitions.nonEmpty && specList.size == _partitions.size && specList.forall(_._2 != null)
  }

  private def partitionSpec(options: CaseInsensitiveStringMap): List[(String, String)] = {
    val optionalSpec = options.get(OdpsSourceOptions.ODPS_PARTITION_SPEC)
    if (optionalSpec == null || optionalSpec.isEmpty) {
      _partitions.map(f => (f.name, null)).toList
    } else {
      optionalSpec.split(",").map(keyEqValue => {
        val pair = keyEqValue.split('=')
        if (pair.length == 2) {
          (pair(0), pair(1))
        } else {
          (pair(0), null)
        }
      }).toList
    }
  }
}
