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

package org.apache.spark.sql.odps.reader

import com.aliyun.odps.cupid.table.v1.reader.{InputSplit, SplitReaderBuilder}
import com.aliyun.odps.cupid.table.v1.util.TableUtils
import org.apache.spark.sql.odps.converter.TypesConverter
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory
import org.apache.spark.sql.types.StructType

/**
  * @author renxiang
  * @date 2021-12-19
  */
class InputSplitReaderFactory(tableSchema: StructType, inputSplit: InputSplit) extends DataReaderFactory[Row] {

  override def createDataReader(): InputSplitReader = {
    val recordReader = new SplitReaderBuilder(inputSplit).buildRecordReader()

    /**
      * table schema what was composed of both partition schema and output DataSchema
      * was different from output DataSchema.
      */
    val outputDataSchema = TableUtils.toColumnArray(inputSplit.getReadDataColumns).map(_.getTypeInfo).toList

    val sparkDataConverters = outputDataSchema.map(TypesConverter.odpsData2SparkData)

    new InputSplitReader(
      tableSchema,
      inputSplit.getPartitionColumns,
      inputSplit.getPartitionSpec,
      outputDataSchema,
      sparkDataConverters,
      recordReader)
  }
}
