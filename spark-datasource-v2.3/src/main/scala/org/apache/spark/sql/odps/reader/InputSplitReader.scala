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

import java.util

import com.aliyun.odps.`type`.TypeInfo
import com.aliyun.odps.cupid.table.v1.Attribute
import com.aliyun.odps.cupid.table.v1.reader.SplitReader
import com.aliyun.odps.data.ArrayRecord
import org.apache.spark.sql.odps.converter.TypesConverter
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * @author renxiang
  * @date 2021-12-20
  */
class InputSplitReader(
                        readSchema: StructType,
                        partitionSchema: util.List[Attribute],
                        partitionSpec: util.Map[String, String],
                        outputDataSchema: List[TypeInfo],
                        converters: List[Object => Any],
                        recordReader: SplitReader[ArrayRecord]) extends DataReader[Row]{

  private val currentRow = {
    val row = new Array[Any](readSchema.fields.length)
    val offset = outputDataSchema.length
    var i = 0

    while (i < partitionSchema.size()) {
      val attr = partitionSchema.get(i)
      val value = partitionSpec.get(attr.getName)
      val sparkType = TypesConverter.odpsTypeStr2SparkType(attr.getType)

      sparkType match {
        case StringType =>
          row.update(offset + i, UTF8String.fromString(value).toString)
        case LongType =>
          row.update(offset + i, value.toLong)
        case IntegerType =>
          row.update(offset + i, value.toInt)
        case ShortType =>
          row.update(offset + i, value.toShort)
        case ByteType =>
          row.update(offset + i, value.toByte)
        case dt: DataType =>
          throw new SparkException(s"Unsupported partition column type: ${dt.simpleString}")
      }
      i += 1
    }

    row
  }

  override final def next: Boolean = recordReader.hasNext

  override final def get(): Row = {
    val record = recordReader.next()
    var i = 0
    if (record ne null) {
      while (i < converters.length) {
        val value = record.get(i)
        if (value ne null) {
          currentRow.update(i, converters(i)(value))
        } else {
          currentRow.update(i, null)
        }
        i += 1
      }
    } else {
      while (i < converters.length) {
        currentRow.update(i, null)
        i += 1
      }
    }

    val dataRow = new Array[Any](currentRow.length)
    currentRow.copyToArray(dataRow)
    new GenericRowWithSchema(dataRow, readSchema)
  }

  override final def close(): Unit = recordReader.close()
}
