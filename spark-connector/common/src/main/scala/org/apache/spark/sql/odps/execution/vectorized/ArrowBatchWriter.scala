/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.odps.execution.vectorized

import com.aliyun.odps.Column
import com.aliyun.odps.table.arrow.constructor.{ArrowArrayWriter, ArrowBigIntWriter, ArrowBitWriter, ArrowDateDayWriter, ArrowDateMilliWriter, ArrowDecimalWriter, ArrowFieldWriter, ArrowFloat4Writer, ArrowFloat8Writer, ArrowIntWriter, ArrowMapWriter, ArrowSmallIntWriter, ArrowStructWriter, ArrowTimeStampWriter, ArrowTinyIntWriter, ArrowVarBinaryWriter, ArrowVarCharWriter}
import com.aliyun.odps.table.write.BatchWriter
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.odps.ArrowUtils
import org.apache.spark.sql.odps.table.utils.DateTimeConstants.{MICROS_PER_MILLIS, NANOS_PER_MICROS}

class ArrowBatchWriter(outputColumns: Array[Column],
                       batch: VectorSchemaRoot,
                       batchSize: Long) {

  private var rowCnt = 0
  private var batchCnt = 1
  private var fields: Seq[Array[ArrowFieldWriter[SpecializedGetters]]] = Seq(new Array(outputColumns.length))
  private var roots: Seq[VectorSchemaRoot] = Seq(batch)
  private var currentBatchIdx = 0
  private var currentBatchRowCnt = 0
  private var resume = true
  private var flushSuccess = false
  private var writeBatchIdx = 0

  initVector()

  private def initVector(): Unit = {
    fields.indices.foreach { ind =>
      fields(ind).indices.foreach { i =>
        val vector = roots(ind).getFieldVectors().get(i)
        vector.allocateNew()
        fields(ind)(i) = createFieldWriter(vector)
      }
    }
  }

  def canResume(): Boolean = {
    resume
  }

  def insertRecord(rec: InternalRow): Unit = {
    if (isFull()) {
      throw new Exception("Batch is full")
    }
    if (isCurrentBatchFull()) {
      roots(currentBatchIdx).setRowCount(currentBatchRowCnt)
      fields(currentBatchIdx).foreach(_.finish())
      currentBatchRowCnt = 0
      currentBatchIdx = currentBatchIdx + 1
    }
    if (currentBatchIdx >= batchCnt) {
      throw new Exception("All batch is full")
    }

    var i = 0
    while (i < fields(currentBatchIdx).length) {
      fields(currentBatchIdx)(i).write(rec, i)
      i += 1
    }
    currentBatchRowCnt = currentBatchRowCnt + 1
    rowCnt = rowCnt + 1
  }

  def writeBatch(fileWriter: BatchWriter[VectorSchemaRoot], flushAll: Boolean): Unit = {
    if (rowCnt > 0) {
      roots(currentBatchIdx).setRowCount(currentBatchRowCnt)
      fields(currentBatchIdx).foreach(_.finish())

      roots.indices.foreach { ind =>
        if (!flushAll) {
          if (ind >= writeBatchIdx) {
            fileWriter.write(roots(ind))
          }
        } else {
          fileWriter.write(roots(ind))
        }
      }

      writeBatchIdx = roots.length

      if (flushSuccess) {
        reset()
      }
    }
  }

  def writeBatchToFile(fileWriter: BatchWriter[VectorSchemaRoot]): Unit = {
    if (rowCnt > 0) {
      roots.indices.foreach { ind =>
        fileWriter.write(roots(ind))
      }
    }
  }

  def addBufferedBatch(batch: VectorSchemaRoot): Unit = {
    val field : Array[ArrowFieldWriter[SpecializedGetters]] = new Array(outputColumns.length)
    field.indices.foreach { i =>
      val vector = batch.getFieldVectors().get(i)
      vector.allocateNew()
      field(i) = createFieldWriter(vector)
    }
    fields = fields :+ field
    roots = roots :+ batch

    batchCnt = batchCnt + 1
    currentBatchRowCnt = 0
    currentBatchIdx = currentBatchIdx + 1
  }

  def setFlushSuccess(): Unit = {
    flushSuccess = true
    resume = false
    reset()
  }

  def reset(): Unit = {
    roots.indices.foreach { ind =>
      roots(ind).setRowCount(0)
      fields(ind).foreach(_.reset())
    }

    rowCnt = 0
    currentBatchRowCnt = 0
    currentBatchIdx = 0
    writeBatchIdx = 0
  }

  def close(): Unit = {
    roots.indices.foreach { ind =>
      roots(ind).close()
    }
  }

  def isCurrentBatchFull(): Boolean = currentBatchRowCnt >= batchSize

  def isFull(): Boolean = rowCnt >= batchSize * batchCnt

  private def createFieldWriter(vector: ValueVector): ArrowFieldWriter[SpecializedGetters] = {
    val field = vector.getField()
    (ArrowUtils.fromArrowField(field), vector) match {
      case (BooleanType, vector: BitVector) => new BooleanWriter(vector)
      case (ByteType, vector: TinyIntVector) => new ByteWriter(vector)
      case (ShortType, vector: SmallIntVector) => new ShortWriter(vector)
      case (IntegerType, vector: IntVector) => new IntegerWriter(vector)
      case (LongType, vector: BigIntVector) => new LongWriter(vector)
      case (FloatType, vector: Float4Vector) => new FloatWriter(vector)
      case (DoubleType, vector: Float8Vector) => new DoubleWriter(vector)
      case (DecimalType.Fixed(precision, scale), vector: DecimalVector) =>
        new DecimalWriter(vector, precision, scale)
      case (StringType, vector: VarCharVector) => new StringWriter(vector)
      case (BinaryType, vector: VarBinaryVector) => new BinaryWriter(vector)
      case (DateType, vector: DateDayVector) => new DateWriter(vector)
      case (TimestampType, vector: TimeStampMicroVector) => new TimestampWriter(vector)
      case (TimestampType, vector: TimeStampMilliVector) => new DatetimeWriter(vector)
      case (TimestampType, vector: DateMilliVector) => new DatetimeMillWriter(vector)
      case (TimestampType, vector: TimeStampNanoVector) => new TimestampNanoWriter(vector)
      case (ArrayType(_, _), vector: ListVector) =>
        val elementVector = createFieldWriter(vector.getDataVector())
        new ArrayWriter(vector, elementVector)
      case (MapType(_, _, _), vector: MapVector) =>
        val structVector = vector.getDataVector.asInstanceOf[StructVector]
        val keyWriter = createFieldWriter(structVector.getChild(MapVector.KEY_NAME))
        val valueWriter = createFieldWriter(structVector.getChild(MapVector.VALUE_NAME))
        new MapWriter(vector, keyWriter, valueWriter)
      case (StructType(_), vector: StructVector) =>
        val children = (0 until vector.size()).map { ordinal =>
          createFieldWriter(vector.getChildByOrdinal(ordinal))
        }
        new StructWriter(vector, children.toArray)
      case (dt, _) =>
        throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}, " +
          s"Vector: ${vector.getMinorType.getType}")
    }
  }

}

private class BooleanWriter(valueVector: BitVector)
  extends ArrowBitWriter[SpecializedGetters](valueVector) {

  override def readBoolean(input: SpecializedGetters, ordinal: Int): Boolean = {
    input.getBoolean(ordinal)
  }

  override def isNullAt(input: SpecializedGetters, ordinal: Int): Boolean = {
    input.isNullAt(ordinal)
  }
}

private class ByteWriter(valueVector: TinyIntVector)
  extends ArrowTinyIntWriter[SpecializedGetters](valueVector) {
  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }

  override def readByte(row: SpecializedGetters, ordinal: Int): Byte = {
    row.getByte(ordinal)
  }
}

private class ShortWriter(valueVector: SmallIntVector)
  extends ArrowSmallIntWriter[SpecializedGetters](valueVector) {
  override def readShort(row: SpecializedGetters, ordinal: Int): Short = {
    row.getShort(ordinal)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class IntegerWriter(valueVector: IntVector)
  extends ArrowIntWriter[SpecializedGetters](valueVector) {
  override def readInt(row: SpecializedGetters, ordinal: Int): Int = {
    row.getInt(ordinal)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class LongWriter(valueVector: BigIntVector)
  extends ArrowBigIntWriter[SpecializedGetters](valueVector) {
  override def readLong(row: SpecializedGetters, ordinal: Int): Long = {
    row.getLong(ordinal)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class FloatWriter(valueVector: Float4Vector)
  extends ArrowFloat4Writer[SpecializedGetters](valueVector) {
  override def readFloat(row: SpecializedGetters, ordinal: Int): Float = {
    row.getFloat(ordinal)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class DoubleWriter(valueVector: Float8Vector)
  extends ArrowFloat8Writer[SpecializedGetters](valueVector) {
  override def readDouble(row: SpecializedGetters, ordinal: Int): Double = {
    row.getDouble(ordinal)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class DecimalWriter(valueVector: DecimalVector,
                            precision: Int,
                            scale: Int)
  extends ArrowDecimalWriter[SpecializedGetters](valueVector, precision, scale) {
  override def readBigDecimal(row: SpecializedGetters, ordinal: Int): java.math.BigDecimal = {
    val decimal = row.getDecimal(ordinal, precision, scale)
    if (decimal.changePrecision(precision, scale)) {
      decimal.toJavaBigDecimal
    } else {
      throw new IllegalArgumentException("change precision failed")
    }
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class StringWriter(valueVector: VarCharVector)
  extends ArrowVarCharWriter[SpecializedGetters](valueVector) {
  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }

  override def setValue(row: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = row.getUTF8String(ordinal)
    val utf8ByteBuffer = utf8.getByteBuffer
    // todo: for off-heap UTF8String, how to pass in to arrow without copy?
    valueVector.setSafe(count, utf8ByteBuffer, utf8ByteBuffer.position(), utf8.numBytes())
  }
}

private class BinaryWriter(valueVector: VarBinaryVector)
  extends ArrowVarBinaryWriter[SpecializedGetters](valueVector) {
  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueVector.setSafe(count, bytes, 0, bytes.length)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class DateWriter(valueVector: DateDayVector)
  extends ArrowDateDayWriter[SpecializedGetters](valueVector) {
  override def readEpochDay(row: SpecializedGetters, ordinal: Int): Int = {
    row.getInt(ordinal)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class TimestampWriter(valueVector: TimeStampMicroVector)
  extends ArrowTimeStampWriter[SpecializedGetters](valueVector) {
  override def readEpochTime(row: SpecializedGetters, ordinal: Int): Long = {
    row.getLong(ordinal)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class TimestampNanoWriter(valueVector: TimeStampNanoVector)
  extends ArrowTimeStampWriter[SpecializedGetters](valueVector) {
  override def readEpochTime(row: SpecializedGetters, ordinal: Int): Long = {
    Math.multiplyExact(row.getLong(ordinal), NANOS_PER_MICROS)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class DatetimeWriter(valueVector: TimeStampMilliVector)
  extends ArrowTimeStampWriter[SpecializedGetters](valueVector) {
  override def readEpochTime(row: SpecializedGetters, ordinal: Int): Long = {
    Math.floorDiv(row.getLong(ordinal), MICROS_PER_MILLIS)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class DatetimeMillWriter(valueVector: DateMilliVector)
  extends ArrowDateMilliWriter[SpecializedGetters](valueVector) {
  override def readEpochTime(row: SpecializedGetters, ordinal: Int): Long = {
    Math.floorDiv(row.getLong(ordinal), MICROS_PER_MILLIS)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class ArrayWriter(valueVector: ListVector,
                          elementWriter: ArrowFieldWriter[SpecializedGetters])
  extends ArrowArrayWriter[SpecializedGetters, SpecializedGetters](valueVector, elementWriter) {

  override def readArray(input: SpecializedGetters, ordinal: Int): SpecializedGetters = {
    input.getArray(ordinal)
  }

  override def numElements(input: SpecializedGetters): Int = {
    input.asInstanceOf[ArrayData].numElements()
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class MapWriter(valueVector: MapVector,
                        keyWriter: ArrowFieldWriter[SpecializedGetters],
                        valueWriter: ArrowFieldWriter[SpecializedGetters])
  extends ArrowMapWriter[SpecializedGetters, MapData, SpecializedGetters, SpecializedGetters](valueVector, keyWriter, valueWriter) {

  override def readMap(input: SpecializedGetters, ordinal: Int): MapData = {
    input.getMap(ordinal)
  }

  override def readKeyArray(mapData: MapData): SpecializedGetters = {
    mapData.keyArray
  }

  override def readValueArray(mapData: MapData): SpecializedGetters = {
    mapData.valueArray
  }

  override def numElements(mapData: MapData): Int = {
    mapData.numElements()
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}

private class StructWriter(valueVector: StructVector,
                           children: Array[ArrowFieldWriter[SpecializedGetters]])
  extends ArrowStructWriter[SpecializedGetters, SpecializedGetters](valueVector, children) {

  override def readStruct(input: SpecializedGetters, ordinal: Int): SpecializedGetters = {
    input.getStruct(ordinal, children.length)
  }

  override def isNullAt(row: SpecializedGetters, ordinal: Int): Boolean = {
    row.isNullAt(ordinal)
  }
}
