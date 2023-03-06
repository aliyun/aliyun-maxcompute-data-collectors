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

package org.apache.spark.sql.odps

import scala.collection.JavaConverters._

import com.aliyun.odps.{Column, OdpsType}
import com.aliyun.odps.`type`.TypeInfo
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.odps.arrow.OdpsTimestampType
import org.apache.spark.sql.types._

private[sql] object ArrowUtils {

  val rootAllocator = new RootAllocator(Long.MaxValue)

  // todo: support more types.

  /** Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes */
  def toArrowType(dt: DataType, odpsTypeInfo: TypeInfo, timeZoneId: String): ArrowType = dt match {
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, true)
    case ShortType => new ArrowType.Int(8 * 2, true)
    case IntegerType => new ArrowType.Int(8 * 4, true)
    case LongType => new ArrowType.Int(8 * 8, true)
    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case StringType => ArrowType.Utf8.INSTANCE
    case BinaryType => ArrowType.Binary.INSTANCE
    case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale)
    case DateType => new ArrowType.Date(DateUnit.DAY)
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
  }

  def fromArrowType(dt: ArrowType): DataType = dt match {
    case ArrowType.Bool.INSTANCE => BooleanType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 => ByteType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 2 => ShortType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => IntegerType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
    case float: ArrowType.FloatingPoint
      if float.getPrecision() == FloatingPointPrecision.SINGLE => FloatType
    case float: ArrowType.FloatingPoint
      if float.getPrecision() == FloatingPointPrecision.DOUBLE => DoubleType
    case ArrowType.Utf8.INSTANCE => StringType
    case ArrowType.Binary.INSTANCE => BinaryType
    case d: ArrowType.Decimal => if (d.getPrecision == 54 && d.getScale == 18) {
      // set decimal to decimal(38, 18)
      DecimalType(OdpsUtils.ODPS_DECIMAL_DEFAULT_PRECISION, OdpsUtils.ODPS_DECIMAL_DEFAULT_SCALE)
    } else {
      DecimalType(d.getPrecision, d.getScale)
    }
    case date: ArrowType.Date if date.getUnit == DateUnit.DAY => DateType
    /** for tunnel datetime */
    case datetime: ArrowType.Date if datetime.getUnit == DateUnit.MILLISECOND => TimestampType
    case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND => TimestampType
    case tsNano: ArrowType.Timestamp if tsNano.getUnit == TimeUnit.NANOSECOND => TimestampType

    /** for odps datetime */
    case dt: ArrowType.Timestamp if dt.getUnit == TimeUnit.MILLISECOND => TimestampType
    /** for odps extension timestamp */
    case ots: OdpsTimestampType => TimestampType
    /** TODO: for odps extension legacy decimal */
    case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dt")
  }

  /** Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType */
  def toArrowField(name: String,
                   dt: DataType,
                   odpsTypeInfo: TypeInfo,
                   nullable: Boolean,
                   timeZoneId: String,
                   enableArrowExtension: Boolean): Field = {
    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(name, fieldType,
          Seq(toArrowField("element", elementType, odpsTypeInfo,
            containsNull, timeZoneId, enableArrowExtension)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(name, fieldType,
          fields.map { field =>
            toArrowField(field.name, field.dataType, odpsTypeInfo,
              field.nullable, timeZoneId, enableArrowExtension)
          }.toSeq.asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(name, mapType,
          Seq(toArrowField(MapVector.DATA_VECTOR_NAME,
            new StructType()
              .add(MapVector.KEY_NAME, keyType, nullable = false)
              .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
            odpsTypeInfo,
            nullable = false,
            timeZoneId,
            enableArrowExtension
          )).asJava)
      case TimestampType =>
        if (timeZoneId == null) {
          throw new UnsupportedOperationException(
            s"${TimestampType.catalogString} must supply timeZoneId parameter")
        } else {
          if (odpsTypeInfo.getOdpsType.equals(OdpsType.DATETIME)) {
            /** for odps datetime */
            val fieldType = new FieldType(nullable, new ArrowType.Timestamp(TimeUnit.MILLISECOND, timeZoneId), null)
            new Field(name, fieldType, Seq.empty[Field].asJava)
          } else if (enableArrowExtension) {
            /** for odps extension timestamp */
            val fieldType = new FieldType(nullable, new OdpsTimestampType, null)
            new Field(name, fieldType, OdpsTimestampType.getChildrenFields)
          } else {
            /** for odps compatible datetime */
            val fieldType = new FieldType(nullable, new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId), null)
            new Field(name, fieldType, Seq.empty[Field].asJava)
          }
        }
      case dataType =>
        val fieldType = new FieldType(nullable,
          toArrowType(dataType, odpsTypeInfo, timeZoneId), null)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  def fromArrowField(field: Field): DataType = {
    field.getType match {
      case _: ArrowType.Map =>
        val elementField = field.getChildren.get(0)
        val keyType = fromArrowField(elementField.getChildren.get(0))
        val valueType = fromArrowField(elementField.getChildren.get(1))
        MapType(keyType, valueType, elementField.getChildren.get(1).isNullable)
      case ArrowType.List.INSTANCE =>
        val elementField = field.getChildren().get(0)
        val elementType = fromArrowField(elementField)
        ArrayType(elementType, containsNull = elementField.isNullable)
      case ArrowType.Struct.INSTANCE =>
        val fields = field.getChildren().asScala.map { child =>
          val dt = fromArrowField(child)
          StructField(child.getName, dt, child.isNullable)
        }
        StructType(fields)
      case arrowType => fromArrowType(arrowType)
    }
  }

  /** Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType */
  def toArrowSchema(schema: Seq[Attribute],
                    odpsColumns: Array[Column],
                    timeZoneId: String,
                    enableArrowExtension: Boolean): Schema = {
    new Schema(schema.zip(odpsColumns).map {
      case (field, col) =>
        toArrowField(field.name, field.dataType, col.getTypeInfo,
          field.nullable, timeZoneId, enableArrowExtension)
    }.asJava)
  }

  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map { field =>
      val dt = fromArrowField(field)
      StructField(field.getName, dt, field.isNullable)
    })
  }
  /*
    /** Return Map with conf settings to be used in ArrowPythonRunner */
    def getPythonRunnerConfMap(conf: SQLConf): Map[String, String] = {
      val timeZoneConf = Seq(SQLConf.SESSION_LOCAL_TIMEZONE.key -> conf.sessionLocalTimeZone)
      val pandasColsByName = Seq(SQLConf.PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME.key ->
        conf.pandasGroupedMapAssignColumnsByName.toString)
      val arrowSafeTypeCheck = Seq(SQLConf.PANDAS_ARROW_SAFE_TYPE_CONVERSION.key ->
        conf.arrowSafeTypeConversion.toString)
      Map(timeZoneConf ++ pandasColsByName ++ arrowSafeTypeCheck: _*)
    }

   */
}
