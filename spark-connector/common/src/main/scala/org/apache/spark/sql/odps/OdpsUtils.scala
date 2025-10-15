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

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import scala.collection.JavaConverters._

import com.aliyun.odps.OdpsType
import com.aliyun.odps.`type`._
import com.aliyun.odps.commons.util.DateUtils
import com.aliyun.odps.data.{Binary, Char, SimpleStruct, Varchar}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.odps.table.utils.DateTimeConstants.{MICROS_PER_MILLIS, MICROS_PER_SECOND, SECONDS_PER_DAY}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object OdpsUtils extends Logging {

  private def nullSafeEval(func: Object => Any): Object => Any =
    (v: Object) => if (v ne null) func(v) else null

  // convert from Odps DataType to Spark DataType
  def odpsData2SparkData(t: TypeInfo): Object => Any = {
    val func = t.getOdpsType match {
      case OdpsType.BOOLEAN => (v: Object) => v.asInstanceOf[java.lang.Boolean]
      case OdpsType.DOUBLE => (v: Object) => v.asInstanceOf[java.lang.Double]
      case OdpsType.BIGINT => (v: Object) => v.asInstanceOf[java.lang.Long]
      case OdpsType.STRING => (v: Object) => v match {
        case str: String =>
          UTF8String.fromString(str)
        case array: Array[Byte] =>
          UTF8String.fromBytes(array)
      }
      case OdpsType.DECIMAL => (v: Object) => {
        val ti = t.asInstanceOf[DecimalTypeInfo]
        if (ti.getPrecision == 54 && ti.getScale == 18) {
          (new Decimal).set(v.asInstanceOf[java.math.BigDecimal], ODPS_DECIMAL_DEFAULT_PRECISION, ODPS_DECIMAL_DEFAULT_SCALE)
        } else {
          (new Decimal).set(v.asInstanceOf[java.math.BigDecimal], ti.getPrecision, ti.getScale)
        }
      }
      case OdpsType.VARCHAR => (v: Object) => {
        val varchar = v.asInstanceOf[Varchar]
        UTF8String.fromString(varchar.getValue.substring(0, varchar.length()))
      }
      case OdpsType.CHAR => (v: Object) => {
        val char = v.asInstanceOf[Char]
        UTF8String.fromString(char.getValue.substring(0, char.length())).trimRight()
      }
      case OdpsType.DATE => (v: Object) => {
        v match {
          case date: LocalDate =>
            DateTimeUtils.localDateToDays(date)
          case _ =>
            DateUtils.getDayOffset(v.asInstanceOf[Date]).toInt
        }
      }
      case OdpsType.DATETIME => (v: Object) => {
        v match {
          case zt : java.time.ZonedDateTime =>
            val instant = Instant.from(zt)
            DateTimeUtils.instantToMicros(instant)
          case _ =>
            Math.multiplyExact(v.asInstanceOf[java.util.Date].getTime, MICROS_PER_MILLIS)
        }
      }
      case OdpsType.TIMESTAMP => (v: Object) => {
        v match {
          case ts : java.time.Instant =>
            DateTimeUtils.instantToMicros(ts)
          case _ =>
            DateTimeUtils.fromJavaTimestamp(v.asInstanceOf[java.sql.Timestamp])
        }
      }
      case OdpsType.FLOAT => (v: Object) => v.asInstanceOf[java.lang.Float]
      case OdpsType.INT => (v: Object) => v.asInstanceOf[java.lang.Integer]
      case OdpsType.SMALLINT => (v: Object) => v.asInstanceOf[java.lang.Short]
      case OdpsType.TINYINT => (v: Object) => v.asInstanceOf[java.lang.Byte]
      case OdpsType.ARRAY => (v: Object) => {
        val array = v.asInstanceOf[java.util.ArrayList[Object]]
        new GenericArrayData(array.toArray().
          map(odpsData2SparkData(t.asInstanceOf[ArrayTypeInfo].getElementTypeInfo)(_)))
      }
      case OdpsType.BINARY => (v: Object) => v.asInstanceOf[Binary].data()
      case OdpsType.MAP => (v: Object) => {
        val m = v.asInstanceOf[java.util.HashMap[Object, Object]]
        val keyArray = m.keySet().toArray()
        new ArrayBasedMapData(
          new GenericArrayData(keyArray.
            map(odpsData2SparkData(t.asInstanceOf[MapTypeInfo].getKeyTypeInfo)(_))),
          new GenericArrayData(keyArray.map(m.get(_)).
            map(odpsData2SparkData(t.asInstanceOf[MapTypeInfo].getValueTypeInfo)(_)))
        )
      }
      case OdpsType.STRUCT => (v: Object) => {
        val struct = v.asInstanceOf[com.aliyun.odps.data.Struct]
        org.apache.spark.sql.catalyst.InternalRow
          .fromSeq(struct.getFieldValues.asScala.zipWithIndex
            .map(x => odpsData2SparkData(struct.getFieldTypeInfo(x._2))(x._1)))
      }
    }
    nullSafeEval(func)
  }

  // convert from Spark DataType to Odps DataType
  def sparkData2OdpsData(t: TypeInfo): Object => AnyRef = {
    t.getOdpsType match {
      case OdpsType.BOOLEAN => v: Object => v.asInstanceOf[java.lang.Boolean]
      case OdpsType.DOUBLE => v: Object => v.asInstanceOf[java.lang.Double]
      case OdpsType.BIGINT => v: Object => v.asInstanceOf[java.lang.Long]
      case OdpsType.DATETIME => v: Object =>
        if (v != null)  {
          new java.util.Date(Math.floorDiv(v.asInstanceOf[Long], MICROS_PER_MILLIS))
        }
        else null
      case OdpsType.STRING => v: Object =>
        if (v != null) v.asInstanceOf[UTF8String].toString
        else null
      case OdpsType.DECIMAL => v: Object =>
        val ti = t.asInstanceOf[DecimalTypeInfo]
        if (v != null) new BigDecimal(v.asInstanceOf[Decimal].toString).setScale(ti.getScale)
        else null
      case OdpsType.VARCHAR => v: Object =>
        val ti = t.asInstanceOf[VarcharTypeInfo]
        if (v != null) new Varchar(v.asInstanceOf[UTF8String].toString, ti.getLength)
        else null
      case OdpsType.CHAR => v: Object =>
        val ti = t.asInstanceOf[CharTypeInfo]
        if (v != null) new Char(v.asInstanceOf[UTF8String].toString, ti.getLength)
        else null
      case OdpsType.DATE => v: Object =>
        // TODO: use odps-sdk setDateAsLocalDate
        if (v != null) new java.sql.Date(v.asInstanceOf[Int].toLong * SECONDS_PER_DAY)
        else null
      case OdpsType.TIMESTAMP => v: Object =>
        if (v != null) {
          val microSeconds = v.asInstanceOf[Long]
          var seconds = microSeconds / MICROS_PER_SECOND
          var micros = microSeconds % MICROS_PER_SECOND
          if (micros < 0) {
            micros += MICROS_PER_SECOND
            seconds -= 1
          }
          val ts = new Timestamp(seconds * 1000)
          ts.setNanos(micros.toInt * 1000)
          ts
        }
        else null
      case OdpsType.FLOAT => v: Object => v.asInstanceOf[java.lang.Float]
      case OdpsType.INT => v: Object => v.asInstanceOf[java.lang.Integer]
      case OdpsType.SMALLINT => v: Object => v.asInstanceOf[java.lang.Short]
      case OdpsType.TINYINT => v: Object => v.asInstanceOf[java.lang.Byte]
      case OdpsType.ARRAY => v: Object =>
        val ti = t.asInstanceOf[ArrayTypeInfo]
        if (v != null) {
          v.asInstanceOf[org.apache.spark.sql.catalyst.expressions.UnsafeArrayData]
            .toArray[Object](typeInfo2Type(ti.getElementTypeInfo))
            .map(e => sparkData2OdpsData(ti.getElementTypeInfo)(e)).toList.asJava
        } else null
      case OdpsType.BINARY => v: Object => new Binary(v.asInstanceOf[Array[Byte]])
      case OdpsType.MAP => v: Object =>
        val ti = t.asInstanceOf[MapTypeInfo]
        if (v != null) {
          val m = new java.util.HashMap[Object, Object]
          val mapData = v.asInstanceOf[org.apache.spark.sql.catalyst.expressions.UnsafeMapData]
          mapData.keyArray.toArray[Object](typeInfo2Type(ti.getKeyTypeInfo))
            .zip(
              mapData.valueArray.toArray[Object](
                typeInfo2Type(ti.getValueTypeInfo)))
            .foreach(p => m.put(
              sparkData2OdpsData(ti.getKeyTypeInfo)(p._1),
              sparkData2OdpsData(ti.getValueTypeInfo)(p._2)
                .asInstanceOf[Object])
            )
          m
        } else null
      case OdpsType.STRUCT => v: Object => {
        val ti = t.asInstanceOf[StructTypeInfo]
        if (v != null) {
          val r = v.asInstanceOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow]
          val l = (0 until r.numFields).zip(ti.getFieldTypeInfos.toArray()).map(p =>
            sparkData2OdpsData(p._2.asInstanceOf[TypeInfo])(r.get(p._1,
              typeInfo2Type(p._2.asInstanceOf[TypeInfo])))
          ).toList.asJava
          new SimpleStruct(ti, l)
        } else null
      }
    }
  }

  val ODPS_DECIMAL_DEFAULT_PRECISION = 38
  val ODPS_DECIMAL_DEFAULT_SCALE = 18

  val ODPS_EXTERNAL_TABLE = "EXTERNAL_TABLE"
  val ODPS_MANAGED_TABLE = "MANAGED_TABLE"
  val ODPS_VIRTUAL_VIEW = "VIRTUAL_VIEW"
  val SESSION_ERROR_MESSAGE: Set[String] = Set[String]("OTSTimeout")

  /** Given the string representation of a type, return its DataType */
  def typeInfo2Type(typeInfo: TypeInfo): DataType = {
    typeStr2Type(typeInfo.getTypeName.toLowerCase())
  }

  private def splitTypes(types: String): List[String] = {
    var unclosedAngles = 0
    val sb = new StringBuilder()
    var typeList = List.empty[String]
    types foreach (c => {
      if (c == ',' && unclosedAngles == 0) {
        typeList :+= sb.toString()
        sb.clear()
      } else if (c == '<') {
        unclosedAngles += 1
        sb.append(c)
      } else if (c == '>') {
        unclosedAngles -= 1
        sb.append(c)
      } else {
        sb.append(c)
      }
    })
    typeList :+= sb.toString()
    typeList
  }

  /** Given the string representation of a type, return its DataType */
  def typeStr2Type(typeStr: String): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    val CHAR = """char\(\s*(\d+)\s*\)""".r
    val VARCHAR = """varchar\(\s*(\d+)\s*\)""".r
    val ARRAY = """array<\s*(.+)\s*>""".r
    val MAP = """map<\s*(.+)\s*>""".r
    val STRUCT = """struct<\s*(.+)\s*>""".r

    typeStr.toLowerCase match {
      case "decimal" => DecimalType(ODPS_DECIMAL_DEFAULT_PRECISION, ODPS_DECIMAL_DEFAULT_SCALE)
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case "float" => FloatType
      case "double" => DoubleType
      case "boolean" => BooleanType
      case "datetime" => TimestampType
      case "date" => DateType
      case "timestamp" => TimestampType
      case "tinyint" => ByteType
      case "smallint" => ShortType
      case "int" => IntegerType
      case "bigint" => LongType
      case "string" => StringType
      case CHAR(_) => StringType
      case VARCHAR(_) => StringType
      case "binary" => BinaryType
      case ARRAY(elemType) => ArrayType(typeStr2Type(elemType))
      case MAP(types) =>
        val List(keyType, valType) = splitTypes(types)
        MapType(typeStr2Type(keyType), typeStr2Type(valType))
      case STRUCT(types) =>
        val elemTypes = splitTypes(types).map(elem => {
          val Array(name, typeStr) = elem.split(":", 2)
          StructField(name, typeStr2Type(typeStr))
        })
        StructType(elemTypes)
      case _ =>
        throw new AnalysisException(s"ODPS data type: $typeStr not supported!")
    }
  }

  def retryOnSpecificError[T](maxRetries: Int, errorMessages: Set[String])(f: () => T): T = {
    var currentRetry = 0
    var success = false
    var result: Option[T] = None

    while (currentRetry <= maxRetries && !success) {
      try {
        result = Some(f())
        success = true
      } catch {
        case e: Throwable if Option(e.getMessage).exists(msg => errorMessages.exists(msg.contains)) =>
          if (currentRetry < maxRetries) {
            Thread.sleep(3000 * (1 << currentRetry))
            logError(s"Retrying after failure, retry count: $currentRetry", e)
            currentRetry += 1
          } else {
            throw e
          }
      }
    }
    result.getOrElse(throw new RuntimeException("Unexpected error"))
  }
}
