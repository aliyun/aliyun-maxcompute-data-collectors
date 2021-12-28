package org.apache.spark.sql.odps.converter

import java.math.BigDecimal

import com.aliyun.odps.`type`._
import com.aliyun.odps.commons.util.DateUtils
import com.aliyun.odps.data.{Binary, Char, SimpleStruct, Varchar}
import com.aliyun.odps.{Column, OdpsType, Partition}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * @author renxiang
  * @date 2021-12-19
  */
object TypesConverter {

  private val ODPS_DECIMAL_DEFAULT_PRECISION = 38
  private val ODPS_DECIMAL_DEFAULT_SCALE = 18


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

  def odpsTypeStr2SparkType(typeInfo: String): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    val CHAR = """char\(\s*(\d+)\s*\)""".r
    val VARCHAR = """varchar\(\s*(\d+)\s*\)""".r
    val ARRAY = """array<\s*(.+)\s*>""".r
    val MAP = """map<\s*(.+)\s*>""".r
    val STRUCT = """struct<\s*(.+)\s*>""".r

    typeInfo.toLowerCase match {
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
      case ARRAY(elemType) => ArrayType(odpsTypeStr2SparkType(elemType))
      case MAP(types) =>
        val List(keyType, valType) = splitTypes(types)
        MapType(odpsTypeStr2SparkType(keyType), odpsTypeStr2SparkType(valType))
      case STRUCT(types) =>
        val elemTypes = splitTypes(types).map(elem => {
          val Array(name, typeStr) = elem.split(":", 2)
          StructField(name, odpsTypeStr2SparkType(typeStr))
        })
        StructType(elemTypes)
      case _ =>
        throw new Exception(s"ODPS data type: $typeInfo not supported!")
    }
  }

  def partitionValueEqualTo(partitionValue: String, t: Any): Boolean = {
    t match {
      case i: Int => partitionValue.toInt == i
      case l: Long => partitionValue.toLong == l
      case d: Double => partitionValue.toDouble == d
      case f: Float => partitionValue.toFloat == f
      case b: Byte => partitionValue.toByte == b
      case s: Short => partitionValue.toShort == s
      case s: String => partitionValue == s
      case b: Boolean => partitionValue.toBoolean == b
      case _ =>
        throw new RuntimeException("Unsupported partition type " + partitionValue.getClass + " " + partitionValue)
    }
  }

  def partitionValueGreaterThan(partitionValue: String, t: Any): Boolean = {
    t match {
      case i: Int => partitionValue.toInt > i
      case l: Long => partitionValue.toLong > l
      case d: Double => partitionValue.toDouble > d
      case f: Float => partitionValue.toFloat > f
      case b: Byte => partitionValue.toByte > b
      case s: Short => partitionValue.toShort > s
      case s: String => partitionValue > s
      case _ =>
        throw new RuntimeException("Unsupported partition type " + partitionValue.getClass + " " + partitionValue)
    }
  }

  def partitionValueGreaterThanOrEqualTo(partitionValue: String, t: Any): Boolean = {
    t match {
      case i: Int => partitionValue.toInt >= i
      case l: Long => partitionValue.toLong >= l
      case d: Double => partitionValue.toDouble >= d
      case f: Float => partitionValue.toFloat >= f
      case b: Byte => partitionValue.toByte >= b
      case s: Short => partitionValue.toShort >= s
      case s: String => partitionValue >= s
      case _ =>
        throw new RuntimeException("Unsupported partition type " + partitionValue.getClass + " " + partitionValue)
    }
  }

  def partitionValueLessThanOrEqualTo(partitionValue: String, t: Any): Boolean = {
    t match {
      case i: Int => partitionValue.toInt <= i
      case l: Long => partitionValue.toLong <= l
      case d: Double => partitionValue.toDouble <= d
      case f: Float => partitionValue.toFloat <= f
      case b: Byte => partitionValue.toByte <= b
      case s: Short => partitionValue.toShort <= s
      case s: String => partitionValue <= s
      case _ =>
        throw new RuntimeException("Unsupported partition type " + partitionValue.getClass + " " + partitionValue)
    }
  }

  def partitionValueLessThan(partitionValue: String, t: Any): Boolean = {
    t match {
      case i: Int => partitionValue.toInt < i
      case l: Long => partitionValue.toLong < l
      case d: Double => partitionValue.toDouble < d
      case f: Float => partitionValue.toFloat < f
      case b: Byte => partitionValue.toByte < b
      case s: Short => partitionValue.toShort < s
      case s: String => partitionValue < s
      case _ =>
        throw new RuntimeException("Unsupported partition type " + partitionValue.getClass + " " + partitionValue)
    }
  }

  def odpsType2SparkType (odpsType: TypeInfo): DataType = {
    odpsTypeStr2SparkType(odpsType.getTypeName.toLowerCase)
  }

  private def nullSafeEval(func: Object => Any): Object => Any =
    (v: Object) => if (v ne null) func(v) else null

  // converting data from Odps-type to Spark-type
  def odpsData2SparkData(t: TypeInfo): Object => Any = {
    val func = t.getOdpsType match {
      case OdpsType.BOOLEAN => (v: Object) => v.asInstanceOf[java.lang.Boolean]
      case OdpsType.DOUBLE => (v: Object) => v.asInstanceOf[java.lang.Double]
      case OdpsType.BIGINT => (v: Object) => v.asInstanceOf[java.lang.Long]
      case OdpsType.DATETIME => (v: Object) =>
        v.asInstanceOf[java.util.Date].getTime / 1000 * 1000000
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
        UTF8String.fromString(char.getValue.substring(0, char.length()))
      }
      case OdpsType.DATE => (v: Object) =>
        DateUtils.getDayOffset(v.asInstanceOf[java.sql.Date]).toInt
      case OdpsType.TIMESTAMP => (v: Object) => v.asInstanceOf[java.sql.Timestamp].getTime * 1000
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

  // converting data from Spark-type to Odps-type
  def sparkData2OdpsData(t: TypeInfo): Object => AnyRef = {
    t.getOdpsType match {
      case OdpsType.BOOLEAN => v: Object => v.asInstanceOf[java.lang.Boolean]
      case OdpsType.DOUBLE => v: Object => v.asInstanceOf[java.lang.Double]
      case OdpsType.BIGINT => v: Object => v.asInstanceOf[java.lang.Long]
      case OdpsType.DATETIME => v: Object =>
        if (v != null) new java.util.Date(v.asInstanceOf[Long]/1000)
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
        if (v != null) new java.sql.Date(v.asInstanceOf[Int].toLong * (3600 * 24 * 1000))
        else null
      case OdpsType.TIMESTAMP => v: Object =>
        if (v != null) new java.sql.Timestamp(v.asInstanceOf[Long]/1000)
        else null
      case OdpsType.FLOAT => v: Object => v.asInstanceOf[java.lang.Float]
      case OdpsType.INT => v: Object => v.asInstanceOf[java.lang.Integer]
      case OdpsType.SMALLINT => v: Object => v.asInstanceOf[java.lang.Short]
      case OdpsType.TINYINT => v: Object => v.asInstanceOf[java.lang.Byte]
      case OdpsType.ARRAY => v: Object =>
        val ti = t.asInstanceOf[ArrayTypeInfo]
        if (v != null) {
          v.asInstanceOf[org.apache.spark.sql.catalyst.expressions.UnsafeArrayData]
            .toArray[Object](odpsType2SparkType(ti.getElementTypeInfo))
            .map(e => sparkData2OdpsData(ti.getElementTypeInfo)(e)).toList.asJava
        } else null
      case OdpsType.BINARY => v: Object => new Binary(v.asInstanceOf[Array[Byte]])
      case OdpsType.MAP => v: Object =>
        val ti = t.asInstanceOf[MapTypeInfo]
        if (v != null) {
          val m = new java.util.HashMap[Object, Object]
          val mapData = v.asInstanceOf[org.apache.spark.sql.catalyst.expressions.UnsafeMapData]
          mapData.keyArray.toArray[Object](odpsType2SparkType(ti.getKeyTypeInfo))
            .zip(
              mapData.valueArray.toArray[Object](
                odpsType2SparkType(ti.getValueTypeInfo)))
            .foreach(p => m.put(
              sparkData2OdpsData(ti.getKeyTypeInfo)(p._1.asInstanceOf[Object]),
              sparkData2OdpsData(ti.getValueTypeInfo)(p._2.asInstanceOf[Object])
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
              odpsType2SparkType(p._2.asInstanceOf[TypeInfo])))
          ).toList.asJava
          new SimpleStruct(ti, l)
        } else null
      }
    }
  }

  def odpsColumn2SparkStructField(odpsColumn: Column, isPartitionColumn: Boolean): StructField = {
    StructField(odpsColumn.getName, TypesConverter.odpsType2SparkType(odpsColumn.getTypeInfo))
  }

  def odpsPartition2SparkMap(odpsPartition: Partition): Map[String, String] = {
    val partSpec = odpsPartition.getPartitionSpec()
    val partitionMap = new mutable.LinkedHashMap[String, String]

    partSpec.keys().asScala.foreach(key => {
      partitionMap.put(key, partSpec.get(key))
    })

    partitionMap.toMap
  }

}
