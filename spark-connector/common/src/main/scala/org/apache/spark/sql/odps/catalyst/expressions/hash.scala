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

package org.apache.spark.sql.odps.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, HashExpression, InterpretedHashFunction}
import org.apache.spark.sql.odps.bucket.OdpsDefaultHasher
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

import scala.annotation.tailrec

/**
 * Simulates Odps's hashing function
 *
 * We should use this hash function for shuffle and bucket of Odps bucket tables, so that
 * we can guarantee shuffle and bucketing have same data distribution
 */
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2, ...) - Returns a hash value of the arguments.")
case class OdpsHash(children: Seq[Expression]) extends HashExpression[Int] {
  // unused for Odps
  override val seed = 0

  override def dataType: DataType = IntegerType

  override def prettyName: String = "odps-hash"

  override protected def hasherClassName: String = classOf[OdpsDefaultHasher].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Int): Int = {
    OdpsHashFunction.hash(value, dataType, this.seed).toInt
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.isNull = FalseLiteral

    val childHash = ctx.freshName("childHash")
    val childrenHash = children.map { child =>
      val childGen = child.genCode(ctx)
      val codeToComputeHash = ctx.nullSafeExec(child.nullable, childGen.isNull) {
        computeHash(childGen.value, child.dataType, childHash, ctx)
      }
      s"""
         |${childGen.code}
         |$childHash = 0;
         |$codeToComputeHash
         |${ev.value} = ${ev.value} + $childHash;
       """.stripMargin
    } ++ List(
      s"""
         |${ev.value} = (${ev.value} ^ (${ev.value} >> 8));
      """.stripMargin
    )

    val hashResultType = CodeGenerator.javaType(dataType)
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = childrenHash,
      funcName = "computeHash",
      extraArguments = Seq(hashResultType -> ev.value),
      returnType = hashResultType,
      makeSplitFunction = body =>
        s"""
           |${hashResultType} $childHash = 0;
           |$body
           |return ${ev.value};
         """.stripMargin,
      foldFunctions = _.map(funcCall => s"${ev.value} = $funcCall;").mkString("\n"))

    ev.copy(code =
      code"""
            |${hashResultType} ${ev.value} = 0;
            |${hashResultType} $childHash = 0;
            |$codes
       """.stripMargin)
  }

  override def eval(input: InternalRow = null): Int = {
    var i = 0
    val len = children.length
    val hashVals = new Array[Int](len)
    while (i < len) {
      hashVals(i) = computeHash(children(i).eval(input), children(i).dataType, seed)
      i += 1
    }
    OdpsDefaultHasher.CombineHashVal(hashVals)
  }

  @tailrec
  private def computeHashWithTailRec(
                                      input: String,
                                      dataType: DataType,
                                      result: String,
                                      ctx: CodegenContext): String = dataType match {
    case NullType => ""
    case BooleanType => genHashBoolean(input, result)
    case ByteType | ShortType | IntegerType => genHashInt(input, result)
    // fix for odps date type
    case DateType => genHashDate(input, result)
    case LongType => genHashLong(input, result)
    case TimestampType | TimestampNTZType => genHashTimestamp(input, result)
    case FloatType => genHashFloat(input, result)
    case DoubleType => genHashDouble(input, result)
    case d: DecimalType => genHashDecimal(ctx, d, input, result)
    case CalendarIntervalType => genHashCalendarInterval(input, result)
    case _: DayTimeIntervalType => genHashLong(input, result)
    case _: YearMonthIntervalType => genHashInt(input, result)
    case BinaryType => genHashBytes(input, result)
    case StringType => genHashString(input, result)
    case ArrayType(et, containsNull) => genHashForArray(ctx, input, result, et, containsNull)
    case MapType(kt, vt, valueContainsNull) =>
      genHashForMap(ctx, input, result, kt, vt, valueContainsNull)
    case StructType(fields) => genHashForStruct(ctx, input, result, fields)
    case udt: UserDefinedType[_] => computeHashWithTailRec(input, udt.sqlType, result, ctx)
  }

  override protected def computeHash(input: String, dataType: DataType, result: String, ctx: CodegenContext): String =
    computeHashWithTailRec(input, dataType, result, ctx)

  override protected def genHashInt(i: String, result: String): String = {
    s"$result = $hasherClassName.hashInt($i);"
  }

  override protected def genHashLong(l: String, result: String): String = {
    s"$result = $hasherClassName.hashLong($l);"
  }

  override protected def genHashBoolean(b: String, result: String): String = {
    s"$result = $hasherClassName.hashBoolean($b);"
  }

  override protected def genHashFloat(f: String, result: String): String = {
    s"$result = $hasherClassName.hashFloat($f);"
  }

  override protected def genHashDouble(d: String, result: String): String = {
    s"$result = $hasherClassName.hashDouble($d);"
  }

  override protected def genHashString(s: String, result: String): String = {
    s"$result = $hasherClassName.hashString($s);"
  }

  override protected def genHashBytes(b: String, result: String): String = {
    s"$result = $hasherClassName.hashUnsafeBytes($b, Platform.BYTE_ARRAY_OFFSET, $b.length);"
  }

  override protected def genHashTimestamp(input: String, result: String): String = {
    s"$result = $hasherClassName.hashTimestamp($input);"
  }

  protected def genHashDate(i: String, result: String): String = {
    s"$result = $hasherClassName.hashDate($i);"
  }

  override protected def genHashDecimal(
                                         ctx: CodegenContext,
                                         d: DecimalType,
                                         input: String,
                                         result: String): String = {
    val precision = d.precision
    val scale = d.scale
    s"$result = $hasherClassName.hashDecimal($input.toJavaBigDecimal(), $precision, $scale);"
  }

  override protected def genHashCalendarInterval(input: String, result: String): String = {
    throw new UnsupportedOperationException("CalendarInterval is not supported yet!")
  }

  override protected def genHashForArray(
                                          ctx: CodegenContext,
                                          input: String,
                                          result: String,
                                          elementType: DataType,
                                          containsNull: Boolean): String = {
    throw new UnsupportedOperationException("Array is not supported yet!")
  }

  override protected def genHashForMap(
                                        ctx: CodegenContext,
                                        input: String,
                                        result: String,
                                        keyType: DataType,
                                        valueType: DataType,
                                        valueContainsNull: Boolean): String = {
    throw new UnsupportedOperationException("Map is not supported yet!")
  }

  override protected def genHashForStruct(
                                           ctx: CodegenContext,
                                           input: String,
                                           result: String,
                                           fields: Array[StructField]): String = {
    throw new UnsupportedOperationException("Struct is not supported yet!")
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): OdpsHash = {
    copy(children = newChildren)
  }
}

object OdpsHashFunction
  extends InterpretedHashFunction {

  override protected def hashInt(i: Int, seed: Long): Long = {
    OdpsDefaultHasher.hashInt(i)
  }

  override protected def hashLong(l: Long, seed: Long): Long = {
    OdpsDefaultHasher.hashLong(l)
  }

  override protected def hashUnsafeBytes(base: AnyRef, offset: Long, len: Int, seed: Long): Long = {
    OdpsDefaultHasher.hashUnsafeBytes(base, offset, len)
  }

  override def hash(value: Any, dataType: DataType, seed: Long): Long = {
    value match {
      case null => 0
      case b: Boolean => OdpsDefaultHasher.hashBoolean(b)
      case b: Byte => OdpsDefaultHasher.hashTinyInt(b)
      case s: Short => OdpsDefaultHasher.hashSmallInt(s)
      case i: Int if dataType.isInstanceOf[DateType] =>
        OdpsDefaultHasher.hashDate(i)
      case i: Int => OdpsDefaultHasher.hashInt(i)
      // TODO: for odps datetime type
      case timestamp: Long if dataType.isInstanceOf[TimestampType] =>
        OdpsDefaultHasher.hashTimestamp(timestamp)
      case l: Long =>  OdpsDefaultHasher.hashLong(l)
      case f: Float => OdpsDefaultHasher.hashFloat(f)
      case d: Double => OdpsDefaultHasher.hashDouble(d)
      case a: Array[Byte] =>
        hashUnsafeBytes(a, Platform.BYTE_ARRAY_OFFSET, a.length, seed)
      case s: UTF8String =>
        hashUnsafeBytes(s.getBaseObject, s.getBaseOffset, s.numBytes(), seed)
      case d: Decimal =>
        OdpsDefaultHasher.hashDecimal(d.toJavaBigDecimal, dataType.asInstanceOf[DecimalType].precision,
          dataType.asInstanceOf[DecimalType].scale)
      case _ => throw new UnsupportedOperationException(s"unsupported type ${dataType}")
    }
  }
}