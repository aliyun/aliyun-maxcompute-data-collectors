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
import org.apache.spark.unsafe.types.CalendarInterval

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

  override protected def genHashDecimal(
      ctx: CodegenContext,
      d: DecimalType,
      input: String,
      result: String): String = {
    throw new UnsupportedOperationException("Decimal is not supported yet!")
  }

  override protected def genHashCalendarInterval(input: String, result: String): String = {
    throw new UnsupportedOperationException("CalendarInterval is not supported yet!")
  }

  override protected def genHashTimestamp(input: String, result: String): String = {
    throw new UnsupportedOperationException("Timestamp is not supported yet!")
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
    assert(len > 0)
    val bytes = new Array[Char](len)
    Platform.copyMemory(base, offset, bytes, Platform.BYTE_ARRAY_OFFSET, len)
    OdpsDefaultHasher.hashUnsafeBytes(base, offset, len)
  }

  def hashTimestamp(timestamp: Long): Long = {
    throw new UnsupportedOperationException("Timestamp type is not supported!")
  }

  def hashCalendarInterval(calendarInterval: CalendarInterval): Long = {
    throw new UnsupportedOperationException("CalendarInterval type is not supported!")
  }

  override def hash(value: Any, dataType: DataType, seed: Long): Long = {
    dataType match {
      case StringType | BinaryType =>
        OdpsDefaultHasher.hashString(
          if (value == null) null.asInstanceOf[String] else String.valueOf(value))
      case LongType =>
        OdpsDefaultHasher.hashLong(
          if (value == null) null.asInstanceOf[Long] else value.asInstanceOf[Long])
      case IntegerType | ShortType =>
        OdpsDefaultHasher.hashInt(
          if (value == null) null.asInstanceOf[Integer] else value.asInstanceOf[Integer])
      case DoubleType =>
        OdpsDefaultHasher.hashDouble(
          if (value == null) null.asInstanceOf[Double] else value.asInstanceOf[Double])
      case FloatType =>
        OdpsDefaultHasher.hashFloat(
          if (value == null) null.asInstanceOf[Float] else value.asInstanceOf[Float])
      case BooleanType =>
        OdpsDefaultHasher.hashBoolean(
          if (value == null) null.asInstanceOf[Boolean] else value.asInstanceOf[Boolean])
      case _ => throw new UnsupportedOperationException(s"unsupported type ${dataType}")
    }
  }
}
