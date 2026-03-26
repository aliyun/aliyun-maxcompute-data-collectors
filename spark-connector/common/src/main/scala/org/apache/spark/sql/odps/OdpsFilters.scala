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

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime}

object OdpsFilters {

    private val timeStampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
    private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

    /**
     * Create Odps sql filter.
     */
    def createOdpsSqlFilter(dataTypeMap: Map[String, StructField], filters: Seq[Filter]): Option[String] = {
        if (filters.isEmpty) {
            None
        } else {
            Some(convertibleFilters(filters)
              .map(compileFilter(dataTypeMap, _).getOrElse(""))
              .filter(_.nonEmpty)
              .mkString("(", ") AND (", ")")
            )
        }
    }

    def convertibleFilters(filters: Seq[Filter]): Seq[Filter] = {
        import org.apache.spark.sql.sources._

        def convertibleFiltersHelper(
                                      filter: Filter,
                                      canPartialPushDown: Boolean): Option[Filter] = filter match {
            // At here, it is not safe to just convert one side and remove the other side
            // if we do not understand what the parent filters are.
            //
            // Here is an example used to explain the reason.
            // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
            // convert b in ('1'). If we only convert a = 2, we will end up with a filter
            // NOT(a = 2), which will generate wrong results.
            //
            // Pushing one side of AND down is only safe to do at the top level or in the child
            // AND before hitting NOT or OR conditions, and in this case, the unsupported predicate
            // can be safely removed.
            case And(left, right) =>
                val leftResultOptional = convertibleFiltersHelper(left, canPartialPushDown)
                val rightResultOptional = convertibleFiltersHelper(right, canPartialPushDown)
                (leftResultOptional, rightResultOptional) match {
                    case (Some(leftResult), Some(rightResult)) => Some(And(leftResult, rightResult))
                    case (Some(leftResult), None) if canPartialPushDown => Some(leftResult)
                    case (None, Some(rightResult)) if canPartialPushDown => Some(rightResult)
                    case _ => None
                }

            // The Or predicate is convertible when both of its children can be pushed down.
            // That is to say, if one/both of the children can be partially pushed down, the Or
            // predicate can be partially pushed down as well.
            //
            // Here is an example used to explain the reason.
            // Let's say we have
            // (a1 AND a2) OR (b1 AND b2),
            // a1 and b1 is convertible, while a2 and b2 is not.
            // The predicate can be converted as
            // (a1 OR b1) AND (a1 OR b2) AND (a2 OR b1) AND (a2 OR b2)
            // As per the logical in And predicate, we can push down (a1 OR b1).
            case Or(left, right) =>
                for {
                    lhs <- convertibleFiltersHelper(left, canPartialPushDown)
                    rhs <- convertibleFiltersHelper(right, canPartialPushDown)
                } yield Or(lhs, rhs)
            case Not(pred) =>
                val childResultOptional = convertibleFiltersHelper(pred, canPartialPushDown = false)
                childResultOptional.map(Not)
            case other =>
                Some(other).filter(knownPredicate)
        }
        filters.flatMap { filter =>
            convertibleFiltersHelper(filter, true)
        }
    }

    private def knownPredicate(predicate: Filter): Boolean = {
        import org.apache.spark.sql.sources._

        predicate match {
            case EqualTo(_, value) => knownConstantType(value)
            case EqualNullSafe(_, value) => knownConstantType(value)
            case GreaterThan(_, value) => knownConstantType(value)
            case GreaterThanOrEqual(_, value) => knownConstantType(value)
            case LessThan(_, value) => knownConstantType(value)
            case LessThanOrEqual(_, value) => knownConstantType(value)
            case In(_, values) => values.forall(knownConstantType)
            case StringStartsWith(_, value) => knownConstantType(value)
            case StringEndsWith(_, value) => knownConstantType(value)
            case StringContains(_, value) => knownConstantType(value)
            case IsNull(_) => true
            case IsNotNull(_) => true
            case _ => false
        }
    }

    /**
     * The following is a mapping between Spark SQL types and return types:
     *
     * {{{
     *   BooleanType -> java.lang.Boolean
     *   ByteType -> java.lang.Byte
     *   ShortType -> java.lang.Short
     *   IntegerType -> java.lang.Integer
     *   LongType -> java.lang.Long
     *   FloatType -> java.lang.Float
     *   DoubleType -> java.lang.Double
     *   StringType -> String
     *   DecimalType -> java.math.BigDecimal
     *
     *   DateType -> java.sql.Date if spark.sql.datetime.java8API.enabled is false
     *   DateType -> java.time.LocalDate if spark.sql.datetime.java8API.enabled is true
     *
     *   TimestampType -> java.sql.Timestamp if spark.sql.datetime.java8API.enabled is false
     *   TimestampType -> java.time.Instant if spark.sql.datetime.java8API.enabled is true
     *
     *   BinaryType -> byte array
     *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
     *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
     *   StructType -> org.apache.spark.sql.Row
     * }}}
     */
    private def knownConstantType(value: Any): Boolean = value match {
        case _: String => true
        case _: Int => true
        case _: Long => true
        case _: Double => true
        case _: Float => true
        case _: Short => true
        case _: Byte => true
        case _: Boolean => true
        case _: java.math.BigDecimal => true
        case _: java.sql.Date => true
        case _: java.time.LocalDate => true
        case _: java.sql.Timestamp => true
        case _: java.time.Instant => true
        case _: LocalDateTime => true
        case _ => false
    }

    private def compileFilter(dataTypeMap: Map[String, StructField],
                              expression: Filter): Option[String] = {

        import org.apache.spark.sql.sources._

        expression match {
            case EqualTo(name, value) if dataTypeMap.contains(name) =>
                val castedValue = castLiteralValue(value, dataTypeMap(name))
                Some(String.format("%s = %s", quoteAttribute(name), castedValue))

            case EqualNullSafe(name, value) if dataTypeMap.contains(name) =>
                val castedValue = castLiteralValue(value, dataTypeMap(name))
                Some(String.format("(%1$s IS NULL AND %2$s IS NULL) " +
                  "OR (%1$s IS NOT NULL AND %2$s IS NOT NULL AND %1$s = %2$s)",
                    quoteAttribute(name), castedValue))

            case LessThan(name, value) if dataTypeMap.contains(name) =>
                val castedValue = castLiteralValue(value, dataTypeMap(name))
                Some(String.format("%s < %s", quoteAttribute(name), castedValue))

            case LessThanOrEqual(name, value) if dataTypeMap.contains(name) =>
                val castedValue = castLiteralValue(value, dataTypeMap(name))
                Some(String.format("%s <= %s", quoteAttribute(name), castedValue))

            case GreaterThan(name, value) if dataTypeMap.contains(name) =>
                val castedValue = castLiteralValue(value, dataTypeMap(name))
                Some(String.format("%s > %s", quoteAttribute(name), castedValue))

            case GreaterThanOrEqual(name, value) if dataTypeMap.contains(name) =>
                val castedValue = castLiteralValue(value, dataTypeMap(name))
                Some(String.format("%s >= %s", quoteAttribute(name), castedValue))

            case IsNull(name) if dataTypeMap.contains(name) =>
                Some(String.format("%s IS NULL", quoteAttribute(name)))

            case IsNotNull(name) if dataTypeMap.contains(name) =>
                Some(String.format("%s IS NOT NULL", quoteAttribute(name)))

            case In(name, values) if dataTypeMap.contains(name) =>
                val castedValues = values.map(v => castLiteralValue(v, dataTypeMap(name)))
                Some(String.format("%s IN (%s)", quoteAttribute(name), castedValues.mkString(",")))

            case StringStartsWith(name, value) if dataTypeMap.contains(name) =>
                Some(String.format("%s LIKE '%s%%'", quoteAttribute(name),
                    escapeSpecialCharsForLikePattern(escape(value))))

            case StringEndsWith(name, value) if dataTypeMap.contains(name) =>
                Some(String.format("%s LIKE '%%%s'", quoteAttribute(name),
                    escapeSpecialCharsForLikePattern(escape(value))))

            case StringContains(name, value) if dataTypeMap.contains(name) =>
                Some(String.format("%s LIKE '%%%s%%'", quoteAttribute(name),
                    escapeSpecialCharsForLikePattern(escape(value))))

            case And(left, right) =>
                Some(String.format("(%s) AND (%s)", compileFilter(dataTypeMap, left), compileFilter(dataTypeMap, right)))

            case Or(left, right) =>
                Some(String.format("(%s) OR (%s)", compileFilter(dataTypeMap, left), compileFilter(dataTypeMap, right)))

            case Not(child) =>
                Some(String.format("NOT (%s)", compileFilter(dataTypeMap, child)))

            case _ => None
        }
    }

    /**
     * Cast literal values for filters.
     */
    def castLiteralValue(value: Any, structField: StructField): String = {
        if (value == null) return null
        value match {
            case s: String =>
                s"'" + escape(s) + "'"
            case _: Date | _: LocalDate =>
                s"DATE '$value'"
            case _: Timestamp | _: Instant =>
                val instant = value match {
                    case t: Timestamp => t.toInstant
                    case i: Instant => i
                }
                if (structField.metadata.contains(OdpsUtils.DATETIME_TYPE_STRING_METADATA_KEY)) {
                    s"DATETIME '${dateTimeFormatter.format(instant)}'"
                } else {
                    s"TIMESTAMP '${timeStampFormatter.format(instant)}'"
                }
            case ldt: LocalDateTime =>
                s"TIMESTAMP_NTZ '${timeStampFormatter.format(ldt)}'"
            case _ =>
                value.toString
        }
    }

    /**
     * all Attribute will quote by [[org.apache.spark.sql.catalyst.util#quoteIfNeeded]],
     * so here we need to determine whether the attribute has already been quoted
     */
    def quoteAttribute(value: String): String = {
        if (value.startsWith("`") && value.endsWith("`") && value.length() > 1) {
            value
        } else {
            s"`${value.replace("`", "``")}`"
        }
    }

    def escape(value: String): String = {
        value.replace("'", "\\'")
    }

    // TODO: add escape
    def escapeSpecialCharsForLikePattern(str: String): String = {
        val builder = new StringBuilder
        for (c <- str.toCharArray) {
            c match {
                // TODO: https://help.aliyun.com/zh/maxcompute/user-guide/like-usage
                case '_' =>
                    builder.append("\\\\_")
                case '%' =>
                    builder.append("\\\\%")
                case _ =>
                    builder.append(c)
            }
        }
        builder.toString
    }

}