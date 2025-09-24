package org.apache.spark.sql.odps

import com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator
import com.aliyun.odps.table.optimizer.predicate._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructField

import java.sql.{Date, Timestamp}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.TimeZone
import scala.collection.JavaConverters.seqAsJavaListConverter

object ExecutionUtils {


  def convertToOdpsPredicate(filters: Seq[Filter]): Predicate = {
    if (filters.isEmpty) {
      return Predicate.NO_PREDICATE
    }
    new CompoundPredicate(Operator.AND, convertibleFilters(filters, false).map(convertToOdpsPredicate).asJava)
  }

  def createOdpsSqlFilter(dataTypeMap: Map[String, StructField], filters: Seq[Filter], timeZoneId: String = TimeZone.getDefault.getID): Option[String] = {
    if (filters.isEmpty) {
      None
    } else {
      val resultFilters = convertibleFilters(filters, true)
        .map(compileFilter(dataTypeMap, _, timeZoneId))
      if (resultFilters.isEmpty) {
        None
      } else {
        Some(
          resultFilters.mkString("(", ") AND (", ")")
        )
      }
    }
  }

  def convertibleFilters(filters: Seq[Filter], convertToSql: Boolean): Seq[Filter] = {
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
        Some(other).filter(knownPredicate(_, convertToSql))
    }

    filters.flatMap { filter =>
      convertibleFiltersHelper(filter, true)
    }
  }

  private def knownPredicate(predicate: Filter, convertToSql: Boolean): Boolean = {
    predicate match {
      case EqualTo(_, value) => knownConstantType(value, convertToSql)
      case GreaterThan(_, value) => knownConstantType(value, convertToSql)
      case GreaterThanOrEqual(_, value) => knownConstantType(value, convertToSql)
      case LessThan(_, value) => knownConstantType(value, convertToSql)
      case LessThanOrEqual(_, value) => knownConstantType(value, convertToSql)
      case In(_, values) => values.forall(knownConstantType(_, convertToSql))
      case IsNull(_) => true
      case IsNotNull(_) => true
      case EqualNullSafe(_, value) if convertToSql => knownConstantType(value, convertToSql)
      case StringStartsWith(_, value) if convertToSql => knownConstantType(value, convertToSql)
      case StringEndsWith(_, value) if convertToSql => knownConstantType(value, convertToSql)
      case StringContains(_, value) if convertToSql => knownConstantType(value, convertToSql)
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
  private def knownConstantType(value: Any, convertToSql: Boolean): Boolean = value match {
    case _: String => true
    case _: Int => true
    case _: Long => true
    case _: Double => true
    case _: Float => true
    case _: Short => true
    case _: Byte => true
    case _: Boolean => true
    case _: java.math.BigDecimal => true
    case _: java.sql.Date if convertToSql => true
    case _: java.time.LocalDate if convertToSql => true
    case _: java.sql.Timestamp if convertToSql => true
    case _: java.time.Instant if convertToSql => true
    case _: LocalDateTime if convertToSql => true
    case _ => false
  }

  private def convertToOdpsPredicate(filter: Filter): Predicate = filter match {
    case EqualTo(attribute, value) => BinaryPredicate.equals(quoteAttribute(attribute), Constant.of(value))
    case GreaterThan(attribute, value) => BinaryPredicate.greaterThan(quoteAttribute(attribute), Constant.of(value))
    case GreaterThanOrEqual(attribute, value) => BinaryPredicate.greaterThanOrEqual(quoteAttribute(attribute), Constant.of(value))
    case LessThan(attribute, value) => BinaryPredicate.lessThan(quoteAttribute(attribute), Constant.of(value))
    case LessThanOrEqual(attribute, value) => BinaryPredicate.lessThanOrEqual(quoteAttribute(attribute), Constant.of(value))
    case In(attribute, values) => InPredicate.in(quoteAttribute(attribute), values.map(Constant.of).toList.asJava.asInstanceOf[java.util.List[java.io.Serializable]])
    case IsNull(attribute) => UnaryPredicate.isNull(quoteAttribute(attribute))
    case IsNotNull(attribute) => UnaryPredicate.notNull(quoteAttribute(attribute))
    case And(left, right) => CompoundPredicate.and(convertToOdpsPredicate(left), convertToOdpsPredicate(right))
    case Or(left, right) => CompoundPredicate.or(convertToOdpsPredicate(left), convertToOdpsPredicate(right))
    case Not(child) => CompoundPredicate.not(convertToOdpsPredicate(child))
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported filter: $filter")
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

  private def compileFilter(dataTypeMap: Map[String, StructField],
                            expression: Filter,
                            timeZoneId: String): String = {

    import org.apache.spark.sql.sources._

    expression match {
      case EqualTo(name, value) if dataTypeMap.contains(name) =>
        val castedValue = castLiteralValue(value, dataTypeMap(name), timeZoneId)
        String.format("%s = %s", quoteAttribute(name), castedValue)

      case EqualNullSafe(name, value) if dataTypeMap.contains(name) =>
        val castedValue = castLiteralValue(value, dataTypeMap(name), timeZoneId)
        String.format("(%1$s IS NULL AND %2$s IS NULL) " +
          "OR (%1$s IS NOT NULL AND %2$s IS NOT NULL AND %1$s = %2$s)",
          quoteAttribute(name), castedValue)

      case LessThan(name, value) if dataTypeMap.contains(name) =>
        val castedValue = castLiteralValue(value, dataTypeMap(name), timeZoneId)
        String.format("%s < %s", quoteAttribute(name), castedValue)

      case LessThanOrEqual(name, value) if dataTypeMap.contains(name) =>
        val castedValue = castLiteralValue(value, dataTypeMap(name), timeZoneId)
        String.format("%s <= %s", quoteAttribute(name), castedValue)

      case GreaterThan(name, value) if dataTypeMap.contains(name) =>
        val castedValue = castLiteralValue(value, dataTypeMap(name), timeZoneId)
        String.format("%s > %s", quoteAttribute(name), castedValue)

      case GreaterThanOrEqual(name, value) if dataTypeMap.contains(name) =>
        val castedValue = castLiteralValue(value, dataTypeMap(name), timeZoneId)
        String.format("%s >= %s", quoteAttribute(name), castedValue)

      case IsNull(name) if dataTypeMap.contains(name) =>
        String.format("%s IS NULL", quoteAttribute(name))

      case IsNotNull(name) if dataTypeMap.contains(name) =>
        String.format("%s IS NOT NULL", quoteAttribute(name))

      case In(name, values) if dataTypeMap.contains(name) =>
        val castedValues = values.map(v => castLiteralValue(v, dataTypeMap(name), timeZoneId))
        String.format("%s IN (%s)", quoteAttribute(name), castedValues.mkString(","))

      case StringStartsWith(name, value) if dataTypeMap.contains(name) =>
        String.format("%s LIKE '%s%%'", quoteAttribute(name),
          escapeSpecialCharsForLikePattern(escape(value)))

      case StringEndsWith(name, value) if dataTypeMap.contains(name) =>
        String.format("%s LIKE '%%%s'", quoteAttribute(name),
          escapeSpecialCharsForLikePattern(escape(value)))

      case StringContains(name, value) if dataTypeMap.contains(name) =>
        String.format("%s LIKE '%%%s%%'", quoteAttribute(name),
          escapeSpecialCharsForLikePattern(escape(value)))

      case And(left, right) =>
        String.format("(%s) AND (%s)", compileFilter(dataTypeMap, left, timeZoneId), compileFilter(dataTypeMap, right, timeZoneId))

      case Or(left, right) =>
        String.format("(%s) OR (%s)", compileFilter(dataTypeMap, left, timeZoneId), compileFilter(dataTypeMap, right, timeZoneId))

      case Not(child) =>
        String.format("NOT (%s)", compileFilter(dataTypeMap, child, timeZoneId))

      case _ =>
        throw new UnsupportedOperationException(s"Unsupported filter: $expression")
    }
  }

  /**
   * Cast literal values for filters.
   */
  def castLiteralValue(value: Any, structField: StructField, timeZoneId: String): String = {
    if (value == null) return null
    value match {
      case s: String =>
        s"'" + escape(s) + "'"
      case _: Date | _: LocalDate =>
        s"DATE '$value'"
      case _: Timestamp | _: Instant =>
        val instant = value match {
          case t: Timestamp => t.toInstant.atZone(DateTimeUtils.getZoneId(timeZoneId))
          case i: Instant => i
        }
        if (structField.metadata.contains(OdpsUtils.DATETIME_TYPE_STRING_METADATA_KEY)) {
          s"DATETIME '${getDateTimeFormatter(timeZoneId).format(instant)}'"
        } else {
          s"TIMESTAMP '${getTimeStampFormatter(timeZoneId).format(instant)}'"
        }
      case ldt: LocalDateTime =>
        s"TIMESTAMP_NTZ '${ldt.format(getTimeStampFormatter("UTC"))}'"
      case d: java.math.BigDecimal =>
        d.stripTrailingZeros.toPlainString
      case _ =>
        value.toString
    }
  }

  def escape(value: String): String = {
    value.replace("'", "\\'")
  }

  def escapeSpecialCharsForLikePattern(str: String): String = {
    val builder = new StringBuilder
    for (c <- str.toCharArray) {
      c match {
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

  def getDateTimeFormatter(timeZoneId: String): DateTimeFormatter = {
    DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
      .withZone(DateTimeUtils.getZoneId(timeZoneId))
  }

  def getTimeStampFormatter(timeZoneId: String): DateTimeFormatter = {
    new DateTimeFormatterBuilder()
      .appendPattern("uuuu-MM-dd HH:mm:ss")
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .toFormatter
      .withZone(DateTimeUtils.getZoneId(timeZoneId))
  }
}