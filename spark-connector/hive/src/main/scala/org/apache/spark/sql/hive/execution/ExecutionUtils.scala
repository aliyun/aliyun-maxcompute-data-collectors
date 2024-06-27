package org.apache.spark.sql.hive.execution

import com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator
import com.aliyun.odps.table.optimizer.predicate._
import org.apache.spark.sql.sources._

import java.util
import scala.collection.JavaConverters.seqAsJavaListConverter

object ExecutionUtils {
  def convertToOdpsPredicate(filters: Seq[Filter]): Predicate = {
    if (filters.isEmpty) {
      return Predicate.NO_PREDICATE
    }
    new CompoundPredicate(Operator.AND, filters.map(convertToOdpsPredicate).asJava)
  }

  private def convertToOdpsPredicate(filter: Filter): Predicate = filter match {
    case AlwaysTrue() => Predicate.NO_PREDICATE
    case AlwaysFalse() => RawPredicate.of("true = false")
    case EqualTo(attribute, value) => BinaryPredicate.equals(Constant.of(attribute), Constant.of(value))
    case GreaterThan(attribute, value) => BinaryPredicate.greaterThan(Constant.of(attribute), Constant.of(value))
    case LessThan(attribute, value) => BinaryPredicate.lessThan(Constant.of(attribute), Constant.of(value))
    case In(attribute, values) => InPredicate.in(Constant.of(attribute), values.map(Constant.of).toList.asJava.asInstanceOf[java.util.List[java.io.Serializable]])
    case IsNull(attribute) => UnaryPredicate.isNull(Constant.of(attribute))
    case IsNotNull(attribute) => UnaryPredicate.notNull(Constant.of(attribute))
    case And(left, right) => CompoundPredicate.and(convertToOdpsPredicate(left), convertToOdpsPredicate(right))
    case Or(left, right) => CompoundPredicate.or(convertToOdpsPredicate(left), convertToOdpsPredicate(right))
    case Not(child) => CompoundPredicate.not(convertToOdpsPredicate(child))
    case StringStartsWith(attribute, value) => BinaryPredicate.equals(Constant.of(attribute), Constant.of(value + '%'))
    case StringEndsWith(attribute, value) => BinaryPredicate.equals(Constant.of(attribute), Constant.of('%' + value))
    case StringContains(attribute, value) => BinaryPredicate.equals(Constant.of(attribute), Constant.of('%' + value + '%'))
    case _ => Predicate.NO_PREDICATE
  }
}