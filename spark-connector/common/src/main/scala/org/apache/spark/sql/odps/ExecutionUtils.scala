package org.apache.spark.sql.odps

import com.aliyun.odps.table.optimizer.predicate.CompoundPredicate.Operator
import com.aliyun.odps.table.optimizer.predicate._
import org.apache.spark.sql.sources._

import scala.collection.JavaConverters.seqAsJavaListConverter

object ExecutionUtils {
  def convertToOdpsPredicate(filters: Seq[Filter]): Predicate = {
    if (filters.isEmpty) {
      return Predicate.NO_PREDICATE
    }
    new CompoundPredicate(Operator.AND, filters.map(convertToOdpsPredicate).asJava)
  }

  private def convertToOdpsPredicate(filter: Filter): Predicate = filter match {
    case EqualTo(attribute, value) => BinaryPredicate.equals(Attribute.of(attribute), Constant.of(value))
    case GreaterThan(attribute, value) => BinaryPredicate.greaterThan(Attribute.of(attribute), Constant.of(value))
    case GreaterThanOrEqual(attribute, value) => BinaryPredicate.greaterThanOrEqual(Attribute.of(attribute), Constant.of(value))
    case LessThan(attribute, value) => BinaryPredicate.lessThan(Attribute.of(attribute), Constant.of(value))
    case LessThanOrEqual(attribute, value) => BinaryPredicate.lessThanOrEqual(Attribute.of(attribute), Constant.of(value))
    case In(attribute, values) => InPredicate.in(Attribute.of(attribute), values.map(Constant.of).toList.asJava.asInstanceOf[java.util.List[java.io.Serializable]])
    case IsNull(attribute) => UnaryPredicate.isNull(Attribute.of(attribute))
    case IsNotNull(attribute) => UnaryPredicate.notNull(Attribute.of(attribute))
    case And(left, right) => CompoundPredicate.and(convertToOdpsPredicate(left), convertToOdpsPredicate(right))
    case Or(left, right) => CompoundPredicate.or(convertToOdpsPredicate(left), convertToOdpsPredicate(right))
    case Not(child) => CompoundPredicate.not(convertToOdpsPredicate(child))
    case _ => Predicate.NO_PREDICATE
  }
}