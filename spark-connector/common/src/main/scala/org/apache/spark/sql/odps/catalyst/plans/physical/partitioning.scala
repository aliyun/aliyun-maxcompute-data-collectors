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

package org.apache.spark.sql.odps.catalyst.plans.physical

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Pmod, Unevaluable}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.odps.catalyst.expressions.OdpsHash
import org.apache.spark.sql.types.{DataType, IntegerType}

import scala.collection.mutable

/**
 * Represents a odps partitioning where rows are split up across partitions based on the hash
 * of `expressions`.  All rows where `expressions` evaluate to the same values are guaranteed to be
 * in the same partition.
 */
case class OdpsHashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[Expression] = expressions

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case h: StatefulOpClusteredDistribution =>
          expressions.length == h.expressions.length && expressions.zip(h.expressions).forall {
            case (l, r) => l.semanticEquals(r)
          }
        case c@ClusteredDistribution(requiredClustering, requireAllClusterKeys, _) =>
          if (requireAllClusterKeys) {
            // Checks `HashPartitioning` is partitioned on exactly same clustering keys of
            // `ClusteredDistribution`.
            c.clustering
            c.areAllClusterKeysMatched(expressions)
          } else {
            expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        case _ => false
      }
    }
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    OdpsHashShuffleSpec(this, distribution)

  /**
   * Returns an expression that will produce a valid partition ID(i.e. non-negative and is less
   * than numPartitions) based on hashing expressions.
   */
  def partitionIdExpression: Expression = Pmod(OdpsHash(expressions), Literal(numPartitions))

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): OdpsHashPartitioning = copy(expressions = newChildren)
}


case class OdpsHashShuffleSpec(
                                partitioning: OdpsHashPartitioning,
                                distribution: ClusteredDistribution) extends ShuffleSpec {

  /**
   * A sequence where each element is a set of positions of the hash partition key to the cluster
   * keys. For instance, if cluster keys are [a, b, b] and hash partition keys are [a, b], the
   * result will be [(0), (1, 2)].
   *
   * This is useful to check compatibility between two `HashShuffleSpec`s. If the cluster keys are
   * [a, b, b] and [x, y, z] for the two join children, and the hash partition keys are
   * [a, b] and [x, z], they are compatible. With the positions, we can do the compatibility check
   * by looking at if the positions of hash partition keys from two sides have overlapping.
   */
  lazy val hashKeyPositions: Seq[mutable.BitSet] = {
    val distKeyToPos = mutable.Map.empty[Expression, mutable.BitSet]
    distribution.clustering.zipWithIndex.foreach { case (distKey, distKeyPos) =>
      distKeyToPos.getOrElseUpdate(distKey.canonicalized, mutable.BitSet.empty).add(distKeyPos)
    }
    partitioning.expressions.map(k => distKeyToPos.getOrElse(k.canonicalized, mutable.BitSet.empty))
  }

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec =>
      partitioning.numPartitions == 1
    case otherHashSpec@OdpsHashShuffleSpec(otherPartitioning, otherDistribution) =>
      // we need to check:
      //  1. both distributions have the same number of clustering expressions
      //  2. both partitioning have the same number of partitions
      //  3. both partitioning have the same number of expressions
      //  4. each pair of partitioning expression from both sides has overlapping positions in their
      //     corresponding distributions.
      distribution.clustering.length == otherDistribution.clustering.length &&
        partitioning.numPartitions == otherPartitioning.numPartitions &&
        partitioning.expressions.length == otherPartitioning.expressions.length && {
        val otherHashKeyPositions = otherHashSpec.hashKeyPositions
        hashKeyPositions.zip(otherHashKeyPositions).forall { case (left, right) =>
          left.intersect(right).nonEmpty
        }
      }
    case ShuffleSpecCollection(specs) =>
      specs.exists(isCompatibleWith)
    case _ =>
      false
  }

  override def canCreatePartitioning: Boolean = {
    // To avoid potential data skew, we don't allow `HashShuffleSpec` to create partitioning if
    // the hash partition keys are not the full join keys (the cluster keys). Then the planner
    // will add shuffles with the default partitioning of `ClusteredDistribution`, which uses all
    // the join keys.
    if (SQLConf.get.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION)) {
      distribution.areAllClusterKeysMatched(partitioning.expressions)
    } else {
      true
    }
  }

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    val exprs = hashKeyPositions.map(v => clustering(v.head))
    OdpsHashPartitioning(exprs, partitioning.numPartitions)
  }

  override def numPartitions: Int = partitioning.numPartitions
}