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

package org.apache.spark.sql.execution.datasources.v2.odps

import org.apache.spark.sql.connector.metric.{CustomSumMetric, CustomTaskMetric}

object OdpsMetrics {

  val NUM_SPLITS = "numSplits"

  val SPLIT_SIZE = "splitSize"

  val SPLIT_DATA_TIME = "splitDataTime"

  val PRUNING_TIME = "pruningTime"
}

// Metrics reported by driver
case class OdpsSplitDataTimeMetric() extends CustomSumMetric {
  override def name(): String = OdpsMetrics.SPLIT_DATA_TIME
  override def description(): String = "split data time (ms)"
}

case class OdpsSplitSizeMetric() extends CustomSumMetric {
  override def name(): String = OdpsMetrics.SPLIT_SIZE
  override def description(): String = "split size (bytes)"
}

case class OdpsNumSplitsMetric() extends CustomSumMetric {
  override def name(): String = OdpsMetrics.NUM_SPLITS
  override def description(): String = "number of splits"
}

case class OdpsPruningTimeMetric() extends CustomSumMetric {
  override def name(): String = OdpsMetrics.PRUNING_TIME
  override def description(): String = "pruning time (ms)"
}

case class OdpsSplitDataTimeTaskMetric(value: Long) extends CustomTaskMetric {
  override def name(): String = OdpsMetrics.SPLIT_DATA_TIME
}

case class OdpsSplitSizeTaskMetric(value: Long) extends CustomTaskMetric {
  override def name(): String = OdpsMetrics.SPLIT_SIZE
}

case class OdpsNumSplitsTaskMetric(value: Long) extends CustomTaskMetric {
  override def name(): String = OdpsMetrics.NUM_SPLITS
}

case class OdpsPruningTimeTaskMetric(value: Long) extends CustomTaskMetric {
  override def name(): String = OdpsMetrics.PRUNING_TIME
}




