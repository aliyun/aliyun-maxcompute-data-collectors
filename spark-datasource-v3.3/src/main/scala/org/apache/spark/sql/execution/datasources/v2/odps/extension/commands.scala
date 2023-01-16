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

package org.apache.spark.sql.execution.datasources.v2.odps.extension

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.datasources.v2.odps.OdpsTable
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

case class ShowColumnsCommand(table: OdpsTable) extends Command {
  override val output: Seq[Attribute] = {
    AttributeReference("col_name", StringType, nullable = false)() :: Nil
  }

  override def children: Seq[LogicalPlan] = Nil

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): ShowColumnsCommand = {
    ShowColumnsCommand(table)
  }
}

case class ShowColumnsExec(output: Seq[Attribute], table: OdpsTable)
  extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    table.schema.map(f => InternalRow(UTF8String.fromString(f.name)))
  }
}

case class TruncateTableExec(table: OdpsTable, refreshCache: () => Unit)
  extends V2CommandExec with LeafExecNode {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    table.catalog.truncateTable(table.tableIdent)
    refreshCache()
    Seq.empty
  }
}

case class OdpsHashRepartition(bucketAttributes: Seq[Attribute],
                               numBuckets: Int,
                               child: LogicalPlan)
  extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): OdpsHashRepartition = {
    copy(child = newChild)
  }
}