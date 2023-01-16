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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.TaskContext

case class OdpsWriterFactory(
                              description: WriteJobDescription) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val attemptNumber = TaskContext.get.attemptNumber()
    if (description.supportArrowWriter) {
      if (description.dynamicPartitionColumns.isEmpty) {
        new SingleDirectoryArrowWriter(description, partitionId, attemptNumber)
      } else {
        new DynamicPartitionArrowWriter(description, partitionId, attemptNumber)
      }
    } else {
      if (description.dynamicPartitionColumns.isEmpty) {
        new SingleDirectoryRecordWriter(description, partitionId, attemptNumber);
      } else {
        throw new UnsupportedOperationException("Unsupported dynamic writer with record writer")
      }
    }
  }
}
