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

package org.apache.spark.sql.odps.read.columnar

import com.aliyun.odps.table.DataSchema
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.odps.vectorized.OdpsArrowColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters.asScalaBufferConverter

case class WrappedColumnarBatch(var columnarBatch: ColumnarBatch,
                                reuseBatch: Boolean) {

  def updateColumnBatch(root: VectorSchemaRoot,
                        allNames: Seq[String],
                        schema: DataSchema): Unit = {
    if (columnarBatch != null && !reuseBatch) {
      columnarBatch.close()
    }
    val vectors = root.getFieldVectors
    val fields = root.getSchema.getFields
    val fieldNameIdxMap = fields.asScala.map(f => f.getName).zipWithIndex.toMap
    if (allNames.nonEmpty) {
      val arrowVectors =
        allNames.map(name => {
          fieldNameIdxMap.get(name) match {
            case Some(fieldIdx) =>
              new OdpsArrowColumnVector(vectors.get(fieldIdx),
                schema.getColumn(name).get().getTypeInfo)
            case None =>
              throw new RuntimeException("Missing column " + name + " from arrow reader.")
          }
        }).toList
      columnarBatch = new ColumnarBatch(arrowVectors.toArray)
    } else {
      columnarBatch =  new ColumnarBatch(new Array[OdpsArrowColumnVector](0).toArray)
    }
    columnarBatch.setNumRows(root.getRowCount)
  }

  def close(): Unit = {
    if (columnarBatch != null) {
      columnarBatch.close()
    }
  }
}
