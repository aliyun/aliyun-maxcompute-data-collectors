package org.apache.spark.sql.odps.reader.columnar

import com.aliyun.odps.table.DataSchema
import org.apache.arrow.vector.{VectorSchemaRoot, IntVector}

import org.apache.spark.sql.odps.vectorized.OdpsArrowColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch
import scala.jdk.CollectionConverters._

import com.aliyun.odps.`type`.TypeInfoFactory

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
