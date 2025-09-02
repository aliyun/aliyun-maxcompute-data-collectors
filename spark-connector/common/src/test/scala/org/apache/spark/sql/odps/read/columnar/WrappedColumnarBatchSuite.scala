package org.apache.spark.sql.odps.read.columnar

import org.apache.spark.sql.odps.vectorized.OdpsArrowColumnVector

class WrappedColumnarBatchSuite extends BaseColumnarReaderSuite {

  test("create WrappedColumnarBatch with reuseBatch = true") {
    val wrappedBatch = WrappedColumnarBatch(null, reuseBatch = true)
    assert(wrappedBatch.reuseBatch)
    assert(wrappedBatch.columnarBatch == null)
  }

  test("create WrappedColumnarBatch with reuseBatch = false") {
    val wrappedBatch = WrappedColumnarBatch(null, reuseBatch = false)
    assert(!wrappedBatch.reuseBatch)
    assert(wrappedBatch.columnarBatch == null)
  }

  test("updateColumnBatch with valid data") {
    val wrappedBatch = WrappedColumnarBatch(null, reuseBatch = false)
    wrappedBatch.updateColumnBatch(generateRoot(), Seq("id", "name"), getSchema)

    assert(wrappedBatch.columnarBatch != null)
    assert(wrappedBatch.columnarBatch.numRows() == 3)
    assert(wrappedBatch.columnarBatch.numCols() == 2)

    val idColumn = wrappedBatch.columnarBatch.column(0).asInstanceOf[OdpsArrowColumnVector]
    val nameColumn = wrappedBatch.columnarBatch.column(1).asInstanceOf[OdpsArrowColumnVector]

    assert(idColumn.getInt(0) == 1)
    assert(idColumn.getInt(1) == 2)
    assert(idColumn.getInt(2) == 3)

    assert(nameColumn.getUTF8String(0).toString == "test1")
    assert(nameColumn.getUTF8String(1).toString == "test2")
    assert(nameColumn.getUTF8String(2).toString == "test3")
  }

  test("updateColumnBatch with empty column names") {
    val wrappedBatch = WrappedColumnarBatch(null, reuseBatch = false)
    wrappedBatch.updateColumnBatch(generateRoot(), Seq.empty, getSchema)

    assert(wrappedBatch.columnarBatch != null)
    assert(wrappedBatch.columnarBatch.numRows() == 3)
    assert(wrappedBatch.columnarBatch.numCols() == 0)
  }

  test("close method should close columnarBatch") {
    val wrappedBatch = WrappedColumnarBatch(null, reuseBatch = false)
    wrappedBatch.updateColumnBatch(generateRoot(), Seq("id", "name"), getSchema)
    wrappedBatch.close()
    assertThrows[IndexOutOfBoundsException] {
      wrappedBatch.columnarBatch.column(0).getInt(0)
    }
  }

  test("updateColumnBatch with reuseBatch = false should close previous batch") {
    val wrappedBatch = WrappedColumnarBatch(null, reuseBatch = false)
    wrappedBatch.updateColumnBatch(generateRoot(), Seq("id", "name"), getSchema)
    val oldBatch = wrappedBatch.columnarBatch
    assert(wrappedBatch.columnarBatch != null)
    assert(wrappedBatch.columnarBatch.numRows() == 3)
    assert(wrappedBatch.columnarBatch.numCols() == 2)
    wrappedBatch.updateColumnBatch(generateRoot(), Seq("id", "name"), getSchema)
    assertThrows[IndexOutOfBoundsException] {
      oldBatch.column(0).getInt(0)
    }
  }

  test("updateColumnBatch with reuseBatch = true should reuse previous batch") {
    val wrappedBatch = WrappedColumnarBatch(null, reuseBatch = true)
    wrappedBatch.updateColumnBatch(generateRoot(), Seq("id", "name"), getSchema)
    val oldBatch = wrappedBatch.columnarBatch
    assert(wrappedBatch.columnarBatch != null)
    assert(wrappedBatch.columnarBatch.numRows() == 3)
    assert(wrappedBatch.columnarBatch.numCols() == 2)
    wrappedBatch.updateColumnBatch(generateRoot(), Seq("id", "name"), getSchema)
    assert(oldBatch.column(0).getInt(0) == 1)
  }

  test("updateColumnBatch with missing column should throw exception") {
    val wrappedBatch = WrappedColumnarBatch(null, reuseBatch = false)
    val thrown = intercept[RuntimeException] {
      wrappedBatch.updateColumnBatch(generateRoot(), Seq("id", "nonexistent"), getSchema)
    }
    assert(thrown.getMessage.contains("Missing column nonexistent from arrow reader"))
  }

}