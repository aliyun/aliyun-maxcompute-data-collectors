package org.apache.spark.sql.odps.read.columnar

import com.aliyun.odps.table.configuration.ReaderOptions
import com.aliyun.odps.table.metrics.Metrics
import com.aliyun.odps.table.read.SplitReader
import com.aliyun.odps.table.read.split.InputSplit
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.sql.odps.OdpsScanPartition
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import java.util.concurrent.{BlockingQueue, ExecutorService, Semaphore}
import scala.collection.mutable.ArrayBuffer

class AsyncPartitionReaderSuite extends BaseColumnarReaderSuite {

  test("AsyncPartitionReader can be created with valid parameters in reuseBatch mode") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10), new IndexedInputSplit("0", 20))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, true, getAllNames, 4)

    assert(reader != null)
    assert(getPrivateField[Int](reader, "queueSize") == 2)
    assert(getPrivateField[Array[Semaphore]](reader, "semaphores")(0) != null)
  }

  test("AsyncPartitionReader can be created with valid parameters in non-reuseBatch mode") {
    when(mockReaderOptions.isReuseBatch).thenReturn(false)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10), new IndexedInputSplit("0", 20))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader with larger queue size
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, false, getAllNames, 4)

    assert(reader != null)
    assert(getPrivateField[Int](reader, "queueSize") == 4)
    assert(getPrivateField[Array[Semaphore]](reader, "semaphores")(0) == null)
  }

  test("AsyncPartitionReader next() returns false when no more data in reuseBatch mode") {
    val mockSplitReader2 = mock(classOf[SplitReader[VectorSchemaRoot]])

    // Setup mocks
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader, mockSplitReader2)
    when(mockSplitReader.hasNext).thenReturn(false)
    when(mockSplitReader2.hasNext).thenReturn(false)
    when(mockSplitReader2.currentMetricsValues()).thenReturn(new Metrics())

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10), new IndexedInputSplit("0", 20))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, true, getAllNames, 2)

    Thread.sleep(100)
    assert(!reader.next())
    verify(mockSplitReader).close()
    verify(mockSplitReader2).close()
  }

  test("AsyncPartitionReader next() returns false when no more data in non-reuseBatch mode") {
    val mockSplitReader2 = mock(classOf[SplitReader[VectorSchemaRoot]])

    // Setup mocks
    when(mockReaderOptions.isReuseBatch).thenReturn(false)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader, mockSplitReader2)
    when(mockSplitReader.hasNext).thenReturn(false)
    when(mockSplitReader2.hasNext).thenReturn(false)
    when(mockSplitReader2.currentMetricsValues()).thenReturn(new Metrics())

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10), new IndexedInputSplit("1", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, false, getAllNames, 8)

    Thread.sleep(100)
    assert(!reader.next())
    verify(mockSplitReader).close()
    verify(mockSplitReader2).close()
  }

  test("AsyncPartitionReader next() returns true and gets data when available in reuseBatch mode") {
    val mockSplitReader2 = mock(classOf[SplitReader[VectorSchemaRoot]])

    // Setup mocks
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader, mockSplitReader2)
    when(mockSplitReader.hasNext).thenReturn(true, true, false)
    when(mockSplitReader2.hasNext).thenReturn(true, true, false)
    when(mockSplitReader.get()).thenReturn(generateRoot(1), generateRoot(2))
    when(mockSplitReader2.get()).thenReturn(generateRoot(3), generateRoot(4))
    when(mockSplitReader2.currentMetricsValues()).thenReturn(new Metrics())

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10), new IndexedInputSplit("1", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, true, getAllNames, 2)

    // Test - should get data from both splits
    assert(reader.next())
    val batch1 = reader.get()
    assert(reader.next())
    val batch2 = reader.get()
    assert(reader.next())
    val batch3 = reader.get()
    assert(reader.next())
    val batch4 = reader.get()
    assert(batch1 != null)
    assert(batch2 != null)
    assert(batch3 != null)
    assert(batch4 != null)
    val numRowsArray = Array(batch1.numRows(), batch2.numRows(), batch3.numRows(), batch4.numRows())
    val numColsArray = Array(batch1.numCols(), batch2.numCols(), batch3.numCols(), batch4.numCols())
    val column0Array = Array(batch1, batch2, batch3, batch4)
      .flatMap(batch => (0 until batch.numRows()).map(i => batch.column(0).getInt(i)))
    val column1Array = Array(batch1, batch2, batch3, batch4)
      .flatMap(batch => (0 until batch.numRows()).map(i => batch.column(1).getUTF8String(i).toString))

    assert(numRowsArray.sorted sameElements Array(1, 2, 3, 4))
    assert(numColsArray.sorted sameElements Array(2, 2, 2, 2))
    assert(column0Array.sorted sameElements Array(1, 1, 1, 1, 2, 2, 2, 3, 3, 4))
    assert(column1Array.sorted sameElements Array("test1", "test1", "test1", "test1", "test2", "test2", "test2", "test3", "test3", "test4"))

    // No more data
    Thread.sleep(100)
    assert(!reader.next())
    reader.close()
    verify(mockSplitReader).close()
    verify(mockSplitReader2).close()
  }

  test("AsyncPartitionReader next() returns true and gets data when available in non-reuseBatch mode") {
    val mockSplitReader2 = mock(classOf[SplitReader[VectorSchemaRoot]])
    val mockReaderOptions = mock(classOf[ReaderOptions])

    // Setup mocks
    when(mockReaderOptions.isReuseBatch).thenReturn(false)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader, mockSplitReader2)
    when(mockSplitReader.hasNext).thenReturn(true, true, false)
    when(mockSplitReader2.hasNext).thenReturn(true, true, false)
    when(mockSplitReader.get()).thenReturn(generateRoot(1), generateRoot(2))
    when(mockSplitReader2.get()).thenReturn(generateRoot(3), generateRoot(4))
    when(mockSplitReader2.currentMetricsValues()).thenReturn(new Metrics())

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10), new IndexedInputSplit("1", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, false, getAllNames, 8)

    val numRowsArray = Array.newBuilder[Int]
    val numColsArray = Array.newBuilder[Int]
    val column0Array = Array.newBuilder[Int]
    val column1Array = Array.newBuilder[String]

    // Test - should get data from both splits
    assert(reader.next())
    var batch = reader.get()
    assert(batch != null)
    metricsValueNonReuse(batch, numRowsArray, numColsArray, column0Array, column1Array)
    assert(reader.next())
    batch = reader.get()
    assert(batch != null)
    metricsValueNonReuse(batch, numRowsArray, numColsArray, column0Array, column1Array)
    assert(reader.next())
    batch = reader.get()
    assert(batch != null)
    metricsValueNonReuse(batch, numRowsArray, numColsArray, column0Array, column1Array)
    assert(reader.next())
    batch = reader.get()
    assert(batch != null)
    metricsValueNonReuse(batch, numRowsArray, numColsArray, column0Array, column1Array)

    assert(numRowsArray.result().sorted sameElements Array(1, 2, 3, 4))
    assert(numColsArray.result().sorted sameElements Array(2, 2, 2, 2))
    assert(column0Array.result().sorted sameElements Array(1, 1, 1, 1, 2, 2, 2, 3, 3, 4))
    assert(column1Array.result().sorted sameElements Array("test1", "test1", "test1", "test1", "test2", "test2", "test2", "test3", "test3", "test4"))

    // No more data
    assert(!reader.next())
    verify(mockSplitReader).close()
    verify(mockSplitReader2).close()
    reader.close()
  }

  test("AsyncPartitionReader close() can be called multiple times safely in reuseBatch mode") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(false)

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, true, getAllNames, 2)

    // Test - should not throw exception
    reader.close()
    Thread.sleep(100)
    verify(mockSplitReader, times(1)).close()
    reader.close()
    verify(mockSplitReader, times(1)).close()
  }

  test("AsyncPartitionReader close() can be called multiple times safely in non-reuseBatch mode") {
    when(mockReaderOptions.isReuseBatch).thenReturn(false)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(false)

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, false, getAllNames, 4)

    // Test - should not throw exception
    reader.close()
    Thread.sleep(100)
    verify(mockSplitReader, times(1)).close()
    reader.close()
    verify(mockSplitReader, times(1)).close()
  }

  test("AsyncPartitionReader close() can be called in reuseBatch mode as expected") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true).thenReturn(false)
    when(mockSplitReader.get()).thenReturn(generateRoot())

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, true, getAllNames, 2)

    // Test - should not throw exception
    while (reader.next()) {
      reader.get()
    }

    // Test - should not throw exception
    Thread.sleep(100)
    verify(mockSplitReader).close()
    // columnarBatch should not be closed
    val columnarBatch = reader.getClass.getDeclaredField("columnarBatch")
    columnarBatch.setAccessible(true)
    val batch = spy(columnarBatch.get(reader).asInstanceOf[ColumnarBatch])
    columnarBatch.set(reader, batch)
    reader.close()
    Thread.sleep(100)
    verify(batch, times(0)).close()
  }

  test("AsyncPartitionReader close() should not be called in non-reuseBatch mode as expected") {
    when(mockReaderOptions.isReuseBatch).thenReturn(false)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true).thenReturn(false)
    when(mockSplitReader.get()).thenReturn(generateRoot())

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, false, getAllNames, 2)

    // Test - should not throw exception
    while (reader.next()) {
      reader.get()
    }
    Thread.sleep(100)
    verify(mockSplitReader).close()
    val columnarBatch = reader.getClass.getDeclaredField("columnarBatch")
    columnarBatch.setAccessible(true)
    val batch = spy(columnarBatch.get(reader).asInstanceOf[ColumnarBatch])
    columnarBatch.set(reader, batch)
    verify(batch, times(0)).close()
    reader.close()
    Thread.sleep(100)
    verify(batch, times(1)).close()
  }

  test("AsyncPartitionReader handles exception in AsyncSingleReaderTask createReader") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenThrow(new RuntimeException("Test Exception"))

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    assertThrows[RuntimeException] {
      new AsyncPartitionReader(partition, mockReaderOptions, true, getAllNames, 1)
    }
  }

  test("AsyncPartitionReader handles exception in AsyncSingleReaderTask in reuseBatch mode") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true, true, false)
    when(mockSplitReader.get()).thenReturn(generateRoot()).thenThrow(new RuntimeException("Test exception"))

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = spy(new AsyncPartitionReader(partition, mockReaderOptions, true, getAllNames, 2))

    assert(reader.next())
    val batch = reader.get()
    assert(batch.numRows() == 3)
    assert(batch.numCols() == 2)
    assert(batch.column(0).getInt(0) == 1)
    assert(batch.column(1).getUTF8String(0).toString == "test1")

    // reuse-batch mode will not call next before semaphore releases
    Thread.sleep(100)
    verify(mockSplitReader, times(0)).close()

    // Test - should propagate exception
    assertThrows[SingleReaderTaskException] {
      reader.next()
    }

    Thread.sleep(100)
    verify(mockSplitReader, times(1)).close()
    // AsyncPartitionReader will call close when encounter exception in next
    verify(reader).close()
  }

  test("AsyncPartitionReader handles exception in AsyncSingleReaderTask in non-reuseBatch mode") {
    when(mockReaderOptions.isReuseBatch).thenReturn(false)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true).thenReturn(true)
    when(mockSplitReader.get()).thenReturn(generateRoot()).thenThrow(new RuntimeException("Test exception"))

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = spy(new AsyncPartitionReader(partition, mockReaderOptions, false, getAllNames, 4))

    assert(reader.next())
    val batch = reader.get()
    assert(batch.numRows() == 3)
    assert(batch.numCols() == 2)
    assert(batch.column(0).getInt(0) == 1)
    assert(batch.column(1).getUTF8String(0).toString == "test1")

    // non-reuse-batch mode will call next, so the reader should have been closed
    Thread.sleep(100)
    verify(mockSplitReader, times(1)).close()

    // Test - should propagate exception
    assertThrows[SingleReaderTaskException] {
      reader.next()
    }

    Thread.sleep(100)
    verify(mockSplitReader, times(1)).close()
    verify(reader).close()
  }

  test("AsyncPartitionReader handles InterruptedException in AsyncSingleReaderTask in reuseBatch mode") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true)
    when(mockSplitReader.hasNext).thenReturn(true, true, false)
    when(mockSplitReader.get()).thenReturn(generateRoot(), generateRoot())

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, true, getAllNames, 2)

    assert(reader.next())
    val batch = reader.get()
    assert(batch.numRows() == 3)
    assert(batch.numCols() == 2)
    assert(batch.column(0).getInt(0) == 1)
    assert(batch.column(1).getUTF8String(0).toString == "test1")

    // the single task thread now wait in put, interrupt it
    val executor = getPrivateField(reader, "multiSplitsExecutor").asInstanceOf[ExecutorService]
    executor.shutdownNow()

    Thread.sleep(100)
    verify(mockSplitReader).close()
  }

  test("AsyncPartitionReader handles InterruptedException in AsyncSingleReaderTask in non-reuseBatch mode") {
    val testVectorSchemaRoot = spy(generateRoot())
    val mockSplitReader2 = mock(classOf[SplitReader[VectorSchemaRoot]])

    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader, mockSplitReader2)
    when(mockSplitReader.hasNext).thenReturn(true, false)
    when(mockSplitReader.get()).thenReturn(generateRoot())
    when(mockSplitReader2.hasNext).thenReturn(true, false)
    when(mockSplitReader2.get()).thenReturn(testVectorSchemaRoot)
    when(mockSplitReader2.currentMetricsValues()).thenReturn(new Metrics())

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10), new IndexedInputSplit("1", 20))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, false, getAllNames, 1)

    // the single task thread now wait in put, interrupt it
    val executor = getPrivateField(reader, "multiSplitsExecutor").asInstanceOf[ExecutorService]
    executor.shutdownNow()

    Thread.sleep(500)
    verify(mockSplitReader).close()
    verify(mockSplitReader2).close()
  }

  test("AsyncPartitionReader with non-reuseBatch mode works correctly with larger queue") {
    val mockSplitReader2 = mock(classOf[SplitReader[VectorSchemaRoot]])
    val mockSplitReader3 = mock(classOf[SplitReader[VectorSchemaRoot]])

    // Setup mocks
    when(mockReaderOptions.isReuseBatch).thenReturn(false)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader, mockSplitReader2, mockSplitReader3)
    when(mockScan.readSchema).thenReturn(getSchema)
    when(mockSplitReader.hasNext).thenReturn(true).thenReturn(false)
    when(mockSplitReader2.hasNext).thenReturn(true).thenReturn(false)
    when(mockSplitReader3.hasNext).thenReturn(true).thenReturn(false)
    when(mockSplitReader.get()).thenReturn(generateRoot(1))
    when(mockSplitReader2.get()).thenReturn(generateRoot(2))
    when(mockSplitReader3.get()).thenReturn(generateRoot())
    when(mockSplitReader2.currentMetricsValues()).thenReturn(new Metrics())
    when(mockSplitReader3.currentMetricsValues()).thenReturn(new Metrics())

    // Create test data with more splits
    val splits = Array(
      new IndexedInputSplit("0", 10),
      new IndexedInputSplit("1", 10),
      new IndexedInputSplit("2", 10)
    )
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader with larger queue size
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, false, getAllNames, 8)

    // Test - should process data from all splits
    var count = 0
    val rows = ArrayBuffer[Int]()
    while (reader.next()) {
      val batch = reader.get()
      assert(batch != null)
      rows.append(batch.numRows())
      count += 1
    }

    // Should have processed data from all splits
    assert(count == 3)
    assert(rows.toSet.equals(Set(1, 2, 3)))
    reader.close()
  }

  test("AsyncPartitionReader handles multiple splits correctly in reuseBatch mode") {
    val mockSplitReader2 = mock(classOf[SplitReader[VectorSchemaRoot]])
    val mockSplitReader3 = mock(classOf[SplitReader[VectorSchemaRoot]])

    // Setup mocks
    when(mockReaderOptions.isReuseBatch).thenReturn(false)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader, mockSplitReader2, mockSplitReader3)
    when(mockScan.readSchema).thenReturn(getSchema)
    when(mockSplitReader.hasNext).thenReturn(true).thenReturn(false)
    when(mockSplitReader2.hasNext).thenReturn(true).thenReturn(false)
    when(mockSplitReader3.hasNext).thenReturn(true).thenReturn(false)
    when(mockSplitReader.get()).thenReturn(generateRoot(1))
    when(mockSplitReader2.get()).thenReturn(generateRoot(2))
    when(mockSplitReader3.get()).thenReturn(generateRoot())
    when(mockSplitReader2.currentMetricsValues()).thenReturn(new Metrics())
    when(mockSplitReader3.currentMetricsValues()).thenReturn(new Metrics())

    // Create test data with more splits
    val splits = Array(
      new IndexedInputSplit("0", 10),
      new IndexedInputSplit("1", 10),
      new IndexedInputSplit("2", 10)
    )
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    val reader = new AsyncPartitionReader(partition, mockReaderOptions, true, getAllNames, 2)

    // Test - should process data from all splits
    var count = 0
    val rows = ArrayBuffer[Int]()
    while (reader.next()) {
      val batch = reader.get()
      assert(batch != null)
      rows.append(batch.numRows())
      count += 1
    }

    // Should have processed data from all splits
    assert(count == 3)
    assert(rows.toSet.equals(Set(1, 2, 3)))
    reader.close()
  }

  test("AsyncPartitionReader handles kill action in reuseBatch mode") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true, true, false)
    when(mockSplitReader.get()).thenReturn(generateRoot())

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    var reader: AsyncPartitionReader = null
    var originalReader: AsyncPartitionReader = null
    val mockBatch = mock(classOf[ColumnarBatch])
    val readerThread = new Thread(() => {
      try {
        val mockTaskContext = mock(classOf[TaskContext])
        TaskContext.setTaskContext(mockTaskContext)
        when(mockTaskContext.taskMetrics()).thenReturn(new TaskMetrics())
        originalReader = new AsyncPartitionReader(partition, mockReaderOptions, true, getAllNames, 2)
        reader = spy(originalReader)
        reader.next()
        reader.get()
        reader.next()
      } finally {
        val queue = getPrivateField(originalReader, "asyncQueueForVisit").asInstanceOf[BlockingQueue[Object]]
        queue.put(OdpsSubReaderData(0, mockBatch))
        reader.close()
      }
    })
    readerThread.start()
    Thread.sleep(500)
    readerThread.interrupt()

    Thread.sleep(500)
    verify(reader).close()
    verify(mockSplitReader).close()
    verify(mockBatch, times(0)).close()
  }

  test("AsyncPartitionReader handles kill action in non-reuseBatch mode") {
    when(mockReaderOptions.isReuseBatch).thenReturn(false)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true, true, false)
    when(mockSplitReader.get()).thenReturn(generateRoot())

    // Create test data
    val splits = Array(new IndexedInputSplit("0", 10))
    val partition = OdpsScanPartition(splits.asInstanceOf[Array[InputSplit]], mockScan)

    // Create reader
    var reader: AsyncPartitionReader = null
    var originalReader: AsyncPartitionReader = null
    val mockBatch = mock(classOf[ColumnarBatch])
    val readerThread = new Thread(() => {
      try {
        val mockTaskContext = mock(classOf[TaskContext])
        TaskContext.setTaskContext(mockTaskContext)
        when(mockTaskContext.taskMetrics()).thenReturn(new TaskMetrics())
        originalReader = new AsyncPartitionReader(partition, mockReaderOptions, false, getAllNames, 2)
        reader = spy(originalReader)
        reader.next()
        reader.get()
        reader.next()
      } finally {
        val queue = getPrivateField(originalReader, "asyncQueueForVisit").asInstanceOf[BlockingQueue[Object]]
        queue.put(OdpsSubReaderData(0, mockBatch))
        reader.close()
      }
    })
    readerThread.start()
    Thread.sleep(500)
    readerThread.interrupt()

    Thread.sleep(500)
    verify(reader).close()
    verify(mockSplitReader).close()
    verify(mockBatch).close()
  }
}