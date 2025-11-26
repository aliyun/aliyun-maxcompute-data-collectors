package org.apache.spark.sql.odps.read.columnar

import com.aliyun.odps.table.read.split.impl.IndexedInputSplit
import org.apache.spark.sql.odps.OdpsScanPartition
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, Semaphore}

class AsyncSingleReaderTaskSuite extends BaseColumnarReaderSuite {

  test("AsyncSingleReaderTask can be created with valid parameters") {
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)

    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
    val schema = partition.scan.readSchema
    val inputBytes = new AtomicLong(0)
    val semaphore = new Semaphore(0)
    val input = OdpsSubReaderInput(
      reuseBatch = false,
      partition = partition,
      schema = schema,
      inputBytes = inputBytes,
      allNames = getAllNames,
      semaphore = semaphore
    )

    val queue: BlockingQueue[Object] = new ArrayBlockingQueue[Object](2)
    val readerClose = new AtomicBoolean(false)
    val endFlag = new Object()

    // Create task
    val task = new AsyncSingleReaderTask(0, mockReaderOptions, input, queue, endFlag, readerClose)

    assert(task != null)
  }

  test("AsyncSingleReaderTask handles normal data flow reuse batch with semaphore") {
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true).thenReturn(false)
    when(mockSplitReader.get()).thenReturn(generateRoot())

    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
    val schema = partition.scan.readSchema
    val inputBytes = new AtomicLong(0)
    val semaphore = new Semaphore(0) // Start with 1 permit so task can run once
    val input = OdpsSubReaderInput(
      reuseBatch = true,
      partition = partition,
      schema = schema,
      inputBytes = inputBytes,
      allNames = getAllNames,
      semaphore = semaphore
    )

    val queue: BlockingQueue[Object] = new ArrayBlockingQueue[Object](2)
    val readerClose = new AtomicBoolean(false)
    val endFlag = new Object()

    // Create and run task in a separate thread
    val task = new AsyncSingleReaderTask(0, mockReaderOptions, input, queue, endFlag, readerClose)
    val thread = new Thread(task)
    thread.start()

    Thread.sleep(500)
    assert(thread.getState == Thread.State.WAITING)
    // Check results
    assert(queue.size() == 1) // One data item
    val data = queue.take()
    semaphore.release()
    assert(data.isInstanceOf[OdpsSubReaderData])
    Thread.sleep(500)
    assert(queue.size() == 1) // One data item
    val end = queue.take()
    assert(end eq endFlag)
  }

  test("AsyncSingleReaderTask handles normal data flow non-reuse batch without semaphore") {
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true).thenReturn(false)
    when(mockSplitReader.get()).thenReturn(generateRoot())

    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
    val schema = partition.scan.readSchema
    val inputBytes = new AtomicLong(0)
    val input = OdpsSubReaderInput(
      reuseBatch = false,
      partition = partition,
      schema = schema,
      inputBytes = inputBytes,
      allNames = getAllNames,
      semaphore = null
    )

    val queue: BlockingQueue[Object] = new ArrayBlockingQueue[Object](2)
    val readerClose = new AtomicBoolean(false)
    val endFlag = new Object()

    // Create and run task in a separate thread
    val task = new AsyncSingleReaderTask(0, mockReaderOptions, input, queue, endFlag, readerClose)
    val thread = new Thread(task)
    thread.start()

    thread.join(2000)
    assert(thread.getState == Thread.State.TERMINATED)
    // Check results
    assert(queue.size() == 2) // One data item and one end flag
    val data = queue.take()
    assert(data.isInstanceOf[OdpsSubReaderData])
    val end = queue.take()
    assert(end eq endFlag)
  }

  test("AsyncSingleReaderTask handles exception in create reader") {
    when(mockScan.createArrowReader(any(), any())).thenThrow(new RuntimeException("Test exception"))

    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
    val schema = partition.scan.readSchema
    val inputBytes = new AtomicLong(0)
    val semaphore = new Semaphore(0)
    val input = OdpsSubReaderInput(
      reuseBatch = true,
      partition = partition,
      schema = schema,
      inputBytes = inputBytes,
      allNames = getAllNames,
      semaphore = semaphore
    )

    val queue: BlockingQueue[Object] = new ArrayBlockingQueue[Object](2)
    val readerClose = new AtomicBoolean(false)
    val endFlag = new Object()

    // Create and run the task
    var task: AsyncSingleReaderTask = null
    assertThrows[RuntimeException] {
      task = new AsyncSingleReaderTask(0, mockReaderOptions, input, queue, endFlag, readerClose)
    }
    val thread = new Thread(task)
    thread.start()

    // Wait for the task to complete
    thread.join(2000)

    // Check that exception was put in the queue
    assert(queue.size() == 0)
  }

  test("AsyncSingleReaderTask handles exception in reader hasNext") {
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenThrow(new RuntimeException("Test exception"))

    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
    val schema = partition.scan.readSchema
    val inputBytes = new AtomicLong(0)
    val semaphore = new Semaphore(0)
    val input = OdpsSubReaderInput(
      reuseBatch = true,
      partition = partition,
      schema = schema,
      inputBytes = inputBytes,
      allNames = getAllNames,
      semaphore = semaphore
    )

    val queue: BlockingQueue[Object] = new ArrayBlockingQueue[Object](2)
    val readerClose = new AtomicBoolean(false)
    val endFlag = new Object()

    // Create and run the task
    val task = new AsyncSingleReaderTask(0, mockReaderOptions, input, queue, endFlag, readerClose)
    val thread = new Thread(task)
    thread.start()

    // Wait for the task to complete
    thread.join(2000)

    assert(queue.size() == 1)
    val exception = queue.take()
    assert(exception.isInstanceOf[SingleReaderTaskException])
  }

  test("AsyncSingleReaderTask handles exception in reader get") {
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true)
    when(mockSplitReader.get()).thenThrow(new RuntimeException("Test exception"))

    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
    val schema = partition.scan.readSchema
    val inputBytes = new AtomicLong(0)
    val semaphore = new Semaphore(0)
    val input = OdpsSubReaderInput(
      reuseBatch = true,
      partition = partition,
      schema = schema,
      inputBytes = inputBytes,
      allNames = getAllNames,
      semaphore = semaphore
    )

    val queue: BlockingQueue[Object] = new ArrayBlockingQueue[Object](2)
    val readerClose = new AtomicBoolean(false)
    val endFlag = new Object()

    // Create and run the task
    val task = new AsyncSingleReaderTask(0, mockReaderOptions, input, queue, endFlag, readerClose)
    val thread = new Thread(task)
    thread.start()

    // Wait for the task to complete
    thread.join(2000)

    // Check that exception was put in the queue
    assert(queue.size() == 1)
    val exception = queue.take()
    assert(exception.isInstanceOf[SingleReaderTaskException])
  }

  test("AsyncSingleReaderTask handles InterruptedException in semaphore.acquire") {
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true)
    when(mockSplitReader.get()).thenReturn(generateRoot())

    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
    val schema = partition.scan.readSchema
    val inputBytes = new AtomicLong(0)
    val semaphore = new Semaphore(0)
    val input = OdpsSubReaderInput(
      reuseBatch = true,
      partition = partition,
      schema = schema,
      inputBytes = inputBytes,
      allNames = getAllNames,
      semaphore = semaphore
    )

    val queue: BlockingQueue[Object] = new ArrayBlockingQueue[Object](2)
    val readerClose = new AtomicBoolean(false)
    val endFlag = new Object()

    @volatile var uncaughtException: Throwable = null
    val originalHandler = Thread.getDefaultUncaughtExceptionHandler
    Thread.setDefaultUncaughtExceptionHandler((t, e) => {
      uncaughtException = e
    })

    // Create and run the task
    val task = spy(new AsyncSingleReaderTask(0, mockReaderOptions, input, queue, endFlag, readerClose))
    val thread = new Thread(task)
    thread.start()

    // Wait for the task to complete
    Thread.sleep(500)
    thread.interrupt()

    assert(uncaughtException == null)
    Thread.setDefaultUncaughtExceptionHandler(originalHandler)
  }
}