package org.apache.spark.sql.odps.read.columnar

import com.aliyun.odps.table.read.split.impl.IndexedInputSplit
import org.apache.spark.sql.odps.OdpsScanPartition
import org.apache.spark.sql.odps.vectorized.OdpsArrowColumnVector
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
 
class SyncPartitionReaderSuite extends BaseColumnarReaderSuite {
 
  test("SyncPartitionReader can be created with valid parameters") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
 
    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
 
    // Create reader
    val reader = new SyncPartitionReader(partition, mockReaderOptions, getAllNames)
 
    assert(reader != null)
    reader.close()
  }
 
  test("SyncPartitionReader next() returns false when no more data") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(false)
 
    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
 
    // Create reader
    val reader = new SyncPartitionReader(partition, mockReaderOptions, getAllNames)
 
    // Test
    assert(!reader.next())
    reader.close()
  }
 
  test("SyncPartitionReader next() returns true and gets data when available") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true)
    when(mockSplitReader.get()).thenReturn(generateRoot())
 
    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
 
    // Create reader
    val reader = new SyncPartitionReader(partition, mockReaderOptions, getAllNames)
 
    // Test
    assert(reader.next())
    val batch = reader.get()
    val idColumn = batch.column(0).asInstanceOf[OdpsArrowColumnVector]
    val nameColumn = batch.column(1).asInstanceOf[OdpsArrowColumnVector]
    assert(idColumn.getInt(0) == 1)
    assert(idColumn.getInt(1) == 2)
    assert(idColumn.getInt(2) == 3)
 
    assert(nameColumn.getUTF8String(0).toString == "test1")
    assert(nameColumn.getUTF8String(1).toString == "test2")
    assert(nameColumn.getUTF8String(2).toString == "test3")
 
    reader.close()
  }
 
  test("SyncPartitionReader handles exception during arrow reader creation") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenThrow(new RuntimeException("Test exception"))
 
    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
 
    // Test - should throw exception
    assertThrows[RuntimeException] {
      new SyncPartitionReader(partition, mockReaderOptions, getAllNames)
    }
  }
 
  test("SyncPartitionReader handles exception in next() method") {
    when(mockReaderOptions.isReuseBatch).thenReturn(false)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenThrow(new RuntimeException("Test exception"))
 
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
 
    val reader = new SyncPartitionReader(partition, mockReaderOptions, getAllNames)
 
    assertThrows[RuntimeException] {
      reader.next()
    }
    reader.close()
  }
 
  test("SyncPartitionReader handles exception in get() method") {
    when(mockReaderOptions.isReuseBatch).thenReturn(false)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
    when(mockSplitReader.hasNext).thenReturn(true)
    when(mockSplitReader.get()).thenThrow(new RuntimeException("Test exception"))
 
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
 
    val reader = new SyncPartitionReader(partition, mockReaderOptions, getAllNames)
 
    assertThrows[RuntimeException] {
      reader.next()
    }
    reader.close()
  }
 
  test("SyncPartitionReader close() closes resources properly") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
 
    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
 
    // Create reader
    val reader = new SyncPartitionReader(partition, mockReaderOptions, getAllNames)
 
    // Test
    reader.close()
    verify(mockSplitReader).close()
  }
 
  test("SyncPartitionReader close() can be called multiple times safely") {
    when(mockReaderOptions.isReuseBatch).thenReturn(true)
    when(mockScan.createArrowReader(any(), any())).thenReturn(mockSplitReader)
 
    // Create test data
    val split = new IndexedInputSplit("0", 10)
    val partition = OdpsScanPartition(Array(split), mockScan)
 
    // Create reader
    val reader = new SyncPartitionReader(partition, mockReaderOptions, getAllNames)
 
    // Test - should not throw exception
    reader.close()
    verify(mockSplitReader, times(1)).close()
    reader.close()
    verify(mockSplitReader, times(1)).close()
  }
}