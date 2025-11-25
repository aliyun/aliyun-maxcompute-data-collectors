package org.apache.spark.sql.odps.read.columnar

import com.aliyun.odps.`type`.TypeInfoFactory
import com.aliyun.odps.table.DataSchema
import com.aliyun.odps.table.configuration.ReaderOptions
import com.aliyun.odps.table.metrics.Metrics
import com.aliyun.odps.table.read.{SplitReader, TableBatchReadSession}
import com.aliyun.odps.{Column, OdpsType}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.arrow.vector.{FieldVector, IntVector, VarCharVector, VectorSchemaRoot}
import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.lang.Thread.UncaughtExceptionHandler
import java.nio.charset.StandardCharsets
import java.util
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable

trait BaseColumnarReaderSuite extends AnyFunSuite with BeforeAndAfterEach {

  private var allocator: RootAllocator = _
  private val schema: DataSchema = new DataSchema(List(
    new Column("id", TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.INT)),
    new Column("name", TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING))
  ).asJava)
  private val allNames: Seq[String] = Seq("id", "name")
  private val roots: util.List[VectorSchemaRoot] = new util.ArrayList[VectorSchemaRoot]()
  private var mockTaskContext: TaskContext = _
  private var originalHandler: UncaughtExceptionHandler = _
  var mockScan: TableBatchReadSession= _
  var mockSplitReader: SplitReader[VectorSchemaRoot] = _
  var mockReaderOptions: ReaderOptions = _

  override def beforeEach(): Unit = {
    originalHandler = Thread.getDefaultUncaughtExceptionHandler
    Thread.setDefaultUncaughtExceptionHandler((t, e) => {
      println("Caught exception in " + t.getName + ": " + e.getMessage)
    })
    allocator = new RootAllocator(Long.MaxValue)
    mockTaskContext = mock(classOf[TaskContext])
    mockScan = mock(classOf[TableBatchReadSession])
    mockSplitReader = mock(classOf[SplitReader[VectorSchemaRoot]])
    mockReaderOptions = mock(classOf[ReaderOptions])
    TaskContext.setTaskContext(mockTaskContext)
    when(mockScan.readSchema).thenReturn(getSchema)
    when(mockSplitReader.currentMetricsValues()).thenReturn(new Metrics())
    when(mockTaskContext.taskMetrics()).thenReturn(new TaskMetrics())

  }

  override def afterEach(): Unit = {
    Thread.setDefaultUncaughtExceptionHandler(originalHandler)
    roots.forEach(_.close())
    roots.clear()
    if (allocator != null) {
      allocator.close()
    }
    TaskContext.unset()
  }

  def generateRoot(nums: Int = 3): VectorSchemaRoot = {
    val idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), List.empty[Field].asJava)
    val nameField = new Field("name", FieldType.nullable(new ArrowType.Utf8), List.empty[Field].asJava)

    val idVector = new IntVector("id", allocator)
    idVector.allocateNew(nums)
    (0 until nums).foreach(i => idVector.setSafe(i, i + 1))
    idVector.setValueCount(nums)

    val nameVector = new VarCharVector("name", allocator)
    nameVector.allocateNew(nums)
    (0 until nums).foreach(i => nameVector.setSafe(i, ("test" + (i + 1)).getBytes(StandardCharsets.UTF_8)))
    nameVector.setValueCount(nums)

    val fields = new util.ArrayList[Field]()
    fields.add(idField)
    fields.add(nameField)
    val vectors = new util.ArrayList[FieldVector]()
    vectors.add(idVector)
    vectors.add(nameVector)

    val root = new VectorSchemaRoot(fields, vectors, nums)
    roots.add(root)
    root
  }

  def getPrivateField[T](obj: Object, name: String): T = {
    val field = obj.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.get(obj).asInstanceOf[T]
  }

  def getSchema: DataSchema = schema

  def getAllNames: Seq[String] = allNames

  def metricsValueNonReuse(batch: ColumnarBatch,
                           numRowsArray: mutable.ArrayBuilder[Int],
                           numColsArray: mutable.ArrayBuilder[Int],
                           column0Array: mutable.ArrayBuilder[Int],
                           column1Array: mutable.ArrayBuilder[String]): Unit = {
    numRowsArray += batch.numRows()
    numColsArray += batch.numCols()
    (0 until batch.numRows()).foreach(column0Array += batch.column(0).getInt(_))
    (0 until batch.numRows()).foreach(column1Array += batch.column(1).getUTF8String(_).toString)
  }
}