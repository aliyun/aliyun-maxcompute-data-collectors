/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import java.util.concurrent.TimeUnit._
import java.util.concurrent.Executors.newFixedThreadPool

import com.aliyun.odps.table.TableIdentifier
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit
import com.aliyun.odps.table.configuration.{ArrowOptions, SplitOptions}
import com.aliyun.odps.table.read.{TableBatchReadSession, TableReadSessionBuilder}
import com.aliyun.odps.{OdpsException, PartitionSpec}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{CatalogTablePartition, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{ExplainUtils, LeafExecNode, SQLExecution}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.sql.odps.OdpsEmptyColumnPartition
import org.apache.spark.sql.odps.OdpsScanPartition
import org.apache.spark.sql.odps.OdpsPartitionReaderFactory
import org.apache.spark.sql.odps.vectorized.OdpsArrowColumnVector
import org.apache.spark.sql.odps.OdpsClient
import org.apache.spark.sql.hive.OdpsOptions
import org.apache.spark.util.ThreadUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

case class OdpsTableScanExec(
                              @transient relation: HiveTableRelation,
                              output: Seq[Attribute],
                              requiredSchema: StructType,
                              dataColumns: StructType,
                              partitionColumns: StructType,
                              readDataSchema: StructType,
                              readPartitionSchema: StructType,
                              partitionFilters: Seq[Expression],
                              bucketFilters: Seq[Expression],
                              dataFilters: Seq[Expression])(
                              @transient private val sparkSession: SparkSession)
  extends LeafExecNode {

  require(partitionFilters.isEmpty || relation.isPartitioned,
    "Partition pruning predicates only supported for partitioned tables.")

  override def conf: SQLConf = sparkSession.sessionState.conf

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "splitDataTime" -> SQLMetrics.createTimingMetric(sparkContext, "split data time (ms)"),
  ) ++ {
    // Tracking scan time has overhead, we can't afford to do it for each row, and can only do
    // it for each batch.
    if (supportsColumnar) {
      Some("scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))
    } else {
      None
    }
  } ++ {
    if (relation.partitionCols.nonEmpty) {
      Map(
        "pruningTime" ->
          SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"))
    } else {
      Map.empty[String, SQLMetric]
    }
  }

  @transient
  private lazy val hadoopConf = sparkSession.sessionState.newHadoopConf()

  // Note that some vals referring the file-based relation are lazy intentionally
  // so that this plan can be canonicalized on executor side too. See SPARK-23731.
  override lazy val supportsColumnar: Boolean = {
    if (readDataSchema.isEmpty && readPartitionSchema.isEmpty) false else true
  }

  override def vectorTypes: Option[Seq[String]] = {
    Option(
      Seq.fill(output.length)(classOf[OdpsArrowColumnVector].getName))
  }

  private lazy val driverMetrics: HashMap[String, Long] = HashMap.empty

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has
   * been initialized. See SPARK-26327 for more details.
   */
  private def sendDriverMetrics(): Unit = {
    driverMetrics.foreach(e => metrics(e._1).add(e._2))
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics.filter(e => driverMetrics.contains(e._1)).values.toSeq)
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

  @transient lazy val selectedPartitions: Array[CatalogTablePartition] = {
    val project = relation.tableMeta.database
    if ("true".equals(sparkSession.sessionState.conf.
      getConfString("spark.sql.runSQLOnFiles", "true"))) {
      val allowFullScan = sparkSession.sessionState.catalog.getDatabaseMetadata(project)
        .properties.getOrElse("odps.sql.allow.fullscan", "true").toBoolean
      if (!allowFullScan && partitionFilters.isEmpty) {
        throw new OdpsException(s"odps.sql.allow.fullscan is $allowFullScan")
      }
    }
    val startTime = System.nanoTime()
    val ret = sparkSession.sessionState.catalog
      .listPartitionsByFilter(relation.tableMeta.identifier, partitionFilters)
    val timeTakenMs = NANOSECONDS.toMillis((System.nanoTime() - startTime))
    driverMetrics("pruningTime") = timeTakenMs
    ret
  }.toArray

  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
  @transient private lazy val dynamicallySelectedPartitions: Array[CatalogTablePartition] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)

    if (dynamicPartitionFilters.nonEmpty) {
      // call the file index for the files matching all filters except dynamic partition filters
      val predicate = dynamicPartitionFilters.reduce(And)
      val boundPredicate = Predicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      }, Nil)
      val ret = selectedPartitions.filter(
        p => boundPredicate.eval(p.toRow(partitionColumns, conf.sessionLocalTimeZone)))
      ret
    } else {
      selectedPartitions
    }
  }

  @transient
  private val pushedDownFilters =
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, false))
  logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")

  private var metadataTime = 0L

  private def createTableScan(emptyColumn: Boolean,
                              selectedPartitions: Array[CatalogTablePartition]): TableBatchReadSession = {
    val project = relation.tableMeta.database
    val table = relation.tableMeta.identifier.table
    // TODO: support three tier model
    val schema = "default"

    val settings = OdpsClient.get.getEnvironmentSettings
    val provider = OdpsOptions.odpsTableReaderProvider(conf)

    val requiredDataSchema = readDataSchema.map(attr => attr.name).asJava
    val requiredPartitionSchema = readPartitionSchema.map(attr => attr.name).asJava

    val scanBuilder = new TableReadSessionBuilder()
      .identifier(TableIdentifier.of(project, schema, table))
      .requiredDataColumns(requiredDataSchema)
      .requiredPartitionColumns(requiredPartitionSchema)
      .withSettings(settings)
      .withSessionProvider(provider)

    // TODO: bucketIds
    val bucketIds: Seq[Integer] = Nil

    if (bucketIds.nonEmpty) {
      scanBuilder.requiredBucketIds(bucketIds.asJava)
    }

    if (relation.partitionCols.nonEmpty) {
      scanBuilder.requiredPartitions(selectedPartitions.map(partition => {
        val staticPartition = new mutable.LinkedHashMap[String, String]
        relation.partitionCols.foreach { attr =>
          staticPartition.put(attr.name, partition.spec.getOrElse(attr.name,
            throw new IllegalArgumentException(
              s"Partition spec is missing a value for column '$attr.name': $partition")))
        }
        new PartitionSpec(staticPartition.map {
          case (key, value) => key + "=" + value
        }.mkString(","))
      }).toList.asJava)
    }

    val readSizeInBytes = relation.tableMeta.stats.get.sizeInBytes.longValue

    val splitOptions = if (!emptyColumn) {
      val rawSizePerCore = ((readSizeInBytes / 1024 / 1024) /
        SparkContext.getActive.get.defaultParallelism) + 1
      val sizePerCore = math.max(math.min(rawSizePerCore, Int.MaxValue).toInt, 10)
      val splitSizeInMB = math.min(OdpsOptions.odpsSplitSize(conf), sizePerCore)
      SplitOptions.newBuilder().SplitByByteSize(splitSizeInMB * 1024L * 1024L).build()
    } else {
      SplitOptions.newBuilder().SplitByRowOffset().build()
    }

    scanBuilder.withSplitOptions(splitOptions)
      .withArrowOptions(ArrowOptions.newBuilder()
        .withDatetimeUnit(TimestampUnit.MILLI)
        .withTimestampUnit(TimestampUnit.MICRO).build())

    val scan = scanBuilder.buildBatchReadSession
    logInfo(s"Create table scan ${scan.getId} for ${scan.getTableIdentifier}")
    scan
  }

  private def createPartitions(): Array[InputPartition] = {
    if (relation.partitionCols.nonEmpty) {
      if (dynamicallySelectedPartitions.isEmpty) {
        return Array.empty
      }
    }
    val emptyColumn =
      if (readDataSchema.isEmpty && readPartitionSchema.isEmpty) true else false

    if (!emptyColumn) {
      if (relation.partitionCols.nonEmpty) {
        val partSplits = collection.mutable.Map[Int, ArrayBuffer[CatalogTablePartition]]()
        val splitPar = OdpsOptions.odpsSplitSessionParallelism(conf)
        val concurrentNum = Math.min(Math.max(splitPar, dynamicallySelectedPartitions.length / 200), 16)

        dynamicallySelectedPartitions.zipWithIndex.foreach {
          case (x, i) =>
            val key = if (concurrentNum == 1) 1 else i % concurrentNum
            partSplits.getOrElse(key, {
              val pList = ArrayBuffer[CatalogTablePartition]()
              partSplits.put(key, pList)
              pList
            }) += x
        }

        import OdpsTableScanExec._

        val future = Future.sequence(partSplits.keys.map(key =>
          Future[Array[InputPartition]] {
            val scan = createTableScan(emptyColumn, partSplits(key).toArray)
            scan.getInputSplitAssigner.getAllSplits
              .map(split => OdpsScanPartition(split, scan))
          }(executionContext)
        ))
        val futureResults = ThreadUtils.awaitResult(future, Duration(15, MINUTES))
        futureResults.flatten.toArray
      } else {
        val scan = createTableScan(emptyColumn, Array.empty)
        scan.getInputSplitAssigner.getAllSplits
          .map(split => OdpsScanPartition(split, scan))
      }
    } else {
      val scan = if (relation.partitionCols.nonEmpty) {
        createTableScan(emptyColumn, dynamicallySelectedPartitions)
      } else {
        createTableScan(emptyColumn, Array.empty)
      }
      Array(OdpsEmptyColumnPartition(scan.getInputSplitAssigner.getTotalRowCount))
    }
  }

  lazy val inputRDD: RDD[InternalRow] = {
    val readerFactory = OdpsPartitionReaderFactory(
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf)),
      readDataSchema,
      readPartitionSchema,
      OdpsOptions.odspVectorizedReaderEnabled(conf),
      OdpsOptions.odpsVectorizedReaderBatchSize(conf),
      true,
      OdpsOptions.odpsTableReaderCompressCodec(conf))

    val startTime = System.nanoTime()
    val partitions = createPartitions()
    val timeTakenMs = NANOSECONDS.toMillis(System.nanoTime() - startTime)
    driverMetrics("splitDataTime") = timeTakenMs

    sendDriverMetrics()
//    require(partitions.forall(readerFactory.supportColumnarReads) ||
//      !partitions.exists(readerFactory.supportColumnarReads),
//      "Cannot mix row-based and columnar input partitions.")
    val supportsColumnar = partitions.exists(readerFactory.supportColumnarReads)
    new DataSourceRDD(
      sparkContext, partitions.map(Seq(_)), readerFactory, supportsColumnar, Map.empty)
  }

  def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.map { r =>
      numOutputRows += 1
      r
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          val startNs = System.nanoTime()
          val res = batches.hasNext
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          res
        }

        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          batch
        }
      }
    }
  }

  private def filterUnusedDynamicPruningExpressions(predicates: Seq[Expression]):
        Seq[Expression] = {
    predicates.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral))
  }

  override def doCanonicalize(): OdpsTableScanExec = {
    OdpsTableScanExec(
      relation,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      dataColumns,
      partitionColumns,
      readDataSchema,
      readPartitionSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters), output),
      Seq.empty,
      QueryPlan.normalizePredicates(dataFilters, output))(sparkSession)
  }

  override def otherCopyArgs: Seq[AnyRef] = Seq(sparkSession)

  override def nodeName: String = s"Scan odps ${relation.tableMeta.qualifiedName}"

  // Metadata that describes more details of this scan.
  lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")
    Map(
      "Format" -> "Odps",
      "ReadSchema" -> requiredSchema.catalogString,
      "Batched" -> supportsColumnar.toString,
      "PartitionFilters" -> seqToString(partitionFilters),
      "PushedFilters" -> seqToString(pushedDownFilters),
      "DataFilters" -> seqToString(dataFilters))
    // TODO: bucket
  }

  protected val maxMetadataValueLength = conf.maxMetadataStringLength

  protected val nodeNamePrefix: String = ""

  override def simpleString(maxFields: Int): String = {
    val metadataEntries = metadata.toSeq.sorted.map {
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(redact(value), maxMetadataValueLength)
    }
    val metadataStr = truncatedString(metadataEntries, " ", ", ", "", maxFields)
    redact(
      s"$nodeNamePrefix$nodeName${truncatedString(output, "[", ",", "]", maxFields)}$metadataStr")
  }

  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted.filterNot {
      case (_, value) if (value.isEmpty || value.equals("[]")) => true
      case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
      case (_, _) => false
    }.map {
      case (key, value) => s"$key: ${redact(value)}"
    }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  /**
   * Shorthand for calling redactString() without specifying redacting rules
   */
  protected def redact(text: String): String = {
    Utils.redact(conf.stringRedactionPattern, text)
  }

}

object OdpsTableScanExec {
  private val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(16))
}
