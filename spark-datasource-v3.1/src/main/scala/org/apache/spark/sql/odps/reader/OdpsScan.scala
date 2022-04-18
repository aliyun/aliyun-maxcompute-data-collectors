/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.odps.reader

import java.util.Collections
import java.util.Objects

import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.cupid.table.v1.Attribute
import com.aliyun.odps.cupid.table.v1.reader._
import com.aliyun.odps.cupid.table.v1.util.{Options, TableUtils}
import com.aliyun.odps.{Odps, OdpsException, Partition}
import org.apache.spark.sql.odps.converter.TypesConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class OdpsScan(provider: String,
                    //read schema containing both fields and partition fields
                    readDataSchema: StructType,
                    //only partition fields
                    partitionSchema: StructType,
                    options: Options,
                    splitSize: Int,
                    table: String,
                    partitionFilters: Option[Array[Filter]],
                    allowFullScan: Boolean)
  extends Scan with Batch with PartitionReaderFactory {

  override def readSchema(): StructType = {
    val size = readDataSchema.fields.size
    readDataSchema
  }

  override def toBatch: Batch = this

  private lazy val partitions = createPartitions()

  override def planInputPartitions(): Array[InputPartition] = {
    partitions
  }

  override def createReaderFactory(): PartitionReaderFactory = this

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val inputSplit = partition.asInstanceOf[OdpsScanPartition].inputSplit

    val recordReader = new SplitReaderBuilder(inputSplit).buildRecordReader()

    /**
      * table schema what was composed of both partition schema and output DataSchema
      * was different from output DataSchema.
      */
    val outputDataSchema = TableUtils.toColumnArray(inputSplit.getReadDataColumns).map(_.getTypeInfo).toList

    val sparkDataConverters = outputDataSchema.map(TypesConverter.odpsData2SparkData)

    val partitionSpec = if (Objects.nonNull(inputSplit)) {
      inputSplit.getPartitionSpec
    } else {
      Collections.emptyMap[String, String]()
    }

    val partitionColumns = if (Objects.nonNull(inputSplit)) {
      inputSplit.getPartitionColumns
    } else {
      Collections.emptyList[Attribute]()
    }

    new InputSplitReader(
      readDataSchema,
      partitionColumns,
      partitionSpec,
      outputDataSchema,
      sparkDataConverters,
      recordReader)
  }

  private def createPartitions(): Array[InputPartition] = {
    val odpsTable = {
      val akId = options.getOdpsConf.getAccessId
      val akSecret = options.getOdpsConf.getAccessKey
      val endpoint = options.getOdpsConf.getEndpoint
      val project = options.getOdpsConf.getProject

      val odps = {
        val account = new AliyunAccount(akId, akSecret)
        val retOdps = new Odps(account)
        retOdps.setEndpoint(endpoint)
        retOdps.setDefaultProject(project)
        retOdps
      }

      val odpsTable = odps.tables.get(table)
      odpsTable.reload()
      odpsTable
    }

    val partitionNameSet = partitionSchema.fields
      .map(f => f.name)
      .toSet

    val dataSchema = RequiredSchema.columns(
      readSchema().fields
        .filter(f => !partitionNameSet.contains(f.name))
        .map(f => new Attribute(f.name, f.dataType.catalogString))
        .toList.asJava)

    val sessionBuilder = new TableReadSessionBuilder(provider, odpsTable.getProject, table)
      .readDataColumns(dataSchema)
      .options(options)
      .splitBySize(splitSize)

    if (partitionSchema.fields.nonEmpty) {
      if (!allowFullScan && partitionFilters.isEmpty) {
        throw new OdpsException(s"odps.sql.allow.fullscan is $allowFullScan")
      }

      val prunedPartitions = odpsTable.getPartitions.asScala
        .filter { p =>
          partitionFilters
            .getOrElse(new Array[Filter](0))
            .forall(f => filterPartition(p, f))
        }
        .map(TypesConverter.odpsPartition2SparkMap)
        .toList

      if (prunedPartitions.isEmpty) {
        return Array.empty
      }

      val partSpecs = prunedPartitions.map {
        partSpec => new PartitionSpecWithBucketFilter(partSpec.asJava)
      }
      sessionBuilder.readPartitions(partSpecs.asJava)
    }

    sessionBuilder
      .build()
      .getOrCreateInputSplits()
      .map(OdpsScanPartition)
  }

  private def filterPartition(odpsPartition: Partition, f: Filter): Boolean = {
    val partitionMap: Map[String, String] = TypesConverter.odpsPartition2SparkMap(odpsPartition)

    f match {
      case EqualTo(attr, value) =>
        val columnValue: Option[String] = partitionMap.get(attr)
        columnValue.exists(TypesConverter.partitionValueEqualTo(_, value))

      case EqualNullSafe(attr, value) =>
        val columnValue = partitionMap.get(attr)
        columnValue.isEmpty && value == Nil

      case LessThan(attr, value) =>
        val columnValue: Option[String] = partitionMap.get(attr)
        columnValue.exists(TypesConverter.partitionValueLessThan(_, value))

      case GreaterThan(attr, value) =>
        val columnValue = partitionMap.get(attr)
        columnValue.exists(TypesConverter.partitionValueGreaterThan(_, value))

      case LessThanOrEqual(attr, value) =>
        val columnValue = partitionMap.get(attr)
        columnValue.exists(TypesConverter.partitionValueLessThanOrEqualTo(_, value))

      case GreaterThanOrEqual(attr, value) =>
        val columnValue = partitionMap.get(attr)
        columnValue.exists(TypesConverter.partitionValueGreaterThanOrEqualTo(_, value))

      case IsNull(attr) =>
        val columnValue = partitionMap.get(attr)
        columnValue.isEmpty || !partitionMap.contains(attr) || columnValue == null

      case IsNotNull(attr) =>
        val columnValue = partitionMap.get(attr)
        columnValue.isDefined && partitionMap.contains(attr) && columnValue != null

      case StringStartsWith(attr, value) =>
        val columnValue = partitionMap.get(attr)
        val targetValue = value.asInstanceOf[String]
        columnValue.exists(_.startsWith(targetValue))

      case StringEndsWith(attr, value) =>
        val columnValue = partitionMap.get(attr)
        val targetValue = value.asInstanceOf[String]
        columnValue.exists(_.endsWith(targetValue))

      case StringContains(attr, value) =>
        val columnValue = partitionMap.get(attr)
        val targetValue = value.asInstanceOf[String]
        columnValue.exists(_.contains(targetValue))

      case In(attr, value) =>
        val columnValue = partitionMap.get(attr)
        if (columnValue.isEmpty) {
          false
        }
        val strValue = columnValue.getOrElse("")
        value.asInstanceOf[Array[Any]]
          .exists(TypesConverter.partitionValueEqualTo(strValue, _))

      case Not(f: Filter) =>
        !filterPartition(odpsPartition, f)

      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val f1Ret = filterPartition(odpsPartition, f1)
        if (f1Ret) {
          true
        } else {
          filterPartition(odpsPartition, f2)
        }

      case And(f1, f2) =>
        val f1Ret = filterPartition(odpsPartition, f1)

        if (!f1Ret) {
          false
        } else {
          filterPartition(odpsPartition, f2)
        }

      case _ => false
    }
  }

  private def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")
}

case class OdpsScanPartition(inputSplit: InputSplit) extends InputPartition
