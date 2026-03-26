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

import org.apache.spark.sql.catalyst.catalog.BucketSpec

import java.util
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast, GenericInternalRow}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, WriteBuilder}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.odps.OdpsAnalysisException

case class OdpsTableType private(name: String)
object OdpsTableType {
  val EXTERNAL_TABLE = new OdpsTableType("EXTERNAL_TABLE")
  val MANAGED_TABLE = new OdpsTableType("MANAGED_TABLE")
  val VIRTUAL_VIEW = new OdpsTableType("VIRTUAL_VIEW")

  val tableTypes = Seq(EXTERNAL_TABLE, MANAGED_TABLE, VIRTUAL_VIEW)
}

case class SortColumn(name: String, order: String)
object SortColumn {
  def apply(name: String, orderTag: Int): SortColumn = {
    val order = orderTag match {
      case 0 => "asc"
      case 1 => "desc"
    }
    new SortColumn(name, order)
  }
}

case class OdpsBucketSpec(
    clusterType: String,
    numBuckets: Int,
    bucketColumnNames: Seq[String],
    sortColumns: Seq[SortColumn]) extends SQLConfHelper {

  if (numBuckets <= 0 || numBuckets > conf.bucketingMaxBuckets) {
    throw new OdpsAnalysisException(
      s"Number of buckets should be greater than 0 but less than or equal to " +
        s"bucketing.maxBuckets (`${conf.bucketingMaxBuckets}`). Got `$numBuckets`")
  }

  override def toString: String = {
    val typeString = s"cluster type: $clusterType"
    val bucketString = s"bucket columns: [${bucketColumnNames.mkString(", ")}]"
    val sortString = if (sortColumns.nonEmpty) {
      s", sort columns: [${sortColumns.mkString(", ")}]"
    } else {
      ""
    }
    s"$numBuckets buckets, $typeString, $bucketString$sortString"
  }
}

case class OdpsTable(
    catalog: OdpsTableCatalog,
    tableIdent: Identifier,
    tableType: OdpsTableType,
    dataSchema: StructType,
    partitionSchema: StructType,
    stats: OdpsStatistics,
    isTransactional: Boolean,
    isAppend2Table: Boolean,
    bucketSpec: Option[OdpsBucketSpec] = None,
    viewText: Option[String] = None)
  extends SupportsPartitionManagement
    with SupportsRowLevelOperations
    with SupportsMetadataColumns
    with SupportsRead
    with SupportsWrite {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import OdpsTableType._

  override def name(): String = tableIdent.toString

  private val metaSchema = StructType(Array.empty[StructField])

  override lazy val schema: StructType = {
    StructType(metaSchema ++ dataSchema ++ partitionSchema)
  }

  override def partitioning: Array[Transform] = partitionSchema.names.toSeq.asTransforms ++
    bucketSpec.map(convertToBucketSpec).map(_.asTransform)

  override def capabilities(): util.Set[TableCapability] = OdpsTable.CAPABILITIES

  override def createPartition(ident: InternalRow, properties: util.Map[String, String]): Unit =
    catalog.createPartition(tableIdent, ident)

  override def dropPartition(ident: InternalRow): Boolean =
    catalog.dropPartition(tableIdent, ident)

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit = throw new UnsupportedOperationException()

  override def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] =
    throw new UnsupportedOperationException()

  override def listPartitionIdentifiers(
      names: Array[String],
      ident: InternalRow): Array[InternalRow] = {
    val partitionSpec = convertToTablePartitionSpec(names, ident)
    if (ident.numFields == partitionSchema.length) {
      if (catalog.hasPartition(tableIdent, partitionSpec)) Array(ident) else Array.empty
    } else {
      val indexes = names.map(partitionSchema.fieldIndex)
      val dataTypes = names.map(partitionSchema(_).dataType)
      val currentRow = new GenericInternalRow(new Array[Any](names.length))
      catalog.listPartitions(tableIdent)
        .map(OdpsTableCatalog.convertToPartIdent(_, partitionSchema))
        .filter { partition =>
          for (i <- names.indices) {
            currentRow.values(i) = partition.get(indexes(i), dataTypes(i))
          }
          currentRow == ident
        }
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    OdpsScanBuilder(catalog, this, tableIdent, dataSchema, partitionSchema, stats, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    OdpsWriteBuilder(catalog, this, tableIdent, metaSchema, dataSchema, partitionSchema,
      catalog.odpsOptions, bucketSpec, info)
  }

  override def newRowLevelOperationBuilder(info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    throw new UnsupportedOperationException("Row level operation is not supported now")
  }

  private def convertToTablePartitionSpec(
      names: Array[String],
      ident: InternalRow): TablePartitionSpec =
    names.zipWithIndex.map { case (name, index) =>
      val value = Cast(
        BoundReference(index, partitionSchema(name).dataType, nullable = false),
        StringType).eval(ident).toString
      (name, value)
    }.toMap

  private def convertToBucketSpec(bucketSpec: OdpsBucketSpec): BucketSpec =
    BucketSpec(bucketSpec.numBuckets, bucketSpec.bucketColumnNames, bucketSpec.sortColumns.map(_.name))

  override def metadataColumns(): Array[MetadataColumn] = Array.empty
}

object OdpsTable {
  private val CAPABILITIES = Set(BATCH_READ, BATCH_WRITE, OVERWRITE_DYNAMIC, STREAMING_WRITE).asJava
}
