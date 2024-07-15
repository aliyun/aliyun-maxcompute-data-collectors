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

import java.util
import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast, GenericInternalRow}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}

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
    throw new AnalysisException(
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
    bucketSpec: Option[OdpsBucketSpec] = None,
    viewText: Option[String] = None)
  extends SupportsPartitionManagement with SupportsRead {

import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import OdpsTableType._

  override def name(): String = tableIdent.toString

  override lazy val schema: StructType = StructType(dataSchema ++ partitionSchema)

  override def partitioning: Array[Transform] = partitionSchema.names.toSeq.asTransforms

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

  private def convertToTablePartitionSpec(
      names: Array[String],
      ident: InternalRow): TablePartitionSpec =
    names.zipWithIndex.map { case (name, index) =>
      val value = Cast(
        BoundReference(index, partitionSchema(name).dataType, nullable = false),
        StringType).eval(ident).toString
      (name, value)
    }.toMap
}

object OdpsTable {
  private val CAPABILITIES = Set(BATCH_READ).asJava
}
