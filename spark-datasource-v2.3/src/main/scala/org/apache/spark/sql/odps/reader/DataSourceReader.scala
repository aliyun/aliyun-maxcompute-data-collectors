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

import java.util

import com.aliyun.odps.cupid.table.v1.Attribute
import com.aliyun.odps.cupid.table.v1.reader.{InputSplit, PartitionSpecWithBucketFilter, RequiredSchema, TableReadSessionBuilder}
import org.apache.spark.sql.odps.converter._
import org.apache.spark.sql.odps.datasource.{OdpsBaseSource, OdpsSourceOptions}
import com.aliyun.odps.{OdpsException, Partition}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * @author renxiang
 * @date 2021-12-18
 */
class DataSourceReader(dataSourceOptions: DataSourceOptions) extends OdpsBaseSource(dataSourceOptions)
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

    private val _splitSizeInConfig = dataSourceOptions.getInt(OdpsSourceOptions.ODPS_SPLIT_SIZE, 256)

    private val _requiredColumns: ArrayBuffer[StructField] = new ArrayBuffer[StructField]

    private var _partitionFilters: Option[Array[Filter]] = None

    private var _columnFilters: Option[Array[Filter]] = None

    override def pruneColumns(structType: StructType): Unit = {
        structType
          .fields
          .filter(!_requiredColumns.contains(_))
          .foreach(_requiredColumns.append(_))
    }

    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
        // Filters on this table fall into four categories based on where we can use them to avoid
        // reading unneeded data:
        //  - partition keys only - used to prune directories to read
        //  - bucket keys only - optionally used to prune files to read
        //  - keys stored in the data only - optionally used to skip groups of data in files
        //  - filters that need to be evaluated again after the scan

        val partitionKeyFilters = filters
          .filter( f => {
              val references = f.references.toSet
              references.nonEmpty && references.subsetOf(_odpsPartitionNameSet)
          })
          .toSet

        _partitionFilters = if (partitionKeyFilters.nonEmpty) {
            Some(partitionKeyFilters.toArray)
        } else {
            None
        }

        // Predicates with both partition keys and attributes need to be evaluated after the scan.
        _columnFilters = Option((filters.toSet -- partitionKeyFilters).toArray)

        _columnFilters.getOrElse(new Array[Filter](0))
    }

    override def pushedFilters(): Array[Filter] = {
        _partitionFilters.getOrElse(new Array[Filter](0))
    }

    override def readSchema(): StructType = {
      if (_requiredColumns.isEmpty) {
          _schema
      } else {
          val partitionFilterNames = if (_partitionFilters.isDefined) {
              _partitionFilters.get
                .flatMap(f => f.references)
                .toSet
          } else {
              Set.empty
          }

          val normalFilterNames = if (_columnFilters.isDefined) {
              _columnFilters.get
                .flatMap(f => f.references)
                .toSet
          } else {
              Set.empty
          }

          val requiredColumsNames = _requiredColumns.map(f => f.name).toSet

          val filterColumnsSet = partitionFilterNames ++ normalFilterNames ++ requiredColumsNames

          val fields = _schema
            .fields
            .filter(f => filterColumnsSet.contains(f.name))

          StructType(fields)
      }
    }

    override def createDataReaderFactories(): util.List[org.apache.spark.sql.sources.v2.reader.DataReaderFactory[Row]] = {
        val inputSplits = createInputSplits()

        inputSplits
          .map(new InputSplitReaderFactory(readSchema(), _))
          .map(_.asInstanceOf[org.apache.spark.sql.sources.v2.reader.DataReaderFactory[Row]])
          .toList
          .asJava
    }

    private def createInputSplits(): Array[InputSplit] = {
        val dataSchema = RequiredSchema.columns(
            readSchema()
              .fields
              //partition column value is constant for one split
              .filter(f => !_odpsPartitionNameSet.contains(f.name))
              .map(attr => new Attribute(attr.name, attr.dataType.catalogString))
              .toList
              .asJava)




        val sessionBuilder = new TableReadSessionBuilder(
            _tableProvider,
            _odpsProject,
            _odpsTableName)
          .readDataColumns(dataSchema)
          .options(_options)
          .splitBySize(splitSize())

        if (_odpsTablePartitions.nonEmpty) {
            val allowFullScan = dataSourceOptions.getBoolean(OdpsSourceOptions.ODPS_SQL_FULL_SCAN, true)
            if (!allowFullScan && _partitionFilters.isEmpty) {
                throw new OdpsException(s"odps.sql.allow.fullscan is $allowFullScan")
            }

            val prunedPartitions = _odpsTable.getPartitions.asScala
              .filter { p =>
                  _partitionFilters
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

    private def splitSize(): Int = {
        val totalSizeInMB = _odpsTable.getSize / 1024 / 1024
        val defaultParallelism = Math.max(2, SparkContext.getOrCreate.defaultParallelism)
        val parallelism = dataSourceOptions.getInt(OdpsSourceOptions.ODPS_SPLIT_PARALLELISM, defaultParallelism)

        val rawSizePerSplit = ( totalSizeInMB / parallelism) + 1
        val sizePerSplit = math.min(rawSizePerSplit, Int.MaxValue).toInt
        val splitSize = math.min(_splitSizeInConfig, sizePerSplit)

        println("splitSize, tableSizeInMB: " + totalSizeInMB +
          ", defaultParallelism: " + defaultParallelism +
          ", parallelism: " + parallelism +
          ", splitSize: " + splitSize)

        splitSize
    }
}
