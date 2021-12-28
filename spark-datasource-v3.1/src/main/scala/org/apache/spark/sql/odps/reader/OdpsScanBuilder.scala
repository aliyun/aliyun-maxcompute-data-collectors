package org.apache.spark.sql.odps.reader

import com.aliyun.odps.Table
import com.aliyun.odps.cupid.table.v1.reader.InputSplit
import com.aliyun.odps.cupid.table.v1.util.Options
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.ArrayBuffer

class OdpsScanBuilder(provider: String,
                           dataSchema: StructType,
                           partitionSchema: StructType,
                           sessionOptions: Options,
                           splitSize: Int,
                           odpsTable: Table,
                           allowFullScan: Boolean)
  extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters {

  protected var _partitionFilters: Option[Array[Filter]] = None

  protected var _columnFilters: Option[Array[Filter]] = None

  protected lazy val _odpsPartitionNameSet = {
    partitionSchema.fields.map(_.name).toSet
  }

  private val _requiredColumns: ArrayBuffer[StructField] = new ArrayBuffer[StructField]

  override def pruneColumns(requiredSchema: StructType): Unit = {
    requiredSchema
      .fields
      .filter(!_requiredColumns.contains(_))
      .foreach(_requiredColumns.append(_))
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (nestedFilters, normalFilters) = filters.partition(_.containsNestedColumn)

    // Filters on this table fall into four categories based on where we can use them to avoid
    // reading unneeded data:
    //  - partition keys only - used to prune directories to read
    //  - bucket keys only - optionally used to prune files to read
    //  - keys stored in the data only - optionally used to skip groups of data in files
    //  - filters that need to be evaluated again after the scan
    val partitionKeyFilters = normalFilters
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

  override def build(): Scan = {
    new OdpsScan(provider,
      scanSchema(),
      partitionSchema,
      sessionOptions,
      splitSize: Int,
      odpsTable.getName,
      _partitionFilters,
      allowFullScan)
  }

  private def scanSchema(): StructType = {

    if (_requiredColumns.isEmpty) {
      dataSchema
    } else {
      val partitionFilterNamesSet: Set[String] = if (_partitionFilters.isDefined) {
        _partitionFilters.get
          .flatMap(_.references)
          .toSet
      } else {
        Seq().toSet
      }

      val normalFilterNamesSet: Set[String] = if (_columnFilters.isDefined) {
        _columnFilters.get
          .flatMap(_.references)
          .toSet
      } else {
        Seq().toSet
      }

      val requirdAttributeNameSet = _requiredColumns.map(_.name).toSet

      val allNeedAttrNameSet: Set[String] = partitionFilterNamesSet ++ normalFilterNamesSet ++ requirdAttributeNameSet

      val fields = dataSchema
        .fields
        .filter(field =>{
          val name = field.name
          allNeedAttrNameSet.contains(name)
        })


      StructType(fields)
    }
  }

}
