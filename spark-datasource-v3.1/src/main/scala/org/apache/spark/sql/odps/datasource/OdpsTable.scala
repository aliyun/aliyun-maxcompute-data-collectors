package org.apache.spark.sql.odps.datasource

import java.util

import com.aliyun.odps.cupid.table.v1.util.Options
import org.apache.spark.SparkContext
import org.apache.spark.sql.odps.reader.OdpsScanBuilder
import org.apache.spark.sql.odps.writer.OdpsWriteBuilder
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class OdpsTable(odpsTable: com.aliyun.odps.Table,
                fields: StructType,
                partitions: StructType)
  extends Table with SupportsRead with SupportsWrite {

  override def name(): String = odpsTable.getName

  override def schema(): StructType = {
    StructType((fields.fields.toSet ++ partitions.fields.toSet).toArray)
  }

  override def capabilities(): util.Set[TableCapability] = OdpsTable.CAPABILITIES

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val allowFullScan = options.getBoolean(OdpsSourceOptions.ODPS_SQL_FULL_SCAN, true)

    new OdpsScanBuilder(OdpsTable.PROVIDER,
      schema(),
      partitions,
      sessionBuildOptions(options),
      splitSize(options),
      odpsTable: com.aliyun.odps.Table,
      allowFullScan)
  }

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = {
    schemaValid(logicalWriteInfo.schema())

    new OdpsWriteBuilder(
      OdpsTable.PROVIDER,
      odpsTable,
      logicalWriteInfo.schema(),
      partitions,
      sessionBuildOptions(logicalWriteInfo.options()),
      logicalWriteInfo)
  }

  private def splitSize(options: CaseInsensitiveStringMap): Int = {
    val totalSizeInMB = odpsTable.getSize / 1024 / 1024
    val defaultParallelism = Math.max(2, SparkContext.getOrCreate.defaultParallelism)
    val parallelism = options.getInt(OdpsSourceOptions.ODPS_SPLIT_PARALLELISM, defaultParallelism)


    val rawSizePerSplit = ( totalSizeInMB / parallelism) + 1
    val sizePerSplit = math.min(rawSizePerSplit, Int.MaxValue).toInt

    val _splitSizeInConfig = options.getInt(OdpsSourceOptions.ODPS_SPLIT_SIZE, 256)
    val splitSize = math.min(_splitSizeInConfig, sizePerSplit)

    println("splitSize, tableSizeInMB: " + totalSizeInMB +
      ", defaultParallelism: " + defaultParallelism +
      ", parallelism: " + parallelism +
      ", splitSize: " + splitSize)

    splitSize
  }

  private def sessionBuildOptions(options: CaseInsensitiveStringMap): Options = {
    val accessKeyId = options.get(OdpsSourceOptions.ODPS_ACCESS_KEY_ID)
    val accessKeySecret = options.get(OdpsSourceOptions.ODPS_ACCESS_KEY_SECRET)
    val odpsEndpoint = options.get(OdpsSourceOptions.ODPS_ENDPOINT)
    val odpsProject = options.get(OdpsSourceOptions.ODPS_PROJECT)

    new Options.OptionsBuilder[Object]
      .accessId(accessKeyId)
      .accessKey(accessKeySecret)
      .endpoint(odpsEndpoint)
      .project(odpsProject)
      .build
  }

  private def schemaValid(rddSchema: StructType): Unit = {

    val errorItems = schema().fields.zipWithIndex
      .map{ case(c, index) =>
        val inputRddField = rddSchema.fields(index)
        if (inputRddField.name != c.name) {
          s"rdd[$index] actual name is $inputRddField.name, expected $c.getName"
        } else {
          None
        }
      }
      .filter(_ != None)

    if (errorItems.nonEmpty) {
      throw new RuntimeException(s"input rdd schema not same to odps table")
    }
  }
}

object OdpsTable {
  private val CAPABILITIES = Set(BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER).asJava
  private val PROVIDER = OdpsSourceOptions.CUPID_TABLE_PROVIDER
}


