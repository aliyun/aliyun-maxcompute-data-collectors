package org.apache.spark.sql.odps.datasource

import com.aliyun.odps.Odps
import com.aliyun.odps.account.AliyunAccount
import org.apache.spark.sql.odps.converter.TypesConverter
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class DefaultSource extends TableProvider {

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    val (_, fields, partitions) = odpsTablePrerequisite(caseInsensitiveStringMap)
    StructType(fields ::: partitions)
  }

  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

    val (_, _, partitions) = odpsTablePrerequisite(options)
    partitions.map(_.name).asTransforms
  }

  override
  def getTable(structType: StructType,
               transforms: Array[Transform],
               map: java.util.Map[String, String]): Table = {
    val (odpsTable, fields, partitions) = odpsTablePrerequisite(map)
    new OdpsTable(odpsTable, StructType(fields), StructType(partitions))
  }

  private def requiredOptions(options: java.util.Map[String, String]): (String, String, String, String, String) = {

    val odpsProject = options.get(OdpsSourceOptions.ODPS_PROJECT)
    val odpsTable = options.get(OdpsSourceOptions.ODPS_TABLE)
    val odpsEndpoint = options.get(OdpsSourceOptions.ODPS_ENDPOINT)
    val odpsAccessKeyId = options.get(OdpsSourceOptions.ODPS_ACCESS_KEY_ID)
    val odpsAccessKeySecret = options.get(OdpsSourceOptions.ODPS_ACCESS_KEY_SECRET)

    if (!odpsProject.isEmpty && !odpsTable.isEmpty && !odpsEndpoint.isEmpty && !odpsAccessKeyId.isEmpty && !odpsAccessKeySecret.isEmpty) {
      (odpsProject, odpsTable, odpsEndpoint, odpsAccessKeyId, odpsAccessKeySecret)
    } else {
      throw new Exception("invalid options")
    }
  }

  private def odpsTablePrerequisite(options: java.util.Map[String, String]): (com.aliyun.odps.Table, List[StructField], List[StructField]) = {
    val (project, table, endpoint, akId, akSecret) = requiredOptions(options)

    val odps = {
      val account = new AliyunAccount(akId, akSecret)
      val retOdps = new Odps(account)
      retOdps.setEndpoint(endpoint)
      retOdps.setDefaultProject(project)
      retOdps
    }

    val odpsTable = {
      val odpsTable = odps.tables.get(table)
      odpsTable.reload()
      odpsTable
    }

    val odpsTableSchema = odpsTable.getSchema

    val odpsTableFields = {
      odpsTableSchema
        .getColumns
        .asScala
        .map(c => TypesConverter.odpsColumn2SparkStructField(c, false))
        .toList
    }

    val odpsTablePartitions = {
      odpsTableSchema
        .getPartitionColumns
        .asScala
        .map(c =>TypesConverter.odpsColumn2SparkStructField(c, true))
        .toList
    }

    (odpsTable, odpsTableFields, odpsTablePartitions)
  }

}