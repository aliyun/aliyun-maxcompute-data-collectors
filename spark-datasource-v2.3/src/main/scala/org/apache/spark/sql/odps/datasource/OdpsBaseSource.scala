package org.apache.spark.sql.odps.datasource

import com.aliyun.odps.Odps
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.cupid.table.v1.util.Options
import org.apache.spark.sql.odps.converter.TypesConverter
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * @author renxiang
  * @date 2021-12-23
  */
class OdpsBaseSource(dataSourceOptions: DataSourceOptions) {
  protected val _accessKeyId: String = dataSourceOptions.get(OdpsSourceOptions.ODPS_ACCESS_KEY_ID).get

  protected val _accessKeySecret: String = dataSourceOptions.get(OdpsSourceOptions.ODPS_ACCESS_KEY_SECRET).get

  protected val _odpsEndpoint: String = dataSourceOptions.get(OdpsSourceOptions.ODPS_ENDPOINT).get

  protected val _odpsProject = dataSourceOptions.get(OdpsSourceOptions.ODPS_PROJECT).get

  protected val _odpsTableName = dataSourceOptions.get(OdpsSourceOptions.ODPS_TABLE).get

  protected val _tableProvider = OdpsSourceOptions.CUPID_TABLE_PROVIDER

  protected val _options = new Options.OptionsBuilder[Object]
    .accessId(_accessKeyId)
    .accessKey(_accessKeySecret)
    .endpoint(_odpsEndpoint)
    .project(_odpsProject)
    .build

  protected lazy val _odps: Odps = {
    val account = new AliyunAccount(_accessKeyId, _accessKeySecret)
    val retOdps = new Odps(account)
    retOdps.setEndpoint(_odpsEndpoint)
    retOdps.setDefaultProject(_odpsProject)
    retOdps
  }

  protected lazy val _odpsTable = {
    //using odps-sdk to get table schema
    val odpsTable = _odps.tables.get(_odpsTableName)
    odpsTable.reload()
    odpsTable
  }

  protected lazy val _odpsTableSchema = _odpsTable.getSchema

  protected lazy val _odpsTableFields = {
    _odpsTableSchema
      .getColumns
      .asScala
      .map(c => TypesConverter.odpsColumn2SparkStructField(c, false))
      .toList
  }

  protected lazy val _odpsTablePartitions = {
    _odpsTableSchema
      .getPartitionColumns
      .asScala
      .map(c =>TypesConverter.odpsColumn2SparkStructField(c, true))
      .toList
  }

  protected lazy val _odpsPartitionNameSet = {
    _odpsTablePartitions
      .map(_.name)
      .toSet
  }

  protected lazy val _schema: StructType = StructType(_odpsTableFields ::: _odpsTablePartitions)

}
