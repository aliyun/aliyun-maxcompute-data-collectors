package org.apache.spark.sql.odps.datasource

/**
  * @author renxiang
  * @date 2021-12-26
  */
private[spark] object OdpsSourceOptions {

  val ODPS_PROJECT = "spark.hadoop.odps.project.name"

  val ODPS_TABLE = "spark.hadoop.odps.table.name"

  val ODPS_ACCESS_KEY_ID = "spark.hadoop.odps.access.id"

  val ODPS_ACCESS_KEY_SECRET = "spark.hadoop.odps.access.key"

  val ODPS_ENDPOINT = "spark.hadoop.odps.end.point"

  val CUPID_TABLE_PROVIDER = "tunnel"

  val ODPS_SQL_FULL_SCAN = "odps.sql.allow.fullscan"

  val ODPS_SPLIT_PARALLELISM = "spark.sql.odps.split.parallelism"

  val ODPS_SPLIT_SIZE = "spark.sql.odps.split.size"

  // 是否启用动态分区，默认false
  val ODPS_DYNAMIC_PARTITION_ENABLED = "spark.sql.odps.dynamic.partition"

  // 默认动态分区仅支持append模式
  // 如果用户打开了overwrite，需要保证queryPlan的分区与目标表分区完全相同。
  val ODPS_DYNAMIC_PARTITION_MODE = "spark.sql.odps.dynamic.insert.mode"

  //format partition_column1=xx,partition_column2=yy,...
  val ODPS_PARTITION_SPEC = "spark.sql.odps.partition.spec"

}
