import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author renxiang
  * @date 2021-12-24
  */
object DataWriterTest {
  val ODPS_DATA_SOURCE = "org.apache.spark.sql.odps.datasource.DefaultSource"
  val ODPS_ENDPOINT = "http://service.cn.maxcompute.aliyun.com/api"

  def main(args: Array[String]): Unit = {
    val odpsProject = args(0)
    val odpsAkId = args(1)
    val odpsAkKey = args(2)
    val odpsTable = args(3)
    val partition = args(4)

    val spark = SparkSession
      .builder()
      .appName("odps-datasource-writer")
      .getOrCreate()

    import spark._
    import sqlContext.implicits._

    val p0 = s"static-overwrite-$partition"
    val dfSingleOverwritePartition = spark.sparkContext
      .parallelize(0 to 5, 2)
      .map(f => new TestO(s"a_test$f", s"b_test$f", p0))
      .toDF

    //静态分区写入overwrite
    println(s"single-partition overwrite into $p0")
    dfSingleOverwritePartition.write
      .format(ODPS_DATA_SOURCE)
      .option("spark.hadoop.odps.project.name", odpsProject)
      .option("spark.hadoop.odps.access.id", odpsAkId)
      .option("spark.hadoop.odps.access.key", odpsAkKey)
      .option("spark.hadoop.odps.end.point", ODPS_ENDPOINT)
      .option("spark.hadoop.odps.table.name", odpsTable)
      .option("spark.sql.odps.dynamic.partition", true)
      .option("spark.sql.odps.partition.spec", s"ds=$p0")
      .mode(SaveMode.Overwrite)
      .save()

    val p1 = s"static-append-$partition"
    val dfSingleAppendPartition = spark.sparkContext
      .parallelize(0 to 5, 2)
      .map(f => new TestO(s"a_test$f", s"b_test$f", p1))
      .toDF
    println(s"single-partition append into $p1")
    //静态分区写入append
    dfSingleAppendPartition.write
      .format(ODPS_DATA_SOURCE)
      .option("spark.hadoop.odps.project.name", odpsProject)
      .option("spark.hadoop.odps.access.id", odpsAkId)
      .option("spark.hadoop.odps.access.key", odpsAkKey)
      .option("spark.hadoop.odps.end.point", ODPS_ENDPOINT)
      .option("spark.hadoop.odps.table.name", odpsTable)
      .option("spark.sql.odps.dynamic.partition", true)
      .option("spark.sql.odps.partition.spec", s"ds=$p1")
      .mode(SaveMode.Append)
      .save()

    try {
      val p2 = s"dynamic-overwrite-$partition"
      val dfDynamicOverwritePartition = spark.sparkContext
        .parallelize(0 to 5, 2)
        .map(f => {
          val suffix = f % 2
          new TestO(s"a_test$f", s"b_test$f", s"$p2-$suffix")
        })
        .toDF
      println(s"dynamic-partition overwrite into $p2")
      //动态分区overwrite写入
      dfDynamicOverwritePartition.write
        .format(ODPS_DATA_SOURCE)
        .option("spark.hadoop.odps.project.name", odpsProject)
        .option("spark.hadoop.odps.access.id", odpsAkId)
        .option("spark.hadoop.odps.access.key", odpsAkKey)
        .option("spark.hadoop.odps.end.point", ODPS_ENDPOINT)
        .option("spark.hadoop.odps.table.name", odpsTable)
        .option("spark.sql.odps.dynamic.partition", true)
        .option("spark.sql.odps.dynamic.insert.mode", "append,overwrite")
        .mode(SaveMode.Overwrite)
        .save()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    try {
      val p3 = s"dynamic-append-$partition"
      val dfDynamicAppendPartition = spark.sparkContext
        .parallelize(0 to 5, 2)
        .map(f => {
          val suffix = f % 2
          new TestO(s"a_test$f", s"b_test$f", s"$p3-$suffix")
        })
        .toDF
      println(s"dynamic-partition append into $p3")
      //动态分区append写入
      dfDynamicAppendPartition.write
        .format(ODPS_DATA_SOURCE)
        .option("spark.hadoop.odps.project.name", odpsProject)
        .option("spark.hadoop.odps.access.id", odpsAkId)
        .option("spark.hadoop.odps.access.key", odpsAkKey)
        .option("spark.hadoop.odps.end.point", ODPS_ENDPOINT)
        .option("spark.hadoop.odps.table.name", odpsTable)
        .option("spark.sql.odps.dynamic.partition", true)
        .option("spark.sql.odps.dynamic.insert.mode", "append,overwrite")
        .mode(SaveMode.Append)
        .save()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    Thread.sleep(72*3600*1000)
  }
}
