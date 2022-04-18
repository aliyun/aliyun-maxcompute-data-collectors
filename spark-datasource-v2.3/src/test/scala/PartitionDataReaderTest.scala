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

import org.apache.spark.sql.SparkSession

/**
  * @author renxiang
  * @date 2021-12-20
  */
object PartitionDataReaderTest {

  val ODPS_DATA_SOURCE = "org.apache.spark.sql.odps.datasource.DefaultSource"
  val ODPS_ENDPOINT = "http://service.cn.maxcompute.aliyun.com/api"


  def main(args: Array[String]): Unit = {
    val odpsProject = args(0)
    val odpsAkId = args(1)
    val odpsAkKey = args(2)
    val odpsTable = args(3)

    val spark = SparkSession
      .builder()
      .appName("odps-datasource-reader")
      .getOrCreate()

    import spark._

    val df = spark.read.format(ODPS_DATA_SOURCE)
      .option("spark.hadoop.odps.project.name", odpsProject)
      .option("spark.hadoop.odps.access.id", odpsAkId)
      .option("spark.hadoop.odps.access.key", odpsAkKey)
      .option("spark.hadoop.odps.end.point", ODPS_ENDPOINT)
      .option("spark.hadoop.odps.table.name", odpsTable)
      .load()

    df.createOrReplaceTempView("odps_table")

    //test full table scan
    println("select * from odps_table")
    val dfFullScan = sql("select * from odps_table")
    println(dfFullScan.count)
    dfFullScan.show(10)

    //test partition prune
    println("select * from odps_table where ds like '202112%'")
    val dfLikePartition = sql ("select * from odps_table where ds like '202112%'")
    println(dfLikePartition.count)
    dfLikePartition.show(10)

    println("select * from odps_table where ds < '20211101'")
    val dfLowerThan1101  = sql ("select * from odps_table where ds < '20211101'")
    println(dfLowerThan1101.count)
    dfLowerThan1101.show(10)

    println("select * from odps_table where ds < '20211201'")
    val dfLowerThan1201 = sql ("select * from odps_table where ds < '20211201'")
    println(dfLowerThan1201.count)
    dfLowerThan1201.show(10)

    println("select * from odps_table where ds < '20220101'")
    val dfLowerThan0101 = sql ("select * from odps_table where ds < '20220101'")
    println(dfLowerThan0101.count)
    dfLowerThan0101.show(10)

    Thread.sleep(72*3600*1000)
  }
}
