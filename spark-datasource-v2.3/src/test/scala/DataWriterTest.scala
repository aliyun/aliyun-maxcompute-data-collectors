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

    val spark = SparkSession
      .builder()
      .appName("odps-datasource-writer")
      .getOrCreate()

    import spark._
    import sqlContext.implicits._

    val p0 = s"static-overwrite"
    val dfOverwrite = spark.sparkContext
      .parallelize(0 to 5, 2)
      .map(f => new TestData(f, s"into_test$f"))
      .toDF

    //写入overwrite
    println(s"overwrite into $p0")
    dfOverwrite.write
      .format(ODPS_DATA_SOURCE)
      .option("spark.hadoop.odps.project.name", odpsProject)
      .option("spark.hadoop.odps.access.id", odpsAkId)
      .option("spark.hadoop.odps.access.key", odpsAkKey)
      .option("spark.hadoop.odps.end.point", ODPS_ENDPOINT)
      .option("spark.hadoop.odps.table.name", odpsTable)
      .mode(SaveMode.Overwrite)
      .save()

    val p1 = s"static-append"
    val dfAppend = spark.sparkContext
      .parallelize(0 to 5, 2)
      .map(f => new TestData(f, s"append_test$f"))
      .toDF
    println(s"append into $p1")
    //写入append
    dfAppend.write
      .format(ODPS_DATA_SOURCE)
      .option("spark.hadoop.odps.project.name", odpsProject)
      .option("spark.hadoop.odps.access.id", odpsAkId)
      .option("spark.hadoop.odps.access.key", odpsAkKey)
      .option("spark.hadoop.odps.end.point", ODPS_ENDPOINT)
      .option("spark.hadoop.odps.table.name", odpsTable)
      .mode(SaveMode.Append)
      .save()

    Thread.sleep(72*3600*1000)
  }
}
