package org.apache.spark.sql.hive

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.buildConf

private[spark] object OdpsOptions extends Logging {

  val ODPS_META_CACHE_SIZE = buildConf("spark.sql.odps.metaCache.size")
    .internal()
    .intConf
    .createWithDefault(100)

  val ODPS_META_CACHE_EXPIRE_TIME = buildConf("spark.sql.odps.metaCache.expireTime")
    .internal()
    .timeConf(TimeUnit.SECONDS)
    .createWithDefault(60)

  val ODPS_META_STATS_LEVEL = buildConf("spark.sql.odps.metaStats.level")
    .internal()
    .stringConf
    .createWithDefault("none")

  val ODPS_VECTORIZED_READER_ENABLED = buildConf("spark.sql.odps.enableVectorizedReader")
    .doc("Enables vectorized odps decoding.")
    .booleanConf
    .createWithDefault(true)

  val ODPS_VECTORIZED_READER_BATCH_SIZE = buildConf("spark.sql.odps.columnarReaderBatchSize")
    .doc("The number of rows to include in a odps vectorized reader batch. The number should " +
      "be carefully chosen to minimize overhead and avoid OOMs in reading data.")
    .intConf
    .createWithDefault(4096)

  val ODPS_TABLE_READER_COMPRESSION_CODEC = buildConf("spark.sql.odps.table.reader.compressionCodec")
    .stringConf
    .createWithDefault("zstd")

  val ODPS_VECTORIZED_WRITER_ENABLED = buildConf("spark.sql.odps.enableVectorizedWriter")
    .doc("Enables vectorized odps encoding.")
    .booleanConf
    .createWithDefault(true)

  val ODPS_VECTORIZED_WRITER_BATCH_SIZE = buildConf("spark.sql.odps.columnarWriterBatchSize")
    .doc("The number of rows to include in a odps vectorized writer batch. The number should " +
      "be carefully chosen to minimize overhead and avoid OOMs in writting data.")
    .intConf
    .createWithDefault(4096)

  val ODPS_TABLE_WRITER_COMPRESSION_CODEC = buildConf("spark.sql.odps.table.writer.compressionCodec")
    .stringConf
    .createWithDefault("zstd")

  val ODPS_HASH_CLUSTER_ENABLED = buildConf("spark.sql.odps.enableHashCluster")
    .doc("Enables odps hash cluster.")
    .booleanConf
    .createWithDefault(true)

  val ODPS_SPLIT_PARALLELISM = buildConf("spark.sql.odps.split.parallelism")
    .internal()
    .intConf
    .createWithDefault(-1)

  val ODPS_SPLIT_SESSION_PARALLELISM = buildConf("spark.sql.odps.split.session.parallelism")
    .internal()
    .intConf
    .createWithDefault(1)

  val ODPS_SPLIT_SIZE = buildConf("spark.sql.odps.split.size")
    .internal()
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("256m")

  val ODPS_TABLE_READER_PROVIDER = buildConf("spark.sql.odps.table.reader.provider")
    .doc("The table provider of odps sql plugin.")
    .stringConf
    .createWithDefault("v1")

  val ODPS_TABLE_WRITER_PROVIDER = buildConf("spark.sql.odps.table.writer.provider")
    .doc("The table provider of odps sql plugin.")
    .stringConf
    .createWithDefault("v1")

  val ODPS_ARROW_EXTENSION = buildConf("spark.sql.odps.enableArrowExtension")
    .doc("Enable odps arrow extention type.")
    .booleanConf
    .createWithDefault(true)

  val ODPS_EXT_TABLE_ENABLE = buildConf("spark.sql.odps.enableExternalTable")
    .doc("Enable odps external table.")
    .booleanConf
    .createWithDefault(false)

  val ODPS_EXT_PROJECT_ENABLE = buildConf("spark.sql.odps.enableExternalProject")
    .doc("Enable odps external project.")
    .booleanConf
    .createWithDefault(false)

  val ODPS_WRITER_CHUNK_SIZE = buildConf("spark.sql.odps.writerChunkSize")
    .intConf
    .createWithDefault(4 * 1024 * 1024)

  val ODPS_WRITER_MAX_RETRIES = buildConf("spark.sql.odps.writerMaxRetires")
    .intConf
    .createWithDefault(10)

  val ODPS_WRITER_RETRY_SLEEP_INTERVALS = buildConf("spark.sql.odps.writerRetrySleepIntervalMs")
    .intConf
    .createWithDefault(10000)

  val ODPS_TABLE_BUFFERED_READER_ENABLE = buildConf("spark.sql.odps.enableBufferedReader")
    .doc("Enable odps buffered reader.")
    .booleanConf
    .createWithDefault(true)

  val ODPS_TABLE_ASYNC_READ_ENABLE = buildConf("spark.sql.odps.enableAsyncRead")
    .doc("Enable async read.")
    .booleanConf
    .createWithDefault(true)

  val ODPS_TABLE_BUFFERED_WRITER_ENABLE = buildConf("spark.sql.odps.enableBufferedWriter")
    .doc("Enable odps buffered writer.")
    .booleanConf
    .createWithDefault(true)

  def odpsMetaCacheSize(conf: SQLConf): Int = {
    conf.getConf(ODPS_META_CACHE_SIZE)
  }

  def odpsMetaCacheExpireTime(conf: SQLConf): Long = {
    conf.getConf(ODPS_META_CACHE_EXPIRE_TIME)
  }

  def odspVectorizedReaderEnabled(conf: SQLConf): Boolean = {
    conf.getConf(ODPS_VECTORIZED_READER_ENABLED)
  }

  def odpsVectorizedReaderBatchSize(conf: SQLConf): Int = {
    conf.getConf(ODPS_VECTORIZED_READER_BATCH_SIZE)
  }

  def odspVectorizedWriterEnabled(conf: SQLConf): Boolean = {
    conf.getConf(ODPS_VECTORIZED_WRITER_ENABLED)
  }

  def odpsVectorizedWriterBatchSize(conf: SQLConf): Int = {
    conf.getConf(ODPS_VECTORIZED_WRITER_BATCH_SIZE)
  }

  def odpsHashClusterEnabled(conf: SQLConf): Boolean = {
    conf.getConf(ODPS_HASH_CLUSTER_ENABLED)
  }

  def odpsSplitParallelism(conf: SQLConf): Int = {
    conf.getConf(ODPS_SPLIT_PARALLELISM)
  }

  def odpsSplitSize(conf: SQLConf): Long = {
    conf.getConf(ODPS_SPLIT_SIZE)
  }

  def odpsTableReaderProvider(conf: SQLConf): String = {
    conf.getConf(ODPS_TABLE_READER_PROVIDER)
  }

  def odpsTableWriterProvider(conf: SQLConf): String = {
    conf.getConf(ODPS_TABLE_WRITER_PROVIDER)
  }

  def odpsEnableArrowExtension(conf: SQLConf): Boolean = {
    conf.getConf(ODPS_ARROW_EXTENSION)
  }

  def odpsTableReaderCompressCodec(conf: SQLConf): String = {
    conf.getConf(ODPS_TABLE_READER_COMPRESSION_CODEC)
  }

  def odpsTableWriterCompressCodec(conf: SQLConf): String = {
    conf.getConf(ODPS_TABLE_WRITER_COMPRESSION_CODEC)
  }

  def odpsSplitSessionParallelism(conf: SQLConf): Int = {
    conf.getConf(ODPS_SPLIT_SESSION_PARALLELISM)
  }

  def odpsWriterChunkSize(conf: SQLConf): Int = {
    conf.getConf(ODPS_WRITER_CHUNK_SIZE)
  }

  def odpsWriterMaxRetires(conf: SQLConf): Int = {
    conf.getConf(ODPS_WRITER_MAX_RETRIES)
  }

  def odpsWriterRetrySleepIntervalMs(conf: SQLConf): Int = {
    conf.getConf(ODPS_WRITER_RETRY_SLEEP_INTERVALS)
  }

  def odpsTableBufferedReaderEnable(conf: SQLConf): Boolean = {
    conf.getConf(ODPS_TABLE_BUFFERED_READER_ENABLE)
  }

  def odpsTableAsyncReadEnable(conf: SQLConf): Boolean = {
    conf.getConf(ODPS_TABLE_ASYNC_READ_ENABLE)
  }

  def odpsTableBufferedWriterEnable(conf: SQLConf): Boolean = {
    conf.getConf(ODPS_TABLE_BUFFERED_WRITER_ENABLE)
  }
}