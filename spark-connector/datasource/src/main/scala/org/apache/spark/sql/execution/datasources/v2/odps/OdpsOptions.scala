/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.odps

import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Options for the ODPS data source.
 */
class OdpsOptions(val parameters: CaseInsensitiveMap[String]) extends Serializable with Logging {
  import OdpsOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val DEFAULT_TABLE_PROVIDER = MAX_STORAGE_TABLE_PROVIDER

  val tableReadProvider = parameters.getOrElse(ODPS_TABLE_READ_PROVIDER, DEFAULT_TABLE_PROVIDER)

  val tableWriteProvider = parameters.getOrElse(ODPS_TABLE_WRITE_PROVIDER, DEFAULT_TABLE_PROVIDER)

  val metaCacheSize = parameters.getOrElse(ODPS_META_CACHE_SIZE, "100").toInt

  val metaCacheExpireSeconds = parameters.getOrElse(ODPS_META_CACHE_EXPIRE_SECONDS, "30").toInt

  val metaStatsLevel = parameters.getOrElse(ODPS_META_STATS_LEVEL, "none")

  val enableVectorizedReader = parameters.getOrElse(ODPS_VECTORIZED_READER_ENABLED, "true").toBoolean

  val enableReuseBatch = {
    if (tableReadProvider.equals(TUNNEL_TABLE_PROVIDER)) {
      false
    } else {
      parameters.getOrElse(ODPS_BATCH_REUSED_ENABLED, "true").toBoolean
    }
  }

  val columnarReaderBatchSize =
    parameters.getOrElse(ODPS_VECTORIZED_READER_BATCH_SIZE, "4096").toInt

  val enableVectorizedWriter =
    parameters.getOrElse(ODPS_VECTORIZED_WRITER_ENABLED, "true").toBoolean

  val columnarWriterBatchSize =
    parameters.getOrElse(ODPS_VECTORIZED_WRITER_BATCH_SIZE, "4096").toInt

  val enableHashCluster = parameters.getOrElse(ODPS_HASH_CLUSTER_ENABLED, "false").toBoolean

  val splitParallelism = parameters.getOrElse(ODPS_SPLIT_PARALLELISM, "-1").toInt

  val splitSizeInMB = parameters.getOrElse(ODPS_SPLIT_SIZE_IN_MB, "256").toInt

  val splitCrossPartition = parameters.getOrElse(ODPS_SPLIT_CROSS_PARTITION, "true").toBoolean

  val enableArrowExtension =  parameters.getOrElse(ODPS_ARROW_EXTENSION, "false").toBoolean

  val enableExternalProject =  parameters.getOrElse(ODPS_ENABLE_EXTERNAL_PROJECT, "false").toBoolean

  val enableExternalTable =  parameters.getOrElse(ODPS_ENABLE_EXTERNAL_TABLE, "false").toBoolean

  val odpsTableCompressionCodec = parameters.getOrElse(ODPS_TABLE_COMPRESSION_CODEC, "zstd")

  val enableNamespaceSchema =  parameters.getOrElse(ODPS_NAMESPACE_SCHEMA_TABLE, "false").toBoolean

  val defaultSchema =  parameters.getOrElse(ODPS_NAMESPACE_SCHEMA_DEFAULT, "default")

  // 4 * 1024 * 1024
  val writerChunkSize = parameters.getOrElse(ODPS_WRITER_CHUNK_SIZE, "4194304").toInt

  val writerMaxRetires =  parameters.getOrElse(ODPS_WRITER_MAX_RETRIES, "10").toInt

  val maxRetrySleepIntervalMs =  parameters.getOrElse(ODPS_WRITER_MAX_RETRY_SLEEP_INTERVALS, "10000").toInt
}

object OdpsOptions {
  private val odpsOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    odpsOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val TUNNEL_TABLE_PROVIDER = "tunnel"
  val MAX_STORAGE_TABLE_PROVIDER = "v1"
  val ODPS_TABLE_READ_PROVIDER = newOption("tableReadProvider")
  val ODPS_TABLE_WRITE_PROVIDER = newOption("tableWriteProvider")
  val ODPS_META_CACHE_SIZE = newOption("metaCacheSize")
  val ODPS_META_CACHE_EXPIRE_SECONDS = newOption("metaCacheExpireSeconds")
  val ODPS_META_STATS_LEVEL = newOption("metaStatsLevel")
  val ODPS_VECTORIZED_READER_ENABLED = newOption("enableVectorizedReader")
  val ODPS_BATCH_REUSED_ENABLED = newOption("enableBatchReused")
  val ODPS_VECTORIZED_READER_BATCH_SIZE = newOption("columnarReaderBatchSize")
  val ODPS_VECTORIZED_WRITER_ENABLED = newOption("enableVectorizedWriter")
  val ODPS_VECTORIZED_WRITER_BATCH_SIZE = newOption("columnarWriterBatchSize")
  val ODPS_HASH_CLUSTER_ENABLED = newOption("enableHashCluster")
  val ODPS_SPLIT_PARALLELISM = newOption("splitParallelism")
  val ODPS_SPLIT_SIZE_IN_MB = newOption("splitSizeInMB")
  val ODPS_SPLIT_CROSS_PARTITION = newOption("splitCrossPartition")
  val ODPS_ARROW_EXTENSION = newOption("enableArrowExtension")
  val ODPS_ENABLE_EXTERNAL_PROJECT = newOption("enableExternalProject")
  val ODPS_ENABLE_EXTERNAL_TABLE = newOption("enableExternalTable")
  val ODPS_TABLE_COMPRESSION_CODEC = newOption("tableCompressionCodec")
  val ODPS_NAMESPACE_SCHEMA_TABLE = newOption("enableNamespaceSchema")
  val ODPS_NAMESPACE_SCHEMA_DEFAULT = newOption("defaultSchema")

  val ODPS_WRITER_CHUNK_SIZE = newOption("writerChunkSize")
  val ODPS_WRITER_MAX_RETRIES = newOption("writerMaxRetires")
  val ODPS_WRITER_MAX_RETRY_SLEEP_INTERVALS = newOption("writerRetrySleepIntervalMs")
}
