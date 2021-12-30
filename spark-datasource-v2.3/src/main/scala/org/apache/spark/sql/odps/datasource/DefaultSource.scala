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

package org.apache.spark.sql.odps.datasource

import java.util.Optional

import org.apache.spark.sql.odps.reader.DataSourceReader
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

class DefaultSource extends ReadSupport with WriteSupport {
  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = {

    if (!hasRequiredOptions(dataSourceOptions)) {
      throw new Exception("invalid options")
    }

    new DataSourceReader(dataSourceOptions)
  }

  override def createWriter(
    jobId: String,
    schema: StructType,
    mode: SaveMode,
    options: DataSourceOptions): Optional[DataSourceWriter] = {

    if (!hasRequiredOptions(options)) {
      throw new Exception("invalid options")
    }

    Optional.of(new org.apache.spark.sql.odps.writer.DataSourceWriter(jobId, schema, mode, options))
  }

  private def hasRequiredOptions(options: DataSourceOptions): Boolean = {
    val odpsProject = options.get(OdpsSourceOptions.ODPS_PROJECT)
    val odpsTable = options.get(OdpsSourceOptions.ODPS_TABLE)
    val odpsEndpoint = options.get(OdpsSourceOptions.ODPS_ENDPOINT)
    val odpsAccessKeyId = options.get(OdpsSourceOptions.ODPS_ACCESS_KEY_ID)
    val odpsAccessKeySecret = options.get(OdpsSourceOptions.ODPS_ACCESS_KEY_SECRET)

    odpsProject.isPresent && odpsTable.isPresent && odpsEndpoint.isPresent && odpsAccessKeyId.isPresent && odpsAccessKeySecret.isPresent
  }
}