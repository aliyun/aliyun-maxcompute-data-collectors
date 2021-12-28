package org.apache.spark.sql.odps.reader

import com.aliyun.odps.cupid.table.v1.reader.{InputSplit, SplitReaderBuilder}
import com.aliyun.odps.cupid.table.v1.util.TableUtils
import org.apache.spark.sql.odps.converter.TypesConverter
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory
import org.apache.spark.sql.types.StructType

/**
  * @author renxiang
  * @date 2021-12-19
  */
class InputSplitReaderFactory(tableSchema: StructType, inputSplit: InputSplit) extends DataReaderFactory[Row] {

  override def createDataReader(): InputSplitReader = {
    val recordReader = new SplitReaderBuilder(inputSplit).buildRecordReader()

    /**
      * table schema what was composed of both partition schema and output DataSchema
      * was different from output DataSchema.
      */
    val outputDataSchema = TableUtils.toColumnArray(inputSplit.getReadDataColumns).map(_.getTypeInfo).toList

    val sparkDataConverters = outputDataSchema.map(TypesConverter.odpsData2SparkData)

    new InputSplitReader(
      tableSchema,
      inputSplit.getPartitionColumns,
      inputSplit.getPartitionSpec,
      outputDataSchema,
      sparkDataConverters,
      recordReader)
  }
}
