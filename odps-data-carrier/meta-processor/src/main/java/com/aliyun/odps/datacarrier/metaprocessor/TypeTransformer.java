package com.aliyun.odps.datacarrier.metaprocessor;

import com.aliyun.odps.datacarrier.commons.Constants.DATASOURCE_TYPE;
import com.aliyun.odps.datacarrier.commons.MetaManager.ColumnMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.GlobalMetaModel;

public class TypeTransformer {
  public static TypeTransformResult toOdpsType(GlobalMetaModel globalMeta,
      ColumnMetaModel columnMeta) {
    DATASOURCE_TYPE datasourceType = DATASOURCE_TYPE.valueOf(globalMeta.datasourceType);

    TypeTransformResult typeTransformResult;
    if (datasourceType.equals(DATASOURCE_TYPE.HIVE)) {
      typeTransformResult = HiveTypeTransformer.toOdpsType(columnMeta.type, globalMeta.odpsVersion,
          globalMeta.hiveCompatible);
    } else {
      throw new IllegalArgumentException("Unsupported datasource type: " + datasourceType);
    }

    return typeTransformResult;
  }
}
