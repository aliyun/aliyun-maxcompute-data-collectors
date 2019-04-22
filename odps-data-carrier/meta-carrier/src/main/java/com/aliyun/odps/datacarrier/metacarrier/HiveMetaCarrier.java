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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.datacarrier.metacarrier;

import com.aliyun.odps.datacarrier.commons.MetaManager;
import com.aliyun.odps.datacarrier.commons.MetaManager.ColumnMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.DatabaseMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.GlobalMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.PartitionMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TableMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TablePartitionMetaModel;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;

/**
 * @author: Jon (wangzhong.zw@alibaba-inc.com)
 *
 * usage:
 * java -cp /path/to/jar com.aliyun.odps.datacarrier.hive.HiveMetaCarrier SECURITY_OFF
 * [hive metastore thrift address] [result file path]
 *
 * java -cp /path/to/jar com.aliyun.odps.datacarrier.hive.HiveMetaCarrier KERBEROS
 * (not supported yet)
 *
 * java -cp /path/to/jar com.aliyun.odps.datacarrier.hive.HiveMetaCarrier MAPR_SASL
 * (not supported yet)
 *
 */
public class HiveMetaCarrier {
  private enum MODE {
    /**
     * if hive.metastore.sasl.enabled is set to false
     */
    SECURITY_OFF,
    /**
     * if hive.metastore.kerberos.keytab.file and hive.metastore.kerberos.principal are set
     */
    KERBEROS,
    /**
     * Default security mode when hive.metastore.sasl.enabled is set to true
     */
    MAPR_SASL
  }

  private static void printUsage() {
    System.err.println("arguments:\n"
        + "SECURITY_OFF <hive metastore thrift address> <output path>\n"
        + "KERBEROS (not supported)\n"
        + "MAPR_SASL (not supported)\n");
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      printUsage();
      System.exit(128);
    }

    MODE mode = null;
    try {
      mode = MODE.valueOf(args[0]);
    } catch (IllegalArgumentException e) {
      printUsage();
      System.exit(128);
    }

    // Connect to hive meta store
    HiveConf hiveConf = new HiveConf();
    String outputPath;
    if (mode.equals(MODE.SECURITY_OFF)) {
      if (args.length != 3) {
        String msg = "Invalid arguments. Expect hive metastore address, configuration file path "
            + "and output directory.";
        throw new IllegalArgumentException(msg);
      }
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, args[1]);
      outputPath = args[2];
    } else if (mode.equals(MODE.KERBEROS)) {
      throw new IllegalArgumentException("Not supported");
    } else {
      throw new IllegalArgumentException("Not supported");
    }
    HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveConf);


    MetaManager metaManager = new MetaManager(outputPath);
    GlobalMetaModel globalMetaModel = new GlobalMetaModel();
    globalMetaModel.datasourceType = "HIVE";
    metaManager.setGlobalMeta(globalMetaModel);

    List<String> databaseNames = hiveClient.getAllDatabases();
    // Iterate over databases
    for (String databaseName : databaseNames) {
      List<String> tableNames = hiveClient.getAllTables(databaseName);
      DatabaseMetaModel databaseMeta = new DatabaseMetaModel();
      databaseMeta.databaseName = databaseName;
      databaseMeta.odpsProjectName = databaseName;
      metaManager.setDatabaseMeta(databaseMeta);

      // Iterate over tables
      for (String tableName : tableNames) {
        TableMetaModel tableMetaModel = new TableMetaModel();
        TablePartitionMetaModel tablePartitionMetaModel = new TablePartitionMetaModel();

        // Handle table meta
        tableMetaModel.tableName = tableName;
        tableMetaModel.odpsTableName = tableName;
        List<FieldSchema> columns = hiveClient.getFields(databaseName, tableName);
        for (FieldSchema column : columns) {
          ColumnMetaModel columnMetaModel = new ColumnMetaModel();
          columnMetaModel.columnName = column.getName();
          columnMetaModel.odpsColumnName = column.getName();
          columnMetaModel.type = column.getType();
          columnMetaModel.comment = column.getComment();
          tableMetaModel.columns.add(columnMetaModel);
        }
        List<FieldSchema> partitionColumns = hiveClient.getTable(databaseName, tableName)
            .getPartitionKeys();
        for (FieldSchema partitionColumn : partitionColumns) {
          ColumnMetaModel columnMetaModel = new ColumnMetaModel();
          columnMetaModel.columnName = partitionColumn.getName();
          columnMetaModel.type = partitionColumn.getType();
          columnMetaModel.comment = partitionColumn.getComment();
          tableMetaModel.partitionColumns.add(columnMetaModel);
        }
        metaManager.setTableMeta(databaseName, tableMetaModel);

        // Handle partition meta
        // TODO: what if there are more than 32767 partitions
        // TODO: support parquet
        tablePartitionMetaModel.tableName = tableName;
        List<Partition> partitions =
            hiveClient.listPartitions(databaseName, tableName, Short.MAX_VALUE);
        if (!partitions.isEmpty()) {
          for (Partition partition : partitions) {
            PartitionMetaModel partitionMetaModel = new PartitionMetaModel();
            partitionMetaModel.createTime = Integer.toString(partition.getCreateTime());
            partitionMetaModel.location = partition.getSd().getLocation();
            // Generate partition specifications
            List<String> partitionValues = partition.getValues();
            List<String> partitionSpecs = new ArrayList<>();
            for (int i = 0; i < partitionColumns.size(); i++) {
              partitionSpecs.add(partitionColumns.get(i).getName() + "=" + partitionValues.get(i));
            }
            partitionMetaModel.partitionSpec = String.join(",", partitionSpecs);
          }
          metaManager.setTablePartitionMeta(databaseName, tablePartitionMetaModel);
        }
      }
    }
  }
}
