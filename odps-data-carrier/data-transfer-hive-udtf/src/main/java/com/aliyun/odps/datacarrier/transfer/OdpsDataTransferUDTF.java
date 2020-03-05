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

package com.aliyun.odps.datacarrier.transfer;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.datacarrier.transfer.converter.HiveObjectConverter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;


/**
 * Only for odps 2.0
 * @author: Jon (wangzhong.zw@alibaba-inc.com)
 */
public class OdpsDataTransferUDTF extends GenericUDTF {

  /**
   * Won't change once initialized
   */
  Odps odps;
  TableTunnel tunnel;
  ObjectInspector[] objectInspectors;
  private String odpsProjectName;
  private String odpsTableName;
  private List<String> odpsColumnNames;
  private List<String> odpsPartitionColumnNames;
  private TableSchema schema;

  /**
   * Changes with different partition
   */
  private Map<String, UploadSession> partitionSpecToUploadSession = new HashMap<>();
  private UploadSession currentUploadSession;
  private RecordWriter recordWriter;
  private String currentOdpsPartitionSpec;

  /**
   * Reused objects
   */
  private Object[] hiveColumnValues;
  private Object[] hivePartitionColumnValues;
  private Record reusedRecord;

  /**
   * Metrics
   */
  private Long numRecordTransferred = 0L;
  private Object[] forwardObj = new Object[1];

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    objectInspectors = args;
    List<String> fieldNames = new ArrayList<>();
    fieldNames.add("num_record_transferred");
    List<ObjectInspector> outputObjectInspectors = new ArrayList<>();
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                                                                   outputObjectInspectors);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    try {
      if(odps == null) {
        OdpsConfig odpsConfig = new OdpsConfig( "hdfs:///tmp/odps_config.ini");
        AliyunAccount account = new AliyunAccount(odpsConfig.getAccessId(), odpsConfig.getAccessKey());
        odps = new Odps(account);
        odps.setEndpoint(odpsConfig.getOdpsEndpoint());
        tunnel = new TableTunnel(odps);
        if (!StringUtils.isNullOrEmpty(odpsConfig.getTunnelEndpoint())) {
          if (!StringUtils.isNullOrEmpty(odpsConfig.getTunnelEndpoint2())) {
            tunnel.setEndpoint(ThreadLocalRandom.current().nextBoolean() ?
                               odpsConfig.getTunnelEndpoint() : odpsConfig.getTunnelEndpoint2());
          } else {
            tunnel.setEndpoint(odpsConfig.getTunnelEndpoint());
          }
        }
      }

      if (odpsTableName == null) {
        StringObjectInspector soi0 = (StringObjectInspector) objectInspectors[0];
        StringObjectInspector soi1 = (StringObjectInspector) objectInspectors[1];
        StringObjectInspector soi2 = (StringObjectInspector) objectInspectors[2];
        StringObjectInspector soi3 = (StringObjectInspector) objectInspectors[3];

        odpsProjectName = soi0.getPrimitiveJavaObject(args[0]).trim();
        odps.setDefaultProject(odpsProjectName);
        System.out.println("[Data-carrier] MaxCompute project: " + odpsProjectName);

        odpsTableName = soi1.getPrimitiveJavaObject(args[1]).trim();
        System.out.println("[Data-carrier] MaxCompute table: " + odpsTableName);

        schema = odps.tables().get(odpsTableName).getSchema();

        String odpsColumnNameString = soi2.getPrimitiveJavaObject(args[2]).trim();
        odpsColumnNames = new ArrayList<>();
        if (!odpsColumnNameString.isEmpty()) {
          odpsColumnNames.addAll(Arrays.asList(trimAll(odpsColumnNameString.split(","))));
        }
        hiveColumnValues = new Object[odpsColumnNames.size()];

        String odpsPartitionColumnNameString = soi3.getPrimitiveJavaObject(args[3]).trim();
        odpsPartitionColumnNames = new ArrayList<>();
        if (!odpsPartitionColumnNameString.isEmpty()) {
          odpsPartitionColumnNames.addAll(
              Arrays.asList(trimAll(odpsPartitionColumnNameString.split(","))));
        }
        hivePartitionColumnValues = new Object[odpsPartitionColumnNames.size()];
      }

      for (int i = 0; i < odpsColumnNames.size(); i++) {
        hiveColumnValues[i] = args[i + 4];
      }
      for (int i = 0; i < odpsPartitionColumnNames.size(); i++) {
        hivePartitionColumnValues[i] = args[i + 4 + odpsColumnNames.size()];
      }

      // Get partition spec
      String partitionSpec = getPartitionSpec();
      if (partitionSpec.contains("__HIVE_DEFAULT_PARTITION__")) {
        return;
      }

      // Create new tunnel upload session & record writer or reuse the current ones
      if (currentOdpsPartitionSpec == null || !currentOdpsPartitionSpec.equals(partitionSpec)) {
        resetUploadSession(partitionSpec);
      }

      if (reusedRecord == null) {
        reusedRecord = currentUploadSession.newRecord();
      }

      for (int i = 0; i < odpsColumnNames.size(); i++) {
        String odpsColumnName = odpsColumnNames.get(i);
        Object value = hiveColumnValues[i];
        if (value == null) {
          reusedRecord.set(odpsColumnName, null);
        } else {
          // Handle data types
          ObjectInspector objectInspector = objectInspectors[i + 4];
          TypeInfo typeInfo = schema.getColumn(odpsColumnName).getTypeInfo();
          reusedRecord.set(odpsColumnName, HiveObjectConverter.convert(objectInspector, value, typeInfo));
        }
      }

      recordWriter.write(reusedRecord);
      numRecordTransferred += 1;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  private String getPartitionSpec() {
    StringBuilder partitionSpecBuilder = new StringBuilder();
    for (int i = 0; i < odpsPartitionColumnNames.size(); ++i) {
      Object colValue = hivePartitionColumnValues[i];
      if (colValue == null) {
        continue;
      }

      ObjectInspector objectInspector = objectInspectors[i + 4 + odpsColumnNames.size()];
      TypeInfo typeInfo = schema.getPartitionColumn(odpsPartitionColumnNames.get(i)).getTypeInfo();

      Object odpsValue = HiveObjectConverter.convert(objectInspector, colValue, typeInfo);
      partitionSpecBuilder.append(odpsPartitionColumnNames.get(i));
      partitionSpecBuilder.append("='");
      partitionSpecBuilder.append(odpsValue.toString()).append("'");
      if (i != odpsPartitionColumnNames.size() - 1) {
        partitionSpecBuilder.append(",");
      }
    }
    return partitionSpecBuilder.toString();
  }

  private void resetUploadSession(String partitionSpec)
      throws TunnelException, IOException, HiveException {
    // Close current record writer
    if (currentUploadSession != null) {
      recordWriter.close();
    }

    currentUploadSession = getOrCreateUploadSession(partitionSpec);
    recordWriter = currentUploadSession.openBufferedWriter(true);
    ((TunnelBufferedWriter) recordWriter).setBufferSize(64 * 1024 * 1024);
    currentOdpsPartitionSpec = partitionSpec;
  }

  private UploadSession getOrCreateUploadSession(String partitionSpec)
      throws HiveException {
    UploadSession uploadSession = partitionSpecToUploadSession.get(partitionSpec);

    if (uploadSession == null) {
      int retry = 0;
      long sleep = 2000;
      while (true) {
        try {
          if (partitionSpec.isEmpty()) {
            System.out.println("[Data-carrier] creating record worker");
            uploadSession = tunnel.createUploadSession(odps.getDefaultProject(),
                                                       odpsTableName);
            System.out.println("[Data-carrier] creating record worker done");
          } else {
            System.out.println("[Data-carrier] creating record worker for " + partitionSpec);
            uploadSession = tunnel.createUploadSession(odps.getDefaultProject(),
                                                       odpsTableName,
                                                       new PartitionSpec(partitionSpec));
            System.out
                .println("[Data-carrier] creating record worker for " + partitionSpec + " done");
          }
          break;
        } catch (TunnelException e) {
          System.out.println("[Data-carrier] create session failed, retry: " + retry);
          e.printStackTrace(System.out);
          retry++;
          if (retry > 5) {
            throw new HiveException(e);
          }
          try {
            Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
          } catch (InterruptedException ex) {
            ex.printStackTrace();
          }
          sleep = sleep * 2;
        }
      }
      partitionSpecToUploadSession.put(partitionSpec, uploadSession);
    }

    return uploadSession;
  }

  private String[] trimAll(String[] array) {
    for (int i = 0; i < array.length; i++) {
      array[i] = array[i].trim();
    }
    return array;
  }

  @Override
  public void close() throws HiveException {
    if (recordWriter == null) {
      System.out.println("[Data-carrier] recordWriter is null, seems no record is fed to this UDTF");
    } else {
      // TODO: rely on tunnel retry strategy once the RuntimeException bug is fixed
      int retry = 5;
      while (true) {
        try {
          recordWriter.close();
          break;
        } catch (Exception e) {
          System.out.println("[Data-carrier] Record writer failed to close, retry: " + retry);
          e.printStackTrace(System.out);
          retry--;
          if (retry <= 0) {
            throw new HiveException(e);
          }
        }
      }
    }

    for (String partitionSpec : partitionSpecToUploadSession.keySet()) {
      // If the number of parallel commit is huge, commit could fail. So we retry 5 times for each
      // session
      int retry = 5;
      while (true) {
        try {
          System.out.println("[Data-carrier] committing " + partitionSpec);
          partitionSpecToUploadSession.get(partitionSpec).commit();
          System.out.println("[Data-carrier] committing " + partitionSpec + " done");
          break;
        } catch (IOException | TunnelException e) {
          System.out.println("[Data-carrier] committing" + partitionSpec + " failed, retry: " + retry);
          e.printStackTrace(System.out);
          retry--;
          if (retry <= 0) {
            throw new HiveException(e);
          }
        }
      }
    }

    forwardObj[0] = numRecordTransferred;
    forward(forwardObj);
  }
}
