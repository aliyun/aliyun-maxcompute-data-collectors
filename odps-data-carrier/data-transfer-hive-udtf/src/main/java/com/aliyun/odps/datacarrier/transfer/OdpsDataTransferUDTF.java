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
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.datacarrier.transfer.converter.HiveObjectConverter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.TypeInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * Only for odps 2.0
 * @author: Jon (wangzhong.zw@alibaba-inc.com)
 */
public class OdpsDataTransferUDTF extends GenericUDTF {

  private ObjectInspector[] objectInspectors;
  private Odps odps;
  private TableTunnel tunnel;
  private UploadSession uploadSession;
  private RecordWriter recordWriter;
  private String currentOdpsTableName;
  private List<String> odpsColumnNames;
  private List<String> odpsPartitionColumnNames;
  private String currentOdpsPartitionSpec;

  public OdpsDataTransferUDTF() throws IOException {
    OdpsConfig odpsConfig = new OdpsConfig("res/console/conf/odps_config.ini");
    AliyunAccount account = new AliyunAccount(odpsConfig.getAccessId(), odpsConfig.getAccessKey());
    this.odps = new Odps(account);
    this.odps.setDefaultProject(odpsConfig.getProjectName());
    this.odps.setEndpoint(odpsConfig.getOdpsEndpoint());
    this.tunnel = new TableTunnel(odps);
    if (odpsConfig.getTunnelEndpoint() != null) {
      this.tunnel.setEndpoint(odpsConfig.getTunnelEndpoint());
    }
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    this.objectInspectors = args;
    // This UDTF doesn't output anything
    return ObjectInspectorFactory.getStandardStructObjectInspector(new ArrayList<String>(),
        new ArrayList<ObjectInspector>());
  }

  @Override
  public void process(Object[] args) throws HiveException {
    try {
      if (currentOdpsTableName == null) {
        StringObjectInspector soi0 = (StringObjectInspector) objectInspectors[0];
        StringObjectInspector soi1 = (StringObjectInspector) objectInspectors[1];
        StringObjectInspector soi2 = (StringObjectInspector) objectInspectors[2];

        currentOdpsTableName = soi0.getPrimitiveJavaObject(args[0]).trim();

        String odpsColumnNameString = soi1.getPrimitiveJavaObject(args[1]).trim();
        odpsColumnNames = new ArrayList<>();
        if (!odpsColumnNameString.isEmpty()) {
          odpsColumnNames.addAll(Arrays.asList(trimAll(odpsColumnNameString.split(","))));
        }

        String odpsPartitionColumnNameString = soi2.getPrimitiveJavaObject(args[2]).trim();
        odpsPartitionColumnNames = new ArrayList<>();
        if (!odpsPartitionColumnNameString.isEmpty()) {
          odpsPartitionColumnNames.addAll(
              Arrays.asList(trimAll(odpsPartitionColumnNameString.split(","))));
        }
      }

      List<Object> hiveColumnValues = new ArrayList<>();
      List<Object> hivePartitionColumnValues = new ArrayList<>();
      for (int i = 0; i < odpsColumnNames.size(); i++) {
        hiveColumnValues.add(args[i + 3]);
      }
      for (int i = 0; i < odpsPartitionColumnNames.size(); i++) {
        hivePartitionColumnValues.add(args[i + 3 + odpsColumnNames.size()]);
      }

      // Get partition spec
      String partitionSpec = getPartitionSpec(hivePartitionColumnValues);

      // Create new tunnel upload session & record writer or reuse the current ones
      if (currentOdpsPartitionSpec == null || !currentOdpsPartitionSpec.equals(partitionSpec)) {
        resetUploadSession(partitionSpec);
      }

      Record record = uploadSession.newRecord();
      for (int i = 0; i < odpsColumnNames.size(); i++) {
        String odpsColumnName = odpsColumnNames.get(i);
        Object value = hiveColumnValues.get(i);
        if (value == null) {
          continue;
        }

        // Handle data types
        ObjectInspector objectInspector = objectInspectors[i + 3];
        TypeInfo typeInfo = odps.tables()
            .get(currentOdpsTableName)
            .getSchema()
            .getColumn(odpsColumnName)
            .getTypeInfo();

        record.set(odpsColumnName, HiveObjectConverter.convert(objectInspector, value, typeInfo));
      }

      recordWriter.write(record);
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  private String getPartitionSpec(List<Object> hivePartitionColumnValues) {
    StringBuilder partitionSpecBuilder = new StringBuilder();
    for (int i = 0; i < odpsPartitionColumnNames.size(); ++i) {
      Object colValue = hivePartitionColumnValues.get(i);
      if (colValue == null) {
        continue;
      }

      ObjectInspector objectInspector = objectInspectors[i + 3 + odpsColumnNames.size()];
      TypeInfo typeInfo = odps.tables()
          .get(currentOdpsTableName)
          .getSchema()
          .getPartitionColumn(odpsPartitionColumnNames.get(i))
          .getTypeInfo();

      Object odpsValue = HiveObjectConverter.convert(objectInspector, colValue, typeInfo);
      partitionSpecBuilder.append(odpsPartitionColumnNames.get(i));
      partitionSpecBuilder.append("=\'");
      partitionSpecBuilder.append(odpsValue.toString()).append("\'");
      if (i != odpsPartitionColumnNames.size() - 1) {
        partitionSpecBuilder.append(",");
      }
    }
    return partitionSpecBuilder.toString();
  }

  private void resetUploadSession(String partitionSpec) throws TunnelException, IOException {
    currentOdpsPartitionSpec = partitionSpec;

    // End the previous session
    if (uploadSession != null) {
      recordWriter.close();
      uploadSession.commit();
    }

    if (partitionSpec.isEmpty()) {
      uploadSession = tunnel.createUploadSession(odps.getDefaultProject(),
          currentOdpsTableName);
    } else {
      uploadSession = tunnel.createUploadSession(odps.getDefaultProject(),
          currentOdpsTableName, new PartitionSpec(partitionSpec));
    }

    recordWriter = uploadSession.openBufferedWriter(true);
  }

  private String[] trimAll(String[] array) {
    for (int i = 0; i < array.length; i++) {
      array[i] = array[i].trim();
    }
    return array;
  }

  @Override
  public void close() throws HiveException {
    if (uploadSession != null) {
      try {
        recordWriter.close();
        uploadSession.commit();
      } catch (IOException | TunnelException e) {
        e.printStackTrace();
        throw new HiveException(e);
      }
    }
  }
}
