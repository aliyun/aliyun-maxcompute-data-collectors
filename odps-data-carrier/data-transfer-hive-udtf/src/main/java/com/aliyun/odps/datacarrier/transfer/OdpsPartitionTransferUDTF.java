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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class OdpsPartitionTransferUDTF extends GenericUDTF {

  ObjectInspector[] objectInspectors;
  Odps odps;
  TableTunnel tunnel;
  UploadSession uploadSession;
  RecordWriter recordWriter;
  String odpsProjectName;
  String currentOdpsTableName;
  List<String> odpsColumnNames;
  String currentOdpsPartitionSpec;
  TableSchema schema;

  Long numRecordTransferred = 0L;
  Object[] forwardObj = new Object[1];

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
        OdpsConfig odpsConfig = new OdpsConfig("odps_config.ini");
        AliyunAccount account = new AliyunAccount(odpsConfig.getAccessId(), odpsConfig.getAccessKey());
        odps = new Odps(account);
        odps.setDefaultProject(odpsConfig.getProjectName());
        odps.setEndpoint(odpsConfig.getOdpsEndpoint());
        tunnel = new TableTunnel(odps);
        if (odpsConfig.getTunnelEndpoint() != null) {
          tunnel.setEndpoint(odpsConfig.getTunnelEndpoint());
        }
      }

      if (currentOdpsTableName == null) {
        StringObjectInspector soi0 = (StringObjectInspector) objectInspectors[0];
        StringObjectInspector soi1 = (StringObjectInspector) objectInspectors[1];
        StringObjectInspector soi2 = (StringObjectInspector) objectInspectors[2];
        StringObjectInspector soi3 = (StringObjectInspector) objectInspectors[3];

        odpsProjectName = soi0.getPrimitiveJavaObject(args[0]).trim();

        currentOdpsTableName = soi1.getPrimitiveJavaObject(args[1]).trim();
        schema = odps.tables().get(currentOdpsTableName).getSchema();

        currentOdpsPartitionSpec = soi2.getPrimitiveJavaObject(args[2]).trim();
        uploadSession = tunnel.createUploadSession(odps.getDefaultProject(),
            currentOdpsTableName, new PartitionSpec(currentOdpsPartitionSpec));
        recordWriter = uploadSession.openBufferedWriter(true);
        ((TunnelBufferedWriter) recordWriter).setBufferSize(64 * 1024 * 1024);

        String odpsColumnNameString = soi3.getPrimitiveJavaObject(args[3]).trim();
        odpsColumnNames = new ArrayList<>();
        if (!odpsColumnNameString.isEmpty()) {
          odpsColumnNames.addAll(Arrays.asList(trimAll(odpsColumnNameString.split(","))));
        }
      }

      List<Object> hiveColumnValues = new ArrayList<>();
      for (int i = 0; i < odpsColumnNames.size(); i++) {
        hiveColumnValues.add(args[i + 4]);
      }

      Record record = uploadSession.newRecord();
      for (int i = 0; i < odpsColumnNames.size(); i++) {
        String odpsColumnName = odpsColumnNames.get(i);
        Object value = hiveColumnValues.get(i);
        if (value == null) {
          continue;
        }

        // Handle data types
        ObjectInspector objectInspector = objectInspectors[i + 4];
        TypeInfo typeInfo = schema.getColumn(odpsColumnName).getTypeInfo();

        record.set(odpsColumnName, HiveObjectConverter.convert(objectInspector, value, typeInfo));
      }

      recordWriter.write(record);
      numRecordTransferred += 1;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
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

    forwardObj[0] = numRecordTransferred;
    forward(forwardObj);
  }
}
