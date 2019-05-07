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
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import java.util.ArrayList;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class FlexibleOdpsDataTransferUDTF extends OdpsDataTransferUDTF {
  ObjectInspector[] confObjectInspectors = new ObjectInspector[5];

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) {
    // First 5 args are access_id, access_key, end_point, tunnel_endpoint and project_name, skip
    // them
    this.objectInspectors = new ObjectInspector[args.length - 5];
    for (int i = 0; i < 5; i++) {
      this.confObjectInspectors[i] = args[i];
    }
    for (int i = 5; i < args.length; i++) {
      this.objectInspectors[i - 5] = args[i];
    }
    // This UDTF doesn't output anything
    return ObjectInspectorFactory.getStandardStructObjectInspector(new ArrayList<String>(),
        new ArrayList<ObjectInspector>());
  }

  @Override
  public void process(Object[] args) throws HiveException {
    // First 5 args are access_id, access_key, end_point, tunnel_endpoint and project_name
    if (this.odps == null) {
      StringObjectInspector soi0 = (StringObjectInspector) confObjectInspectors[0];
      StringObjectInspector soi1 = (StringObjectInspector) confObjectInspectors[1];
      StringObjectInspector soi2 = (StringObjectInspector) confObjectInspectors[2];
      StringObjectInspector soi3 = (StringObjectInspector) confObjectInspectors[3];
      StringObjectInspector soi4 = (StringObjectInspector) confObjectInspectors[4];

      String accessId = soi0.getPrimitiveJavaObject(args[0]).trim();
      String accessKey = soi1.getPrimitiveJavaObject(args[1]).trim();
      String endpoint = soi2.getPrimitiveJavaObject(args[2]).trim();
      String tunnelEndpoint = soi3.getPrimitiveJavaObject(args[3]).trim();
      String projectName = soi4.getPrimitiveJavaObject(args[4]).trim();

      AliyunAccount account = new AliyunAccount(accessId, accessKey);
      this.odps = new Odps(account);
      this.odps.setDefaultProject(projectName);
      this.odps.setEndpoint(endpoint);
      this.tunnel = new TableTunnel(odps);
      if (!tunnelEndpoint.isEmpty()) {
        this.tunnel.setEndpoint(tunnelEndpoint);
      }
    }

    Object[] restArgs = new Object[args.length - 5];
    for (int i = 5; i < args.length; i++) {
      restArgs[i - 5] = args[i];
    }
    super.process(restArgs);
  }
}
