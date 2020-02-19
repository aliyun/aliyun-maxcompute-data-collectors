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

package com.aliyun.odps.datacarrier.transfer.converter;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class HiveStructObjectConverter extends AbstractHiveObjectConverter {

  @Override
  public Object convert(ObjectInspector objectInspector, Object o, TypeInfo odpsTypeInfo) {
    if (o == null) {
      return null;
    }

    StructObjectInspector structObjectInspector = (StructObjectInspector) objectInspector;
    StructTypeInfo structTypeInfo = (StructTypeInfo) odpsTypeInfo;

    List<Object> odpsValues = new ArrayList<>();
    List<TypeInfo> fieldTypeInfos = structTypeInfo.getFieldTypeInfos();
    List<Object> values = structObjectInspector.getStructFieldsDataAsList(o);
    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    for(int i = 0; i < fields.size(); i++) {
      StructField field = fields.get(i);
      Object value = HiveObjectConverter.convert(
          field.getFieldObjectInspector(), values.get(i), fieldTypeInfos.get(i));
      odpsValues.add(value);
    }
    return new SimpleStruct(structTypeInfo, odpsValues);
  }
}
