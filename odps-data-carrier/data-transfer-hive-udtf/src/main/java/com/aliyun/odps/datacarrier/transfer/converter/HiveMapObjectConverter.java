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

import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class HiveMapObjectConverter extends AbstractHiveObjectConverter {

  @Override
  public Object convert(ObjectInspector objectInspector, Object o, TypeInfo odpsTypeInfo) {
    if (o == null) {
      return null;
    }

    MapObjectInspector mapObjectInspector = (MapObjectInspector) objectInspector;
    ObjectInspector mapKeyObjectInspector = mapObjectInspector.getMapKeyObjectInspector();
    ObjectInspector mapValueObjectInspector = mapObjectInspector.getMapValueObjectInspector();
    TypeInfo mapKeyTypeInfo = ((MapTypeInfo) odpsTypeInfo).getKeyTypeInfo();
    TypeInfo mapValueTypeInfo = ((MapTypeInfo) odpsTypeInfo).getValueTypeInfo();

    Map map = mapObjectInspector.getMap(o);
    Map<Object, Object> newMap = new HashMap<>();
    for (Object k : map.keySet()) {
      Object v = map.get(k);
      newMap.put(HiveObjectConverter.convert(mapKeyObjectInspector, k, mapKeyTypeInfo),
          HiveObjectConverter.convert(mapValueObjectInspector, v, mapValueTypeInfo));
    }

    return newMap;
  }
}
