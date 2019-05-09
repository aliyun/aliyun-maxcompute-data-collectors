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

import com.aliyun.odps.type.TypeInfo;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public class HiveObjectConverter {

  private static HiveListObjectConverter hiveListObjectConverter = new HiveListObjectConverter();
  private static HiveMapObjectConverter hiveMapObjectConverter = new HiveMapObjectConverter();
  private static HiveStructObjectConverter hiveStructObjectConverter =
      new HiveStructObjectConverter();
  private static Map<PrimitiveCategory, AbstractHiveObjectConverter>
      primitiveCategoryToObjectConverter = new HashMap<>();
  static {
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.BINARY, new HiveBinaryObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.BOOLEAN, new HiveBooleanObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.BYTE, new HiveByteObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.CHAR, new HiveCharObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.DATE, new HiveDateObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.DECIMAL, new HiveDecimalObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.DOUBLE, new HiveDoubleObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.FLOAT, new HiveFloatObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.INT, new HiveIntegerObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.LONG, new HiveLongObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.SHORT, new HiveShortObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.STRING, new HiveStringObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.TIMESTAMP, new HiveTimeStampObjectConverter());
    primitiveCategoryToObjectConverter.put(
        PrimitiveCategory.VARCHAR, new HiveVarCharObjectConverter());
  }

  public static Object convert(ObjectInspector objectInspector, Object o, TypeInfo odpsTypeInfo) {

    if (objectInspector.getCategory().equals(Category.PRIMITIVE)) {
      PrimitiveObjectInspector primitiveObjectInspector =
          (PrimitiveObjectInspector) objectInspector;
      AbstractHiveObjectConverter hiveObjectConverter =
          primitiveCategoryToObjectConverter.get(primitiveObjectInspector.getPrimitiveCategory());
      if (hiveObjectConverter == null) {
        throw new IllegalArgumentException(
            "Unsupported hive data type:" + primitiveObjectInspector.getPrimitiveCategory());
      }
      return hiveObjectConverter.convert(objectInspector, o, odpsTypeInfo);
    } else if (objectInspector.getCategory().equals(Category.LIST)) {
      return hiveListObjectConverter.convert(objectInspector, o, odpsTypeInfo);
    } else if (objectInspector.getCategory().equals(Category.MAP)) {
      return hiveMapObjectConverter.convert(objectInspector, o, odpsTypeInfo);
    } else if (objectInspector.getCategory().equals(Category.STRUCT)) {
      return hiveStructObjectConverter.convert(objectInspector, o, odpsTypeInfo);
    } else {
      throw new IllegalArgumentException(
          "Unsupported hive data type: " + objectInspector.getCategory());
    }
  }
}
