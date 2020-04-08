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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class RandomDataGenerateUDTF extends GenericUDTF {

  private static enum DATA_TYPES {
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    TIMESTAMP,
    STRING,
    VARCHAR,
    CHAR,
    BOOLEAN,
    BINARY,
    ARRAY,
    MAP,
    STRUCT
  }

  private ObjectInspector[] objectInspectors;

  private boolean initialized = false;
  private int multiplier = 1;
  private MyRandom random = new MyRandom();

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    objectInspectors = args;
    if (objectInspectors.length != 1) {
      throw new UDFArgumentException(String.format("Unexpected argument count: %d, expect: 1",
                                                   objectInspectors.length));
    }

    List<String> fieldNames = new ArrayList<>();
    // Primitive types
    fieldNames.add("tinyint");
    fieldNames.add("smallint");
    fieldNames.add("int");
    fieldNames.add("bigint");
    fieldNames.add("float");
    fieldNames.add("double");
    fieldNames.add("decimal");
    fieldNames.add("timestamp");
    fieldNames.add("string");
    fieldNames.add("varchar");
    fieldNames.add("char");
    fieldNames.add("boolean");
    fieldNames.add("binary");
    // Complex types
    fieldNames.add("array");
    fieldNames.add("map");
    fieldNames.add("struct");

    List<ObjectInspector> outputObjectInspectors = new ArrayList<>();
    // Primitive object inspectors
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaHiveVarcharObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaHiveCharObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);

    // Complex object inspectors
    StandardListObjectInspector listObjectInspector =
        ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outputObjectInspectors.add(listObjectInspector);

    StandardMapObjectInspector mapObjectInspector =
        ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outputObjectInspectors.add(mapObjectInspector);

    List<String> sturctFieldNames = new ArrayList<>();
    sturctFieldNames.add("c1");
    sturctFieldNames.add("c2");
    List<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
    structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    StandardStructObjectInspector structObjectInspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(sturctFieldNames,
                                                                structFieldObjectInspectors);
    outputObjectInspectors.add(structObjectInspector);

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                                                                   outputObjectInspectors);
  }

  @Override
  public void process(Object[] objects) throws HiveException {
    if (!initialized) {
      IntObjectInspector oi = (IntObjectInspector) objectInspectors[0];
      multiplier = oi.get(objects[0]);
    }

    for (int i = 0; i < multiplier; i++) {
      Object[] ret = new Object[16];
      ret[0] = random.nextByte();
      ret[1] = random.nextShort();
      ret[2] = random.nextInt();
      ret[3] = random.nextLong();
      ret[4] = random.nextFloat();
      ret[5] = random.nextDouble();
      ret[6] = HiveDecimal.create(random.nextDecimal(10, 0));
      ret[7] = random.nextTimestamp();
      ret[8] = random.nextString(255);
      ret[9] = new HiveVarchar(random.nextString(255), 255);
      ret[10] = new HiveChar(random.nextString(255), 255);
      ret[11] = random.nextBoolean();
      ret[12] = random.nextString(255).getBytes();

      List<String> list = new ArrayList<>();
      list.add(random.nextString(255));
      ret[13] = list;

      Map<String, String> map = new HashMap<>();
      map.put(random.nextString(255), random.nextString(255));
      ret[14] = map;

      Object[] struct = new Object[2];
      struct[0] = random.nextString(255);
      struct[1] = random.nextLong();
      ret[15] = struct;

      forward(ret);
    }
  }

  @Override
  public void close() throws HiveException {
    // do nothing
  }

  /**
   * Utils
   */
  public static class MyRandom extends Random {
    private static final String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public byte nextByte() {
      return (byte) next(8);
    }

    public short nextShort() {
      return (short) next(16);
    }

    public BigDecimal nextDecimal(int precision, int scale) {
      if (scale == 0) {
        return new BigDecimal(Integer.toString(nextInt((int) Math.pow(10, precision))));
      } else {
        String bigDecimalStr =  String.format("%d.%d",
                                              nextInt((int) Math.pow(10, (precision - scale))),
                                              nextInt((int) Math.pow(10, scale)));
        return new BigDecimal(bigDecimalStr);
      }
    }

    public Timestamp nextTimestamp() {
      Calendar startYear = Calendar.getInstance();
      startYear.set(Calendar.YEAR, 0);
      long start = startYear.getTimeInMillis();

      Calendar endYear = Calendar.getInstance();
      endYear.set(Calendar.YEAR, 9999);
      long end = endYear.getTimeInMillis();

      return new Timestamp((long) (nextDouble() * (end - start)) + start);
    }

    public String nextString(int length) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < length; i++) {
        sb.append(alphabet.charAt(nextInt(alphabet.length())));
      }
      return sb.toString();
    }
  }
}
