package com.aliyun.odps.datacarrier.transfer;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

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
  private String currentOdpsTableName = "";
  private String currentOdpsPartitionSpec = "";

  public OdpsDataTransferUDTF() throws IOException {
    OdpsConfig odpsConfig = new OdpsConfig("odps_config.properties");
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
      StringObjectInspector soi0 = (StringObjectInspector) objectInspectors[0];
      StringObjectInspector soi1 = (StringObjectInspector) objectInspectors[1];
      StringObjectInspector soi2 = (StringObjectInspector) objectInspectors[2];
      String odpsTableName = soi0.getPrimitiveJavaObject(args[0]).trim();
      String odpsColumnNameString = soi1.getPrimitiveJavaObject(args[1]).trim();
      String odpsPartitionColumnNameString = soi2.getPrimitiveJavaObject(args[2]).trim();

      List<String> odpsColumnNames = new ArrayList<String>();
      if (!odpsColumnNameString.isEmpty()) {
        odpsColumnNames.addAll(Arrays.asList(trimAll(odpsColumnNameString.split(","))));
      }
      List<String> odpsPartitionColumnNames = new ArrayList<String>();
      if (!odpsPartitionColumnNameString.isEmpty()) {
        odpsPartitionColumnNames.addAll(
            Arrays.asList(trimAll(odpsPartitionColumnNameString.split(","))));
      }

      List<Object> hiveColumnValues = new ArrayList<Object>();
      List<Object> hivePartitionColumnValues = new ArrayList<Object>();
      for (int i = 0; i < odpsColumnNames.size(); i++) {
        hiveColumnValues.add(args[i + 3]);
      }
      for (int i = 0; i < odpsPartitionColumnNames.size(); i++) {
        hivePartitionColumnValues.add(args[i + 3 + odpsColumnNames.size()]);
      }

      // Get partition spec
      StringBuilder partitionSpecBuilder = new StringBuilder();
      for (int i = 0; i < odpsPartitionColumnNames.size(); ++i) {
        Object colValue = hivePartitionColumnValues.get(i);
        if (colValue == null) {
          continue;
        }

        ObjectInspector objectInspector = objectInspectors[i + 3 + odpsColumnNames.size()];
        TypeInfo typeInfo = odps.tables()
            .get(odpsTableName)
            .getSchema()
            .getPartitionColumn(odpsPartitionColumnNames.get(i))
            .getTypeInfo();

        Object odpsValue = getOdpsObject(objectInspector, colValue, typeInfo);
        partitionSpecBuilder.append(odpsPartitionColumnNames.get(i));
        partitionSpecBuilder.append("=\'");
        partitionSpecBuilder.append(odpsValue.toString()).append("\'");
        if (i != odpsPartitionColumnNames.size() - 1) {
          partitionSpecBuilder.append(",");
        }
      }
      String partitionSpec = partitionSpecBuilder.toString();

      // Create new tunnel upload session & record writer or reuse the current ones
      if (!currentOdpsTableName.equals(odpsTableName) ||
          !currentOdpsPartitionSpec.equals(partitionSpec)) {
        currentOdpsTableName = odpsTableName;
        currentOdpsPartitionSpec = partitionSpec;

        // End the previous session
        if (uploadSession != null) {
          try {
            recordWriter.close();
            uploadSession.commit();
          } catch (IOException | TunnelException e) {
            e.printStackTrace();
            throw new HiveException(e);
          }
        }

        try {
          if (partitionSpec.isEmpty()) {
            uploadSession = tunnel.createUploadSession(odps.getDefaultProject(), odpsTableName);
          } else {
            uploadSession = tunnel.createUploadSession(odps.getDefaultProject(), odpsTableName,
                new PartitionSpec(partitionSpec));
          }

          recordWriter = uploadSession.openBufferedWriter(true);
        } catch (TunnelException e) {
          e.printStackTrace();
          throw new HiveException(e);
        }
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
            .get(odpsTableName)
            .getSchema()
            .getColumn(odpsColumnName)
            .getTypeInfo();

        record.set(odpsColumnName, getOdpsObject(objectInspector, value, typeInfo));
      }

      try {
        recordWriter.write(record);
      } catch (IOException e) {
        e.printStackTrace();
        throw new HiveException(e);
      }
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

  private Object getOdpsObject(ObjectInspector objectInspector, Object value, TypeInfo typeInfo) {
    if (objectInspector.getCategory().equals(Category.PRIMITIVE)) {
      return getOdpsPrimitiveObject(objectInspector, value);
    } else if (objectInspector.getCategory().equals(Category.LIST)) {
      return getOdpsListObject(objectInspector, value, typeInfo);
    } else if (objectInspector.getCategory().equals(Category.MAP)) {
      return getOdpsMapObject(objectInspector, value, typeInfo);
    } else if (objectInspector.getCategory().equals(Category.STRUCT)) {
      return getOdpsStructObject(objectInspector, value, typeInfo);
    } else {
      throw new IllegalArgumentException(
          "Unsupported hive data type: " + objectInspector.getCategory());
    }
  }

  private Object getOdpsPrimitiveObject(ObjectInspector objectInspector, Object value) {
    // Handle different primitive types respectively
    PrimitiveObjectInspector primitiveObjectInspector =
        (PrimitiveObjectInspector) objectInspector;
    PrimitiveCategory category = primitiveObjectInspector.getPrimitiveCategory();
    System.out.println(primitiveObjectInspector.getClass());

    if (category.equals(PrimitiveCategory.STRING)) {
      StringObjectInspector stringObjectInspector =
          (StringObjectInspector) primitiveObjectInspector;
      return stringObjectInspector.getPrimitiveJavaObject(value);
    } else if (category.equals(PrimitiveCategory.TIMESTAMP)) {
      TimestampObjectInspector timestampObjectInspector =
          (TimestampObjectInspector) primitiveObjectInspector;
      return timestampObjectInspector.getPrimitiveJavaObject(value);
    } else if (category.equals(PrimitiveCategory.BINARY)) {
      BinaryObjectInspector binaryObjectInspector =
          (BinaryObjectInspector) primitiveObjectInspector;
      return new Binary(binaryObjectInspector.getPrimitiveJavaObject(value));
    } else if (category.equals(PrimitiveCategory.BOOLEAN)) {
      return ((BooleanObjectInspector) primitiveObjectInspector).get(value);
    } else if (category.equals(PrimitiveCategory.BYTE)) {
      return ((ByteObjectInspector) primitiveObjectInspector).get(value);
    } else if (category.equals(PrimitiveCategory.CHAR)) {
      // TODO: support hive.compatible (return a Char object)
      HiveCharObjectInspector hiveCharObjectInspector =
          (HiveCharObjectInspector) primitiveObjectInspector;
      return hiveCharObjectInspector.getPrimitiveJavaObject(value).getValue();
    } else if (category.equals(PrimitiveCategory.DATE)) {
      DateObjectInspector dateObjectInspector = (DateObjectInspector) primitiveObjectInspector;
      return new java.sql.Date(dateObjectInspector.getPrimitiveJavaObject(value).getTime());
    } else if (category.equals(PrimitiveCategory.DECIMAL)) {
      HiveDecimalObjectInspector hiveDecimalObjectInspector =
          (HiveDecimalObjectInspector) primitiveObjectInspector;
      return hiveDecimalObjectInspector.getPrimitiveJavaObject(value).bigDecimalValue();
    } else if (category.equals(PrimitiveCategory.DOUBLE)) {
      return ((DoubleObjectInspector) primitiveObjectInspector).get(value);
    } else if (category.equals(PrimitiveCategory.FLOAT)) {
      return ((FloatObjectInspector) primitiveObjectInspector).get(value);
    } else if (category.equals(PrimitiveCategory.INT)) {
      return ((IntObjectInspector) primitiveObjectInspector).get(value);
    } else if (category.equals(PrimitiveCategory.LONG)) {
      return ((LongObjectInspector) primitiveObjectInspector).get(value);
    } else if (category.equals(PrimitiveCategory.SHORT)) {
      return ((ShortObjectInspector) primitiveObjectInspector).get(value);
    } else if (category.equals(PrimitiveCategory.VARCHAR)) {
      // Convert to String
      HiveVarcharObjectInspector hiveVarcharObjectInspector =
          (HiveVarcharObjectInspector) primitiveObjectInspector;
      String varcharValue = hiveVarcharObjectInspector
          .getPrimitiveJavaObject(value).getValue();
      return new Varchar(varcharValue);
    } else {
      throw new IllegalArgumentException(
          "Unsupported hive data type:" + primitiveObjectInspector.getPrimitiveCategory());
    }
  }

  private List getOdpsListObject(ObjectInspector objectInspector, Object value,
      TypeInfo typeInfo) {
    ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
    ObjectInspector elementInspector = listObjectInspector.getListElementObjectInspector();
    TypeInfo elementTypeInfo = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();
    List list = listObjectInspector.getList(value);
    List<Object> newList = new ArrayList<>();
    for (Object element : list) {
      newList.add(getOdpsObject(elementInspector, element, elementTypeInfo));
    }

    return newList;
  }

  private Map getOdpsMapObject(ObjectInspector objectInspector, Object value, TypeInfo typeInfo) {
    MapObjectInspector mapObjectInspector = (MapObjectInspector) objectInspector;
    ObjectInspector mapKeyObjectInspector = mapObjectInspector.getMapKeyObjectInspector();
    ObjectInspector mapValueObjectInspector = mapObjectInspector.getMapValueObjectInspector();
    TypeInfo mapKeyTypeInfo = ((MapTypeInfo) typeInfo).getKeyTypeInfo();
    TypeInfo mapValueTypeInfo = ((MapTypeInfo) typeInfo).getValueTypeInfo();

    Map map = mapObjectInspector.getMap(value);
    Map<Object, Object> newMap = new HashMap<>();
    for (Object k : map.keySet()) {
      Object v = map.get(k);
      newMap.put(getOdpsObject(mapKeyObjectInspector, k, mapKeyTypeInfo),
          getOdpsObject(mapValueObjectInspector, v, mapValueTypeInfo));
    }

    return newMap;
  }

  private Struct getOdpsStructObject(ObjectInspector objectInspector, Object value,
      TypeInfo typeInfo) {
    StructObjectInspector structObjectInspector = (StructObjectInspector) objectInspector;
    StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;

    List<Object> odpsValues = new ArrayList<>();
    List<TypeInfo> fieldTypeInfos = structTypeInfo.getFieldTypeInfos();
    List<Object> values = structObjectInspector.getStructFieldsDataAsList(value);
    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    for(int i = 0; i < fields.size(); i++) {
      StructField field = fields.get(i);
      odpsValues.add(
          getOdpsObject(field.getFieldObjectInspector(), values.get(i), fieldTypeInfos.get(i)));
    }
    return new SimpleStruct(structTypeInfo, odpsValues);
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
