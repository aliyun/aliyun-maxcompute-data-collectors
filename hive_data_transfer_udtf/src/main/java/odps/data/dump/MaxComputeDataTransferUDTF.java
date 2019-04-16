package odps.data.dump;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.aliyun.odps.utils.StringUtils;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import maxcompute.data.collectors.common.maxcompute.RecordUtil;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class MaxComputeDataTransferUDTF extends GenericUDTF{
    private static final String[] TIMESTAMP_PATTERNS = new String[] {
        "yyyy-MM-dd HH:mm:ss.SSSSSSSSS",
        "yyyy-MM-dd HH:mm:ss.SSSSSSSS",
        "yyyy-MM-dd HH:mm:ss.SSSSSSS",
        "yyyy-MM-dd HH:mm:ss.SSSSSS",
        "yyyy-MM-dd HH:mm:ss.SSSSS",
        "yyyy-MM-dd HH:mm:ss.SSSS",
        "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm:ss.SS",
        "yyyy-MM-dd HH:mm:ss.S",
        "yyyy-MM-dd HH:mm:ss"
    };

    private transient ObjectInspector[] argumentOIs;
    private Odps odps;
    private TableTunnel tunnel;
    private UploadSession uploadSession;
    private TableSchema tableSchema;

    private OdpsConfig odpsConfig = new OdpsConfig();

    private RecordWriter writer = null;
    private String tableName = "";
    private String partitionName = "";
    private String[] columns;

    public MaxComputeDataTransferUDTF() throws Exception {
        odpsConfig.init("odps.conf");

        Account account = new AliyunAccount(odpsConfig.getAccessId(), odpsConfig.getAccessKey());
        odps = new Odps(account);
        odps.setEndpoint(odpsConfig.getOdpsEndPoint());
        odps.setDefaultProject(odpsConfig.getProjectName());
        tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(odpsConfig.getTunnelEndPoint());
    }

    @Override public StructObjectInspector initialize(ObjectInspector[] objectInspectors)
        throws UDFArgumentException {
        argumentOIs = objectInspectors;
        // output inspectors -- an object with two fields!  no use
        List<String> fieldNames = new ArrayList<String>(2);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
        fieldNames.add("name");
        fieldNames.add("surname");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // tableName, partitionName, columns, c1, c2, c3
    @Override public void process(Object[] args) throws HiveException {
        Object table = args[0];
        Object partition = args[1];
        Object column = args[2];
        StringObjectInspector soi0 = (StringObjectInspector)argumentOIs[0];
        StringObjectInspector soi1 = (StringObjectInspector)argumentOIs[1];
        StringObjectInspector soi2 = (StringObjectInspector)argumentOIs[2];
        String tmpTableName = soi0.getPrimitiveJavaObject(table);
        String tmpPartitionName = soi1.getPrimitiveJavaObject(partition);
        String columnStr = soi2.getPrimitiveJavaObject(column);
        if (columns == null) {
            columns = columnStr.split(",");
        }
        for (int i = 0; i < columns.length; i++) {
            columns[i] = columns[i].trim();
        }
        if (!tableName.equals(tmpTableName) || !partitionName.equals(tmpPartitionName)) {
            tableName = tmpTableName.trim();
            partitionName = tmpPartitionName.trim();

            if (uploadSession == null) {
                try {
                    Table t = odps.tables().get(tableName);
                    tableSchema = t.getSchema();
                    if (StringUtils.isEmpty(partitionName)) {
                        uploadSession = tunnel.createUploadSession(odpsConfig.getProjectName(), tableName);
                    } else {
                        uploadSession = tunnel.createUploadSession(odpsConfig.getProjectName(), tableName, new PartitionSpec(partitionName));
                    }
                } catch (TunnelException e) {
                    throw new HiveException("create upload session failed!", e);
                }
            }
        }

        try {
            if (writer == null) {
                writer = uploadSession.openBufferedWriter(true); // compress transfer
                ((TunnelBufferedWriter)writer).setBufferSize(256 * 1024 * 1024);
            }
        } catch (TunnelException e) {
            throw new HiveException("create buffered writer failed", e);
        }
        ArrayRecord product = (ArrayRecord) uploadSession.newRecord();
        for (int i = 3; i < args.length; ++i) {
            Object colValue = args[i];
            if (colValue == null) {
                continue;
            }
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector)argumentOIs[i];
            Object colValueJavaObj = poi.getPrimitiveJavaObject(colValue);
            if (colValueJavaObj == null) {
                continue;
            }
            String columnName = columns[i-3];
            try {
                OdpsType odpsType = tableSchema.getColumn(columnName).getType();
                String value = colValueJavaObj.toString();
                SimpleDateFormat format = null;
                if (odpsType.equals(OdpsType.TIMESTAMP)) {
                    for (String fmt : TIMESTAMP_PATTERNS) {
                        if (fmt.length() == value.length()) {
                            format = new SimpleDateFormat(fmt);
                            break;
                        }
                    }
                }
                RecordUtil.setFieldValue(product, columnName, value, odpsType, format);
            } catch (ParseException e) {
                throw new HiveException("set Field value failed", e);
            }
        }
        try {
            writer.write(product);
        } catch (IOException e) {
            throw new HiveException("write record failed", e);
        }
    }

    @Override public void close() {
        if (uploadSession != null) {
            try {
                writer.close();
                uploadSession.commit();
            } catch (Exception e) {
                throw new RuntimeException("close failed", e);
            }
        }
    }
}


