package maxcompute.data.collectors.common.maxcompute;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.*;
import com.aliyun.odps.utils.StringUtils;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.math.BigDecimal;
import java.sql.*;


public class RecordUtil {
    final static Set trueString = new HashSet() {{
        add("true");
        add("1");
        add("y");
    }};

    final static Set falseString = new HashSet() {{
        add("false");
        add("0");
        add("n");
    }};

    public static void setFieldValue(ArrayRecord record, String field, String fieldValue,
        OdpsType odpsType, SimpleDateFormat dateFormat) throws ParseException {
        if (!StringUtils.isEmpty(field) && !StringUtils.isEmpty(fieldValue)) {
            switch (odpsType) {
                case STRING:
                    record.setString(field, fieldValue);
                    break;
                case BIGINT:
                    record.setBigint(field, Long.parseLong(fieldValue));
                    break;
                case DATETIME:
                    record.setDatetime(field, dateFormat.parse(fieldValue));
                    break;
                case DOUBLE:
                    record.setDouble(field, Double.parseDouble(fieldValue));
                    break;
                case BOOLEAN:
                    if (trueString.contains(fieldValue.toLowerCase())) {
                        record.setBoolean(field, true);
                    } else if (falseString.contains(fieldValue.toLowerCase())) {
                        record.setBoolean(field, false);
                    }
                    break;
                case DECIMAL:
                    record.setDecimal(field, new BigDecimal(fieldValue));
                    break;
                case CHAR:
                    record.setChar(field, new Char(fieldValue));
                    break;
                case VARCHAR:
                    record.setVarchar(field, new Varchar(fieldValue));
                    break;
                case TINYINT:
                    record.setTinyint(field, Byte.parseByte(fieldValue));
                    break;
                case SMALLINT:
                    record.setSmallint(field, Short.parseShort(fieldValue));
                    break;
                case INT:
                    record.setInt(field, Integer.parseInt(fieldValue));
                    break;
                case FLOAT:
                    record.setFloat(field, Float.parseFloat(fieldValue));
                    break;
                case DATE:
                    record
                        .setDate(field, new java.sql.Date(dateFormat.parse(fieldValue).getTime()));
                    break;
                case TIMESTAMP:
                    record
                        .setTimestamp(field, new Timestamp(dateFormat.parse(fieldValue).getTime()));
                    break;
                case BINARY:
                    record.setBinary(field, new Binary(fieldValue.getBytes()));
                    break;
                default:
                    throw new RuntimeException("Unknown column type: " + odpsType);
            }
        }
    }

    public static String getFieldValueAsString(ArrayRecord record, Column column, int pos) {
        String colValue = null;
        switch (column.getType()) {
            case BIGINT: {
                Long v = record.getBigint(pos);
                colValue = v == null ? null : String.valueOf(v.longValue());
                break;
            }
            case BOOLEAN: {
                Boolean v = record.getBoolean(pos);
                colValue = v == null ? null : v.toString();
                break;
            }
            case DATETIME: {
                java.util.Date v = record.getDatetime(pos);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                colValue = v == null ? null : sdf.format(v);
                break;
            }
            case DOUBLE: {
                Double v = record.getDouble(pos);
                colValue = v == null ? null : String.valueOf(v.doubleValue());
                break;
            }
            case STRING: {
                String v = record.getString(pos);
                colValue = v == null ? null : v;
                break;
            }
            case DECIMAL: {
                BigDecimal v = record.getDecimal(pos);
                colValue = v == null ? null : v.toPlainString();
                break;
            }
            case CHAR: {
                Char v = record.getChar(pos);
                colValue = v == null ? null : v.getValue();
                break;
            }
            case VARCHAR: {
                Varchar v = record.getVarchar(pos);
                colValue = v == null ? null : v.getValue();
                break;
            }
            case TINYINT: {
                Byte v = record.getTinyint(pos);
                colValue = v == null ? null : String.valueOf(v.byteValue());
                break;
            }
            case SMALLINT: {
                Short v = record.getSmallint(pos);
                colValue = v == null ? null : String.valueOf(v.shortValue());
                break;
            }
            case INT: {
                Integer v = record.getInt(pos);
                colValue = v == null ? null : String.valueOf(v.intValue());
                break;
            }
            case FLOAT: {
                Float v = record.getFloat(pos);
                colValue = v == null ? null : String.valueOf(v.floatValue());
                break;
            }
            case DATE: {
                java.sql.Date v = record.getDate(pos);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                colValue = v == null ? null : sdf.format(v);
                break;
            }
            case TIMESTAMP: {
                Timestamp v = record.getTimestamp(pos);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                colValue = v == null ? null : sdf.format(v);
                break;
            }
            case BINARY: {
                Binary v = record.getBinary(pos);
                colValue = v == null ? null : v.toString();
                break;
            }
            default:
                throw new RuntimeException("Unknown column type: " + column.getType());
        }
        return colValue;

    }
}
