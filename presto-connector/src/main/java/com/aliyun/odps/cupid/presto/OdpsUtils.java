package com.aliyun.odps.cupid.presto;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.type.VarcharTypeInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;

public class OdpsUtils {
    public static OdpsColumnHandle buildOdpsColumn(Column col) {
        boolean isStringType = false;
        Type prestoType = null;
        switch (col.getTypeInfo().getOdpsType()) {
            case TINYINT:
                prestoType = TinyintType.TINYINT;
                break;
            case SMALLINT:
                prestoType = SmallintType.SMALLINT;
                break;
            case INT:
                prestoType = IntegerType.INTEGER;
                break;
            case BIGINT:
                prestoType = BigintType.BIGINT;
                break;
            case CHAR:
                prestoType = CharType.createCharType(((CharTypeInfo) col.getTypeInfo()).getLength());
                break;
            case VARCHAR:
                prestoType = VarcharType.createVarcharType(((VarcharTypeInfo) col.getTypeInfo()).getLength());
                break;
            case STRING:
                prestoType = VarcharType.VARCHAR;
                isStringType = true;
                break;
            case BINARY:
                prestoType = VarbinaryType.VARBINARY;
                break;
            case DATE:
                prestoType = DateType.DATE;
                break;
            case TIMESTAMP:
                prestoType = TimestampType.TIMESTAMP;
                break;
            case DATETIME:
                prestoType = TimestampType.TIMESTAMP;
                break;
            case FLOAT:
                prestoType = RealType.REAL;
                break;
            case DOUBLE:
                prestoType = DoubleType.DOUBLE;
                break;
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) col.getTypeInfo();
                int precision = decimalTypeInfo.getPrecision();
                int scale = decimalTypeInfo.getScale();
                if (decimalTypeInfo.getPrecision() > DecimalType.DEFAULT_PRECISION) {
                    precision = DecimalType.DEFAULT_PRECISION;
                }
                prestoType = DecimalType.createDecimalType(precision, scale);
                break;
            case BOOLEAN:
                prestoType = BooleanType.BOOLEAN;
                break;
            default:
                throw new PrestoException(OdpsErrorCode.ODPS_INTERNAL_ERROR, "unsupported type: " + col.getTypeInfo().getTypeName());

        }
        return new OdpsColumnHandle(col.getName(), prestoType, isStringType);
    }

    public static Column toOdpsColumn(OdpsColumnHandle columnHandle) {
        return new Column(columnHandle.getName(), toOdpsType(columnHandle.getType(), columnHandle.getIsStringType()));
    }

    public static TypeInfo toOdpsType(Type type, boolean isStringType) {
        if (isStringType) {
            return TypeInfoFactory.STRING;
        }
        if (type instanceof TinyintType) {
            return TypeInfoFactory.TINYINT;
        } else if (type instanceof SmallintType) {
            return TypeInfoFactory.SMALLINT;
        } else if (type instanceof IntegerType) {
            return TypeInfoFactory.INT;
        } else if (type instanceof BigintType) {
            return TypeInfoFactory.BIGINT;
        } else if (type instanceof CharType) {
            return TypeInfoFactory.getCharTypeInfo(((CharType) type).getLength());
        } else if (type instanceof VarcharType) {
            int length = ((VarcharType) type).getLength();
            if (length > 0xffff) {
                // exceeds the max length of odps varchar, use string instead
                return TypeInfoFactory.STRING;
            }
            return TypeInfoFactory.getVarcharTypeInfo(length);
        } else if (type instanceof VarbinaryType) {
            return TypeInfoFactory.BINARY;
        }else if (type instanceof DateType) {
            return TypeInfoFactory.DATE;
        } else if (type instanceof TimestampType) {
            return TypeInfoFactory.TIMESTAMP;
        } else if (type instanceof RealType) {
            return TypeInfoFactory.FLOAT;
        }else if (type instanceof DoubleType) {
            return TypeInfoFactory.DOUBLE;
        } else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return TypeInfoFactory.getDecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
        } else if (type instanceof BooleanType) {
            return TypeInfoFactory.BOOLEAN;
        } else {
            throw new RuntimeException("unsupported type" + type.toString());
        }
    }
}
