package maxcompute.data.collectors.common.datahub;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.odps.utils.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class RecordUtil {

    final public static Set trueString = new HashSet() {{
        add("true");
        add("1");
        add("y");
    }};

    final public static Set falseString = new HashSet() {{
        add("false");
        add("0");
        add("n");
    }};

    public static void setFieldValue(RecordEntry recordEntry,
        Field field,
        boolean isValueNull,
        String fieldValue,
        boolean isDateFormat,
        SimpleDateFormat simpleDateFormat,
        boolean isBlankValueAsNull) throws ParseException {
        if (field != null && !isValueNull) {
            if (isBlankValueAsNull && StringUtils.isEmpty(fieldValue)) {
                return;
            }
            switch (field.getType()) {
                case STRING:
                    recordEntry.setString(field.getName(), fieldValue);
                    break;
                case BIGINT:
                    recordEntry.setBigint(field.getName(), Long.parseLong(fieldValue));
                    break;
                case DOUBLE:
                    recordEntry.setDouble(field.getName(), Double.parseDouble(fieldValue));
                    break;
                case BOOLEAN:
                    if (trueString.contains(fieldValue.toLowerCase())) {
                        recordEntry.setBoolean(field.getName(), true);
                    } else if (falseString.contains(fieldValue.toLowerCase())) {
                        recordEntry.setBoolean(field.getName(), false);
                    }
                    break;
                case TIMESTAMP:
                    // datahub中存储的时间是微秒
                    if (isDateFormat) {
                        Date date = simpleDateFormat.parse(fieldValue);
                        recordEntry.setTimeStamp(field.getName(), date.getTime() * 1000);
                    } else {
                        recordEntry.setTimeStamp(field.getName(), Long.parseLong(fieldValue));
                    }

                    break;
                default:
                    throw new RuntimeException("Unknown column type: " + field.getType() + " ,value is: " + fieldValue);
            }
        }
    }
}
