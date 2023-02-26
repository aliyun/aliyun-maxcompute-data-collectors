package org.apache.flink.odps.sink.utils;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class RowDataProjection implements Serializable {
    private static final long serialVersionUID = 1L;

    private final RowData.FieldGetter[] fieldGetters;

    protected RowDataProjection(LogicalType[] types, int[] positions) {
        Preconditions.checkArgument(types.length == positions.length,
                "types and positions should have the equal number");
        this.fieldGetters = new RowData.FieldGetter[types.length];
        for (int i = 0; i < types.length; i++) {
            final LogicalType type = types[i];
            final int pos = positions[i];
            this.fieldGetters[i] = RowData.createFieldGetter(type, pos);
        }
    }

    public static RowDataProjection instance(RowType rowType, int[] positions) {
        List<LogicalType> fieldTypes = rowType.getChildren();
        final LogicalType[] types = Arrays.stream(positions).mapToObj(fieldTypes::get).toArray(LogicalType[]::new);
        return new RowDataProjection(types, positions);
    }

    public static RowDataProjection instance(LogicalType[] types, int[] positions) {
        return new RowDataProjection(types, positions);
    }

    public RowData project(RowData rowData) {
        GenericRowData genericRowData = new GenericRowData(this.fieldGetters.length);
        genericRowData.setRowKind(rowData.getRowKind());
        for (int i = 0; i < this.fieldGetters.length; i++) {
            final Object val = this.fieldGetters[i].getFieldOrNull(rowData);
            genericRowData.setField(i, val);
        }
        return genericRowData;
    }

    public Object[] projectAsValues(RowData rowData) {
        Object[] values = new Object[this.fieldGetters.length];
        for (int i = 0; i < this.fieldGetters.length; i++) {
            final Object val = this.fieldGetters[i].getFieldOrNull(rowData);
            values[i] = val;
        }
        return values;
    }
}
