package org.apache.spark.sql.odps.udf;

import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MaxPtUnboundFunction implements UnboundFunction {

    @Override
    public BoundFunction bind(StructType inputType) {
        if (inputType.fields().length != 1) {
            throw new UnsupportedOperationException("Expect one argument");
        }
        for (StructField field : inputType.fields()) {
            DataType type = field.dataType();
            if (!(type instanceof StringType)) {
                throw new UnsupportedOperationException(
                        "Expect StringType but found " + type);
            };
        }
        return new MaxPtBoundFunction();
    }

    @Override
    public String description() {
        return "Returns the specific of the largest partition if the table is single-level partitioned; " +
                "Returns the highest-level partition specific of the largest partition if the table has multi-level partitioning " +
                "(In this case, it is recommended to use the MAX aggregate function instead).";
    }

    @Override
    public String name() {
        return "odps_max_pt";
    }
}
