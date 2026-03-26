package org.apache.spark.sql.odps.udf;

import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class UDFManager {

    // TODO control accessibility across namespaces
    private static final Map<String, UnboundFunction> udfMap = new HashMap<>(){
        {
            put("odps_max_pt", new MaxPtUnboundFunction());
        }
    };

    public static Set<String> listFunctionNames() {
        return udfMap.keySet();
    }

    public static UnboundFunction getUnboundFunction(String name) {
        return udfMap.get(name);
    }
}
