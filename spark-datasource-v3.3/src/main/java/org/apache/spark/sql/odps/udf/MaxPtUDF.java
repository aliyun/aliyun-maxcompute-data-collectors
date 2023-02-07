package org.apache.spark.sql.odps.udf;
 
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import org.apache.spark.sql.odps.OdpsClient;
import org.apache.hadoop.hive.ql.exec.UDF;
 
import java.util.List;
 
public class MaxPtUDF extends UDF {
 
    public static String evaluate(String tableName) throws OdpsException {
        Instance instance =  SQLTask.run(OdpsClient.builder().getOrCreate().odps(), "select max_pt(\"" + tableName + "\");");
        instance.waitForSuccess();
        List<Record> res = SQLTask.getResult(instance);
        if (res != null && res.size() > 0) {
            return res.get(0).getString(0);
        }
        throw new OdpsException("Get max pt from " + tableName + " failed!");
    }
 
}