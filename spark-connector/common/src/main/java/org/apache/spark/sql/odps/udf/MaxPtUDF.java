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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.odps.udf;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.spark.sql.odps.OdpsClient;

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