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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.odps.OdpsClient;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

public class MaxPtBoundFunction implements ScalarFunction<UTF8String> {

    @Override
    public DataType[] inputTypes() {
        return new DataType[]{DataTypes.StringType};
    }

    @Override
    public DataType resultType() {
        return DataTypes.StringType;
    }

    @Override
    public boolean isResultNullable() {
        return false;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public String canonicalName() {
        return "org.apache.spark.sql.odps.udf.MaxPtUDF";
    }

    @Override
    public String name() {
        return "odps_max_pt";
    }

    @Override
    public UTF8String produceResult(InternalRow input) {
        try {
            return evaluate(input.getString(0));
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
    }

    private UTF8String evaluate(String tableName) throws OdpsException {
        Instance instance =  SQLTask.run(OdpsClient.builder().getOrCreate().odps(), "select max_pt(\"" + tableName + "\");");
        instance.waitForSuccess();
        List<Record> res = SQLTask.getResult(instance);
        if (res != null && !res.isEmpty()) {
            String result = res.get(0).getString(0);
            return UTF8String.fromString(result);
        }
        throw new OdpsException("Get max pt from " + tableName + " failed!");
    }
}