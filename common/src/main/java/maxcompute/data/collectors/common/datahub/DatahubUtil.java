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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package maxcompute.data.collectors.common.datahub;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;

import java.util.ArrayList;

public class DatahubUtil {
    public static final String SEPERATOR = ",";
    public static final String DELIMITER = ":";

    public static DatahubClient getDatahubClientInstance(DatahubConfiguration conf) throws DatahubClientException {
        return new DatahubClient(conf);
    }

    /**
     * Transform a schema string to RecordSchema.
     *
     * @param str, the form is like "c1:string,c2:string,c3:bigint,pt:string"
     * @return Datahub RecordSchema object
     */
    public static RecordSchema buildDatahubSchema(String str) {
        if (str == null || str.isEmpty()) {
            return new RecordSchema();
        }
        String remain = str;
        ArrayList<Field> cols = new ArrayList<Field>();
        int pos;
        while (remain.length() > 0) {
            pos = remain.indexOf(SEPERATOR);
            if (pos < 0) {
                // Last one
                pos = remain.length();
            }
            String tok = remain.substring(0, pos);
            String[] knv = tok.split(DELIMITER, 2);
            if (knv.length != 2) {
                throw new IllegalArgumentException(
                    "Malformed schema definition, expecting \"name:type\" but was \"" + tok + "\"");
            }
            if (pos == remain.length()) {
                remain = "";
            } else {
                remain = remain.substring(pos + 1);
            }
            cols.add(new Field(knv[0], FieldType.valueOf(knv[1].toUpperCase())));
        }
        RecordSchema schema = new RecordSchema();
        schema.setFields(cols);
        return schema;
    }
}
