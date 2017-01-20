/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.flume.sink.dataobject;

import java.util.Map;

/**
 * A data object that record a row of the ODPS table.
 * <p><tt>rowMap:</tt> A field name to value map of a row.</p>
 * <p><tt>partitionSpec:</tt> The partition of the record in the ODPS table.</p>
 */
public class OdpsRowDO {
    public Map<String, String> getRowMap() {
        return rowMap;
    }

    public void setRowMap(Map<String, String> rowMap) {
        this.rowMap = rowMap;
    }

    public void setPartitionSpec(String partitionSpec) {
        this.partitionSpec = partitionSpec;
    }

    public String getPartitionSpec() {
        return partitionSpec;
    }

    private String partitionSpec;
    private Map<String, String> rowMap;
}
