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

package org.apache.flink.odps.schema;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.cupid.table.v1.util.TableUtils;
import com.aliyun.odps.type.TypeInfo;

import java.io.Serializable;

public class OdpsColumn implements Serializable {
    private static final long serialVersionUID = 3385061492593160182L;
    private final String name;
    private final OdpsType type;
    private final String typeName;
    private final boolean isPartition;
    private transient TypeInfo typeInfo;

    public OdpsColumn(String name, TypeInfo typeInfo) {
        this(name, typeInfo, false);
    }

    public OdpsColumn(String name, TypeInfo typeInfo, boolean isPartition) {
        this.name = name;
        this.typeInfo = typeInfo;
        this.type = typeInfo.getOdpsType();
        this.typeName = typeInfo.getTypeName();
        this.isPartition = isPartition;
    }

    public TypeInfo getTypeInfo() {
        if (typeInfo == null) {
            typeInfo = TableUtils.getTypeInfoFromString(this.typeName);
        }
        return typeInfo;
    }

    public String getName() {
        return name;
    }

    public OdpsType getType() {
        return type;
    }

    public boolean isPartition() {
        return isPartition;
    }

    public String getTypeName() {
        return typeName;
    }

    public static Column toColumn(OdpsColumn odpsColumn) {
        return new Column(odpsColumn.getName(),
                TableUtils.getTypeInfoFromString(odpsColumn.getTypeName()));
    }

    @Override
    public String toString() {
        return "ODPSColumn{" +
                "name='" + name + '\'' +
                ", typeName=" + typeName +
                ", isPartition=" + isPartition +
                '}';
    }
}
