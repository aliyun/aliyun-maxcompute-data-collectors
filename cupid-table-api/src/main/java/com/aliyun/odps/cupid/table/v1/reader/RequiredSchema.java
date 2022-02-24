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

package com.aliyun.odps.cupid.table.v1.reader;

import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.util.Schema;
import com.aliyun.odps.cupid.table.v1.util.Validator;

import java.util.List;

public final class RequiredSchema extends Schema {

    public static RequiredSchema all() {
        return ALL;
    }

    public static RequiredSchema columns(List<Attribute> columns) {
        Validator.checkList(columns, 0, "columns");
        return new RequiredSchema(Type.COLUMNS_SPECIFIED, columns.toArray(new Attribute[0]));
    }

    public enum Type {
        ALL,
        COLUMNS_SPECIFIED
    }

    public Type getType() {
        return type;
    }

    private Type type;

    private RequiredSchema(Type type, Attribute[] columns) {
        super(columns);
        this.type = type;
    }

    private static RequiredSchema ALL = new RequiredSchema(Type.ALL, new Attribute[0]);
}
