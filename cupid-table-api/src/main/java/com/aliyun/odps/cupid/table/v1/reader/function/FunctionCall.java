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

package com.aliyun.odps.cupid.table.v1.reader.function;

import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.type.TypeInfoFactory;

public abstract class FunctionCall {

    public Attribute toAttribute() {
        return new Attribute(makeFullName(), TypeInfoFactory.STRING.getTypeName());
    }

    private final String functionName;
    private final String attribute;
    private final String[] literalArgs;

    protected FunctionCall(String functionName, String attribute, String[] literalArgs) {
        this.functionName = functionName;
        this.attribute = attribute;
        this.literalArgs = literalArgs;
    }

    private String makeFullName() {
        StringBuilder b = new StringBuilder(functionName);
        b.append("(`");
        b.append(attribute);
        b.append("`");

        String[] args = literalArgs;
        for (int i = 0; i < args.length; i++) {
            b.append("`");
            b.append(args[i]);
            b.append("`");
            if (i < args.length - 1) {
                b.append(", ");
            }
        }
        return b.append(')').toString();
    }
}
