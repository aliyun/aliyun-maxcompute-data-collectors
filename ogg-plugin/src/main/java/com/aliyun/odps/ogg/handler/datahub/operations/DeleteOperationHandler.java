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

package com.aliyun.odps.ogg.handler.datahub.operations;

import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.PluginStatictics;
import oracle.goldengate.datasource.adapt.Op;

public class DeleteOperationHandler extends OperationHandler {

    @Override
    public void process(Op op, Configure configure)
            throws Exception {
        processOperation(op, configure);
        PluginStatictics.addTotalDeletes();
    }

    @Override
    public String getOperateType() {
        return "D";
    }
}
