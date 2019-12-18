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

import com.google.common.collect.Maps;
import oracle.goldengate.datasource.DsOperation;

import java.util.Map;

/**
 * Created by yongfeng.liang on 2016/3/18.
 */
public class OperationHandlerManager {
    private static Map<DsOperation.OpType, OperationHandler> handlers;

    public static void init() {
        handlers = Maps.newHashMap();

        DeleteOperationHandler deleteOperationHandler = new DeleteOperationHandler();
        InsertOperationHandler insertOperationHandler = new InsertOperationHandler();
        UpdateOperationHandler updateOperationHandler = new UpdateOperationHandler();

        handlers.put(DsOperation.OpType.DO_INSERT, insertOperationHandler);
        handlers.put(DsOperation.OpType.DO_UPDATE, updateOperationHandler);
        handlers.put(DsOperation.OpType.DO_UPDATE_AC, updateOperationHandler);
        handlers.put(DsOperation.OpType.DO_UPDATE_FIELDCOMP, updateOperationHandler);
        handlers.put(DsOperation.OpType.DO_UPDATE_FIELDCOMP_PK, updateOperationHandler);
        handlers.put(DsOperation.OpType.DO_UNIFIED_UPDATE_VAL, updateOperationHandler);
        handlers.put(DsOperation.OpType.DO_UNIFIED_PK_UPDATE_VAL, updateOperationHandler);
        handlers.put(DsOperation.OpType.DO_DELETE, deleteOperationHandler);
    }

    public static OperationHandler getHandler(DsOperation.OpType opType){
        return handlers.get(opType);
    }

}
