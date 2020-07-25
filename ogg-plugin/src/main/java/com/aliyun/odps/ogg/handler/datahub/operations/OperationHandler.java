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

import com.aliyun.odps.ogg.handler.datahub.HandlerInfoManager;
import com.aliyun.odps.ogg.handler.datahub.RecordBuilder;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import oracle.goldengate.datasource.adapt.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OperationHandler {

    private final static Logger logger = LoggerFactory.getLogger(OperationHandler.class);
    private final static int ADD_RECORD_RETRY_INTERVAL_MS = 1000;

    public abstract void process(Op op, Configure configure)
            throws Exception;

    public abstract String getOperateType();

    protected void processOperation(Op op, Configure configure) {

        while (!RecordBuilder.instance().buildRecord(op, getOperateType(),
                Long.toString(HandlerInfoManager.instance().getRecordId()))) {
            logger.warn("add record to record build failed, will retry after [{}] ms, table: {}.",
                    ADD_RECORD_RETRY_INTERVAL_MS, op.getTableName().getFullName().toLowerCase());

            try {
                Thread.sleep(ADD_RECORD_RETRY_INTERVAL_MS);
            } catch (InterruptedException ignored) {
            }
        }
    }
}
