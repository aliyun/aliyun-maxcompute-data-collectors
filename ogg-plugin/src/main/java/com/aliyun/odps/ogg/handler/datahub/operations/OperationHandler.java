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

import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.odps.ogg.handler.datahub.BadOperateWriter;
import com.aliyun.odps.ogg.handler.datahub.DataHubWriter;
import com.aliyun.odps.ogg.handler.datahub.RecordBuilder;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import oracle.goldengate.datasource.adapt.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OperationHandler {

    private final static Logger logger = LoggerFactory.getLogger(OperationHandler.class);

    public abstract void process(Op op, Configure configure)
            throws Exception;

    public abstract String getOperateType();

    protected void processOperation(Op op, Configure configure) throws Exception {
        String oracleFullTableName = op.getTableName().getFullName().toLowerCase();

        TableMapping tableMapping = configure.getTableMapping(oracleFullTableName);

        if(tableMapping != null){
            RecordEntry record;

            try {
                record = RecordBuilder.instance().buildRecord(op,
                        getOperateType(),
                        tableMapping);
            } catch (Exception e){
                logger.error("dirty data : {}", op.toString(), e);

                if(configure.isDirtyDataContinue()){

                    BadOperateWriter.write(op,
                            oracleFullTableName,
                            tableMapping.getTopicName(),
                            configure.getDirtyDataFile(),
                            configure.getDirtyDataFileMaxSize(),
                            e.getMessage());

                    return;
                } else{
                    throw e;
                }
            }
            DataHubWriter.instance().addRecord(tableMapping.getOracleFullTableName(), record);

        } else{
            logger.warn("oracle table: {} not config",oracleFullTableName);
        }

    }
}
