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

package com.aliyun.odps.ogg.handler.datahub;

import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.PluginStatictics;
import com.aliyun.odps.ogg.handler.datahub.operations.OperationHandler;
import com.aliyun.odps.ogg.handler.datahub.operations.OperationHandlerManager;
import com.goldengate.atg.datasource.*;
import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.meta.DsMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatahubHandler extends AbstractHandler {
    private final static Logger logger = LoggerFactory
            .getLogger(DatahubHandler.class);

    private String configureFileName;
    private Configure configure;

    @Override
    public void init(DsConfiguration dsConf, DsMetaData dsMeta) {
        try{
            configure = ConfigureReader.reader(configureFileName);

            HandlerInfoManager.init(configure);
            logger.info("Init HandlerInfoManager success");

            RecordBuilder.init(configure);
            logger.info("Init RecordBuilder success");

            DataHubWriter.init(configure);
            logger.info("Init DataHubWriter success");

            OperationHandlerManager.init();
            logger.info("Init OperationHandlerManager success");
        } catch (Exception e){
            logger.error("Init error", e);
            throw new RuntimeException("init error:" + e.getMessage());
        }

        if(dsConf != null){
            super.init(dsConf, dsMeta);
        }
    }

    @Override
    public Status metaDataChanged(DsEvent e, DsMetaData meta) {
        return super.metaDataChanged(e, meta);
    }

    @Override
    public Status transactionBegin(DsEvent e, DsTransaction tx) {
        PluginStatictics.setSendTimesInTx(0);
        return super.transactionBegin(e, tx);
    }

    @Override
    public Status operationAdded(DsEvent e, DsTransaction tx, DsOperation dsOperation) {
        if(logger.isDebugEnabled()){
            logger.debug(e.toString());
            logger.debug(tx.toString());
            logger.debug("operation add:" + dsOperation.toString());
        }

        Status status = Status.OK;
        super.operationAdded(e, tx, dsOperation);

        Op op = new Op(dsOperation, e.getMetaData().getTableMetaData(dsOperation.getTableName()), getConfig());
        OperationHandler operationHandler = OperationHandlerManager.getHandler(dsOperation.getOperationType());

        if (operationHandler != null) {
            if ((!configure.isCheckPointFileDisabled()) && dsOperation.getPosition().
                compareTo(HandlerInfoManager.instance().getSendPosition()) <= 0) {
                logger.warn("dsOperation.getPosition(): " + dsOperation.getPosition() +
                    " old sendPosition is: " + HandlerInfoManager.instance().getSendPosition()
                    + ", Skip this operation, it maybe duplicated!!!");
                return status;
            } else {
                // update handler info
                HandlerInfoManager.instance().updateHandlerInfos(
                    dsOperation.getReadTime().getTime(),
                    dsOperation.getPosition());
            }

            try {
                operationHandler.process(op, configure);
                PluginStatictics.addTotalOperations();
            } catch (Exception e1) {
                logger.error("process error", e1);
                status = Status.ABEND;
            }
        } else {
            String msg = "Unable to instantiate operation handler. Transaction ID: " + tx.getTranID()
                    + ". Operation type: " + dsOperation.getOperationType().toString();
            logger.error(msg);
            status = Status.ABEND;
        }
       return status;
    }

    @Override
    public Status transactionCommit(DsEvent e, DsTransaction tx) {
        Status status = super.transactionCommit(e, tx);
        try {
            DataHubWriter.instance().flushAll();
        } catch (Exception e1) {
            status = status.ABEND;
            logger.error("Unable to deliver records", e1);
        }

        // save checkpoints
        HandlerInfoManager.instance().saveHandlerInfos();

        PluginStatictics.addTotalTxns();
        return status;
    }

    @Override
    public String reportStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append(":- Status report: mode=").append(getMode());
        sb.append(", transactions=").append(PluginStatictics.getTotalTxns());
        sb.append(", operations=").append(PluginStatictics.getTotalOperations());
        sb.append(", inserts=").append(PluginStatictics.getTotalInserts());
        sb.append(", updates=").append(PluginStatictics.getTotalUpdates());
        sb.append(", deletes=").append(PluginStatictics.getTotalDeletes());

        return sb.toString();
    }

    @Override
    public void destroy() {
        logger.warn("Handler destroying...");
        super.destroy();
    }

    public String getConfigureFileName() {
        return configureFileName;
    }

    public void setConfigureFileName(String configureFileName) {
        this.configureFileName = configureFileName;
    }

    public Configure getConfigure() {
        return configure;
    }

    public void setConfigure(Configure configure) {
        this.configure = configure;
    }

}
