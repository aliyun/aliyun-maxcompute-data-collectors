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
import com.aliyun.odps.ogg.handler.datahub.modle.MetricHelper;
import com.aliyun.odps.ogg.handler.datahub.modle.PluginStatictics;
import com.aliyun.odps.ogg.handler.datahub.operations.OperationHandler;
import com.aliyun.odps.ogg.handler.datahub.operations.OperationHandlerManager;
import oracle.goldengate.datasource.AbstractHandler;
import oracle.goldengate.datasource.DsConfiguration;
import oracle.goldengate.datasource.DsEvent;
import oracle.goldengate.datasource.DsOperation;
import oracle.goldengate.datasource.DsTransaction;
import oracle.goldengate.datasource.GGDataSource;
import oracle.goldengate.datasource.GGDataSource.Status;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.adapt.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatahubHandler extends AbstractHandler {
    private final static Logger logger = LoggerFactory
            .getLogger(DatahubHandler.class);

    private String configureFileName;
    private Configure configure;

    @Override
    public void init(DsConfiguration dsConf, DsMetaData dsMeta) {
        super.init(dsConf, dsMeta);

        try {
            configure = ConfigureReader.reader(configureFileName);

            ClientHelper.init(configure);
            logger.info("Init DataHub client success");

            RecordBuilder.init(configure);
            logger.info("Init RecordBuilder success");

            RecordWriter.init(configure);
            logger.info("Init RecordWriter success");

            OperationHandlerManager.init();
            logger.info("Init OperationHandlerManager success");

            if (configure.isReportMetric()) {
                MetricHelper.init(configure);
            }

        } catch (Exception e) {
            logger.error("Init error", e);
            throw new RuntimeException("init error:" + e.getMessage());
        }
    }

    @Override
    public GGDataSource.Status metaDataChanged(DsEvent e, DsMetaData meta) {
        return super.metaDataChanged(e, meta);
    }

    @Override
    public Status transactionBegin(DsEvent e, DsTransaction tx) {
        PluginStatictics.setSendTimesInTx(0);
        return super.transactionBegin(e, tx);
    }

    @Override
    public Status operationAdded(DsEvent e, DsTransaction tx, DsOperation dsOperation) {
        long startTime = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("DsEvent: {}, DsTransaction: {}, DsOperation: {}, Record: {}",
                    e.toString(), tx.toString(), dsOperation.toString(), dsOperation.getRecord());
        }

        Status status = Status.OK;
        super.operationAdded(e, tx, dsOperation);

        Op op = new Op(dsOperation, e.getMetaData().getTableMetaData(dsOperation.getTableName()), getConfig());
        OperationHandler operationHandler = OperationHandlerManager.getHandler(dsOperation.getOperationType());

        if (operationHandler != null) {
            try {
                String recordId = HandlerInfoManager.instance().genRecordId(dsOperation.getReadTime().getEpochSecond());
                operationHandler.process(recordId, op);
                PluginStatictics.addTotalOperations();
            } catch (Exception e1) {
                logger.error("process error", e1);
                status = Status.ABEND;
            }
        } else {
            logger.error("Unable to instantiate operation handler. Transaction ID: {}, Operation type: {}",
                    tx.getTranID(), dsOperation.getOperationType().toString());
            status = Status.ABEND;
        }

        if (configure.isReportMetric()) {
            MetricHelper.instance().addHandle(System.currentTimeMillis() - startTime);
        }
        return status;
    }

    @Override
    public Status transactionCommit(DsEvent e, DsTransaction tx) {
        long startTime = System.currentTimeMillis();
        RecordBuilder.instance().flushAll();
        RecordWriter.instance().flushAll();

        if (configure.isReportMetric()) {
            MetricHelper.instance().addCommit(System.currentTimeMillis() - startTime);
        }
        return super.transactionCommit(e, tx);
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
        RecordBuilder.destroy();
        RecordWriter.destroy();
        MetricHelper.destroy();
        super.destroy();
        logger.warn("Handler destroy success");
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
