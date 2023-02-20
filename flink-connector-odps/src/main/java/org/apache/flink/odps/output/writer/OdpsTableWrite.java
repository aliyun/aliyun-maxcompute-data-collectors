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

package org.apache.flink.odps.output.writer;

import com.aliyun.odps.*;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.cupid.table.v1.writer.TableWriteSession;
import com.aliyun.odps.cupid.table.v1.writer.TableWriteSessionBuilder;
import com.aliyun.odps.cupid.table.v1.writer.WriteSessionInfo;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsMetaDataProvider;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.odps.util.Constants.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class OdpsTableWrite<T> implements Serializable,
        OdpsWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsTableWrite.class);

    protected OdpsConf odpsConf;
    protected String projectName;
    protected String tableName;
    protected String staticPartition;
    protected boolean isOverwrite;
    protected boolean isPartitioned;

    protected boolean isLocal = false;
    protected boolean useBatch = true;
    protected String tableApiProvider = DEFAULT_TABLE_API_PROVIDER;

    protected transient Odps odps;
    protected transient OdpsMetaDataProvider tableMetaProvider;
    protected transient TableSchema tableSchema;

    protected OdpsWriteOptions writeOptions;
    protected WriterContext writerContext;

    protected WriteSessionInfo writeSessionInfo;
    protected transient TableWriteSession tableWriteSession;
    protected long sessionCreateTime;

    public OdpsTableWrite(OdpsConf odpsConf,
                          String projectName,
                          String tableName,
                          String partition,
                          boolean isOverwrite,
                          OdpsWriteOptions options) {
        this.projectName = Preconditions.checkNotNull(projectName, "project cannot be null");
        this.tableName = Preconditions.checkNotNull(tableName, "table cannot be null");
        this.isOverwrite = isOverwrite;

        if (odpsConf == null) {
            this.odpsConf = OdpsUtils.getOdpsConf();
        } else {
            this.odpsConf = odpsConf;
        }
        Preconditions.checkNotNull(this.odpsConf, "odps conf cannot be null");

        if (!this.odpsConf.isClusterMode()) {
            this.isLocal = true;
            this.useBatch = false;
            this.tableApiProvider = TUNNEL_TABLE_API_PROVIDER;
        }

        this.writeOptions = options;
        this.tableMetaProvider = getTableMetaProvider();
        this.tableSchema = getTableSchema();
        try {
            this.isPartitioned = getTableMetaProvider().getTable(projectName, tableName).isPartitioned();
            checkPartition(partition);
        } catch (IOException | OdpsException e) {
            throw new FlinkOdpsException(e);
        }
        this.writerContext = new WriterContext(staticPartition);
        if (useBatch) {
            this.useBatch = supportBatchWrite(tableSchema, isLocal);
        }
    }

    protected void checkPartition(String partitionSpec) throws IOException {
        if (isPartitioned) {
            if (StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
                LOG.error("The partitionSpec cannot be null or whitespace with partition table: " + tableName);
                throw new IOException("check partition failed.");
            } else {
                this.staticPartition = new PartitionSpec(partitionSpec).toString();
                createPartitionIfNeeded(this.staticPartition);
            }
        } else {
            if (!StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
                throw new IOException("The partitionSpec should be null or whitespace with non partition odps table: " + tableName);
            } else {
                this.staticPartition = "";
            }
        }
    }

    @Override
    public void updateWriteContext(SinkFunction.Context context) {
        // TODO: Caused by: java.lang.RuntimeException: Not implemented
        //	at org.apache.flink.streaming.api.functions.sink.SinkContextUtil$1.currentWatermark
//        writerContext.update(
//                context.timestamp(),
//                context.currentWatermark(),
//                context.currentProcessingTime());
    }

    protected void createWriteSession(Options options, String partition) throws IOException {
        try {
            if (isPartitioned && !partition.isEmpty()) {
                tableWriteSession = new TableWriteSessionBuilder(tableApiProvider, projectName, tableName)
                        .tableSchema(getTableSchema())
                        .overwrite(isOverwrite)
                        .partitionSpec(OdpsUtils.getPartitionSpecKVMap(new PartitionSpec(partition)))
                        .options(options)
                        .build();
            } else {
                tableWriteSession = new TableWriteSessionBuilder(tableApiProvider, projectName, tableName)
                        .tableSchema(getTableSchema())
                        .overwrite(isOverwrite)
                        .options(options)
                        .build();
            }
            writeSessionInfo = tableWriteSession.getOrCreateSessionInfo();
        } catch (Exception e) {
            LOG.error("Fail to create odps writeSession!", e);
            throw new IOException("Fail to create odps writeSession!", e);
        }
    }

    protected void rebuildWriteSession() throws IOException {
        Preconditions.checkNotNull(writeSessionInfo, "Write session cannot be null!");
        try {
            if (tableWriteSession == null) {
                tableWriteSession = new TableWriteSessionBuilder(tableApiProvider, projectName, tableName)
                        .writeSessionInfo(writeSessionInfo)
                        .build();
            }
        } catch (Exception e) {
            LOG.error("Fail to set odps writeSession!", e);
            throw new IOException("Fail to set odps write session!", e);
        }
    }

    protected Odps getOdps() {
        if (odps == null) {
            this.odps = OdpsUtils.getOdps(this.odpsConf);
        }
        return odps;
    }

    protected OdpsMetaDataProvider getTableMetaProvider() {
        if (tableMetaProvider == null) {
            tableMetaProvider = new OdpsMetaDataProvider(this.getOdps());
        }
        return tableMetaProvider;
    }

    public TableSchema getTableSchema() {
        if (tableSchema == null) {
            tableSchema = getTableMetaProvider().getTableSchema(projectName, tableName, false);
        }
        return tableSchema;
    }

    protected boolean supportBatchWrite(TableSchema tableSchema, boolean isLocal) {
        if (isLocal || !odpsConf.getPropertyOrDefault(ODPS_VECTORIZED_WRITE_ENABLE, false)) {
            return false;
        }
        checkNotNull(tableSchema, "TableSchema cannot be null");
        for (Column column : tableSchema.getColumns()) {
            if (!column.getType().equals(OdpsType.BOOLEAN) &&
                    !column.getType().equals(OdpsType.STRING) &&
                    !column.getType().equals(OdpsType.CHAR) &&
                    !column.getType().equals(OdpsType.VARCHAR) &&
                    !column.getType().equals(OdpsType.BINARY) &&
                    !column.getType().equals(OdpsType.DECIMAL) &&
                    !column.getType().equals(OdpsType.DATETIME) &&
                    !column.getType().equals(OdpsType.DATE) &&
                    !column.getType().equals(OdpsType.TIMESTAMP) &&
                    !column.getType().equals(OdpsType.DOUBLE) &&
                    !column.getType().equals(OdpsType.FLOAT) &&
                    !column.getType().equals(OdpsType.BIGINT) &&
                    !column.getType().equals(OdpsType.INT) &&
                    !column.getType().equals(OdpsType.TINYINT) &&
                    !column.getType().equals(OdpsType.SMALLINT)) {
                return false;
            }
        }
        return true;
    }

    protected void createPartitionIfNeeded(String targetPartition) throws IOException {
        int attemptNum = 1;
        while (true) {
            try {
                synchronized (this) {
                    Partition partition = getTableMetaProvider().getPartition(projectName,
                            tableName, targetPartition, true);
                    if (partition == null) {
                        Table table = getTableMetaProvider().getTable(projectName, tableName);
                        table.createPartition(new PartitionSpec(targetPartition), true);
                        LOG.info("Create partition: " + tableName + "/" + targetPartition);
                    }
                }
                break;
            } catch (Throwable e) {
                if (attemptNum++ > 5) {
                    LOG.error(
                            "Failed to create partition: " + targetPartition + " after retrying...");
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    LOG.error("Failed to create partition: " + targetPartition);
                    throw new IOException(e);
                }
            }
        }
    }
}
