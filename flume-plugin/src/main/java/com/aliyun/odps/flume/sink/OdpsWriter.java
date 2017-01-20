/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.flume.sink;

import com.aliyun.odps.*;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.flume.sink.dataobject.OdpsRowDO;
import com.aliyun.odps.flume.sink.dataobject.OdpsStreamRecordPackDO;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.StreamRecordPack;
import com.aliyun.odps.tunnel.io.StreamWriter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * A writer for the ODPS table. It writes the list of {@link com.aliyun.odps.flume.sink.dataobject.OdpsRowDO} to the
 * ODPS table. It handles the exception in the writing process by retrying after a while (0.2 to 10 seconds).
 */
public class OdpsWriter {
    private static final Logger logger = LoggerFactory.getLogger(OdpsWriter.class);

    private StreamWriter[] streamWriters;
    private Map<String, OdpsType> colNameTypeMap;
    private DateFormat dateFormat;
    private Column[] odpsColumns;
    private Random random;
    private TableSchema tableSchema;

    public OdpsWriter(Table odpsTable, StreamWriter[] streamWriters, String dateFormat, String[] inputColNames) {
        this.streamWriters = streamWriters;
        tableSchema = odpsTable.getSchema();
        odpsColumns = tableSchema.getColumns().toArray(new Column[0]);
        this.dateFormat = new SimpleDateFormat(dateFormat);
        this.colNameTypeMap = buildColNameTypeMap(inputColNames, tableSchema);
        this.random = new Random();
    }

    /**
     * Write list of {@link com.aliyun.odps.flume.sink.dataobject.OdpsRowDO} to ODPS table.
     * @param rowList List of {@link com.aliyun.odps.flume.sink.dataobject.OdpsRowDO} built from event batch.
     */
    public void write(List<OdpsRowDO> rowList) throws InterruptedException {
        if (rowList == null || rowList.isEmpty()) {
            return;
        }
        List<OdpsStreamRecordPackDO> packDOList = buildRecordPackList(rowList);
        if (packDOList == null || packDOList.isEmpty()) {
            return;
        }
        for (OdpsStreamRecordPackDO streamRecordPackDO : packDOList) {
            boolean exceptionFlag = true;
            int i = 0;
            while (exceptionFlag) {
                try {
                    writePack(streamRecordPackDO);
                    exceptionFlag = false;
                } catch (Exception e) {
                    ++ i;
                    logger.error("OdpsWriter write() error, will retry after " + i * 200 + "ms...", e);
                    if (i == 50) {
                        i = 1;
                    }
                    try {
                        Thread.sleep(i * 200);
                    } catch (InterruptedException e1) {
                        logger.error("OdpsWriter write() failed, user stopped retry.");
                        throw new InterruptedException("OdpsWriter write() failed, user stopped retry.");
                    }
                }
            }
        }
    }

    private void writePack(OdpsStreamRecordPackDO packDO) throws IOException, TunnelException {
        if (StringUtils.isEmpty(packDO.getPartitionSpec())) {
            streamWriters[random.nextInt(streamWriters.length)].write(packDO.getRecordPack());
        } else {
            streamWriters[random.nextInt(streamWriters.length)].write(new PartitionSpec(packDO.getPartitionSpec()), packDO.getRecordPack());
        }
    }

    private List<OdpsStreamRecordPackDO> buildRecordPackList(List<OdpsRowDO> rowDOList) {
        if (rowDOList == null || rowDOList.isEmpty()) {
            return null;
        }
        List<OdpsStreamRecordPackDO> recordPackDOList = Lists.newArrayList();
        Map<String, OdpsStreamRecordPackDO> partitionPackMap = Maps.newHashMap();
        for (OdpsRowDO rowDO : rowDOList) {
            OdpsStreamRecordPackDO packDO = partitionPackMap.get(rowDO.getPartitionSpec());
            if (packDO == null) {
                packDO = new OdpsStreamRecordPackDO();
                StreamRecordPack streamRecordPack = new StreamRecordPack(tableSchema);
                packDO.setPartitionSpec(rowDO.getPartitionSpec());
                packDO.setRecordPack(streamRecordPack);
                partitionPackMap.put(rowDO.getPartitionSpec(), packDO);
            }
            Record record = buildRecord(rowDO.getRowMap());
            try {
                packDO.getRecordPack().append(record);
            } catch (IOException e) {
                logger.error("OdpsWriter buildRecordList() error, discard record.", e);
            }
        }
        if (partitionPackMap.keySet().size() > 0) {
            recordPackDOList.addAll(partitionPackMap.values());
        }
        return recordPackDOList;
    }

    private Record buildRecord(Map<String, String> rowMap) {
        Record record = new ArrayRecord(odpsColumns);
        for (Map.Entry<String, String> mapEntry : rowMap.entrySet()) {
            setField(record, mapEntry.getKey(), mapEntry.getValue(), colNameTypeMap.get(mapEntry.getKey()));
        }
        return record;
    }

    private void setField(Record record, String field, String fieldValue, OdpsType odpsType) {

        if (StringUtils.isNotEmpty(field) && StringUtils.isNotEmpty(fieldValue)) {
            switch (odpsType) {
                case STRING:
                    record.setString(field, fieldValue);
                    break;
                case BIGINT:
                    record.setBigint(field, Long.parseLong(fieldValue));
                    break;
                case DATETIME:
                    if (dateFormat != null) {
                        try {
                            record.setDatetime(field, dateFormat.parse(fieldValue));
                        } catch (ParseException e) {
                            logger.error("OdpsWriter parse date error. Date value = " + fieldValue, e);
                        }
                    }
                    break;
                case DOUBLE:
                    record.setDouble(field, Double.parseDouble(fieldValue));
                    break;
                case BOOLEAN:
                    if (StringUtils.equalsIgnoreCase(fieldValue, "true")) {
                        record.setBoolean(field, true);
                    } else if (StringUtils.equalsIgnoreCase(fieldValue, "false")) {
                        record.setBoolean(field, false);
                    }
                    break;
                case DECIMAL:
                    record.setDecimal(field, new BigDecimal(fieldValue));
                default:
                    throw new RuntimeException("Unknown column type: " + odpsType);
            }
        }
    }

    private Map<String, OdpsType> buildColNameTypeMap(String[] inputColNames, TableSchema tableSchema) {
        Map<String, OdpsType> odpsNameTypeMap = Maps.newHashMap();
        for (Column column : tableSchema.getColumns()) {
            odpsNameTypeMap.put(column.getName(), column.getType());
        }
        Map<String, OdpsType> colNameTypeMap = Maps.newHashMap();
        for (String colName : inputColNames) {
            if (!StringUtils.isEmpty(colName)) {
                if (odpsNameTypeMap.containsKey(colName)) {
                    colNameTypeMap.put(colName, odpsNameTypeMap.get(colName));
                } else {
                    throw new RuntimeException(this.getClass().getName() + " buildColNameTypeMap() error, field not exists in odps table, field=" + colName);
                }
            }
        }
        return colNameTypeMap;
    }
}
