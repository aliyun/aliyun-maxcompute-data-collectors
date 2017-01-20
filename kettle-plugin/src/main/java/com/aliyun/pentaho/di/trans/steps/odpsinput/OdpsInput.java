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

package com.aliyun.pentaho.di.trans.steps.odpsinput;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Varchar;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;

public class OdpsInput extends BaseStep implements StepInterface {

    private OdpsInputMeta meta;
    private OdpsInputData data;

    private Map<String, Integer> odpsColumnPosMap;
    private TableSchema schema;

    private int errorLine;

    public OdpsInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
        TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    private void initOdpsFieldPosMap(TableSchema schema) {
        odpsColumnPosMap = new HashMap<String, Integer>();

        List<Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            odpsColumnPosMap.put(columns.get(i).getName().toLowerCase(), i);
        }
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
        throws KettleException {
        try {
            if (first) {
                first = false;

                data.outputRowMeta = new RowMeta();
                meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
            }

            ArrayRecord record = (ArrayRecord) (data.tunnelRecordReader.read());
            if (record != null) {
                Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());

                for (int i = 0; i < meta.getOdpsFields().size(); i++) {
                    Integer pos =
                        odpsColumnPosMap.get(meta.getOdpsFields().get(i).getName().toLowerCase());
                    if (pos == null) {
                        throw new Exception(
                            "Invalid column: " + meta.getOdpsFields().get(i).getName());
                    }
                    outputRow[i] = getRecordColumn(record, pos);
                }

                putRow(data.outputRowMeta, outputRow);
            } else {
                setOutputDone();
                return false;
            }
            return true;
        } catch (Exception e) {
            errorLine++;
            if (errorLine > meta.getErrorLine()) {
                setOutputDone();
                return false;
            } else {
                return true;
            }
        }
    }

    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (super.init(smi, sdi)) {
            meta = (OdpsInputMeta) smi;
            data = (OdpsInputData) sdi;

            Account account = new AliyunAccount(environmentSubstitute(meta.getAccessId()),
                environmentSubstitute(meta.getAccessKey()));
            Odps odps = new Odps(account);
            odps.setEndpoint(environmentSubstitute(meta.getEndpoint()));
            odps.setDefaultProject(environmentSubstitute(meta.getProjectName()));
            odps.setUserAgent("Maxcompute-Kettle-Plugin-2.0.0");

            TableTunnel tableTunnel = new TableTunnel(odps);
            DownloadSession downloadSession = null;
            try {
                if (meta.getPartition() != null && !meta.getPartition().trim().equals("")) {
                    PartitionSpec partitionSpec =
                        new PartitionSpec(environmentSubstitute(meta.getPartition()));
                    downloadSession = tableTunnel
                        .createDownloadSession(environmentSubstitute(meta.getProjectName()),
                            environmentSubstitute(meta.getTableName()), partitionSpec);
                } else {
                    downloadSession = tableTunnel
                        .createDownloadSession(environmentSubstitute(meta.getProjectName()),
                            environmentSubstitute(meta.getTableName()));
                }

                schema = downloadSession.getSchema();
                initOdpsFieldPosMap(schema);

                long count = downloadSession.getRecordCount();
                logBasic("count is: " + count);
                data.tunnelRecordReader = downloadSession.openRecordReader(0L, count);
                return true;
            } catch (TunnelException e) {
                logError(e.getMessage(), e);
            } catch (Exception ex) {
                logError(ex.getMessage(), ex);
            }
        }
        return false;
    }

    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {

        try {
            if (data.tunnelRecordReader != null) {
                data.tunnelRecordReader.close();
            }
        } catch (IOException e) {
            logError(e.getMessage(), e);
        }

        super.dispose(smi, sdi);
    }

    private String getRecordColumn(ArrayRecord record, int pos) {
        Column column = schema.getColumn(pos);
        String colValue = null;
        switch (column.getType()) {
            case BIGINT: {
                Long v = record.getBigint(pos);
                colValue = v == null ? null : String.valueOf(v.longValue());
                break;
            }
            case BOOLEAN: {
                Boolean v = record.getBoolean(pos);
                colValue = v == null ? null : v.toString();
                break;
            }
            case DATETIME: {
                Date v = record.getDatetime(pos);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                colValue = v == null ? null : sdf.format(v);
                break;
            }
            case DOUBLE: {
                Double v = record.getDouble(pos);
                colValue = v == null ? null : String.valueOf(v.doubleValue());
                break;
            }
            case STRING: {
                String v = record.getString(pos);
                colValue = v == null ? null : v;
                break;
            }
            case DECIMAL: {
                BigDecimal v = record.getDecimal(pos);
                colValue = v == null ? null : v.toPlainString();
                break;
            }
            case CHAR: {
                Char v = record.getChar(pos);
                colValue = v == null ? null : v.getValue();
                break;
            }
            case VARCHAR: {
                Varchar v = record.getVarchar(pos);
                colValue = v == null ? null : v.getValue();
                break;
            }
            case TINYINT: {
                Byte v = record.getTinyint(pos);
                colValue = v == null ? null : String.valueOf(v.byteValue());
                break;
            }
            case SMALLINT: {
                Short v = record.getSmallint(pos);
                colValue = v == null ? null : String.valueOf(v.shortValue());
                break;
            }
            case INT: {
                Integer v = record.getInt(pos);
                colValue = v == null ? null : String.valueOf(v.intValue());
                break;
            }
            case FLOAT: {
                Float v = record.getFloat(pos);
                colValue = v == null ? null : String.valueOf(v.floatValue());
                break;
            }
            case DATE: {
                java.sql.Date v = record.getDate(pos);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                colValue = v == null ? null : sdf.format(v);
                break;
            }
            case TIMESTAMP: {
                Timestamp v = record.getTimestamp(pos);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                colValue = v == null ? null : sdf.format(v);
                break;
            }
            case BINARY: {
                Binary v = record.getBinary(pos);
                colValue = v == null ? null : v.toString();
                break;
            }
            default:
                throw new RuntimeException("Unknown column type: " + column.getTypeInfo().getOdpsType());
        }
        return colValue;
    }
}
