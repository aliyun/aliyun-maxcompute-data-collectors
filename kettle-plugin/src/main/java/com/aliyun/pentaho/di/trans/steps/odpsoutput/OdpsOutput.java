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

package com.aliyun.pentaho.di.trans.steps.odpsoutput;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import maxcompute.data.collectors.common.maxcompute.*;
import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

public class OdpsOutput extends BaseStep implements StepInterface {

    private OdpsOutputMeta meta;
    private OdpsOutputData data;
    private TableSchema schema;

    private Map<String, Integer> odpsColumnPosMap;
    private Map<String, Integer> streamFieldPosMap;
    private Map<String, String> odpsColumn2StreamFieldMap;
    private SimpleDateFormat dateFormat = null;
    private int errorLine = 0;

    public OdpsOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
        TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    }

    private void initStreamFieldPosMap(RowMetaInterface rowMeta) {
        streamFieldPosMap = new HashMap<String, Integer>();
        for (int i = 0; i < rowMeta.size(); i++) {
            ValueMetaInterface v = rowMeta.getValueMeta(i);
            String fieldName = v.getName();
            streamFieldPosMap.put(fieldName.toLowerCase(), i);
        }
    }

    private void initOdpsFieldPosMap(TableSchema schema) {
        odpsColumnPosMap = new HashMap<String, Integer>();

        List<Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            odpsColumnPosMap.put(columns.get(i).getName().toLowerCase(), i);
        }
    }

    private void initOdpsColumn2StreamFieldMap() {
        odpsColumn2StreamFieldMap = new HashMap<String, String>();
        for (int i = 0; i < meta.getStreamFields().size(); i++) {
            logBasic("initStreamFieldMap2OdpsColumn:" + meta.getStreamFields().get(i));
            odpsColumn2StreamFieldMap.put(meta.getOdpsFields().get(i).getName().toLowerCase(),
                meta.getStreamFields().get(i).toLowerCase());
        }
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
        throws KettleException {
        try {
            Object[] row = getRow();
            if (row == null) {
                logBasic("input row is null. setOutputDone.");
                setOutputDone();
                return false;
            }

            if (first) {
                first = false;
                data.outputRowMeta = getInputRowMeta().clone();

                initStreamFieldPosMap(data.outputRowMeta);
            }

            if (streamFieldPosMap.size() != odpsColumnPosMap.size()) {
                logBasic("Error: Stream Field size not equal to table field size!");
                throw new RuntimeException("Error: Stream Field size not equal to table field size!");
            }

            if (!isStopped()) {
                ArrayRecord record = (ArrayRecord)(data.uploadSession.newRecord());

                for (int i = 0; i < meta.getOdpsFields().size(); i++) {
                    String odpsColumn = meta.getOdpsFields().get(i).getName().toLowerCase();
                    String streamField = odpsColumn2StreamFieldMap.get(odpsColumn);
                    if (!Const.isEmpty(streamField)) {
                        int pos = streamFieldPosMap.get(streamField);
                        Column column = schema.getColumn(i);
                        String fieldValue = String.valueOf(row[pos]);
                        RecordUtil.setFieldValue(record, odpsColumn, fieldValue, column.getType(), dateFormat);
                    }
                }
                data.recordWriter.write(record);
            } else {
                setOutputDone();
                return false;
            }
            return true;
        } catch (Exception e) {
            logError(e.getMessage(), e);
            errorLine++;
            if (errorLine > meta.getErrorLine()) {
                setOutputDone();
                throw new KettleException(e);
            } else {
                return true;
            }
        }
    }

    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (super.init(smi, sdi)) {
            meta = (OdpsOutputMeta) smi;
            data = (OdpsOutputData) sdi;

            Account account = new AliyunAccount(environmentSubstitute(meta.getAccessId()),
                environmentSubstitute(meta.getAccessKey()));
            Odps odps = new Odps(account);
            odps.setEndpoint(environmentSubstitute(meta.getEndpoint()));
            odps.setDefaultProject(environmentSubstitute(meta.getProjectName()));
            odps.setUserAgent("Maxcompute-Kettle-Plugin-2.0.0");

            TableTunnel tableTunnel = new TableTunnel(odps);
            try {
                MaxcomputeUtil.dealTruncate(odps,
                    odps.tables().get(environmentSubstitute(meta.getTableName())),
                    environmentSubstitute(meta.getPartition()), meta.isTruncate());

                if (meta.getPartition() != null && !meta.getPartition().trim().equals("")) {
                    PartitionSpec partitionSpec =
                        new PartitionSpec(environmentSubstitute(meta.getPartition()));
                    data.uploadSession = tableTunnel
                        .createUploadSession(environmentSubstitute(meta.getProjectName()),
                            environmentSubstitute(meta.getTableName()), partitionSpec);
                } else {
                    data.uploadSession = tableTunnel
                        .createUploadSession(environmentSubstitute(meta.getProjectName()),
                            environmentSubstitute(meta.getTableName()));
                }

                schema = data.uploadSession.getSchema();
                initOdpsFieldPosMap(schema);
                initOdpsColumn2StreamFieldMap();

                data.recordWriter = data.uploadSession.openBufferedWriter();

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
            if (data.recordWriter != null) {
                data.recordWriter.close();
            }
            if (data.uploadSession != null) {
                data.uploadSession.commit();
            }
        } catch (IOException e) {
            logError(e.getMessage(), e);
        } catch (TunnelException ex) {
            logError(ex.getMessage(), ex);
        }

        super.dispose(smi, sdi);
    }

}
