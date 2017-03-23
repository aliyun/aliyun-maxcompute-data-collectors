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

package com.aliyun.pentaho.di.ui.trans.steps.odpsoutput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import maxcompute.data.collectors.common.maxcompute.MaxcomputeUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ShowMessageDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsField;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.pentaho.di.trans.steps.odpsoutput.OdpsOutputMeta;

public class OdpsOutputDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = OdpsOutputMeta.class;

    protected OdpsOutputMeta meta;

    // text field holding the name of the field to add to the row stream
    protected Text m_wEndpoint;
    protected Text m_wAccessId;
    protected Text m_wAccessKey;
    protected Text m_wProjectName;
    protected Text m_wTableName;
    protected Text m_wPartition;
    protected Button m_wTruncate;

    protected TableView m_wFieldsTable;

    public OdpsOutputDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        meta = (OdpsOutputMeta) in;
    }

    public String open() {

        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

        props.setLook(shell);
        setShellImage(shell, meta);

        ModifyListener lsMod = new ModifyListener() {
            public void modifyText(ModifyEvent e) {
                meta.setChanged();
            }
        };
        changed = meta.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "ODPS.Shell.Output.Title", new String[0]));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(BaseMessages.getString(PKG, "ODPS.Shell.Step.Name"));
        props.setLook(wlStepname);
        FormData fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -margin);
        fd.top = new FormAttachment(0, margin);
        wlStepname.setLayoutData(fd);

        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wStepname.setText(stepname);
        props.setLook(wStepname);
        wStepname.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(0, margin);
        fd.right = new FormAttachment(100, 0);
        wStepname.setLayoutData(fd);

        //ODPS connection group
        Group gConnect = new Group(shell, SWT.SHADOW_ETCHED_IN);
        gConnect.setText(BaseMessages.getString(PKG, "ODPS.ConfigTab.TabTitle"));
        FormLayout gConnectLayout = new FormLayout();
        gConnectLayout.marginWidth = 3;
        gConnectLayout.marginHeight = 3;
        gConnect.setLayout(gConnectLayout);
        props.setLook(gConnect);

        //endpoint
        Label wlEndpoint = new Label(gConnect, SWT.RIGHT);
        wlEndpoint.setText(BaseMessages.getString(PKG, "ODPS.Endpoint.Label"));
        props.setLook(wlEndpoint);
        FormData fdlEndpoint = new FormData();
        fdlEndpoint.left = new FormAttachment(0, 0);
        fdlEndpoint.right = new FormAttachment(middle, -margin);
        fdlEndpoint.top = new FormAttachment(0, margin);
        wlEndpoint.setLayoutData(fdlEndpoint);

        m_wEndpoint = new Text(gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wEndpoint);
        m_wEndpoint.addModifyListener(lsMod);
        FormData fdEndpoint = new FormData();
        fdEndpoint.left = new FormAttachment(middle, 0);
        fdEndpoint.right = new FormAttachment(100, 0);
        fdEndpoint.top = new FormAttachment(0, margin);
        m_wEndpoint.setLayoutData(fdEndpoint);
        Control lastControl = m_wEndpoint;

        //accessId
        Label wlAccessId = new Label(gConnect, SWT.RIGHT);
        wlAccessId.setText(BaseMessages.getString(PKG, "ODPS.AccessId.Label"));
        props.setLook(wlAccessId);
        FormData fdlAccessId = new FormData();
        fdlAccessId.left = new FormAttachment(0, 0);
        fdlAccessId.right = new FormAttachment(middle, -margin);
        fdlAccessId.top = new FormAttachment(lastControl, margin);
        wlAccessId.setLayoutData(fdlAccessId);

        m_wAccessId = new Text(gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wAccessId);
        m_wAccessId.addModifyListener(lsMod);
        FormData fdAccessId = new FormData();
        fdAccessId.left = new FormAttachment(middle, 0);
        fdAccessId.right = new FormAttachment(100, 0);
        fdAccessId.top = new FormAttachment(lastControl, margin);
        m_wAccessId.setLayoutData(fdAccessId);
        lastControl = m_wAccessId;

        //accessKey
        Label wlAccessKey = new Label(gConnect, SWT.RIGHT);
        wlAccessKey.setText(BaseMessages.getString(PKG, "ODPS.AccessKey.Label"));
        props.setLook(wlAccessKey);
        FormData fdlAccessKey = new FormData();
        fdlAccessKey.left = new FormAttachment(0, 0);
        fdlAccessKey.right = new FormAttachment(middle, -margin);
        fdlAccessKey.top = new FormAttachment(lastControl, margin);
        wlAccessKey.setLayoutData(fdlAccessKey);

        m_wAccessKey = new Text(gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wAccessKey);
        m_wAccessKey.addModifyListener(lsMod);
        FormData fdAccessKey = new FormData();
        fdAccessKey.left = new FormAttachment(middle, 0);
        fdAccessKey.right = new FormAttachment(100, 0);
        fdAccessKey.top = new FormAttachment(lastControl, margin);
        m_wAccessKey.setLayoutData(fdAccessKey);
        lastControl = m_wAccessKey;

        //project name
        Label wlProjectName = new Label(gConnect, SWT.RIGHT);
        wlProjectName.setText(BaseMessages.getString(PKG, "ODPS.ProjectName.Label"));
        props.setLook(wlProjectName);
        FormData fdlProjectName = new FormData();
        fdlProjectName.left = new FormAttachment(0, 0);
        fdlProjectName.right = new FormAttachment(middle, -margin);
        fdlProjectName.top = new FormAttachment(lastControl, margin);
        wlProjectName.setLayoutData(fdlProjectName);

        m_wProjectName = new Text(gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wProjectName);
        m_wProjectName.addModifyListener(lsMod);
        FormData fdProjectName = new FormData();
        fdProjectName.left = new FormAttachment(middle, 0);
        fdProjectName.right = new FormAttachment(100, 0);
        fdProjectName.top = new FormAttachment(lastControl, margin);
        m_wProjectName.setLayoutData(fdProjectName);
        lastControl = m_wProjectName;

        //table name
        Label wlTableName = new Label(gConnect, SWT.RIGHT);
        wlTableName.setText(BaseMessages.getString(PKG, "ODPS.TableName.Label"));
        props.setLook(wlTableName);
        FormData fdlTableName = new FormData();
        fdlTableName.left = new FormAttachment(0, 0);
        fdlTableName.right = new FormAttachment(middle, -margin);
        fdlTableName.top = new FormAttachment(lastControl, margin);
        wlTableName.setLayoutData(fdlTableName);

        m_wTableName = new Text(gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wTableName);
        m_wTableName.addModifyListener(lsMod);
        FormData fdTableName = new FormData();
        fdTableName.left = new FormAttachment(middle, 0);
        fdTableName.right = new FormAttachment(100, 0);
        fdTableName.top = new FormAttachment(lastControl, margin);
        m_wTableName.setLayoutData(fdTableName);
        lastControl = m_wTableName;

        //parition
        Label wlPartition = new Label(gConnect, SWT.RIGHT);
        wlPartition.setText(BaseMessages.getString(PKG, "ODPS.Partition.Label"));
        props.setLook(wlPartition);
        FormData fdlPartition = new FormData();
        fdlPartition.left = new FormAttachment(0, 0);
        fdlPartition.right = new FormAttachment(middle, -margin);
        fdlPartition.top = new FormAttachment(lastControl, margin);
        wlPartition.setLayoutData(fdlPartition);

        m_wPartition = new Text(gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wPartition);
        m_wPartition.addModifyListener(lsMod);
        FormData fdPartition = new FormData();
        fdPartition.left = new FormAttachment(middle, 0);
        fdPartition.right = new FormAttachment(100, 0);
        fdPartition.top = new FormAttachment(lastControl, margin);
        m_wPartition.setLayoutData(fdPartition);
        lastControl = m_wPartition;

        //truncate
        Label wlTruncate = new Label(gConnect, SWT.RIGHT);
        wlTruncate.setText(BaseMessages.getString(PKG, "ODPS.Truncate.Label"));
        props.setLook(wlTruncate);
        FormData fdlTruncate = new FormData();
        fdlTruncate.left = new FormAttachment(0, 0);
        fdlTruncate.right = new FormAttachment(middle, -margin);
        fdlTruncate.top = new FormAttachment(lastControl, margin);
        wlTruncate.setLayoutData(fdlTruncate);

        m_wTruncate = new Button(gConnect, SWT.CHECK);
        props.setLook(m_wTruncate);
        FormData fdTruncate = new FormData();
        fdTruncate.left = new FormAttachment(middle, 0);
        fdTruncate.right = new FormAttachment(100, 0);
        fdTruncate.top = new FormAttachment(lastControl, margin);
        m_wTruncate.setLayoutData(fdTruncate);
        lastControl = m_wTruncate;
        //

        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(wStepname, margin);
        gConnect.setLayoutData(fd);

        //fields
        Label wlFields = new Label(shell, SWT.NONE);
        wlFields.setText(BaseMessages.getString(PKG, "ODPS.Table.Fields.Label"));
        props.setLook(wlFields);
        FormData fdlFields = new FormData();
        fdlFields.left = new FormAttachment(0, 0);
        fdlFields.top = new FormAttachment(gConnect, margin);
        wlFields.setLayoutData(fdlFields);

        int keyWidgetCols = 2;
        int keyWidgetRows = meta.getOdpsFields() == null ? 1 : meta.getOdpsFields().size();
        ColumnInfo[] ciKeys = new ColumnInfo[keyWidgetCols];
        ciKeys[0] = new ColumnInfo(BaseMessages.getString(PKG, "ODPS.ColumnInfo.FieldName"),
            ColumnInfo.COLUMN_TYPE_TEXT, false);
        ciKeys[1] = new ColumnInfo(BaseMessages.getString(PKG, "Kettle.Stream.Fieldname"),
            ColumnInfo.COLUMN_TYPE_TEXT, false);

        //        fieldColumn = ciKeys[0];

        m_wFieldsTable = new TableView(transMeta, shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKeys,
            keyWidgetRows, lsMod, props);

        FormData fdFields = new FormData();
        fdFields.left = new FormAttachment(0, 0);
        fdFields.top = new FormAttachment(wlFields, margin);
        fdFields.right = new FormAttachment(100, 0);
        fdFields.bottom = new FormAttachment(100, -50);
        m_wFieldsTable.setLayoutData(fdFields);

        // get fields button
        wGet = new Button(shell, SWT.PUSH);
        wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
        wGet.addSelectionListener(new SelectionAdapter() {
            @Override public void widgetSelected(SelectionEvent e) {
                // populate table from schema
                OdpsOutputMeta newMeta = (OdpsOutputMeta) meta.clone();
                getFields(newMeta);
            }
        });

        // OK and cancel buttons
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        setButtonPositions(new Button[] {wGet, wOK, wCancel}, margin, m_wFieldsTable);

        // Add listeners for cancel and OK
        lsCancel = new Listener() {
            public void handleEvent(Event e) {
                cancel();
            }
        };
        lsOK = new Listener() {
            public void handleEvent(Event e) {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);

        // default listener (for hitting "enter")
        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };
        wStepname.addSelectionListener(lsDef);
        m_wEndpoint.addSelectionListener(lsDef);

        // Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
        shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        // Set/Restore the dialog size based on last position on screen
        // The setSize() method is inherited from BaseStepDialog
        setSize();

        // populate the dialog with the values from the meta object
        populateDialog();

        // restore the changed flag to original value, as the modify listeners fire during dialog population
        meta.setChanged(changed);

        // open dialog and enter event loop
        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch())
                display.sleep();
        }

        // at this point the dialog has closed, so either ok() or cancel() have been executed
        // The "stepname" variable is inherited from BaseStepDialog
        return stepname;
    }

    /**
     * This helper method puts the step configuration stored in the meta object
     * and puts it into the dialog controls.
     */
    private void populateDialog() {
        wStepname.selectAll();

        if (meta.getEndpoint() != null) {
            m_wEndpoint.setText(meta.getEndpoint());
        }
        if (meta.getAccessId() != null) {
            m_wAccessId.setText(meta.getAccessId());
        }
        if (meta.getAccessKey() != null) {
            m_wAccessKey.setText(meta.getAccessKey());
        }
        if (meta.getProjectName() != null) {
            m_wProjectName.setText(meta.getProjectName());
        }
        if (meta.getTableName() != null) {
            m_wTableName.setText(meta.getTableName());
        }
        if (meta.getPartition() != null) {
            m_wPartition.setText(meta.getPartition());
        }
        m_wTruncate.setSelection(meta.isTruncate());

        if (meta.getOdpsFields() != null && meta.getStreamFields() != null) {
            for (int i = 0; i < meta.getOdpsFields().size(); i++) {
                OdpsField odpsField = meta.getOdpsFields().get(i);
                String streamField = meta.getStreamFields().get(i);
                TableItem item = m_wFieldsTable.table.getItem(i);
                if (odpsField != null) {
                    if (odpsField.getName() != null)
                        item.setText(1, odpsField.getName());
                    if (streamField != null)
                        item.setText(2, streamField);
                }
            }
        }
    }

    /**
     * Called when the user cancels the dialog.
     */
    private void cancel() {
        // The "stepname" variable will be the return value for the open() method.
        // Setting to null to indicate that dialog was cancelled.
        stepname = null;
        // Restoring original "changed" flag on the met aobject
        meta.setChanged(changed);
        // close the SWT dialog window
        dispose();
    }

    /**
     * Called when the user confirms the dialog
     */
    private void ok() {
        // The "stepname" variable will be the return value for the open() method.
        // Setting to step name from the dialog control
        stepname = wStepname.getText();
        // Setting the  settings to the meta object
        meta.setEndpoint(m_wEndpoint.getText());
        meta.setAccessId(m_wAccessId.getText());
        meta.setAccessKey(m_wAccessKey.getText());
        meta.setProjectName(m_wProjectName.getText());
        meta.setTableName(m_wTableName.getText());
        meta.setPartition(m_wPartition.getText());

        meta.setTruncate(m_wTruncate.getSelection());

        int nrFields = m_wFieldsTable.nrNonEmpty();
        meta.setOdpsFields(new ArrayList<OdpsField>(nrFields));
        meta.setStreamFields(new ArrayList<String>(nrFields));
        for (int i = 0; i < nrFields; i++) {
            TableItem item = m_wFieldsTable.getNonEmpty(i);
            OdpsField odpsField = new OdpsField();
            odpsField.setName(item.getText(1));
            meta.getOdpsFields().add(odpsField);
            meta.getStreamFields().add(item.getText(2));
        }

        // close the SWT dialog window
        dispose();
    }

    private void getFields(OdpsOutputMeta meta) {
        if (!Const.isEmpty(m_wEndpoint.getText()) && !Const.isEmpty(m_wAccessId.getText()) && !Const
            .isEmpty(m_wAccessKey.getText()) && !Const.isEmpty(m_wProjectName.getText()) && !Const
            .isEmpty(m_wTableName.getText())) {

            TableSchema schema = MaxcomputeUtil
                .getTableSchema(new AliyunAccount(m_wAccessId.getText(), m_wAccessKey.getText()),
                    m_wEndpoint.getText(), m_wProjectName.getText(), m_wTableName.getText());

            List<Column> columns = schema.getColumns();
            List<OdpsField> odpsFields = new ArrayList<OdpsField>();
            for (int i = 0; i < columns.size(); i++) {
                OdpsField field = new OdpsField();
                field.setName(columns.get(i).getName());
                field.setType(columns.get(i).getType().name());
                field.setComment(columns.get(i).getCategoryLabel());
                odpsFields.add(field);
            }

            meta.setOdpsFields(odpsFields);
            RowMetaInterface r = null;
            try {
                r = transMeta.getPrevStepFields(stepname);
            } catch (KettleStepException e) {
                logError(e.getMessage(), e);
            }

            if (r != null) {
                logBasic("prev step fields: " + Arrays.toString(r.getFieldNames()));
                meta.setStreamFields(Arrays.asList(r.getFieldNames()));
            } else {
                logBasic("prev step fields empty!!!");
                meta.setStreamFields(new ArrayList<String>());
            }

            if (meta.getOdpsFields() != null && meta.getStreamFields() != null) {
                m_wFieldsTable.table.clearAll();
                m_wFieldsTable.table.setItemCount(meta.getOdpsFields().size());
                for (int i = 0; i < meta.getOdpsFields().size(); i++) {
                    OdpsField odpsField = meta.getOdpsFields().get(i);
                    String streamField = "";
                    if (meta.getStreamFields().size() > i) {
                        streamField = meta.getStreamFields().get(i);
                    }
                    TableItem item = m_wFieldsTable.table.getItem(i);
                    if (odpsField != null) {
                        if (odpsField.getName() != null)
                            item.setText(1, odpsField.getName());
                        if (streamField != null)
                            item.setText(2, streamField);
                    }
                }
            }

        } else {
            // pop up an error dialog
            String missingConDetails = "";
            if (Const.isEmpty(m_wEndpoint.getText())) {
                missingConDetails += " odps endpoint";
            }
            if (Const.isEmpty(m_wAccessId.getText())) {
                missingConDetails += " accessId";
            }
            if (Const.isEmpty(m_wAccessKey.getText())) {
                missingConDetails += " accessKey";
            }
            if (Const.isEmpty(m_wProjectName.getText())) {
                missingConDetails += " project name";
            }
            if (Const.isEmpty(m_wTableName.getText())) {
                missingConDetails += " table name";
            }
            ShowMessageDialog smd = new ShowMessageDialog(shell, SWT.ICON_WARNING | SWT.OK,
                BaseMessages.getString(PKG, "ODPS.ErrorMessage.MissingConnectionDetails.Title"),
                BaseMessages.getString(PKG, "ODPS.ErrorMessage.MissingConnectionDetails",
                    missingConDetails));
            smd.open();
        }
    }
}
