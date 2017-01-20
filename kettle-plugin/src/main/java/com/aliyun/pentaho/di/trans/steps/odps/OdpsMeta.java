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

package com.aliyun.pentaho.di.trans.steps.odps;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import com.aliyun.odps.OdpsField;

public abstract class OdpsMeta extends BaseStepMeta implements StepMetaInterface {

    private String endpoint = "";
    private String accessId = "";
    private String accessKey = "";
    private String projectName = "";
    private String tableName = "";
    private String partition = "";

    protected List<OdpsField> odpsFields = new ArrayList<OdpsField>();

    private int errorLine = 0;

    public int getErrorLine() {
        return errorLine;
    }

    public void setErrorLine(int errorLine) {
        this.errorLine = errorLine;
    }

    public List<OdpsField> getOdpsFields() {
        return odpsFields;
    }

    public void setOdpsFields(List<OdpsField> odpsFields) {
        this.odpsFields = odpsFields;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    @Override public Object clone() {
        OdpsMeta retval = (OdpsMeta) super.clone();
        retval.odpsFields = new ArrayList<OdpsField>();
        for (int i = 0; i < odpsFields.size(); i++) {
            OdpsField field = odpsFields.get(i);
            OdpsField cloneField = new OdpsField();
            cloneField.setName(field.getName());
            cloneField.setType(field.getType());
            cloneField.setComment(field.getComment());
            retval.odpsFields.add(field);
        }
        return retval;
    }

    @Override public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore)
        throws KettleXMLException {
        setEndpoint(XMLHandler.getTagValue(stepnode, "endpoint"));
        setAccessId(XMLHandler.getTagValue(stepnode, "accessId"));
        setAccessKey(XMLHandler.getTagValue(stepnode, "accessKey"));
        setProjectName(XMLHandler.getTagValue(stepnode, "projectName"));
        setTableName(XMLHandler.getTagValue(stepnode, "tableName"));
        setPartition(XMLHandler.getTagValue(stepnode, "partition"));

        int nrFields = XMLHandler.countNodes(stepnode, "fields");
        odpsFields = new ArrayList<OdpsField>(nrFields);
        for (int i = 0; i < nrFields; i++) {
            Node fieldNode = XMLHandler.getSubNodeByNr(stepnode, "fields", i);
            OdpsField field = new OdpsField();
            field.setName(XMLHandler.getTagValue(fieldNode, "name"));
            field.setType(XMLHandler.getTagValue(fieldNode, "type"));
            field.setComment(XMLHandler.getTagValue(fieldNode, "comment"));
            odpsFields.add(field);
        }
    }

    @Override public String getXML() throws KettleException {
        StringBuilder retVal = new StringBuilder();
        retVal.append("  ").append(XMLHandler.addTagValue("endpoint", getEndpoint())).append("  ")
            .append(XMLHandler.addTagValue("accessId", getAccessId())).append("  ")
            .append(XMLHandler.addTagValue("accessKey", getAccessKey())).append("  ")
            .append(XMLHandler.addTagValue("projectName", getProjectName())).append("  ")
            .append(XMLHandler.addTagValue("tableName", getTableName())).append("  ")
            .append(XMLHandler.addTagValue("partition", getPartition()));

        for (int i = 0; i < odpsFields.size(); i++) {
            OdpsField field = odpsFields.get(i);
            retVal.append("  <fields>").append(Const.CR);
            retVal.append("  ")
                .append(XMLHandler.addTagValue("name", field.getName().toLowerCase()));
            retVal.append("  ").append(XMLHandler.addTagValue("type", field.getType()));
            retVal.append("  ").append(XMLHandler.addTagValue("comment", field.getComment()));
            retVal.append("  </fields>").append(Const.CR);
        }

        return retVal.toString();
    }

    @Override public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step,
        List<DatabaseMeta> databases) throws KettleException {
        setEndpoint(rep.getStepAttributeString(id_step, "endpoint"));
        setAccessId(rep.getStepAttributeString(id_step, "accessId"));
        setAccessKey(rep.getStepAttributeString(id_step, "accessKey"));
        setProjectName(rep.getStepAttributeString(id_step, "projectName"));
        setTableName(rep.getStepAttributeString(id_step, "tableName"));
        setPartition(rep.getStepAttributeString(id_step, "partition"));

        int nrFields = rep.countNrStepAttributes(id_step, "field_name");
        odpsFields = new ArrayList<OdpsField>(nrFields);
        for (int i = 0; i < nrFields; i++) {
            OdpsField field = new OdpsField();
            field.setName(rep.getStepAttributeString(id_step, i, "field_name"));
            field.setType(rep.getStepAttributeString(id_step, i, "field_type"));
            field.setComment(rep.getStepAttributeString(id_step, i, "field_comment"));
            odpsFields.add(field);
        }
    }

    @Override public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation,
        ObjectId id_step) throws KettleException {
        rep.saveStepAttribute(id_transformation, id_step, "endpoint", getEndpoint());
        rep.saveStepAttribute(id_transformation, id_step, "accessId", getAccessId());
        rep.saveStepAttribute(id_transformation, id_step, "accessKey", getAccessKey());
        rep.saveStepAttribute(id_transformation, id_step, "projectName", getProjectName());
        rep.saveStepAttribute(id_transformation, id_step, "tableName", getTableName());
        rep.saveStepAttribute(id_transformation, id_step, "partition", getPartition());

        for (int i = 0; i < odpsFields.size(); i++) {
            OdpsField field = odpsFields.get(i);
            rep.saveStepAttribute(id_transformation, id_step, i, "field_name",
                field.getName().toLowerCase());
            rep.saveStepAttribute(id_transformation, id_step, i, "field_type", field.getType());
            rep.saveStepAttribute(id_transformation, id_step, i, "field_comment",
                field.getComment());
        }
    }

    @Override public void setDefault() {
        setEndpoint("http://service.odps.aliyun.com/api");
        setAccessId("");
        setAccessKey("");
        setProjectName("");
        setTableName("");
        setPartition("");
    }

}
