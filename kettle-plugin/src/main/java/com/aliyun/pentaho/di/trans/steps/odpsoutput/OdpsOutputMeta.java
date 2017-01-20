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

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import com.aliyun.pentaho.di.trans.steps.odps.OdpsMeta;

@Step(
    id = "OdpsOutput",
    image = "images/OdpsOutput.svg",
    name = "Aliyun MaxCompute Output",
    description = "Write data to MaxCompute",
    categoryDescription = "Big Data") public class OdpsOutputMeta extends OdpsMeta {

    protected static Class<?> PKG = OdpsOutputMeta.class;

    private boolean truncate;

    private List<String> streamFields = new ArrayList<String>();

    public List<String> getStreamFields() {
        return streamFields;
    }

    public void setStreamFields(List<String> streamFields) {
        this.streamFields = streamFields;
    }

    public boolean isTruncate() {
        return truncate;
    }

    public void setTruncate(boolean truncate) {
        this.truncate = truncate;
    }

    @Override public Object clone() {
        OdpsOutputMeta retval = (OdpsOutputMeta) super.clone();
        retval.truncate = truncate;
        retval.streamFields = new ArrayList<String>();
        for (int i = 0; i < streamFields.size(); i++) {
            retval.streamFields.add(streamFields.get(i));
        }
        return retval;
    }

    @Override public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore)
        throws KettleXMLException {
        super.loadXML(stepnode, databases, metaStore);
        String truncateStr = XMLHandler.getTagValue(stepnode, "truncate");
        if (truncateStr.equals("true")) {
            setTruncate(true);
        } else {
            setTruncate(false);
        }

        int nrStreamFields = XMLHandler.countNodes(stepnode, "stream_fields");
        streamFields = new ArrayList<String>(nrStreamFields);
        for (int i = 0; i < nrStreamFields; i++) {
            Node fieldNode = XMLHandler.getSubNodeByNr(stepnode, "stream_fields", i);
            streamFields.add(XMLHandler.getTagValue(fieldNode, "name"));
        }
    }

    @Override public String getXML() throws KettleException {

        StringBuilder retVal = new StringBuilder();
        retVal.append(super.getXML());

        retVal.append("  ")
            .append(XMLHandler.addTagValue("truncate", isTruncate() ? "true" : "false"));

        for (int i = 0; i < streamFields.size(); i++) {
            String streamField = streamFields.get(i);
            retVal.append("  <stream_fields>").append(Const.CR);
            retVal.append("  ").append(XMLHandler.addTagValue("name", streamField.toLowerCase()));
            retVal.append("  </stream_fields>").append(Const.CR);
        }

        return retVal.toString();
    }

    @Override public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step,
        List<DatabaseMeta> databases) throws KettleException {
        super.readRep(rep, metaStore, id_step, databases);

        String truncateStr = rep.getStepAttributeString(id_step, "truncate");
        if (truncateStr.equals("true")) {
            setTruncate(true);
        } else {
            setTruncate(false);
        }

        int nrStreamFields = rep.countNrStepAttributes(id_step, "stream_field_name");
        streamFields = new ArrayList<String>(nrStreamFields);
        for (int i = 0; i < nrStreamFields; i++) {
            streamFields.add(rep.getStepAttributeString(id_step, i, "stream_field_name"));
        }
    }

    @Override public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation,
        ObjectId id_step) throws KettleException {

        super.saveRep(rep, metaStore, id_transformation, id_step);

        rep.saveStepAttribute(id_transformation, id_step, "truncate",
            isTruncate() ? "true" : "false");

        for (int i = 0; i < streamFields.size(); i++) {
            String streamField = streamFields.get(i);
            rep.saveStepAttribute(id_transformation, id_step, i, "stream_field_name",
                streamField.toLowerCase());
        }
    }


    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
        TransMeta tr, Trans trans) {
        return new OdpsOutput(stepMeta, stepDataInterface, cnr, tr, trans);
    }

    @Override public StepDataInterface getStepData() {
        return new OdpsOutputData();
    }

}
