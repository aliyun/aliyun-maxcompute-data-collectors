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

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import com.aliyun.pentaho.di.trans.steps.odps.OdpsMeta;

@Step(
    id = "OdpsInput",
    image = "images/OdpsInput.svg",
    name = "Aliyun MaxCompute Input",
    description = "Read data from MaxCompute",
    categoryDescription = "Big Data") public class OdpsInputMeta extends OdpsMeta {

    protected static Class<?> PKG = OdpsInputMeta.class;

    @Override public Object clone() {
        return (OdpsInputMeta) super.clone();
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
        TransMeta tr, Trans trans) {
        return new OdpsInput(stepMeta, stepDataInterface, cnr, tr, trans);
    }

    @Override public StepDataInterface getStepData() {
        return new OdpsInputData();
    }

    @Override
    public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info,
        StepMeta nextStep, VariableSpace space) throws KettleStepException {
        for (int i = 0; i < odpsFields.size(); i++) {
            ValueMetaInterface v = new ValueMeta(odpsFields.get(i).getName().toLowerCase(),
                ValueMetaInterface.TYPE_STRING);
            v.setOrigin(name);
            inputRowMeta.addValueMeta(v);
        }
    }

}
