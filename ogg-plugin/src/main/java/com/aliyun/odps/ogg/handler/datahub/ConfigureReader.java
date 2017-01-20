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

import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.wrapper.Project;
import com.aliyun.datahub.wrapper.Topic;
import com.aliyun.odps.ogg.handler.datahub.modle.ColumnMapping;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import com.aliyun.odps.ogg.handler.datahub.util.JsonHelper;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

/**
 * Created by yongfeng.liang on 2016/3/28.
 */
public class ConfigureReader {

    private final static Logger logger = LoggerFactory.getLogger(ConfigureReader.class);

    public static Configure reader(String configueFileName) throws DocumentException {
        logger.info("Begin read configure[" + configueFileName + "]");

        Configure configure = new Configure();
        SAXReader reader = new SAXReader();
        File file = new File(configueFileName);

        Document document = reader.read(file);
        Element root = document.getRootElement();

        String elementText = root.elementTextTrim("batchSize");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setBatchSize(Integer.parseInt(elementText));
        }

        elementText = root.elementTextTrim("dirtyDataContinue");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setDirtyDataContinue(Boolean.parseBoolean(elementText));
        }

        elementText = root.elementTextTrim("dirtyDataFile");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setDirtyDataFile(elementText);
        }

        elementText = root.elementTextTrim("dirtyDataFileMaxSize");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setDirtyDataFileMaxSize(Integer.parseInt(elementText));
        }

        elementText = root.elementTextTrim("retryTimes");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setRetryTimes(Integer.parseInt(elementText));
        }

        elementText = root.elementTextTrim("retryInterval");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setRetryInterval(Integer.parseInt(elementText));
        }

        elementText = root.elementTextTrim("disableCheckPointFile");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setDisableCheckPointFile(Boolean.parseBoolean(elementText));
        }

        elementText = root.elementTextTrim("checkPointFileName");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setCheckPointFileName(elementText);
        }

        Element element = root.element("defaultOracleConfigure");
        if (element == null) {
            throw new RuntimeException("defaultOracleConfigure is null");
        }

        elementText = element.elementTextTrim("sid");
        if (StringUtils.isBlank(elementText)) {
            throw new RuntimeException("defaultOracleConfigure.sid is null");
        }
        configure.setSid(elementText);

        String defaultOracleSchema = element.elementTextTrim("schema");

        SimpleDateFormat defaultSimpleDateFormat;
        elementText = element.elementTextTrim("dateFormat");
        if (StringUtils.isNotBlank(elementText)) {
            defaultSimpleDateFormat = new SimpleDateFormat(elementText);
        } else {
            defaultSimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        element = root.element("defalutDatahubConfigure");
        if (element == null) {
            throw new RuntimeException("defalutDatahubConfigure is null");
        }

        String endPoint = element.elementText("endPoint");
        if (StringUtils.isBlank(endPoint)) {
            throw new RuntimeException("defalutDatahubConfigure.endPoint is null");

        }

        String defaultDatahubProject = element.elementText("project");
        String defaultDatahubAccessID = element.elementText("accessId");
        String defaultDatahubAccessKey = element.elementText("accessKey");

        Field defaultCTypeField = null;
        String defaultCTypeColumn = element.elementText("ctypeColumn");
        if (StringUtils.isNotBlank(defaultCTypeColumn)) {
            defaultCTypeField = new Field(defaultCTypeColumn, FieldType.STRING);
        }
        Field defaultCTimeField = null;
        String defaultCTimeColumn = element.elementText("ctimeColumn");
        if (StringUtils.isNotBlank(defaultCTimeColumn)) {
            defaultCTimeField = new Field(defaultCTimeColumn, FieldType.STRING);
        }
        Field defaultCidField = null;
        String defaultCidColumn = element.elementText("cidColumn");
        if (StringUtils.isNotBlank(defaultCidColumn)) {
            defaultCidField = new Field(defaultCidColumn, FieldType.STRING);
        }


        String defaultConstColumnMapStr = element.elementText("constColumnMap");
        Map<String, String> defalutConstColumnMappings = Maps.newHashMap();
        Map<String, Field> defaultConstColumnFieldMappings = Maps.newHashMap();
        parseConstColumnMap(defaultConstColumnMapStr, defalutConstColumnMappings,
            defaultConstColumnFieldMappings);

        element = root.element("mappings");
        if (element == null) {
            throw new RuntimeException("mappings is null");
        }

        List<Element> mappingElements = element.elements("mapping");
        if (mappingElements == null || mappingElements.size() == 0) {
            throw new RuntimeException("mappings.mapping is null");
        }

        //init table mapping
        for (Element e : mappingElements) {
            String oracleSchema = e.elementTextTrim("oracleSchema");
            if (StringUtils.isNotBlank(oracleSchema)) {
                //nothing
            } else if (StringUtils.isNotBlank(defaultOracleSchema)) {
                oracleSchema = defaultOracleSchema;
            } else {
                throw new RuntimeException(
                    "both mappings.mapping.oracleSchema and defaultOracleConfigure.schema is null");
            }

            String oracleTable = e.elementTextTrim("oracleTable");
            if (StringUtils.isBlank(oracleTable)) {
                throw new RuntimeException("mappings.mapping.oracleTable is null");
            }

            String datahubProject = e.elementTextTrim("datahubProject");
            if (StringUtils.isNotBlank(datahubProject)) {
                //nothing
            } else if (StringUtils.isNotBlank(defaultOracleSchema)) {
                datahubProject = defaultDatahubProject;
            } else {
                throw new RuntimeException(
                    "both mappings.mapping.datahubProject and defalutDatahubConfigure.project is null");
            }

            String datahubAccessId = e.elementTextTrim("datahubAccessId");
            if (StringUtils.isNotBlank(datahubAccessId)) {
                //nothing
            } else if (StringUtils.isNotBlank(defaultDatahubAccessID)) {
                datahubAccessId = defaultDatahubAccessID;
            } else {
                throw new RuntimeException(
                    "both mappings.mapping.datahubAccessId and defalutDatahubConfigure.accessId is null");
            }

            String datahubAccessKey = e.elementTextTrim("datahubAccessKey");
            if (StringUtils.isNotBlank(datahubAccessKey)) {
                //nothing
            } else if (StringUtils.isNotBlank(defaultDatahubAccessKey)) {
                datahubAccessKey = defaultDatahubAccessKey;
            } else {
                throw new RuntimeException(
                    "both mappings.mapping.datahubAccessKey and defalutDatahubConfigure.accessKey is null");
            }

            String topicName = e.elementTextTrim("datahubTopic");
            if (topicName == null) {
                throw new RuntimeException("mappings.mapping.datahubTopic is null");
            }

            String ctypeColumn = e.elementText("ctypeColumn");
            String ctimeColumn = e.elementText("ctimeColumn");
            String cidColumn = e.elementText("cidColumn");

            DatahubConfiguration datahubConfiguration =
                new DatahubConfiguration(new AliyunAccount(datahubAccessId, datahubAccessKey),
                    endPoint);
            Project project = Project.Builder.build(datahubProject, datahubConfiguration);
            Topic topic = project.getTopic(topicName);
            if (topic == null) {
                throw new RuntimeException("Can not find datahub topic[" + topicName + "]");
            } else {
                logger.info("topic name: " + topicName + ", topic schema: " + topic.getRecordSchema().toJsonString());
            }

            TableMapping tableMapping = new TableMapping();
            tableMapping.setTopic(topic);
            tableMapping.setOracleSchema(oracleSchema.toLowerCase());
            tableMapping.setOracleTableName(oracleTable.toLowerCase());
            tableMapping.setOracleFullTableName(
                tableMapping.getOracleSchema() + "." + tableMapping.getOracleTableName());
            tableMapping.setCtypeField(StringUtils.isNotBlank(ctypeColumn) ?
                new Field(ctypeColumn, FieldType.STRING) :
                defaultCTypeField);
            tableMapping.setCtimeField(StringUtils.isNotBlank(ctimeColumn) ?
                new Field(ctimeColumn, FieldType.STRING) :
                defaultCTimeField);
            tableMapping.setCidField(StringUtils.isNotBlank(cidColumn) ?
                new Field(cidColumn, FieldType.STRING) :
                defaultCidField);

            String constColumnMapStr = e.elementText("constColumnMap");
            Map<String, String> constColumnMappings = Maps.newHashMap();
            Map<String, Field> constColumnFieldMappings = Maps.newHashMap();
            parseConstColumnMap(constColumnMapStr, constColumnMappings, constColumnFieldMappings);

            tableMapping.setConstColumnMappings(
                constColumnMappings.isEmpty() ?
                    defalutConstColumnMappings :
                    constColumnMappings);
            tableMapping.setConstFieldMappings(
                constColumnFieldMappings.isEmpty() ?
                    defaultConstColumnFieldMappings :
                    constColumnFieldMappings);

            Map<String, ColumnMapping> columnMappings = Maps.newHashMap();
            tableMapping.setColumnMappings(columnMappings);

            elementText = e.elementTextTrim("shardId");
            if (StringUtils.isNotBlank(elementText)) {
                tableMapping.setShardId(elementText);
            }

            configure.addTableMapping(tableMapping);

            RecordSchema recordSchema = topic.getRecordSchema();

            Element columnMappingElement = e.element("columnMapping");
            List<Element> columns = columnMappingElement.elements("column");
            for (Element columnElement : columns) {
                String oracleColumnName = columnElement.attributeValue("src");
                if (StringUtils.isBlank(oracleColumnName)) {
                    throw new RuntimeException("Topic[" + topicName + "] src attribute is null");
                }

                oracleColumnName = oracleColumnName.toLowerCase();
                ColumnMapping columnMapping = new ColumnMapping();
                columnMappings.put(oracleColumnName, columnMapping);
                columnMapping.setOracleColumnName(oracleColumnName);

                String datahubFieldName = columnElement.attributeValue("dest");
                if (datahubFieldName == null) {
                    throw new RuntimeException("Topic[" + topicName + "] dest attribute is null");
                }

                Field field = recordSchema.getField(datahubFieldName.toLowerCase());
                if (field == null) {
                    throw new RuntimeException(
                        "Topic[" + topicName + "] Field[" + datahubFieldName + "] is not exist");
                }

                columnMapping.setField(field);

                String datahubOldFieldName = columnElement.attributeValue("destOld");
                if (StringUtils.isNotBlank(datahubOldFieldName)) {
                    Field oldField = recordSchema.getField(datahubOldFieldName);

                    if (field == null) {
                        throw new RuntimeException(
                            "Topic[" + topicName + "] Field[" + datahubOldFieldName
                                + "] is not exist");
                    }
                    columnMapping.setOldFiled(oldField);
                }

                String isShardColumn = columnElement.attributeValue("isShardColumn");
                if (StringUtils.isNotBlank(isShardColumn) && Boolean.TRUE
                    .equals(Boolean.valueOf(isShardColumn))) {
                    tableMapping.setIsShardHash(true);
                    columnMapping.setIsShardColumn(true);
                } else {
                    columnMapping.setIsShardColumn(false);
                }

                String dateFormat = columnElement.attributeValue("dateFormat");

                if (StringUtils.isNotBlank(dateFormat)) {
                    columnMapping.setSimpleDateFormat(new SimpleDateFormat(dateFormat));
                } else {
                    columnMapping.setSimpleDateFormat(defaultSimpleDateFormat);
                }

                String isDateFormat = columnElement.attributeValue("isDateFormat");

                if (StringUtils.isNotBlank(isDateFormat) && Boolean.FALSE
                    .equals(Boolean.valueOf(isDateFormat))) {
                    columnMapping.setIsDateFormat(false);
                } else {
                    columnMapping.setIsDateFormat(true);
                }
            }
        }

        logger.info("Read configure success: " + JsonHelper.beanToJson(configure));
        return configure;
    }

    private static void parseConstColumnMap(String constColumnMapStr,
        Map<String, String> constColumnMappings, Map<String, Field> constColumnFieldMappings) {
        if (StringUtils.isBlank(constColumnMapStr)) {
            return;
        }
        String[] constColumns = constColumnMapStr.split(",");
        for (String c: constColumns) {
            String[] kv = c.split("=");
            if (kv.length != 2) {
                throw new RuntimeException(
                    "Const column map configure is wrong, should like c1=xxx,c2=xxx,c3=xxx");
            }
            constColumnMappings.put(kv[0], kv[1]);
            constColumnFieldMappings.put(kv[0], new Field(kv[0], FieldType.STRING));
        }
    }
}
