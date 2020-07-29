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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by yongfeng.liang on 2016/3/28.
 */
public class ConfigureReader {

    private final static Logger logger = LoggerFactory.getLogger(ConfigureReader.class);

    public static Configure reader(String configueFileName) throws DocumentException {
        logger.info("Begin read configure[{}]", configueFileName);

        Configure configure = new Configure();
        SAXReader reader = new SAXReader();
        File file = new File(configueFileName);

        Document document = reader.read(file);
        Element root = document.getRootElement();

        /* for root element */
        String elementText = root.elementTextTrim("batchSize");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setBatchSize(Integer.parseInt(elementText));
        }

        elementText = root.elementTextTrim("batchTimeoutMs");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setBatchTimeoutMs(Integer.parseInt(elementText));
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
            configure.setRetryIntervalMs(Integer.parseInt(elementText));
        }

        elementText = root.elementTextTrim("disableCheckPointFile");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setCheckPointFileDisable(Boolean.parseBoolean(elementText));
        }

        elementText = root.elementTextTrim("checkPointFileName");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setCheckPointFileName(elementText);
        }

        elementText = root.elementTextTrim("charset");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setCharsetName(elementText);
        }

        elementText = root.elementTextTrim("buildRecordQueueSize");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setBuildRecordQueueSize(Integer.parseInt(elementText));
        }

        elementText = root.elementTextTrim("buildRecordQueueTimeoutMs");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setBuildRecordQueueTimeoutMs(Integer.parseInt(elementText));
        }

        elementText = root.elementTextTrim("putRecordQueueSize");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setPutRecordQueueSize(Integer.parseInt(elementText));
        }

        elementText = root.elementTextTrim("putRecordQueueTimeoutMs");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setPutRecordQueueTimeoutMs(Integer.parseInt(elementText));
        }

        elementText = root.elementTextTrim("commitFlush");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setCommitFlush(Boolean.parseBoolean(elementText));
        }

        elementText = root.elementTextTrim("reportMetric");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setReportMetric(Boolean.parseBoolean(elementText));
        }

        elementText = root.elementTextTrim("reportMetricIntervalMs");
        if (StringUtils.isNotBlank(elementText)) {
            configure.setReportMetricIntervalMs(Integer.parseInt(elementText));
        }

        /* for oracle default config */
        Element element = root.element("defaultOracleConfigure");
        if (element == null) {
            throw new RuntimeException("defaultOracleConfigure is null");
        }

        elementText = element.elementTextTrim("sid");
        if (StringUtils.isNotBlank(elementText)){
            configure.setOracleSid(elementText);
        }

        String defaultOracleSchema = element.elementTextTrim("schema");

        /* for DataHub default config */
        element = root.element("defaultDatahubConfigure");
        if (element == null) {
            throw new RuntimeException("defaultDatahubConfigure is null");
        }

        String endPoint = element.elementText("endPoint");
        if (StringUtils.isBlank(endPoint)) {
            throw new RuntimeException("defaultDatahubConfigure.endPoint is null");
        }
        configure.setDatahubEndpoint(endPoint);

        String defaultDatahubAccessID = element.elementText("accessId");
        if (StringUtils.isNotBlank(defaultDatahubAccessID)) {
            configure.setDatahubAccessId(defaultDatahubAccessID);
        }

        String defaultDatahubAccessKey = element.elementText("accessKey");
        if (StringUtils.isNotBlank(defaultDatahubAccessKey)) {
            configure.setDatahubAccessKey(defaultDatahubAccessKey);
        }

        String defaultDatahubProject = element.elementText("project");
        String defaultCTypeColumn = element.elementText("ctypeColumn");
        String defaultCTimeColumn = element.elementText("ctimeColumn");
        String defaultCidColumn = element.elementText("cidColumn");

        String defaultConstColumnMapStr = element.elementText("constColumnMap");
        Map<String, String> defalutConstColumnMappings = Maps.newHashMap();
        parseConstColumnMap(defaultConstColumnMapStr, defalutConstColumnMappings);

        String compressType = element.elementText("compressType");
        if (StringUtils.isNotBlank(compressType)) {
            configure.setCompressType(compressType);
        }

        String enablePb = element.elementText("enablePb");
        if (StringUtils.isNotBlank(enablePb)) {
            configure.setEnablePb(Boolean.parseBoolean(enablePb));
        }

        /** for table mappings **/
        element = root.element("mappings");
        if (element == null) {
            throw new RuntimeException("mappings is null");
        }


        List<Element> mappingElements = element.elements("mapping");
        System.out.println(mappingElements.size());
        if (mappingElements == null || mappingElements.size() == 0) {
            throw new RuntimeException("mappings.mapping is null");
        }

        /** for table mapping **/
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
            } else if (StringUtils.isNotBlank(defaultDatahubProject)) {
                datahubProject = defaultDatahubProject;
            } else {
                throw new RuntimeException(
                        "both mappings.mapping.datahubProject and defaultDatahubConfigure.project is null");
            }

            String datahubAccessId = e.elementTextTrim("datahubAccessId");
            if (StringUtils.isNotBlank(datahubAccessId)) {
                //nothing
            } else if (StringUtils.isNotBlank(defaultDatahubAccessID)) {
                datahubAccessId = defaultDatahubAccessID;
            } else {
                throw new RuntimeException(
                        "both mappings.mapping.datahubAccessId and defaultDatahubConfigure.accessId is null");
            }

            String datahubAccessKey = e.elementTextTrim("datahubAccessKey");
            if (StringUtils.isNotBlank(datahubAccessKey)) {
                //nothing
            } else if (StringUtils.isNotBlank(defaultDatahubAccessKey)) {
                datahubAccessKey = defaultDatahubAccessKey;
            } else {
                throw new RuntimeException(
                        "both mappings.mapping.datahubAccessKey and defaultDatahubConfigure.accessKey is null");
            }

            String topicName = e.elementTextTrim("datahubTopic");
            if (StringUtils.isBlank(topicName)) {
                throw new RuntimeException("mappings.mapping.datahubTopic is null");
            }

            int buildSpeed = 0;
            elementText = e.elementText("buildSpeed");
            if (StringUtils.isNotBlank(elementText)) {
                buildSpeed = Integer.parseInt(elementText);
            }

            String rowIdColumn = e.elementText("rowIdColumn");

            String cTypeColumn = e.elementText("ctypeColumn");
            cTypeColumn = StringUtils.isNotBlank(cTypeColumn) ? cTypeColumn : defaultCTypeColumn;

            String cTimeColumn = e.elementText("ctimeColumn");
            cTimeColumn = StringUtils.isNotBlank(cTimeColumn) ? cTimeColumn : defaultCTimeColumn;

            String cIdColumn = e.elementText("cidColumn");
            cIdColumn = StringUtils.isNotBlank(cIdColumn) ? cIdColumn : defaultCidColumn;

            String constColumnMapStr = e.elementText("constColumnMap");
            Map<String, String> constColumnMappings = Maps.newHashMap();
            parseConstColumnMap(constColumnMapStr, constColumnMappings);
            constColumnMappings = constColumnMappings.isEmpty() ? defalutConstColumnMappings : constColumnMappings;

            elementText = e.elementTextTrim("shardId");
            List<String> shardIds = new ArrayList<String>();
            paraseShardList(elementText, shardIds);

            TableMapping tableMapping = new TableMapping();

            tableMapping.setOracleSchema(oracleSchema.toLowerCase());
            tableMapping.setOracleTableName(oracleTable.toLowerCase());
            tableMapping.setOracleFullTableName(
                    tableMapping.getOracleSchema() + "." + tableMapping.getOracleTableName());
            tableMapping.setProjectName(datahubProject);
            tableMapping.setTopicName(topicName);
            tableMapping.setAccessId(datahubAccessId);
            tableMapping.setAccessKey(datahubAccessKey);
            tableMapping.setRowIdColumn(rowIdColumn);
            tableMapping.setcTypeColumn(cTypeColumn);
            tableMapping.setcTimeColumn(cTimeColumn);
            tableMapping.setcIdColumn(cIdColumn);
            tableMapping.setConstColumnMappings(constColumnMappings);
            tableMapping.setShardIds(shardIds);
            if (!shardIds.isEmpty()) {
                tableMapping.setSetShardId(true);
            }
            if (buildSpeed > 0) {
                tableMapping.setBuildSpeed(buildSpeed);
            }

            configure.addTableMapping(tableMapping);
            Map<String, ColumnMapping> columnMappings = Maps.newHashMap();
            tableMapping.setColumnMappings(columnMappings);


            /** for column mapping **/
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
                columnMapping.setSrc(oracleColumnName);

                String dest = columnElement.attributeValue("dest");
                if (StringUtils.isBlank(dest)) {
                    throw new RuntimeException("Topic[" + topicName + "] dest attribute is null");
                }
                columnMapping.setDest(dest);

                String destOld = columnElement.attributeValue("destOld");
                if (StringUtils.isNotBlank(destOld)) {
                    columnMapping.setDestOld(destOld);
                }

                String isShardColumn = columnElement.attributeValue("isShardColumn");
                if (StringUtils.isNotBlank(isShardColumn) && Boolean.TRUE
                        .equals(Boolean.valueOf(isShardColumn))) {
                    tableMapping.setShardHash(true);
                    columnMapping.setIsShardColumn(true);
                } else {
                    columnMapping.setIsShardColumn(false);
                }

                String isKeyColumn = columnElement.attributeValue("isKeyColumn");
                if (StringUtils.isNotBlank(isKeyColumn) && Boolean.TRUE
                        .equals(Boolean.valueOf(isKeyColumn))) {
                    columnMapping.setIsKeyColumn(true);
                } else {
                    columnMapping.setIsKeyColumn(false);
                }

                String isDateFormat = columnElement.attributeValue("isDateFormat");
                if (StringUtils.isNotBlank(isDateFormat)) {
                    columnMapping.setIsDateFormat(Boolean.parseBoolean(isDateFormat));
                }

                String dateFormat = columnElement.attributeValue("dateFormat");
                if (StringUtils.isNotBlank(dateFormat)) {
                    columnMapping.setDateFormat(dateFormat);
                }

                String charset = columnElement.attributeValue("isDefaultCharset");
                if (StringUtils.isNotBlank(charset)) {
                    columnMapping.setDefaultCharset(Boolean.parseBoolean(isDateFormat));
                }
            }
        }

        logger.info("Read configure success: {} ", JsonHelper.beanToJson(configure));
        return configure;
    }

    private static void parseConstColumnMap(String constColumnMapStr,
                                            Map<String, String> constColumnMappings) {
        if (StringUtils.isBlank(constColumnMapStr)) {
            return;
        }
        String[] constColumns = constColumnMapStr.split(",");
        for (String c : constColumns) {
            String[] kv = c.split("=");
            if (kv.length != 2) {
                throw new RuntimeException(
                        "Const column map configure is wrong, should like c1=xxx,c2=xxx,c3=xxx. But found " + constColumnMapStr);
            }
            constColumnMappings.put(kv[0], kv[1]);
        }
    }

    private static void paraseShardList(String shardListStr, List<String> shardIds) {
        if (shardIds == null) {
            shardIds = new ArrayList<String>();
        }

        if (StringUtils.isBlank(shardListStr)) {
            return;
        }

        String[] strs = StringUtils.split(shardListStr, ",");
        shardIds.addAll(Arrays.asList(strs));
    }
}
