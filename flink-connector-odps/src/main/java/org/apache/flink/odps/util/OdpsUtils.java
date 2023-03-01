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

package org.apache.flink.odps.util;

import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.type.AbstractCharTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.odps.*;
import org.apache.flink.odps.output.stream.PartitionComputer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.odps.util.Constants.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class OdpsUtils {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsUtils.class);

    private static URL odpsConfUrl;
    private static OdpsConf odpsConf;

    public static OdpsConf getOdpsConf() {
        String confPath = System.getenv(ODPS_CONF_DIR);
        return getOdpsConf(confPath);
    }

    public synchronized static OdpsConf getOdpsConf(String confPath) {
        URL url = null;
        if (confPath == null) {
            confPath = System.getenv(ODPS_CONF_DIR);
        }
        if (confPath != null && !confPath.trim().isEmpty()) {
            File confFile = new File(confPath, "odps.conf");
            if (confFile.exists()) {
                try {
                    url = confFile.toURI().toURL();
                } catch (MalformedURLException e) {
                    LOG.error("cannot find user defined odps config file in " + confPath);
                }
            } else {
                LOG.error("cannot find user defined odps config file in " + confPath);
            }
        }
        if (url == null) {
            url = OdpsConf.class.getClassLoader().getResource("odps.conf");
        }

        if (odpsConfUrl != null && odpsConfUrl.equals(url)) {
            return odpsConf;
        }

        if (url != null) {
            Properties props = new Properties();
            try {
                InputStreamReader reader = new InputStreamReader(url.openStream(), StandardCharsets.UTF_8);
                props.load(reader);
            } catch (IOException ex) {
                LOG.error("load odps default configs failed.");
                return null;
            }

            OdpsConf odpsConf = new OdpsConf(
                    props.getProperty(ODPS_ACCESS_ID),
                    props.getProperty(ODPS_ACCESS_KEY),
                    props.getProperty(ODPS_END_POINT),
                    props.getProperty(ODPS_PROJECT_NAME),
                    props.getProperty(ODPS_TUNNEL_END_POINT, ""),
                    Boolean.parseBoolean(props.getProperty(ODPS_CLUSTER_MODE, "false")));
            props.stringPropertyNames().stream().filter(OdpsUtils::validateProperty).forEach(property ->
                    odpsConf.setProperty(property, props.getProperty(property)));
            OdpsUtils.odpsConf = odpsConf;
            OdpsUtils.odpsConfUrl = url;
            return odpsConf;
        } else {
            LOG.error("cannot find user defined odps config file in classpath");
            return null;
        }
    }

    private static boolean validateProperty(String propertyName) {
        return propertyName != null
                && !propertyName.equals(ODPS_ACCESS_ID)
                && !propertyName.equals(ODPS_ACCESS_KEY)
                && !propertyName.equals(ODPS_END_POINT)
                && !propertyName.equals(ODPS_PROJECT_NAME)
                && !propertyName.equals(ODPS_TUNNEL_END_POINT)
                && !propertyName.equals(ODPS_CLUSTER_MODE);
    }

    public static Odps getOdps(OdpsConf odpsConf) {
        Odps odps = new Odps(getDefaultAccount(odpsConf));
        odps.setEndpoint(odpsConf.getEndpoint());
        odps.setDefaultProject(odpsConf.getProject());
        return odps;
    }

    private static Account getDefaultAccount(OdpsConf odpsConf) {
        if (odpsConf.containsProperty("odps.access.security.token")
                && com.aliyun.odps.utils.StringUtils.isNotBlank(odpsConf.getProperty("odps.access.security.token"))) {
            return new StsAccount(odpsConf.getAccessId(),
                    odpsConf.getAccessKey(),
                    odpsConf.getProperty("odps.access.security.token"));
        } else {
            return new AliyunAccount(odpsConf.getAccessId(),
                    odpsConf.getAccessKey());
        }
    }

    public static Options getOdpsOptions(OdpsConf odpsConf) {
        Options.OptionsBuilder builder = new Options.OptionsBuilder();
        builder.project(odpsConf.getProject());
        builder.accessId(odpsConf.getAccessId());
        builder.accessKey(odpsConf.getAccessKey());
        builder.endpoint(odpsConf.getEndpoint());
        builder.tunnelEndpoint(odpsConf.getTunnelEndpoint());
        odpsConf.getProperties().forEach(builder::option);
        return builder.build();
    }

    public static Map<String, String> getPartitionSpecKVMap(PartitionSpec partitionSpec) {
        Map<String, String> parts = new LinkedHashMap<>(2);
        for (String k : partitionSpec.keys()) {
            parts.put(k, partitionSpec.get(k));
        }
        return parts;
    }

    public static String generatePartition(LinkedHashMap<String, String> partitionSpec) {
        checkNotNull(partitionSpec, "partitionSpec cannot be null");
        StringBuilder sb = new StringBuilder();
        String[] keys = partitionSpec.keySet().toArray(new String[0]);

        for (int i = 0; i < keys.length; ++i) {
            sb.append(keys[i]).append("='").append(partitionSpec.get(keys[i])).append("'");
            if (i + 1 < keys.length) {
                sb.append(',');
            }
        }
        return sb.toString();
    }

    public static String[] createPartitionSpec(List<Partition> odpsPartitions) {
        checkNotNull(odpsPartitions, "PartitionList cannot be null");
        String[] partitions = new String[odpsPartitions.size()];
        for (int i = 0; i < odpsPartitions.size(); i++) {
            PartitionSpec partitionSpec = odpsPartitions.get(i).getPartitionSpec();
            partitions[i] = partitionSpec.keys().stream()
                    .map(colName -> colName + "=" + partitionSpec.get(colName))
                    .collect(Collectors.joining(","));
        }
        return partitions;
    }

    public static Object convertPartitionColumn(String value, TypeInfo odpsTypeInfo) {
        switch (odpsTypeInfo.getOdpsType()) {
            case STRING:
                return value;
            case VARCHAR:
                return new Varchar(value, ((AbstractCharTypeInfo)odpsTypeInfo).getLength());
            case CHAR:
                return new Char(value, ((AbstractCharTypeInfo)odpsTypeInfo).getLength());
            case TINYINT:
                return Byte.parseByte(value);
            case SMALLINT:
                return Short.parseShort(value);
            case INT:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            default:
                throw new FlinkOdpsException("Unsupported partition column type: " + odpsTypeInfo.getTypeName());
        }
    }


    public static <T> PartitionComputer<T> getPartitionComputer(TableSchema tableSchema, String staticPartition) {
        List<String> columnWithPartition = new ArrayList<>();
        columnWithPartition.addAll(tableSchema.getColumns()
                .stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
        columnWithPartition.addAll(tableSchema.getPartitionColumns()
                .stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
        return new PartitionComputer<>(
                "defaultPartition",
                columnWithPartition,
                tableSchema.getPartitionColumns()
                        .stream()
                        .map(Column::getName)
                        .collect(Collectors.toList()),
                staticPartition);
    }

    public static void checkPartition(String partitionSpec, TableSchema tableSchema) {
        if (!StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
            PartitionSpec staticPartSpec = new PartitionSpec(partitionSpec);
            List<String> partitionCols = tableSchema
                    .getPartitionColumns()
                    .stream()
                    .map(Column::getName)
                    .collect(Collectors.toList());
            List<String> unknownPartCols = staticPartSpec
                    .keys()
                    .stream()
                    .filter(k -> !partitionCols.contains(k))
                    .collect(Collectors.toList());
            Preconditions.checkArgument(
                    unknownPartCols.isEmpty(),
                    "Static partition spec contains unknown partition column: " + unknownPartCols.toString());
            int numStaticPart = staticPartSpec.keys().size();
            if (numStaticPart < partitionCols.size()) {
                for (String partitionCol : partitionCols) {
                    if (!staticPartSpec.keys().contains(partitionCol)) {
                        Preconditions.checkArgument(numStaticPart == 0,
                                "Dynamic partition cannot appear before static partition");
                        return;
                    } else {
                        numStaticPart--;
                    }
                }
            }
        }
    }

    public enum RecordType {
        FLINK_TUPLE,
        FLINK_ROW,
        POJO,
        FLINK_ROW_DATA
    }

    public static  <T> TypeComparator<T> buildTypeComparator(T record, List<String> columnNames) {
        TypeInformation<T> typeInfo;
        try {
            typeInfo = TypeExtractor.getForObject(record);
        } catch (Exception e) {
            throw new FlinkOdpsException("Could not create TypeInformation for type " + record.getClass().getName(), e);
        }
        Keys<T> keys = new Keys.ExpressionKeys<>(columnNames.toArray(new String[0]), typeInfo);
        CompositeType<T> compositeType = (CompositeType<T>) typeInfo;
        int[] logicalKeyPositions = keys.computeLogicalKeyPositions();
        int numKeyFields = logicalKeyPositions.length;
        boolean[] orders = new boolean[numKeyFields];
        for (int i = 0; i < numKeyFields; i++) {
            orders[i] = true;
        }
        return compositeType.createComparator(logicalKeyPositions, orders, 0, new ExecutionConfig());
    }

    public static String objToString(@Nullable Object obj) {
        if (obj == null) {
            return null;
        }
        return obj instanceof ByteBuffer ? toHexString(((ByteBuffer) obj).array()) : obj.toString();
    }

    public static String toHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
