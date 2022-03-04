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

package org.apache.flink.odps.test.catalog;

import org.apache.flink.odps.catalog.OdpsCatalog;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.CatalogTest;

import java.util.Arrays;
import java.util.HashMap;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

public class OdpsCatalogUtils {


    public static OdpsCatalog createOdpsCatalog() {
        return createOdpsCatalog(null, null);
    }

    public static OdpsCatalog createOdpsCatalog(
            String defaultProjectName, String odpsConfDir) {
        return createOdpsCatalog(CatalogTest.TEST_CATALOG_NAME, defaultProjectName, odpsConfDir);
    }

    public static OdpsCatalog createOdpsCatalog(
            String name, String defaultProjectName, String odpsConfDir) {
        if (name == null) {
            name = CatalogTest.TEST_CATALOG_NAME;
        }
        if (odpsConfDir == null) {
            odpsConfDir = Thread.currentThread()
                    .getContextClassLoader()
                    .getResource(OdpsTestUtils.defaultOdpsConfResource)
                    .getPath();
        }
        OdpsConf odpsConf = OdpsUtils.getOdpsConf(odpsConfDir);
        if (defaultProjectName == null) {
            defaultProjectName = odpsConf.getProject();
        }
        return new OdpsCatalog(name, defaultProjectName, odpsConf);
    }

    public static TableEnvironment createTableEnvInBatchMode() {
        return createTableEnvInBatchMode(SqlDialect.DEFAULT);
    }

    public static TableEnvironment createTableEnvInBatchMode(SqlDialect dialect) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tableEnv.getConfig()
                .getConfiguration()
                .setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);
        tableEnv.getConfig().setSqlDialect(dialect);
        return tableEnv;
    }

    public static StreamTableEnvironment createTableEnvInStreamingMode(
            StreamExecutionEnvironment env) {
        return createTableEnvInStreamingMode(env, SqlDialect.DEFAULT);
    }

    public static StreamTableEnvironment createTableEnvInStreamingMode(
            StreamExecutionEnvironment env, SqlDialect dialect) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig()
                .getConfiguration()
                .setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);
        tableEnv.getConfig().setSqlDialect(dialect);
        return tableEnv;
    }

    public static CatalogTable createCatalogTable(TableSchema tableSchema, int numPartCols) {
        if (numPartCols == 0) {
            return new CatalogTableImpl(tableSchema, new HashMap<>(), "");
        }
        String[] partCols = new String[numPartCols];
        System.arraycopy(tableSchema.getFieldNames(), tableSchema.getFieldNames().length - numPartCols, partCols, 0, numPartCols);
        return new CatalogTableImpl(tableSchema, Arrays.asList(partCols), new HashMap<>(), "");
    }
}
