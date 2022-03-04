/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.test.util;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;
import com.google.gson.GsonBuilder;
import org.apache.flink.odps.catalog.OdpsCatalog;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;

import java.util.*;

public class OdpsTestUtils {
    public static final String catalogName = "myodps";
    public static final String projectName = getOdpsConf().getProject();
    public static final String defaultOdpsConfResource = "test-odps-conf";

    public static OdpsConf getOdpsConf() {
        String odpsConfDir = Thread.currentThread()
                .getContextClassLoader()
                .getResource(defaultOdpsConfResource)
                .getPath();
        return OdpsUtils.getOdpsConf(odpsConfDir);
    }

    public static OdpsCatalog createOdpsCatalog(String name) {
        return new OdpsCatalog(
                name, projectName, getOdpsConf());
    }

    public static String exec(String sql) {
        String result = null;
        String taskName = "SqlTask";
        try {
            Map<String, String> hints = new HashMap<String, String>();
            hints.put("odps.sql.type.system.odps2", "true");
            hints.put("odps.sql.decimal.odps2","true");
            hints.put("odps.sql.hive.compatible","false");
            hints.put("odps.sql.allow.fullscan","true");
            Instance instance = exec(sql, taskName, hints);
            instance.waitForSuccess();

            Map<String, String> resultMap = instance.getTaskResults();
            result = resultMap.get(taskName);

            Instance.TaskStatus taskStatus = instance.getTaskStatus().get(taskName);
            if (Instance.TaskStatus.Status.FAILED.equals(taskStatus.getStatus())) {
                throw new Exception(result);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static Instance exec(String sql, String taskName, Map<String, String> hints) throws OdpsException {
        Odps odps = OdpsUtils.getOdps(getOdpsConf());
        SQLTask task = new SQLTask();
        task.setQuery(sql);
        task.setName(taskName);
        if (hints != null) {
            task.setProperty("settings", new GsonBuilder().disableHtmlEscaping().create().toJson(hints));
        }
        return odps.instances().create(task);
    }

    public static String dropOdpsTable(String table) {
        return "DROP TABLE IF EXISTS " + table + ";";
    }

    public static List<String> QUERY_BY_ODPS_SQL(String sql) throws Exception {
        String odpsSqlResult = OdpsTestUtils.exec(sql +";");
        List<String> result = new ArrayList<>();
        String lines[] = odpsSqlResult.split("\\r?\\n");
        if (lines.length==1){
            return result;
        }
        result.addAll(Arrays.asList(lines).subList(1, lines.length));
        return result;
    }

    public static class WC {
        public String word;
        public long frequency;

        // public constructor to make it a Flink POJO
        public WC() {}

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency;
        }
    }


    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
}



