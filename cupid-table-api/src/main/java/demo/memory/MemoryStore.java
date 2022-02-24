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

package demo.memory;

import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.reader.InputSplit;
import com.aliyun.odps.data.ArrayRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStore {

    static class Project {
        private Map<String, Table> tables = new ConcurrentHashMap<>();

        void createTable(String table) {
            tables.put(table, new Table());
        }

        boolean tableExists(String table) {
            return tables.containsKey(table);
        }

        void dropTable(String table) {
            tables.remove(table);
        }

        Table getTable(String table) {
            return tables.get(table);
        }
    }

    static class IndexSplit extends InputSplit {
        private int index;

        protected IndexSplit(String project,
                             String table,
                             List<Attribute> dataColumns,
                             List<Attribute> partitionColumns,
                             List<Attribute> readDataColumns,
                             Map<String, String> partitionSpec,
                             int index) {
            super(project, table, dataColumns, partitionColumns, readDataColumns, partitionSpec);
            this.index = index;
        }

        @Override
        public String getProvider() {
            return "memory";
        }

        int getIndex() {
            return index;
        }
    }

    static class Table {
        private List<ArrayRecord[]> files =
                Collections.synchronizedList(new ArrayList<ArrayRecord[]>());

        int getFileCount() {
            return files.size();
        }

        ArrayRecord[] read(int index) {
            return files.get(index);
        }

        void write(ArrayRecord[] file) {
            files.add(file);
        }
    }

    private static Map<String, Project> projects = new ConcurrentHashMap<>();

    public static void createProject(String project) {
        projects.put(project, new Project());
    }

    public static boolean projectExists(String project) {
        return projects.containsKey(project);
    }

    public static void dropProject(String project) {
        projects.remove(project);
    }

    public static void createTable(String project, String table) {
        projects.get(project).createTable(table);
    }

    public static boolean tableExists(String project, String table) {
        return projects.get(project).tableExists(table);
    }

    public static void dropTable(String project, String table) {
        projects.get(project).dropTable(table);
    }

    static Table getTable(String project, String table) {
        return projects.get(project).getTable(table);
    }
}
