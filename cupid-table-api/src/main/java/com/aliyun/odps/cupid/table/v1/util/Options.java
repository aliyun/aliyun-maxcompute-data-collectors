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

package com.aliyun.odps.cupid.table.v1.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Options implements Serializable {
    private final OdpsConf odpsConf;
    private final Map<String, String> configs;

    public Options() {
        this.odpsConf = new OdpsConf();
        this.configs = new HashMap<>();
    }

    public Options(OdpsConf odpsConf,
                   Map<String, String> configs) {
        this.odpsConf = odpsConf;
        this.configs = configs;
    }

    public void put(String key, String value) {
        this.configs.put(key, value);
    }

    public String get(String key) {
        return configs.get(key);
    }

    public String getOrDefault(String key, String defaultValue) {
        return configs.getOrDefault(key, defaultValue);
    }

    public int getOrDefault(String key, int defaultValue) {
        if (this.configs.containsKey(key)) {
            return Integer.parseInt(get(key));
        } else {
            return defaultValue;
        }
    }

    public Boolean getOrDefault(String key, boolean defaultValue) {
        if (this.configs.containsKey(key)) {
            return Boolean.parseBoolean(get(key));
        } else {
            return defaultValue;
        }
    }

    public boolean contains(String key) {
        return configs.containsKey(key);
    }

    public OdpsConf getOdpsConf() {
        return odpsConf;
    }

    @Override
    public String toString() {
        return "Options {" +
                "odpsConf='" + odpsConf + '\'' +
                ", configs='" + configs.toString() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Options that = (Options) o;
        return odpsConf.equals(that.odpsConf)
                && configs.equals(that.configs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                odpsConf,
                configs);
    }

    public static class OptionsBuilder {
        private final OdpsConf odpsConf;
        private final Map<String, String> configs;

        public OptionsBuilder() {
            this.odpsConf = new OdpsConf();
            this.configs = new HashMap<>();
        }

        public OptionsBuilder accessId(String accessId) {
            this.odpsConf.setAccessId(accessId);
            return this;
        }

        public OptionsBuilder accessKey(String accessKey) {
            this.odpsConf.setAccessKey(accessKey);
            return this;
        }

        public OptionsBuilder project(String project) {
            this.odpsConf.setProject(project);
            return this;
        }

        public OptionsBuilder endpoint(String endpoint) {
            this.odpsConf.setEndpoint(endpoint);
            return this;
        }

        public OptionsBuilder tunnelEndpoint(String tunnelEndpoint) {
            this.odpsConf.setTunnelEndpoint(tunnelEndpoint);
            return this;
        }

        public OptionsBuilder option(String key, String value) {
            this.configs.put(key, value);
            return this;
        }

        public Options build() {
            return new Options(
                    odpsConf,
                    configs
            );
        }
    }
}
