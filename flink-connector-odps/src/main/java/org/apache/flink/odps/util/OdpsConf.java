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

import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

public class OdpsConf implements Serializable {

    private final String accessId;
    private final String accessKey;
    private final String endpoint;
    private final String project;
    private String tunnelEndpoint;
    private boolean isClusterMode;
    private final Map<String, String> properties;

    public OdpsConf(String accessId, String accessKey, String endpoint, String project) {
        this(accessId, accessKey, endpoint, project, "", false);
    }

    public OdpsConf(String accessId, String accessKey, String endpoint, String project, String tunnelEndpoint) {
        this(accessId, accessKey, endpoint, project, tunnelEndpoint, false);
    }

    public OdpsConf(String accessId, String accessKey, String endpoint, String project, boolean isClusterMode) {
        this(accessId, accessKey, endpoint, project, "", isClusterMode);
    }

    public OdpsConf(String accessId, String accessKey, String endpoint, String project, String tunnelEndpoint, boolean isClusterMode) {
        if (System.getenv("META_LOOKUP_NAME") == null) {
            checkArgument(!StringUtils.isNullOrWhitespaceOnly(accessId), "accessId is whitespace or null!");
            checkArgument(!StringUtils.isNullOrWhitespaceOnly(accessKey), "accessKey is whitespace or null!");
            checkArgument(!StringUtils.isNullOrWhitespaceOnly(endpoint), "endpoint is whitespace or null!");
            checkArgument(!StringUtils.isNullOrWhitespaceOnly(project), "project is whitespace or null!");
        }
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.endpoint = endpoint;
        this.project = project;
        this.tunnelEndpoint = tunnelEndpoint;
        this.isClusterMode = isClusterMode;
        this.properties = new HashMap<>();
    }

    public void setTunnelEndpoint(String tunnelEndpoint) {
        this.tunnelEndpoint = tunnelEndpoint;
    }

    public void setClusterMode(boolean clusterMode) {
        isClusterMode = clusterMode;
    }

    public String getTunnelEndpoint() {
        return tunnelEndpoint;
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getProject() {
        return project;
    }

    public boolean isClusterMode() {
        return isClusterMode;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperty(String key, String value) {
        this.properties.put(key, value);
    }

    public boolean containsProperty(String key) {
        return this.properties.containsKey(key);
    }

    public String getProperty(String key) {
        return this.properties.get(key);
    }

    public String getPropertyOrDefault(String key, String defaultValue) {
        return this.properties.getOrDefault(key, defaultValue);
    }

    public int getPropertyOrDefault(String key, int defaultValue) {
        if (this.properties.containsKey(key)) {
            return Integer.parseInt(getProperty(key));
        } else {
            return defaultValue;
        }
    }

    public Boolean getPropertyOrDefault(String key, boolean defaultValue) {
        if (this.properties.containsKey(key)) {
            return Boolean.parseBoolean(getProperty(key));
        } else {
            return defaultValue;
        }
    }

    @Override
    public String toString() {
        return "OdpsConf {" +
                "accessId='" + accessId + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", project='" + project + '\'' +
                ", tunnelEndpoint='" + tunnelEndpoint + '\'' +
                ", clusterMode='" + isClusterMode + '\'' +
                ", properties='" + properties.toString() + '\'' +
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
        OdpsConf that = (OdpsConf) o;
        return project.equals(that.project)
                && accessId.equals(that.accessId)
                && accessKey.equals(that.accessKey)
                && endpoint.equals(that.endpoint)
                && tunnelEndpoint.equals(that.tunnelEndpoint)
                && isClusterMode == that.isClusterMode
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                project,
                accessId,
                accessKey,
                endpoint,
                tunnelEndpoint,
                isClusterMode,
                properties);
    }
}
