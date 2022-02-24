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
import java.util.Objects;

public class OdpsConf implements Serializable {
    private String accessId;
    private String accessKey;
    private String endpoint;
    private String project;
    private String tunnelEndpoint;

    public OdpsConf() {
        this.accessId = "";
        this.accessKey = "";
        this.endpoint = "";
        this.project = "";
        this.tunnelEndpoint = "";
    }

    public void setProject(String project) {
        Validator.checkString(project, "project");
        this.project = project;
    }

    public void setAccessId(String accessId) {
        Validator.checkString(accessId, "accessId");
        this.accessId = accessId;
    }

    public void setAccessKey(String accessKey) {
        Validator.checkString(accessKey, "accessKey");
        this.accessKey = accessKey;
    }

    public void setEndpoint(String endpoint) {
        Validator.checkString(endpoint, "endpoint");
        this.endpoint = endpoint;
    }

    public void setTunnelEndpoint(String tunnelEndpoint) {
        this.tunnelEndpoint = tunnelEndpoint;
    }

    public String getProject() {
        return project;
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

    public String getTunnelEndpoint() {
        return tunnelEndpoint;
    }

    @Override
    public String toString() {
        return "OdpsConf {" +
                "accessId='" + accessId + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", project='" + project + '\'' +
                ", tunnelEndpoint='" + tunnelEndpoint + '\'' +
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
                && tunnelEndpoint.equals(that.tunnelEndpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                project,
                accessId,
                accessKey,
                endpoint,
                tunnelEndpoint);
    }
}
