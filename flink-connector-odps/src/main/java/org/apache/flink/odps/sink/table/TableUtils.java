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

package org.apache.flink.odps.sink.table;

import com.aliyun.odps.Odps;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.tunnel.TableTunnel;
import org.apache.flink.util.StringUtils;

public class TableUtils {

    public static TableTunnel getTableTunnel(EnvironmentSettings settings) {
        TableTunnel tunnel = new TableTunnel(getOdps(settings));
        if (settings.getTunnelEndpoint().isPresent()) {
            tunnel.setEndpoint(settings.getTunnelEndpoint().get());
        }
        return tunnel;
    }

    public static Odps getOdps(EnvironmentSettings settings) {
        return ExecutionEnvironment.create(settings).createOdpsClient();
    }


    public static EnvironmentSettings getEnvironmentSettings(Odps odps, String tunnelEndpoint) {
        Credentials credentials = Credentials.newBuilder()
                .withAccount(odps.getAccount())
                .withAppAccount(odps.getAppAccount())
                .withAppStsAccount(odps.getAppStsAccount())
                .build();
        EnvironmentSettings.Builder env = EnvironmentSettings.newBuilder()
                .inAutoMode()
                .withDefaultProject(odps.getDefaultProject())
                .withServiceEndpoint(odps.getEndpoint())
                .withCredentials(credentials);
        if (!StringUtils.isNullOrWhitespaceOnly(tunnelEndpoint)) {
            env.withTunnelEndpoint(tunnelEndpoint);
        }
        return env.build();
    }
}
