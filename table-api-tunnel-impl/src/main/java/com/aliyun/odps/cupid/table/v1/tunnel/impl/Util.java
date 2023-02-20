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

package com.aliyun.odps.cupid.table.v1.tunnel.impl;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.utils.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class Util {
    public static final String WRITER_STREAM_ENABLE = "odps.cupid.writer.stream.enable";
    public static final String WRITER_COMPRESS_ENABLE = "odps.cupid.writer.compress.enable";
    public static final String WRITER_BUFFER_ENABLE = "odps.cupid.writer.buffer.enable";
    public static final String WRITER_BUFFER_SHARES = "odps.cupid.writer.buffer.shares";
    public static final String WRITER_BUFFER_SIZE = "odps.cupid.writer.buffer.size";
    public static final int DEFAULT_WRITER_BUFFER_SIZE = 67108864;

    public static final String UPSERT_ENABLE = "odps.upsert.mode.enable";

    public static PartitionSpec toOdpsPartitionSpec(Map<String, String> partitionSpec) {
        if (partitionSpec == null || partitionSpec.isEmpty()) {
            return new PartitionSpec();
        }
        PartitionSpec odpsPartitionSpec = new PartitionSpec();
        for (String key : partitionSpec.keySet()) {
            odpsPartitionSpec.set(key, partitionSpec.get(key));
        }
        return odpsPartitionSpec;
    }

    public static Map<String, String> fromOdpsPartitionSpec(PartitionSpec odpsPartitionSpec) {
        Map<String, String> partitionSpec = new HashMap<>();
        for (String key : odpsPartitionSpec.keys()) {
            partitionSpec.put(key, odpsPartitionSpec.get(key));
        }

        return partitionSpec;
    }

    public static TableTunnel getTableTunnel(Options options) {
        TableTunnel tunnel = new TableTunnel(getOdps(options));
        if (!StringUtils.isNullOrEmpty(options.getOdpsConf().getTunnelEndpoint())) {
            tunnel.setEndpoint(options.getOdpsConf().getTunnelEndpoint());
        }
        return tunnel;
    }

    public static Odps getOdps(Options options) {
        Odps odps = new Odps(getDefaultAccount(options));
        odps.setEndpoint(options.getOdpsConf().getEndpoint());
        odps.setDefaultProject(options.getOdpsConf().getProject());
        return odps;
    }

    private static Account getDefaultAccount(Options options) {
        if (options.contains("odps.access.security.token")
                && StringUtils.isNotBlank(options.get("odps.access.security.token"))) {
            return new StsAccount(options.getOdpsConf().getAccessId(),
                    options.getOdpsConf().getAccessKey(),
                    options.get("odps.access.security.token"));
        } else {
            return new AliyunAccount(options.getOdpsConf().getAccessId(),
                    options.getOdpsConf().getAccessKey());
        }
    }

    public static void createPartition(
            String project,
            String table,
            PartitionSpec partitionSpec,
            Odps odps) throws IOException {
        int retry = 0;
        long sleep = 2000;
        while (true) {
            try {
                odps.tables().get(project, table).createPartition(partitionSpec, true);
                break;
            } catch (OdpsException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                sleep = sleep * 2;
            }
        }
    }

    public static TableTunnel.DownloadSession createDownloadSession(String project,
                                                                    String table,
                                                                    PartitionSpec partitionSpec,
                                                                    TableTunnel tunnel) throws IOException {
        int retry = 0;
        long sleep = 2000;
        TableTunnel.DownloadSession downloadSession;
        while (true) {
            try {
                if (partitionSpec == null || partitionSpec.isEmpty()) {
                    downloadSession = tunnel.createDownloadSession(project,
                            table);
                } else {
                    downloadSession = tunnel.createDownloadSession(project,
                            table,
                            partitionSpec);
                }
                break;
            } catch (TunnelException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                sleep = sleep * 2;
            }
        }
        return downloadSession;
    }

    public static TableTunnel.UploadSession createUploadSession(String project,
                                                                String table,
                                                                PartitionSpec partitionSpec,
                                                                boolean isOverwrite,
                                                                TableTunnel tunnel) throws IOException {
        int retry = 0;
        long sleep = 2000;
        TableTunnel.UploadSession uploadSession;
        while (true) {
            try {
                if (partitionSpec == null || partitionSpec.isEmpty()) {
                    uploadSession = tunnel.createUploadSession(project,
                            table,
                            isOverwrite);
                } else {
                    uploadSession = tunnel.createUploadSession(project,
                            table,
                            partitionSpec,
                            isOverwrite);
                }
                break;
            } catch (TunnelException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                sleep = sleep * 2;
            }
        }
        return uploadSession;
    }

    public static TableTunnel.StreamUploadSession createStreamUploadSession(String project,
                                                                            String table,
                                                                            PartitionSpec partitionSpec,
                                                                            boolean createParitition,
                                                                            TableTunnel tunnel) throws IOException {
        int retry = 0;
        long sleep = 2000;
        TableTunnel.StreamUploadSession uploadSession;
        while (true) {
            try {
                if (partitionSpec == null || partitionSpec.isEmpty()) {
                    // For old sdk version
                    // uploadSession = tunnel.createStreamUploadSession(project, table);
                    uploadSession = tunnel.buildStreamUploadSession(project, table).build();
                } else {
                    // For old sdk version
                    // uploadSession = tunnel.createStreamUploadSession(project,
                    //        table,
                    //        partitionSpec,
                    //        createParitition);
                    uploadSession = tunnel.buildStreamUploadSession(project, table)
                            .setCreatePartition(createParitition)
                            .setPartitionSpec(partitionSpec).build();
                }
                break;
            } catch (TunnelException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                sleep = sleep * 2;
            }
        }
        return uploadSession;
    }

    public static TableTunnel.UpsertSession getOrCreateUpsertSession(String project,
                                                                String table,
                                                                PartitionSpec partitionSpec,
                                                                TableTunnel tunnel,
                                                                String upsertId) throws IOException {
        int retry = 0;
        long sleep = 2000;
        TableTunnel.UpsertSession upsertSession;
        while (true) {
            try {
                if (partitionSpec == null || partitionSpec.isEmpty()) {
                    upsertSession = tunnel.buildUpsertSession(project, table)
                            .setUpsertId(upsertId).build();
                } else {
                    upsertSession = tunnel.buildUpsertSession(project, table)
                            .setPartitionSpec(partitionSpec)
                            .setUpsertId(upsertId)
                            .build();
                }
                break;
            } catch (TunnelException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                sleep = sleep * 2;
            }
        }
        return upsertSession;
    }
}
