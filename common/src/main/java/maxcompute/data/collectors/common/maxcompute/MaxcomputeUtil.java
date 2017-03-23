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

package maxcompute.data.collectors.common.maxcompute;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.odps.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MaxcomputeUtil {
    private static final Logger logger = LoggerFactory.getLogger(MaxcomputeUtil.class);

    public static TableSchema getTableSchema(Account account, String endpoint, String project,
        String table) {
        Odps odps = new Odps(account);
        odps.setEndpoint(endpoint);
        odps.setDefaultProject(project);
        return odps.tables().get(table).getSchema();
    }

    public static void dealTruncate(Odps odps, Table table, String partition, boolean truncate) {
        boolean isPartitionedTable = isPartitionedTable(table);

        if (truncate) {
            //需要 truncate
            if (isPartitionedTable) {
                //分区表
                if (StringUtils.isEmpty(partition)) {
                    throw new RuntimeException(String.format(
                        "没有配置分区信息，而配置的表是分区表:%s 如果需要进行 truncate 操作，必须指定需要清空的具体分区，格式形如 pt=xxxx.",
                        table.getName()));
                } else {
                    truncatePartition(table, partition);
                }
            } else {
                //非分区表
                if (!StringUtils.isEmpty(partition)) {
                    throw new RuntimeException(String
                        .format("分区信息配置错误，MaxCompute表是非分区表:%s 进行 truncate 操作时不需要指定具体分区值.",
                            table.getName()));
                } else {
                    truncateNonPartitionedTable(table);
                }
            }
        } else {
            //不需要 truncate
            if (isPartitionedTable) {
                //分区表
                if (StringUtils.isEmpty(partition)) {
                    throw new RuntimeException(String
                        .format("目的表是分区表，写入分区表:%s 时必须指定具体分区值. 格式形如 格式形如 pt=${bizdate}.",
                            table.getName()));
                } else {
                    boolean isPartitionExists = isPartitionExist(table, partition);
                    if (!isPartitionExists) {
                        addPart(table, partition);
                    }
                }
            } else {
                //非分区表
                if (!StringUtils.isEmpty(partition)) {
                    throw new RuntimeException(
                        String.format("目的表是非分区表，写入非分区表:%s 时不需要指定具体分区值.", table.getName()));
                }
            }
        }
    }

    public static boolean isPartitionedTable(Table table) {
        try {
            return table.isPartitioned();
        } catch (Exception e) {
            throw new RuntimeException(
                String.format("检查 MaxCompute 目的表:%s 是否为分区表失败.", table.getName()), e);
        }
    }

    private static boolean isPartitionExist(Table table, String partition) {
        try {
            return table.hasPartition(new PartitionSpec(partition));
        } catch (Exception e) {
            throw new RuntimeException(
                String.format("检测 MaxCompute 目的表:%s 分区是否存在失败.", table.getName()));
        }
    }

    public static void truncatePartition(Table table, String partition) {
        dropPart(table, partition);
        addPart(table, partition);
    }

    public static void truncateNonPartitionedTable(Table table) {
        try {
            table.truncate();
        } catch (Exception e) {
            throw new RuntimeException(String.format("清空 MaxCompute 目的表:%s 失败.", table.getName()),
                e);
        }
    }

    private static void dropPart(Table table, String partition) {
        try {
            table.deletePartition(new PartitionSpec(partition), true);
        } catch (Exception e) {
            throw new RuntimeException(String
                .format("Drop  MaxCompute 目的表分区失败. 错误发生在项目:%s 的表:%s 的分区:%s.", table.getProject(),
                    table.getName(), partition), e);
        }
    }

    public static void addPart(Table table, String partition) {
        try {
            table.createPartition(new PartitionSpec(partition), true);
        } catch (Exception e) {
            throw new RuntimeException(String
                .format("添加 MaxCompute 目的表的分区失败. 错误发生在添加 MaxCompute 的项目:%s 的表:%s 的分区:%s.",
                    table.getProject(), table.getName(), partition), e);
        }
    }
}
