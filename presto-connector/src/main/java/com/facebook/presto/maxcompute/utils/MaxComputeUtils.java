/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.maxcompute.utils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.utils.StringUtils;
import com.facebook.presto.maxcompute.MaxComputeConfig;
import com.facebook.presto.maxcompute.MaxComputeErrorCode;
import com.facebook.presto.maxcompute.MaxComputeTableHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class MaxComputeUtils
{
    public static final Logger LOG = LoggerFactory.getLogger(MaxComputeUtils.class);
    public static final String SCHEMA_ENABLE_FLAG = "odps.schema.model.enabled";

    private MaxComputeUtils() {}

    public static Odps getOdps(MaxComputeConfig config)
    {
        if (!StringUtils.allNotNullOrEmpty(config.getAccessId(), config.getAccessKey(), config.getEndPoint(), config.getProject())) {
            throw new PrestoException(MaxComputeErrorCode.MAXCOMPUTE_USER_ERROR, "accessId, accessKey, endpoint and project cannot be null.");
        }
        AliyunAccount account = new AliyunAccount(config.getAccessId(), config.getAccessKey());
        Odps odps = new Odps(account);
        odps.setEndpoint(config.getEndPoint());
        odps.setDefaultProject(config.getProject());
        return odps;
    }

    public static EnvironmentSettings getEnvironmentSettings(MaxComputeConfig config)
    {
        Account account = new AliyunAccount(config.getAccessId(), config.getAccessKey());
        EnvironmentSettings.Builder builder = EnvironmentSettings.newBuilder().withServiceEndpoint(config.getEndPoint())
                .withCredentials(Credentials.newBuilder().withAccount(account).build());
        if (!StringUtils.isNullOrEmpty(config.getQuotaName())) {
            builder.withQuotaName(config.getQuotaName());
        }
        return builder.build();
    }

    public static boolean supportSchema(MaxComputeConfig config)
    {
        Odps odps = getOdps(config);
        try {
            boolean flag =
                    Boolean.parseBoolean(
                            odps.projects().get().getProperty(SCHEMA_ENABLE_FLAG));
            LOG.info("project {} is support schema: {}", config.getProject(), flag);
            return flag;
        }
        catch (Exception e) {
            LOG.warn("get project {} schema flag error, default not support schema model, error message: {}", config.getProject(), e.getMessage());
            return false;
        }
    }

    public static RuntimeException wrapOdpsException(OdpsException e)
    {
        return new PrestoException(MaxComputeErrorCode.MAXCOMPUTE_EXTERNAL_ERROR, e.getMessage() + ", RequestId: " + e.getRequestId(), e);
    }

    public static ConnectorTableMetadata getTableMetadata(Odps odps, MaxComputeTableHandle tableHandle)
    {
        Table table = odps.tables().get(odps.getDefaultProject(), tableHandle.getSchemaName(), tableHandle.getTableName());
        TableSchema schema = table.getSchema();
        List<ColumnMetadata> columnMetadata = schema.getAllColumns().stream().map(column -> new ColumnMetadata(column.getName(), TypeConvertUtils.toPrestoType(column.getTypeInfo()))).collect(Collectors.toList());
        return new ConnectorTableMetadata(new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()), columnMetadata);
    }
}
