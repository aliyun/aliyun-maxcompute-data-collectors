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

package com.facebook.presto.maxcompute;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.type.TypeInfoFactory;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.shaded.com.google.errorprone.annotations.Immutable;

import java.util.Optional;

public class ReadNonPartitionedTableTest
{
    private static final String TEST_TABLE = "TEST_TABLE";

    @Test
    public void testReadNonPartitionedTable()
            throws Exception
    {
        DistributedQueryRunner queryRunner = MaxComputeQueryRunner.createMaxComputeEmulatorQueryRunner(Optional.of("8080"));

        prepareTestTable();
        MaterializedResult result = queryRunner.execute("select * from " + TEST_TABLE + " where id = 1");
        int rowCount = result.getRowCount();
        System.out.println(rowCount);
        Assert.assertEquals(10000, rowCount);
    }

    void prepareTestTable()
            throws Exception
    {
        Account account = new AliyunAccount("ak", "sk");
        Odps odps = new Odps(account);
        odps.setEndpoint(MaxComputeQueryRunner.MAXCOMPUTE_EMULATOR_ENDPOINT);
        odps.setDefaultProject("maxcompute_emulator");

        odps.tables().delete("maxcompute_emulator", TEST_TABLE);
        odps.tables().newTableCreator("maxcompute_emulator", TEST_TABLE,
                TableSchema.builder().withColumn(Column.newBuilder("id", TypeInfoFactory.BIGINT).notNull().build())
                        .withStringColumn("name").build()).ifNotExists().withPrimaryKeys(ImmutableList.of("id")).transactionTable().create();

        TableTunnel.UpsertSession session = odps.tableTunnel().buildUpsertSession("project", TEST_TABLE).build();
        UpsertStream stream = session.buildUpsertStream().setCompressOption(new CompressOption(
                CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0)).build();

        Record record = session.newRecord();
        for (long i = 0; i < 10000; i++) {
            record.set(0, i);
            record.set(1, "name" + i);
            stream.upsert(record);
        }
        stream.close();
        session.commit(false);
    }
}
