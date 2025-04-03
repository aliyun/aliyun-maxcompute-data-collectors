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
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
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
import org.junit.Ignore;
import org.junit.Test;

import java.util.Optional;

@Ignore("MaxCompute Emulator not support partition table yet.")
public class ReadPartitionedTableTest
{

    private static final String TEST_TABLE = "PRESTO_TEST_PARTITIONED_TABLE";

    @Test
    public void testReadNonPartitionedTable()
            throws Exception
    {
        DistributedQueryRunner queryRunner = MaxComputeQueryRunner.createMaxComputeQueryRunner(Optional.of("10086"), "http://127.0.0.1:8080", "ak", "sk", "emulator");

        prepareTestTable();
        MaterializedResult result = queryRunner.execute("select * from " + TEST_TABLE + " where id = 1 and ds = '20230101'");
        int rowCount = result.getRowCount();
        Assert.assertEquals(1, rowCount);

        result = queryRunner.execute("select * from " + TEST_TABLE + " where ds = '20230101'");
        rowCount = result.getRowCount();
        Assert.assertEquals(100, rowCount);

        result = queryRunner.execute("select * from " + TEST_TABLE + " where ds > '20230100' and ds < '20230102'");
        rowCount = result.getRowCount();
        Assert.assertEquals(100, rowCount);

        result = queryRunner.execute("select * from " + TEST_TABLE + " where id = 1 and ds > '20230103' and ds < '20230105'");
        rowCount = result.getRowCount();
        Assert.assertEquals(0, rowCount);
    }

    void prepareTestTable()
            throws Exception
    {
        Account account = new AliyunAccount("ak", "sk");
        Odps odps = new Odps(account);
        odps.setEndpoint("http://127.0.0.1:8080");
        odps.setDefaultProject("emulator");

        if (odps.tables().exists(TEST_TABLE)) {
            odps.tables().delete(TEST_TABLE);
        }
        odps.tables().newTableCreator("emulator", TEST_TABLE,
                        TableSchema.builder().withColumn(Column.newBuilder("id", TypeInfoFactory.BIGINT).notNull().build())
                                .withStringColumn("name").withPartitionColumn(Column.newBuilder("ds", TypeInfoFactory.STRING).build()).build())
                .withPrimaryKeys(ImmutableList.of("id"))
                .ifNotExists().transactionTable().create();
        Table table = odps.tables().get("odps_test_tunnel_project_orc", TEST_TABLE);
        table.createPartition(new PartitionSpec("ds='20230101'"));
        table.createPartition(new PartitionSpec("ds='20230102'"));
        table.createPartition(new PartitionSpec("ds='20230103'"));
        table.createPartition(new PartitionSpec("ds='20230104'"));
        table.createPartition(new PartitionSpec("ds='20230105'"));

        TableTunnel.UpsertSession session = odps.tableTunnel().buildUpsertSession("odps_test_tunnel_project_orc", TEST_TABLE).
                setPartitionSpec(new PartitionSpec("ds='20230101'")).build();
        UpsertStream stream = session.buildUpsertStream().setCompressOption(new CompressOption(
                CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0)).build();

        Record record = session.newRecord();
        for (long i = 0; i < 100; i++) {
            record.set(0, i);
            record.set(1, "name" + i);
            stream.upsert(record);
        }
        stream.close();
        session.commit(false);
    }
}
