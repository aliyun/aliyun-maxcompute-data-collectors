package org.apache.flink.odps.test.sink;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.sink.partition.RecordKeySelector;
import org.apache.flink.odps.sink.upsert.UpsertOperatorFactory;
import org.apache.flink.odps.sink.utils.Pipelines;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.odps.test.table.OdpsUpsertSinkFunctionTest.TEST_DATA;
import static org.apache.flink.odps.test.util.BookTableUtils.getOdpsCreatePartitionQueryWithPK;
import static org.apache.flink.odps.test.util.BookTableUtils.getOdpsCreateQueryWithPK;
import static org.apache.flink.odps.test.util.OdpsTestUtils.dropOdpsTable;

public class UpsertOperatorTest {

    private static OdpsConf odpsConf;

    private static String project;
    private static String table1;
    private static String partitionTable1;

    @BeforeClass
    public static void init() {
        odpsConf = OdpsTestUtils.getOdpsConf();
        project = odpsConf.getProject();
        table1 = "book_entry1";
        partitionTable1 = "book_entry_partition1";
    }

    public void initTable(String table) {
        OdpsTestUtils.exec(dropOdpsTable(table));
        OdpsTestUtils.exec(getOdpsCreateQueryWithPK(table));
    }

    public void initPartTable(String partTable) {
        OdpsTestUtils.exec(dropOdpsTable(partTable));
        OdpsTestUtils.exec(getOdpsCreatePartitionQueryWithPK(partTable));
    }

    @Test
    public void testUpsertSink() throws Exception {
        initTable(table1);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        DataStream<RowData> input = env.fromElements(TEST_DATA);
        Configuration conf = new Configuration();
        Odps odps = OdpsUtils.getOdps(odpsConf);
        Table odpsTable = odps.tables().get(project, table1);
        TableSchema tableSchema = odpsTable.getSchema();
        List<String> primaryKeys = odpsTable.getPrimaryKey();

        UpsertOperatorFactory operatorFactory = createOperatorFactory(odpsConf, project, table1, "", tableSchema, conf);
        RecordKeySelector keySelector = new RecordKeySelector(tableSchema, primaryKeys);
        input.keyBy(keySelector).transform(Pipelines.opName("stream_write", table1), TypeInformation.of(Object.class), operatorFactory)
                .uid(Pipelines.opUID("stream_write", table1))
                .setParallelism(4);
        Pipelines.dummySink(input);
        env.execute();
    }

    @Test
    public void testUpsertPartitionSink() throws Exception {
        initPartTable(partitionTable1);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        DataStream<RowData> input = env.fromElements(TEST_DATA);
        Configuration conf = new Configuration();
        Odps odps = OdpsUtils.getOdps(odpsConf);
        Table odpsTable = odps.tables().get(project, partitionTable1);
        TableSchema tableSchema = odpsTable.getSchema();
        List<String> primaryKeys = odpsTable.getPrimaryKey();

        UpsertOperatorFactory operatorFactory = createOperatorFactory(odpsConf, project, partitionTable1, "date=20220228", tableSchema, conf);
        RecordKeySelector keySelector = new RecordKeySelector(tableSchema, primaryKeys);
        input.keyBy(keySelector).transform(Pipelines.opName("stream_write", partitionTable1), TypeInformation.of(Object.class), operatorFactory)
                .uid(Pipelines.opUID("stream_write", partitionTable1))
                .setParallelism(4);
        Pipelines.dummySink(input);
        env.execute();
    }

    private UpsertOperatorFactory createOperatorFactory(OdpsConf odpsConf,
                                                        String project,
                                                        String tableName,
                                                        String partition,
                                                        TableSchema tableSchema,
                                                        Configuration config) {
        UpsertOperatorFactory.OdpsUpsertOperatorFactoryBuilder builder =
                new UpsertOperatorFactory.OdpsUpsertOperatorFactoryBuilder(project, tableName);
        builder.setOdpsConf(odpsConf);
        builder.setConf(config);
        builder.setPartition(partition);
        builder.setTableSchema(tableSchema);
        builder.setWriteOptions(OdpsWriteOptions.builder().build());
        return builder.build();
    }
}
