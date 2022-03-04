package org.apache.flink.odps.test.catalog;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.odps.catalog.OdpsCatalog;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.test.util.CollectionTableSource;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OdpsCatalogITCase {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static OdpsCatalog odpsCatalog;

    private final String sourceTableName = "flink_source";
    private final String sinkTableName = "flink_sink";

    private final String t1 = "t1";
    private final String t2 = "t2";
    private final String t3 = "t3";
    private final String t4 = "t4";
    private final String t5 = "t5";
    private final String t6 = "t6";

    @BeforeClass
    public static void createCatalog() {
        odpsCatalog = OdpsCatalogUtils.createOdpsCatalog();
        odpsCatalog.open();
    }

    @AfterClass
    public static void closeCatalog() {
        if (odpsCatalog != null) {
            odpsCatalog.close();
        }
    }

    @Test
    public void testCreateCatalogUseSql() throws Exception {
        String odpsConfDir = Thread.currentThread()
                .getContextClassLoader()
                .getResource(OdpsTestUtils.defaultOdpsConfResource)
                .getPath();
        TableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        tableEnv.executeSql("CREATE CATALOG myodps WITH( 'type' = 'odps', 'odps-conf-dir' = '" + odpsConfDir + "')");
        tableEnv.executeSql("use catalog myodps");
        Catalog catalog =  tableEnv.getCatalog("myodps").get();
        OdpsConf odpsConf = OdpsUtils.getOdpsConf(odpsConfDir);
        assertEquals(odpsConf.getProject(), catalog.getDefaultDatabase());
    }

    @Test
    public void testTableViaAPI() throws Exception {
        TableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        tableEnv.registerCatalog(odpsCatalog.getName(), odpsCatalog);
        tableEnv.useCatalog(odpsCatalog.getName());

        TableSchema schema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .build();

        tableEnv.executeSql(String.format("DROP TABLE IF EXISTS %s", sourceTableName));
        tableEnv.executeSql(String.format("DROP TABLE IF EXISTS %s", sinkTableName));

        CatalogTable source = OdpsCatalogUtils.createCatalogTable(schema, 0);
        CatalogTable sink = OdpsCatalogUtils.createCatalogTable(schema, 0);

        odpsCatalog.createTable(
                new ObjectPath(odpsCatalog.getDefaultDatabase(), sourceTableName),
                source,
                true
        );

        odpsCatalog.createTable(
                new ObjectPath(odpsCatalog.getDefaultDatabase(), sinkTableName),
                sink,
                true
        );

        tableEnv.executeSql("insert into " + sourceTableName + " values ('1', 1),('2',2),('3',3)").await();
        tableEnv.executeSql(String.format("insert into %s select * from %s",
                sinkTableName,
                sourceTableName)).await();

        Table t = tableEnv.sqlQuery(
                String.format("select * from %s", sinkTableName));

        List<Row> result = Lists.newArrayList(t.execute().collect());
        result.sort(Comparator.comparing(String::valueOf));

        // assert query result
        assertEquals(
                Arrays.asList(
                        Row.of("1", 1),
                        Row.of("2", 2),
                        Row.of("3", 3)),
                result
        );

        tableEnv.executeSql(String.format("DROP TABLE %s", sourceTableName));
        tableEnv.executeSql(String.format("DROP TABLE %s", sinkTableName));
    }

    @Test
    public void testTableViaSql() throws InterruptedException, ExecutionException {
        TableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        tableEnv.registerCatalog(odpsCatalog.getName(), odpsCatalog);
        tableEnv.useCatalog(odpsCatalog.getName());
        tableEnv.executeSql("Drop table IF EXISTS " + t1);
        tableEnv.executeSql("Drop table IF EXISTS " + t2);

        tableEnv.executeSql("create table " + t1 + " (name String, age Int) with ('connector' = 'odps')");
        tableEnv.executeSql("insert into " + t1 + " values ('1', 1),('2',2),('3',3)").await();

        Table t = tableEnv.sqlQuery("SELECT * FROM " + t1);

        List<Row> result = Lists.newArrayList(t.execute().collect());

        assertEquals(
                new HashSet<>(Arrays.asList(
                        Row.of("1", 1),
                        Row.of("2", 2),
                        Row.of("3", 3))),
                new HashSet<>(result)
        );

        tableEnv.executeSql("ALTER TABLE " + t1 + " RENAME TO " + t2);

        t = tableEnv.sqlQuery("SELECT * FROM " + t2);

        result = Lists.newArrayList(t.execute().collect());

        // assert query result
        assertEquals(
                new HashSet<>(Arrays.asList(
                        Row.of("1", 1),
                        Row.of("2", 2),
                        Row.of("3", 3))),
                new HashSet<>(result)
        );

        tableEnv.executeSql(String.format("DROP TABLE %s", t2));
    }

    @Test
    public void testComplexDataTypeViaSql() throws Exception {
        TableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        tableEnv.registerCatalog(OdpsTestUtils.catalogName, odpsCatalog);
        tableEnv.useCatalog(OdpsTestUtils.catalogName);
        tableEnv.executeSql("Drop table IF EXISTS " + t3);
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.fields(new String[]{"a", "m", "s"}, new DataType[]{
                DataTypes.ARRAY(DataTypes.INT()),
                DataTypes.MAP(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.FIELD("f2", DataTypes.MAP(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT()))))});

        RowTypeInfo rowTypeInfo = createOdpsDestTable(OdpsTestUtils.projectName, t3, builder.build(), 0);
        List<Row> toWrite = new ArrayList<>();
        Row row = new Row(rowTypeInfo.getArity());
        Object[] array = new Object[]{1, 2, 3};
        Map<Integer, Object[]> map = new HashMap<Integer, Object[]>() {{
            put(1, array);
            put(2, array);
        }};
        Row struct = new Row(2);
        struct.setField(0, array);
        struct.setField(1, map);

        row.setField(0, array);
        row.setField(1, map);
        row.setField(2, struct);
        toWrite.add(row);

        Table src = tableEnv.fromTableSource(new CollectionTableSource(toWrite, rowTypeInfo, builder.build()));
        tableEnv.registerTable("complexSrc", src);

        tableEnv.executeSql("insert into " + t3 + " select * from complexSrc").await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select * from " + t3)
                                .execute()
                                .collect());
        assertEquals(
                "[+I[[1, 2, 3], {1=[1, 2, 3], 2=[1, 2, 3]}, +I[[1, 2, 3], {1=[1, 2, 3], 2=[1, 2, 3]}]]]",
                results.toString());

        tableEnv.executeSql(String.format("DROP TABLE %s", t3));
    }


    @Test
    public void testWriteNestedComplexType() throws Exception {
        TableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        tableEnv.registerCatalog(OdpsTestUtils.catalogName, odpsCatalog);
        tableEnv.useCatalog(OdpsTestUtils.catalogName);
        tableEnv.executeSql("Drop table IF EXISTS " + t4);

        TableSchema.Builder builder = new TableSchema.Builder();
        builder.fields(new String[]{"a"}, new DataType[]{DataTypes.ARRAY(
                DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.INT()), DataTypes.FIELD("f2", DataTypes.STRING())))});
        RowTypeInfo rowTypeInfo = createOdpsDestTable(OdpsTestUtils.projectName, t4, builder.build(), 0);

        Row row = new Row(rowTypeInfo.getArity());
        Object[] array = new Object[3];
        row.setField(0, array);
        for (int i = 0; i < array.length; i++) {
            Row struct = new Row(2);
            struct.setField(0, 1 + i);
            struct.setField(1, String.valueOf((char) ('a' + i)));
            array[i] = struct;
        }
        List<Row> toWrite = new ArrayList<>();
        toWrite.add(row);

        Table src = tableEnv.fromTableSource(new CollectionTableSource(toWrite, rowTypeInfo, builder.build()));
        tableEnv.registerTable("nestedSrc", src);
        tableEnv.executeSql("insert into " + t4 + " select * from nestedSrc").await();
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select * from " + t4)
                                .execute()
                                .collect());
        assertEquals(
                "[+I[[+I[1, a], +I[2, b], +I[3, c]]]]",
                results.toString());
        tableEnv.executeSql(String.format("DROP TABLE %s", t4));
    }

    @Test
    public void testWriteNullValues() throws Exception {
        TableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        tableEnv.registerCatalog(OdpsTestUtils.catalogName, odpsCatalog);
        tableEnv.useCatalog(OdpsTestUtils.catalogName);
        tableEnv.executeSql("Drop table IF EXISTS " + t5);

        TableSchema.Builder builder = new TableSchema.Builder();
        builder.fields(new String[]{"t","s","i","b","f","d","de","ts","dt","date","str","ch","vch","bl","bin","arr","mp","strt"},
                new DataType[]{
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DECIMAL(10, 5),
                        DataTypes.TIMESTAMP(9),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.DATE(),
                        DataTypes.STRING(),
                        DataTypes.CHAR(10),
                        DataTypes.VARCHAR(5),
                        DataTypes.BOOLEAN(),
                        DataTypes.BYTES(),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                        DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.INT()), DataTypes.FIELD("f2", DataTypes.STRING())),
                });
        RowTypeInfo rowTypeInfo = createOdpsDestTable(OdpsTestUtils.projectName, t5, builder.build(), 0);
        Row row = new Row(rowTypeInfo.getArity());
        for (int i = 0; i < rowTypeInfo.getArity(); i++)
            row.setField(i, null);

        List<Row> toWrite = new ArrayList<>();
        toWrite.add(row);

        Table src = tableEnv.fromTableSource(new CollectionTableSource(toWrite, rowTypeInfo, builder.build()));
        tableEnv.registerTable("nestedSrc", src);
        tableEnv.executeSql("insert into " + t5 + " select * from nestedSrc").await();
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select * from " + t5)
                                .execute()
                                .collect());
        assertEquals(
                "[+I[null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]]",
                results.toString());
        tableEnv.executeSql(String.format("DROP TABLE %s", t5));
    }

    @Test
    public void testPrimitiveType() throws Exception {
        TableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        tableEnv.registerCatalog(OdpsTestUtils.catalogName, odpsCatalog);
        tableEnv.useCatalog(OdpsTestUtils.catalogName);
        tableEnv.executeSql("Drop table IF EXISTS " + t6);

        TableSchema schema = TableSchema.builder()
                .field("d_int", DataTypes.INT().notNull())
                .field("d_tinyint", DataTypes.TINYINT())
                .field("d_short", DataTypes.SMALLINT().notNull())
                .field("d_long", DataTypes.BIGINT())
                .field("d_real", DataTypes.FLOAT())
                .field("d_double_precision", DataTypes.DOUBLE())
                .field("d_decimal", DataTypes.DECIMAL(10, 5))
                .field("d_boolean", DataTypes.BOOLEAN())
                .field("d_string", DataTypes.STRING())
                .field("d_bytes", DataTypes.BYTES())
                .field("d_char", DataTypes.CHAR(3))
                .field("d_vchar", DataTypes.VARCHAR(20))
                .field("d_timestamp", DataTypes.TIMESTAMP(9))
                .field("d_date", DataTypes.DATE())
                .field("d_datatime", DataTypes.TIMESTAMP(3))
                .build();

        RowTypeInfo rowTypeInfo = createOdpsDestTable(OdpsTestUtils.projectName, t6, schema, 0);
        Row row = new Row(rowTypeInfo.getArity());
        row.setField(0, 1);
        row.setField(1, (byte) 2);
        row.setField(2, (short)3);
        row.setField(3, 4L);
        row.setField(4, (float)5.5);
        row.setField(5, (double)6.6);
        row.setField(6, new BigDecimal("7.70000"));
        row.setField(7, true);
        row.setField(8, "a");
        row.setField(9, new byte[]{'2'});
        row.setField(10, "b");
        row.setField(11, "c");
        Timestamp timestamp = Timestamp.valueOf("2016-06-22 19:10:25.123456");
        Timestamp datetime = Timestamp.valueOf("2017-11-11 00:00:00.123");
        LocalDate date = LocalDate.of(2015,10,10);
        row.setField(12, LocalDateTime.ofInstant(timestamp.toInstant(), ZoneId.systemDefault()));
        row.setField(13, date);
        row.setField(14, LocalDateTime.ofInstant(datetime.toInstant(), ZoneId.systemDefault()));

        List<Row> toWrite = new ArrayList<>();
        toWrite.add(row);

        Table src = tableEnv.fromTableSource(new CollectionTableSource(toWrite, rowTypeInfo, schema));
        tableEnv.registerTable("nestedSrc", src);
        tableEnv.executeSql("insert into " + t6 + " select * from nestedSrc").await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery(String.format("select * from `%s`", t6))
                                .execute()
                                .collect());
        assertEquals(
                "[+I[1, 2, 3, 4, 5.5, 6.6, 7.70000, true, a, [50], b  , c, 2016-06-22T19:10:25.123456, 2015-10-10, 2017-11-11T00:00:00.123]]",
                results.toString());

        tableEnv.executeSql(String.format("DROP TABLE %s", t6));
    }


    @Test
    public void testCreateTableLike() throws Exception {
        TableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        tableEnv.registerCatalog(odpsCatalog.getName(), odpsCatalog);

        tableEnv.executeSql(
                String.format(
                        " create table if not exists `%s`.`%s`.generic_table (x int) with ('connector'='odps')",
                        odpsCatalog.getName(), odpsCatalog.getDefaultDatabase())
               );

        tableEnv.useCatalog(EnvironmentSettings.DEFAULT_BUILTIN_CATALOG);
        tableEnv.executeSql(
                String.format(
                        "create table copy like `%s`.`%s`.generic_table",
                        odpsCatalog.getName(), odpsCatalog.getDefaultDatabase()));

        Catalog builtInCat = tableEnv.getCatalog(EnvironmentSettings.DEFAULT_BUILTIN_CATALOG).get();
        CatalogBaseTable catalogTable =
                builtInCat.getTable(
                        new ObjectPath(EnvironmentSettings.DEFAULT_BUILTIN_DATABASE, "copy"));

        assertEquals("odps", catalogTable.getOptions().get(FactoryUtil.CONNECTOR.key()));
        assertEquals(String.format("%s.generic_table", odpsCatalog.getDefaultDatabase()),
                catalogTable.getOptions().get(OdpsOptions.TABLE_PATH.key()));
        assertEquals(1, catalogTable.getSchema().getFieldCount());
        assertEquals("x", catalogTable.getSchema().getFieldNames()[0]);
        assertEquals(DataTypes.INT(), catalogTable.getSchema().getFieldDataTypes()[0]);
    }

    @Test
    public void testViewSchema() throws Exception {
        TableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        tableEnv.registerCatalog(odpsCatalog.getName(), odpsCatalog);
        tableEnv.useCatalog(odpsCatalog.getName());
        tableEnv.useDatabase(odpsCatalog.getDefaultDatabase());
        tableEnv.executeSql(
                "create table if not exists flink_src(x int,ts timestamp(3)) with ('connector'='odps')");
        try {
            tableEnv.executeSql("create view v1 as select x,ts from flink_src order by x limit 3");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                            t,
                            "OdpsCatalog only supports CatalogTable")
                            .isPresent());
        }
    }

    private RowTypeInfo createOdpsDestTable(String dbName, String tblName, TableSchema tableSchema, int numPartCols)
            throws Exception {
        CatalogTable catalogTable = OdpsCatalogUtils.createCatalogTable(tableSchema, numPartCols);
        odpsCatalog.createTable(new ObjectPath(dbName, tblName), catalogTable, false);
        return new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
    }
}
