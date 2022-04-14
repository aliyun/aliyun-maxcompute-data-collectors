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

package org.apache.flink.odps.test.catalog;

import org.apache.flink.odps.catalog.OdpsCatalog;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.Constants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BinaryType;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test for data type mappings in OdpsCatalog.
 */
public class OdpsCatalogDataTypeTest {

    private static OdpsCatalog catalog;
    private static final String t1 = "t1";
    private static ObjectPath path1;
    private static final String DDL_FORMAT = "CREATE TABLE %s (%s)";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void init() {
        catalog = OdpsCatalogUtils.createOdpsCatalog();
        path1 = new ObjectPath(catalog.getDefaultDatabase(), t1);
        catalog.open();
    }

    @After
    public void cleanup() throws Exception {
        if (catalog.tableExists(path1)) {
            catalog.dropTable(path1, true);
        }
    }

    @AfterClass
    public static void closeup() {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    public void testDataTypes() throws Exception {
        DataType[] types = new DataType[]{
                DataTypes.TINYINT(),
                DataTypes.SMALLINT(),
                DataTypes.INT(),
                DataTypes.BIGINT(),
                DataTypes.FLOAT(),
                DataTypes.DOUBLE(),
                DataTypes.BOOLEAN(),
                DataTypes.STRING(),
                // binary
                DataTypes.BYTES(),
                DataTypes.DATE(),
                // datetime
                DataTypes.TIMESTAMP(3),
                // timestamp
                DataTypes.TIMESTAMP(9),
                DataTypes.CHAR(Constants.MAX_CHAR_LENGTH),
                DataTypes.VARCHAR(Constants.MAX_VARCHAR_LENGTH),
                DataTypes.DECIMAL(5, 3)
        };

        verifyDataTypes(types);
    }

    @Test
    public void testNonSupportedBinaryDataTypes() throws Exception {
        DataType[] types = new DataType[]{
                DataTypes.BINARY(BinaryType.MAX_LENGTH)
        };

        CatalogTable table = createCatalogTable(types);
        exception.expect(UnsupportedOperationException.class);
        catalog.createTable(path1, table, false);
    }

    @Test
    public void testNonSupportedVarBinaryDataTypes() throws Exception {
        DataType[] types = new DataType[]{
                DataTypes.VARBINARY(20)
        };

        CatalogTable table = createCatalogTable(types);
        exception.expect(UnsupportedOperationException.class);
        catalog.createTable(path1, table, false);
    }

    @Test
    public void testCharTypeLength() throws Exception {
        DataType[] types = new DataType[]{
                DataTypes.CHAR(Constants.MAX_CHAR_LENGTH + 1)
        };

        exception.expect(CatalogException.class);
        verifyDataTypes(types);
    }

    @Test
    public void testVarCharTypeLength() throws Exception {
        DataType[] types = new DataType[]{
                DataTypes.VARCHAR(Constants.MAX_VARCHAR_LENGTH + 1)
        };

        exception.expect(CatalogException.class);
        verifyDataTypes(types);
    }

    @Test
    public void testComplexDataTypes() throws Exception {
        DataType[] types = new DataType[]{
                DataTypes.ARRAY(DataTypes.DOUBLE()),
                DataTypes.MAP(DataTypes.FLOAT(), DataTypes.BIGINT()),
                DataTypes.ROW(
                        DataTypes.FIELD("col0", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("col1", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("col2", DataTypes.DATE())),

                // nested complex types
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                DataTypes.MAP(DataTypes.STRING(), DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                DataTypes.ROW(
                        DataTypes.FIELD("col3", DataTypes.ARRAY(DataTypes.DECIMAL(5, 3))),
                        DataTypes.FIELD("col4", DataTypes.MAP(DataTypes.TINYINT(), DataTypes.SMALLINT())),
                        DataTypes.FIELD("col5", DataTypes.ROW(DataTypes.FIELD("col6", DataTypes.TIMESTAMP(9))))
                )
        };
        verifyDataTypes(types);
    }

    private CatalogTable createCatalogTable(DataType[] types) {
        String[] colNames = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            colNames[i] = String.format("column%d", i);
        }
        TableSchema schema = TableSchema.builder()
                .fields(colNames, types)
                .build();
        return new CatalogTableImpl(
                schema,
                new HashMap<>(),
                "wuji_test"
        );
    }

    private void verifyDataTypes(DataType[] types) throws Exception {
        CatalogTable table = createCatalogTable(types);
        if (catalog.tableExists(path1)) {
            catalog.dropTable(path1, false);
        }
        catalog.createTable(path1, table, false);
        assertEquals(table.getSchema(), catalog.getTable(path1).getSchema());
    }

    @Test
    public void testDataTypesUseSql() throws Exception {
        if (catalog.tableExists(path1)) {
            catalog.dropTable(path1, false);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = OdpsCatalogUtils.createTableEnvInStreamingMode(env);
        tEnv.registerCatalog(OdpsTestUtils.catalogName, catalog);
        tEnv.useCatalog(OdpsTestUtils.catalogName);
        String sqlDDL = String.format(DDL_FORMAT, t1, createTestNormalColumnForBlink());
        tEnv.executeSql(sqlDDL);
        assertEquals(createNormalTableSchemaForBlink(), catalog.getTable(path1).getSchema());
    }

    @Test
    public void testComplexDataTypesUseSql() throws Exception {
        if (catalog.tableExists(path1)) {
            catalog.dropTable(path1, false);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = OdpsCatalogUtils.createTableEnvInStreamingMode(env);
        tEnv.registerCatalog(OdpsTestUtils.catalogName, catalog);
        tEnv.useCatalog(OdpsTestUtils.catalogName);
        String sqlDDL = String.format(DDL_FORMAT, t1, creatComplexColumnForBlink());
        tEnv.executeSql(sqlDDL);
        assertEquals(creatComplexTableSchema(), catalog.getTable(path1).getSchema());
    }

    private String createTestNormalColumnForBlink() {
        List<TestColumn> testColumns = Arrays.asList(
                createTestColumn("col1", "CHAR(1)"),
                createTestColumn("col2", "VARCHAR(100)"),
                createTestColumn("col3", "BOOLEAN"),
                createTestColumn("col4", "TINYINT"),
                createTestColumn("col5", "SMALLINT"),
                createTestColumn("col6", "INT"),
                createTestColumn("col7", "BIGINT"),
                createTestColumn("col8", "FLOAT"),
                createTestColumn("col9", "DOUBLE"),
                createTestColumn("col10", "DECIMAL(10, 4)"),
                createTestColumn("col11", "DATE"),
                createTestColumn("col12", "TIMESTAMP(3)"),
                createTestColumn("col13", "TIMESTAMP(9)"),
                createTestColumn("col15", "BYTES"),
                createTestColumn("col16", "STRING")
        );
        return testColumns.stream().map(TestColumn::toString).collect(Collectors.joining(","));
    }

    private TableSchema createNormalTableSchemaForBlink() {
        TableSchema schema = TableSchema.builder()
                .field("col1", DataTypes.CHAR(1))
                .field("col2", DataTypes.VARCHAR(100))
                .field("col3", DataTypes.BOOLEAN())
                .field("col4", DataTypes.TINYINT())
                .field("col5", DataTypes.SMALLINT())
                .field("col6", DataTypes.INT())
                .field("col7", DataTypes.BIGINT())
                .field("col8", DataTypes.FLOAT())
                .field("col9", DataTypes.DOUBLE())
                .field("col10", DataTypes.DECIMAL(10, 4))
                .field("col11", DataTypes.DATE())
                .field("col12", DataTypes.TIMESTAMP(3))
                .field("col13", DataTypes.TIMESTAMP(9))
                .field("col15", DataTypes.BYTES())
                .field("col16", DataTypes.STRING())
                .build();
        return schema;
    }

    private String creatComplexColumnForBlink() {
        List<TestColumn> testColumns = Arrays.asList(
                createTestColumn("col1", "array<double>"),
                createTestColumn("col2", "map<string,bigint>"),
                createTestColumn("col3", "row<col0 boolean, col1 boolean, col2 date, col3 array<double>>"),
                createTestColumn("col4", "array<array<int>>")

        );
        return testColumns.stream().map(TestColumn::toString).collect(Collectors.joining(","));
    }

    private TableSchema creatComplexTableSchema() {
        TableSchema schema = TableSchema.builder()
                .field("col1", DataTypes.ARRAY(DataTypes.DOUBLE()))
                .field("col2", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
                .field("col3", DataTypes.ROW(
                        DataTypes.FIELD("col0", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("col1", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("col2", DataTypes.DATE()),
                        DataTypes.FIELD("col3", DataTypes.ARRAY(DataTypes.DOUBLE()))))
                .field("col4", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))
                .build();
        return schema;
    }

    private static TestColumn createTestColumn(Object... args) {
        return new TestColumn((String) args[0], (String) args[1]);
    }

    private static class TestColumn {
        private final String name;
        private final String type;

        private TestColumn(String name, String type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public String toString() {
            return String.format("%s %s", name, type);
        }
    }
}
