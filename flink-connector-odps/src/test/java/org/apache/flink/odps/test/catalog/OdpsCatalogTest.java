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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.util.ExceptionUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.*;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.junit.Assert.*;

/** Test for OdpsCatalog. */
public class OdpsCatalogTest {

    private final TableSchema schema =
            TableSchema.builder()
                    .field("name", DataTypes.STRING())
                    .field("age", DataTypes.INT())
                    .build();

    private final TableSchema partitionSchema =
            TableSchema.builder()
                    .field("name", DataTypes.STRING())
                    .field("age", DataTypes.INT())
                    .field("pt1", DataTypes.STRING().notNull())
                    .field("pt2", DataTypes.STRING().notNull())
                    .build();

    private static OdpsCatalog catalog;
    private static ObjectPath path1;
    private static ObjectPath path2;
    private static ObjectPath partPath1;

    public static String table1 = "OdpsCatalogTable1";
    public static String table2 = "OdpsCatalogTable2";
    public static String partTable1 = "OdpsCatalogPartTable1";
    public static List<String> partTableKey1 = Arrays.asList("pt1", "pt2");

    @Rule
    public ExpectedException exception = ExpectedException.none();


    @BeforeClass
    public static void createCatalog() {
        catalog = OdpsCatalogUtils.createOdpsCatalog();
        path1 = new ObjectPath(catalog.getDefaultDatabase(), table1);
        path2 = new ObjectPath(catalog.getDefaultDatabase(), table2);
        partPath1 = new ObjectPath(catalog.getDefaultDatabase(), partTable1);
        catalog.open();
    }

    @AfterClass
    public static void closeCatalog() {
        if (catalog != null) {
            catalog.close();
        }
    }

    @After
    public void cleanup() throws Exception {
        if (catalog.tableExists(path1)) {
            catalog.dropTable(path1, true);
        }
        if (catalog.tableExists(path2)) {
            catalog.dropTable(path2, true);
        }
        if (catalog.tableExists(partPath1)) {
            catalog.dropTable(partPath1, true);
        }
    }

    @Test
    public void testGetDb_DatabaseNotExistException() throws Exception {
        exception.expect(DatabaseNotExistException.class);
        exception.expectMessage("Database nonexistent does not exist in Catalog");
        catalog.getDatabase("nonexistent");
    }

    @Test
    public void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        assertEquals(Collections.singletonList(catalog.getDefaultDatabase()), actual);
    }

    @Test
    public void testDbExists() throws Exception {
        assertFalse(catalog.databaseExists("nonexistent"));
        assertTrue(catalog.databaseExists(catalog.getDefaultDatabase()));
    }

    @Test
    public void testDropDb() throws Exception {
        exception.expect(CatalogException.class);
        exception.expectMessage("drop database not supported");
        catalog.dropDatabase(catalog.getDefaultDatabase(), true);
    }

    @Test
    public void testTableExists() {
        assertFalse(catalog.tableExists(new ObjectPath(catalog.getDefaultDatabase(), "nonexist")));
        assertTrue(catalog.tableExists(new ObjectPath(catalog.getDefaultDatabase(), "cupid_wordcount")));
    }

    @Test
    public void testGetTables_TableNotExistException() throws TableNotExistException {
        exception.expect(TableNotExistException.class);
        catalog.getTable(
                new ObjectPath(catalog.getDefaultDatabase(), "nonexist"));
    }

    @Test
    public void testCreateOdpsTable() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTOR.key(), "odps");
        String comment = "testCreateOdpsTable";
        catalog.createTable(
                path1,
                new CatalogTableImpl(schema, properties, comment),
                false);

        CatalogBaseTable catalogTable = catalog.getTable(path1);
        assertEquals(schema, catalogTable.getSchema());
        assertEquals(comment, catalogTable.getComment());
    }

    @Test
    public void testCreateOtherTable() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTOR.key(), "hive");
        String comment = "testCreateHiveTable";
        try {
            catalog.createTable(
                    path1,
                    new CatalogTableImpl(schema, properties, comment),
                    false);
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                            t,
                            "Currently odps catalog only supports for odps tables")
                            .isPresent());
        }
    }

    @Test
    public void testRenameOdpsTable() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTOR.key(), "odps");
        catalog.createTable(
                path1,
                new CatalogTableImpl(schema, properties, null),
                false);

        catalog.renameTable(path1, table2, false);
        CatalogBaseTable catalogTable = catalog.getTable(path2);
        assertEquals(schema, catalogTable.getSchema());
        assertFalse(catalog.tableExists(path1));
    }

    @Test
    public void testDropOdpsTable() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTOR.key(), "odps");
        catalog.createTable(
                path1,
                new CatalogTableImpl(schema, properties, null),
                false);

        assertTrue(catalog.tableExists(path1));
        catalog.dropTable(path1, false);
        assertFalse(catalog.tableExists(path1));
    }

    @Test
    public void testOdpsPartitionTable() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTOR.key(), "odps");
        catalog.createTable(
                partPath1,
                new CatalogTableImpl(partitionSchema, partTableKey1, properties, null),
                false);

        CatalogBaseTable catalogTable = catalog.getTable(partPath1);
        assertEquals(partitionSchema, catalogTable.getSchema());

        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("pt1", "pt1Value");
        partitionSpec.put("pt2", "pt2Value");
        CatalogPartitionSpec catalogPartitionSpec =
                new CatalogPartitionSpec(partitionSpec);
        catalog.createPartition(
                partPath1,
                catalogPartitionSpec,
                null,
                false
        );
        assertTrue(catalog.partitionExists(partPath1, catalogPartitionSpec));
        List<CatalogPartitionSpec> partitionList = catalog.listPartitions(partPath1, catalogPartitionSpec);
        assertEquals(partitionList.size(), 1);
        assertEquals(partitionList.get(0), catalogPartitionSpec);

        Map<String, String> partitionSpec2 = new LinkedHashMap<>();
        partitionSpec2.put("pt1", "pt1Value");
        partitionSpec2.put("pt2", "pt2Value2");
        CatalogPartitionSpec catalogPartitionSpec2 =
                new CatalogPartitionSpec(partitionSpec2);
        catalog.createPartition(
                partPath1,
                catalogPartitionSpec2,
                null,
                false
        );

        Map<String, String> partitionSpec3 = new LinkedHashMap<>();
        partitionSpec3.put("pt1", "pt1Value2");
        partitionSpec3.put("pt2", "pt2Value2");
        CatalogPartitionSpec catalogPartitionSpec3 =
                new CatalogPartitionSpec(partitionSpec3);
        catalog.createPartition(
                partPath1,
                catalogPartitionSpec3,
                null,
                false
        );

        Map<String, String> partitionSpecPartial = new LinkedHashMap<>();
        partitionSpecPartial.put("pt1", "pt1Value");
        CatalogPartitionSpec catalogPartitionSpecPartial =
                new CatalogPartitionSpec(partitionSpecPartial);
        List<CatalogPartitionSpec> partitionList2 = catalog.listPartitions(partPath1, catalogPartitionSpecPartial);
        assertEquals(partitionList2.size(), 2);

        Map<String, String> partitionSpecPartial2 = new LinkedHashMap<>();
        CatalogPartitionSpec catalogPartitionSpecPartial2 =
                new CatalogPartitionSpec(partitionSpecPartial2);
        List<CatalogPartitionSpec> partitionList3 = catalog.listPartitions(partPath1, catalogPartitionSpecPartial2);
        assertEquals(partitionList3.size(), 3);


        Map<String, String> partitionSpecPartial3 = new LinkedHashMap<>();
        partitionSpecPartial3.put("pt2", "pt2Value2");
        CatalogPartitionSpec catalogPartitionSpecPartial3 =
                new CatalogPartitionSpec(partitionSpecPartial3);
        List<CatalogPartitionSpec> partitionList4 = catalog.listPartitions(partPath1, catalogPartitionSpecPartial3);
        assertEquals(partitionList4.size(), 0);
    }
}
