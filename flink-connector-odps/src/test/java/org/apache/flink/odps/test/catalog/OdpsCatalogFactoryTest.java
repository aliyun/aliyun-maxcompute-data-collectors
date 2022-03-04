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

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.odps.catalog.OdpsCatalog;
import org.apache.flink.odps.catalog.factories.OdpsCatalogFactory;
import org.apache.flink.odps.catalog.factories.OdpsCatalogFactoryOptions;
import org.apache.flink.odps.util.Constants;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

/** Test for {@link OdpsCatalog} created by {@link OdpsCatalogFactory}. */
public class OdpsCatalogFactoryTest extends TestLogger {

    private static final URL CONF_DIR =
            Thread.currentThread().getContextClassLoader()
                    .getResource("test-catalog-factory-conf");

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCreateOdpsCatalog() {
        final String catalogName = "mycatalog";

        final OdpsCatalog expectedCatalog = new OdpsCatalog(catalogName,
                "cupid_test_release",
                new OdpsConf("a1", "a2", "e1", "cupid_test_release"));

        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), OdpsCatalogFactoryOptions.IDENTIFIER);
        options.put(OdpsCatalogFactoryOptions.ODPS_CONF_DIR.key(), CONF_DIR.getPath());
        options.put(OdpsCatalogFactoryOptions.DEFAULT_PROJECT.key(), "cupid_test_release");

        final Catalog actualCatalog =
                FactoryUtil.createCatalog(
                        catalogName, options, null, Thread.currentThread().getContextClassLoader());

        assertEquals(
                "testValue",
                ((OdpsCatalog) actualCatalog).getOdpsConf().getProperty("odps.test.var"));
        checkEquals(expectedCatalog, (OdpsCatalog) actualCatalog);
    }

    @Test
    public void testCreateOdpsCatalogWithIllegalOdpsConfDir() throws IOException {
        final String catalogName = "mycatalog";

        final String odpsConfDir = tempFolder.newFolder().getAbsolutePath();

        try {
            final Map<String, String> options = new HashMap<>();
            options.put(
                    CommonCatalogOptions.CATALOG_TYPE.key(), OdpsCatalogFactoryOptions.IDENTIFIER);
            options.put(OdpsCatalogFactoryOptions.ODPS_CONF_DIR.key(), odpsConfDir);

            final Catalog actualCatalog =
                    FactoryUtil.createCatalog(
                            catalogName,
                            options,
                            null,
                            Thread.currentThread().getContextClassLoader());
            Assert.fail();
        } catch (ValidationException e) {
        }
    }

    @Test
    public void testLoadOdpsConfigFromEnv() throws IOException {
        final String catalogName = "OdpsCatalog";
        final File odpsConfDir = tempFolder.newFolder();
        final File odpsConfFile = new File(odpsConfDir, "odps.conf");

        Map<String, String> customProps = new HashMap<>();
        customProps.put("odps.access.id", "a1");
        customProps.put("odps.access.key", "a2");
        customProps.put("odps.end.point", "e1");
        customProps.put("odps.project.name", "cupid_test_release");
        customProps.put("odps.test.var", "testValue2");

        try (PrintStream out = new PrintStream(new FileOutputStream(odpsConfFile))) {
            for (String key : customProps.keySet()) {
                out.println(key + "=" + customProps.get(key));
            }
        }

        final Map<String, String> originalEnv = System.getenv();
        final Map<String, String> newEnv = new HashMap<>(originalEnv);
        // set ODPS_CONF_DIR env
        newEnv.put(Constants.ODPS_CONF_DIR, odpsConfDir.getAbsolutePath());
        CommonTestUtils.setEnv(newEnv);

        final OdpsConf odpsConf;
        try {
            final Map<String, String> options = new HashMap<>();
            options.put(
                    CommonCatalogOptions.CATALOG_TYPE.key(), OdpsCatalogFactoryOptions.IDENTIFIER);
            final OdpsCatalog odpsCatalog =
                    (OdpsCatalog)
                            FactoryUtil.createCatalog(
                                    catalogName,
                                    options,
                                    null,
                                    Thread.currentThread().getContextClassLoader());

            odpsConf = odpsCatalog.getOdpsConf();
        } finally {
            // set the Env back
            CommonTestUtils.setEnv(originalEnv);
        }
        // validate the result
        assertEquals(odpsConf.getProperties().size(), 1);
        assertEquals(odpsConf.getProperty("odps.test.var"), customProps.get("odps.test.var"));
    }

    @Test
    public void testCreateMultipleOdpsCatalog() throws Exception {
        final Map<String, String> props1 = new HashMap<>();
        props1.put(CommonCatalogOptions.CATALOG_TYPE.key(), OdpsCatalogFactoryOptions.IDENTIFIER);
        props1.put(
                OdpsCatalogFactoryOptions.ODPS_CONF_DIR.key(),
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("test-multi-odps-conf1")
                        .getPath());

        final Map<String, String> props2 = new HashMap<>();
        props2.put(CommonCatalogOptions.CATALOG_TYPE.key(), OdpsCatalogFactoryOptions.IDENTIFIER);
        props2.put(
                OdpsCatalogFactoryOptions.ODPS_CONF_DIR.key(),
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("test-multi-odps-conf2")
                        .getPath());

        Callable<Catalog> callable1 =
                () ->
                        FactoryUtil.createCatalog(
                                "cat1",
                                props1,
                                null,
                                Thread.currentThread().getContextClassLoader());

        Callable<Catalog> callable2 =
                () ->
                        FactoryUtil.createCatalog(
                                "cat2",
                                props2,
                                null,
                                Thread.currentThread().getContextClassLoader());

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<Catalog> future1 = executorService.submit(callable1);
        Future<Catalog> future2 = executorService.submit(callable2);
        executorService.shutdown();

        OdpsCatalog catalog1 = (OdpsCatalog) future1.get();
        OdpsCatalog catalog2 = (OdpsCatalog) future2.get();

        // verify we read our own props
        assertEquals("val1", catalog1.getOdpsConf().getProperty("key"));
        assertNotNull(catalog1.getOdpsConf().getProperty("conf1"));
        // verify we don't read props from other conf
        assertNull(catalog1.getOdpsConf().getProperty("conf2"));

        // verify we read our own props
        assertEquals("val2", catalog2.getOdpsConf().getProperty("key"));
        assertNotNull(catalog2.getOdpsConf().getProperty("conf2"));
        // verify we don't read props from other conf
        assertNull(catalog2.getOdpsConf().getProperty("conf1"));
    }

    private static void checkEquals(OdpsCatalog c1, OdpsCatalog c2) {
        // Only assert a few selected properties for now
        assertEquals(c1.getName(), c2.getName());
        assertEquals(c1.getDefaultDatabase(), c2.getDefaultDatabase());
    }
}
