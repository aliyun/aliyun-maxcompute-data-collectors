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

package org.apache.flink.odps.catalog.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.odps.catalog.OdpsCatalog;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.odps.catalog.factories.OdpsCatalogFactoryOptions.DEFAULT_PROJECT;
import static org.apache.flink.odps.catalog.factories.OdpsCatalogFactoryOptions.ODPS_CONF_DIR;


/**
 * Catalog factory for {@link OdpsCatalog}.
 */
public class OdpsCatalogFactory implements CatalogFactory {

	private static final Logger LOG = LoggerFactory.getLogger(OdpsCatalogFactory.class);

	@Override
	public String factoryIdentifier() {
		return OdpsCatalogFactoryOptions.IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(DEFAULT_PROJECT);
		options.add(ODPS_CONF_DIR);
		return options;
	}

	@Override
	public Catalog createCatalog(Context context) {
		final FactoryUtil.CatalogFactoryHelper helper =
				FactoryUtil.createCatalogFactoryHelper(this, context);
		helper.validate();

		final String odpsConfDir = helper.getOptions().getOptional(ODPS_CONF_DIR).orElse(null);
		OdpsConf odpsConf = OdpsUtils.getOdpsConf(odpsConfDir);
		Preconditions.checkNotNull(odpsConf, "odps conf cannot be null");
		final String project = odpsConf.getProject();
		final String defaultDatabase =
				helper.getOptions().getOptional(DEFAULT_PROJECT).orElse(project);

		return new OdpsCatalog(context.getName(), defaultDatabase, odpsConf);
	}
}
