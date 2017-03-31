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

package org.apache.sqoop.manager;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.Tables;
import com.aliyun.odps.account.AliyunAccount;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ImportException;
import com.google.common.base.Preconditions;

import org.apache.sqoop.mapreduce.odps.HdfsOdpsImportJob;
import org.apache.sqoop.odps.OdpsUtil;
import org.apache.sqoop.util.SqlTypeMap;
import org.mortbay.log.Log;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

public class HdfsManager extends ConnManager {

  private SqoopOptions options;
  private Odps odps;
  private boolean isCreateTable;

  public HdfsManager(SqoopOptions options) {
    this.options = options;
    this.options.setTableName("sqoop_orm");

    isCreateTable = options.isOdpsCreateTable();

    String tableName = Preconditions.checkNotNull(options.getOdpsTable(),
        "Import to ODPS error: Table name not specified");
    String accessID = Preconditions.checkNotNull(options.getOdpsAccessID(),
        "Error: ODPS access ID not specified");
    String accessKey = Preconditions.checkNotNull(options.getOdpsAccessKey(),
        "Error: ODPS access key not specified");
    String project = Preconditions.checkNotNull(options.getOdpsProject(),
        "Error: ODPS project not specified");
    String endpoint = Preconditions.checkNotNull(options.getOdpsEndPoint(),
        "Error: ODPS endpoint not specified");

    odps = new Odps(new AliyunAccount(accessID, accessKey));
    odps.setUserAgent(OdpsUtil.getUserAgent());
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);
    
    String[] colNames = options.getColumns();
    Tables tables = odps.tables();
    boolean existsTable = false;
    try {
      existsTable = tables.exists(options.getOdpsTable());
    } catch (OdpsException e) {
      throw new RuntimeException("ODPS exception", e);
    }
    if (colNames == null) {
      if (!existsTable) {
        throw new RuntimeException("missing --columns");
      } else {
        Table t = tables.get(options.getOdpsTable());
        colNames = new String[t.getSchema().getColumns().size()];
        Log.info("colNames size: " + colNames.length);
        for (int i = 0; i < colNames.length; ++i) {
          colNames[i] = new String(t.getSchema().getColumns().get(i).getName());
          Log.info("colNames colNames[i]: " + colNames[i]);
        }
      }

      options.setColumns(colNames);
    }

  }

  @Override
  public String[] listDatabases() {
    return new String[0];
  }

  @Override
  public String[] listTables() {
    return new String[0];
  }

  @Override
  public String[] getColumnNames(String tableName) {
    return new String[0];
  }

  @Override
  public String getPrimaryKey(String tableName) {
    return null;
  }

  @Override
  public Map<String, Integer> getColumnTypes(String tableName) {
    String[] colNames = options.getColumns();
    Map<String, Integer> map = new SqlTypeMap<String, Integer>();
    for (String c: colNames) {
      map.put(c, Integer.valueOf(Types.VARCHAR));
    }
    return map;
  }

  @Override
  public ResultSet readTable(String tableName, String[] columns) throws SQLException {
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return null;
  }

  @Override
  public String getDriverClass() {
    return null;
  }

  @Override
  public void execAndPrint(String s) {

  }

  @Override
  public void importTable(ImportJobContext context) throws IOException, ImportException {
    context.setConnManager(this);
    HdfsOdpsImportJob importJob = new HdfsOdpsImportJob(context.getOptions(), context);
    importJob.runImport(context.getTableName(), context.getJarFile());
  }

  @Override
  public void close() throws SQLException {

  }

  @Override
  public void release() {

  }
}
