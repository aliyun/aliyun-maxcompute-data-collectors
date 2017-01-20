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

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ImportException;
import org.apache.sqoop.mapreduce.odps.HdfsOdpsImportJob;
import org.apache.sqoop.util.SqlTypeMap;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

public class HdfsManager extends ConnManager {

  private SqoopOptions options;

  public HdfsManager(SqoopOptions options) {
    this.options = options;
    this.options.setTableName("sqoop_orm");
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
