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

package com.cloudera.sqoop.odps;

import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.cloudera.sqoop.testutil.ExportJobTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class OdpsExportTest extends ExportJobTestCase {

  static final String PROJECT
      = System.getProperty("sqoop.test.odps.project");
  static final String ACCESS_ID
      = System.getProperty("sqoop.test.odps.accessid");
  static final String ACCESS_KEY
      = System.getProperty("sqoop.test.odps.accesskey");
  static final String ENDPOINT
      = System.getProperty("sqoop.test.odps.endpoint",
      "http://service.odps.aliyun.com/api");

  private Odps odps;

  @Override
  @Before
  public void setUp() {
    Account account = new AliyunAccount(ACCESS_ID, ACCESS_KEY);
    odps = new Odps(account);
    odps.setDefaultProject(PROJECT);
    odps.setEndpoint(ENDPOINT);
    super.setUp();
  }

  protected boolean isTableExist(String tableName) throws OdpsException {
    return odps.tables().exists(tableName);
  }

  protected void deleteTable(String tableName) throws OdpsException {
    odps.tables().delete(tableName, true);
  }

  /** When generating data for export tests, each column is generated
   according to a ColumnGenerator. Methods exist for determining
   what to put into text strings in the files to export, as well
   as what the string representation of the column as returned by
   the database should look like.
   */
  public interface ColumnGenerator {
    /** For a row with id rowNum, what should we write into that
     line of the text file to export?
     */
    String getExportText(int rowNum);

    /** For a row with id rowNum, what should the database return
     for the given column's value?
     */
    String getVerifyText(int rowNum);

    /** Return the column type to put in the CREATE TABLE statement. */
    String getType();

    /** Return the column type of the ODPS table. */
    OdpsType getOdpsType();

    String getDateFormat();
  }

  /** Return the column name for a column index.
   *  Each table contains two columns named 'id' and 'msg', and then an
   *  arbitrary number of additional columns defined by ColumnGenerators.
   *  These columns are referenced by idx 0, 1, 2...
   *  @param idx the index of the ColumnGenerator in the array passed to
   *   createTable().
   *  @return the name of the column
   */
  protected String forIdx(int idx) {
    return "col" + idx;
  }

  protected void createOdpsTable(String tableName, String[] partitionCols,
      ColumnGenerator... extraCols) throws OdpsException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("id", OdpsType.BIGINT));
    schema.addColumn(new Column("msg", OdpsType.STRING));
    int colNum = 0;
    for(ColumnGenerator generator: extraCols) {
      schema.addColumn(new Column(forIdx(colNum++), generator.getOdpsType()));
    }
    if (partitionCols != null) {
      for (String partition : partitionCols) {
        schema.addPartitionColumn(new Column(partition, OdpsType.STRING));
      }
    }
    odps.tables().create(tableName, schema);
  }

  private PartitionSpec getSimplePartitionSpec(String[] partitionCols) {
    if (partitionCols == null || partitionCols.length == 0) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (String partitionCol: partitionCols) {
      sb.append(sep).append(partitionCol).append("='pt'");
      sep = ",";
    }
    return new PartitionSpec(sb.toString());
  }

  private Record getOdpsRecord(TableTunnel.UploadSession uploadSession,
      int idx, ColumnGenerator... extraCols) throws ParseException {
    Record record = uploadSession.newRecord();
    record.setBigint("id", (long) idx);
    record.setString("msg", getMsgPrefix() + idx);
    int colNum = 0;
    for (ColumnGenerator generator: extraCols) {
      String field = forIdx(colNum++);
      String fieldValue = generator.getExportText(idx);
      switch (generator.getOdpsType()) {
        case STRING:
          record.setString(field, fieldValue);
          break;
        case BIGINT:
          record.setBigint(field, Long.parseLong(fieldValue));
          break;
        case DATETIME:
          String dateFormat = generator.getDateFormat();
            record.setDatetime(field,
                new SimpleDateFormat(dateFormat).parse(fieldValue));
          break;
        case DOUBLE:
          record.setDouble(field, Double.parseDouble(fieldValue));
          break;
        case DECIMAL:
          record.setDecimal(field, new BigDecimal(fieldValue));
          break;
        default:
          throw new RuntimeException("Unknown column type: "
              + generator.getOdpsType());
      }
    }
    return record;
  }

  protected void createOdpsTableWithRecords(String tableName,
      String[] partitionCols, int numRecords, ColumnGenerator... extraCols)
      throws OdpsException, IOException, ParseException {
    createOdpsTable(tableName, partitionCols, extraCols);
    TableTunnel tunnel = new TableTunnel(odps);
    PartitionSpec partitionSpec = getSimplePartitionSpec(partitionCols);
    TableTunnel.UploadSession uploadSession = partitionSpec == null
        ? tunnel.createUploadSession(PROJECT, tableName)
        : tunnel.createUploadSession(PROJECT, tableName, partitionSpec);
    RecordWriter recordWriter = uploadSession.openRecordWriter(0);
    for (int i = 0; i < numRecords; i++) {
      Record record = getOdpsRecord(uploadSession, i, extraCols);
      recordWriter.write(record);
    }
    recordWriter.close();
    uploadSession.commit(new Long[] {0L});
  }

  /**
   * Return a SQL statement that drops a table, if it exists.
   * @param tableName the table to drop.
   * @return the SQL statement to drop that table.
   */
  protected String getDropTableStatement(String tableName) {
    return "DROP TABLE " + tableName + " IF EXISTS";
  }

  /** Create the table definition to export to, removing any prior table.
   * By specifying ColumnGenerator arguments, you can add extra columns
   * to the table of arbitrary type.
   */
  public void createTable(ColumnGenerator... extraColumns) throws SQLException {
    Connection conn = getConnection();
    PreparedStatement statement = conn.prepareStatement(
        getDropTableStatement(getTableName()),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(getTableName());
    sb.append(" (id INT NOT NULL PRIMARY KEY, msg VARCHAR(64)");
    int colNum = 0;
    for (ColumnGenerator gen : extraColumns) {
      sb.append(", " + forIdx(colNum++) + " " + gen.getType());
    }
    sb.append(")");

    statement = conn.prepareStatement(sb.toString(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }

  @Test
  public void testBasicExport()
      throws OdpsException, SQLException, IOException, ParseException {
    // Make sure test table not exists
    String odpsTableName = "basic_export_" + System.currentTimeMillis();
    assertFalse("Table " + odpsTableName + " already exists",
        isTableExist(odpsTableName));

    // Create the table we're exporting to
    createTable();

    final int numRecords = 10;
    try {
      // Create the source ODPS table
      createOdpsTableWithRecords(odpsTableName, null, numRecords);

      runExport(getArgv(false, 10, 10,
          "--odps-table", odpsTableName,
          "--odps-project", PROJECT,
          "--odps-accessid", ACCESS_ID,
          "--odps-accesskey", ACCESS_KEY,
          "--odps-endpoint", ENDPOINT));

      verifyExport(numRecords);
    } finally {
      deleteTable(odpsTableName);
    }
  }

}
