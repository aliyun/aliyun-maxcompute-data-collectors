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

package com.aliyun.odps.cupid.table.v1.writer.adaptor;

import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Varchar;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

public interface Row {

    boolean isNullAt(int idx);

    boolean getBoolean(int idx);

    byte getByte(int idx);

    short getShort(int idx);

    int getInt(int idx);

    long getLong(int idx);

    float getFloat(int idx);

    double getDouble(int idx);

    Date getDatetime(int idx);

    java.sql.Date getDate(int idx);

    Timestamp getTimeStamp(int idx);

    BigDecimal getDecimal(int idx);

    String getString(int idx);

    Char getChar(int idx);

    Varchar getVarchar(int idx);

    byte[] getBytes(int idx);
}
