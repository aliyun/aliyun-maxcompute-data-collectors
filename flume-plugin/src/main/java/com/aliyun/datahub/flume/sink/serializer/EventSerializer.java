/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.datahub.flume.sink.serializer;

import com.aliyun.datahub.client.model.RecordEntry;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Interface for an event serializer which serializes the event body to write to DataHub.
 */
public interface EventSerializer extends Configurable {

    /**
     * Initialize the event serializer.
     * @param event Event to be written to ODPS
     */
    public void initialize(Event event);

    /**
     * Get field-value map that should be written out to ODPS as a result of this event.
     * @return field-value map: {fieldName : fieldValue, ...}
     * @throws UnsupportedEncodingException
     */
    public Map<String, String> getRow() throws UnsupportedEncodingException;

    /**
     * Get serializer's column names
     * @return  column names
     */
    public String[] getInputColumnNames();

    /**
     *
     * @return
     * @throws UnsupportedEncodingException
     */
    public String getRawBody() throws UnsupportedEncodingException;

    /**
     * generate flume event by DataHub TupleRecordData
     * @param data DataHub TupleRecordData
     * @return
     */
    public Event getEvent(RecordEntry data);
}
