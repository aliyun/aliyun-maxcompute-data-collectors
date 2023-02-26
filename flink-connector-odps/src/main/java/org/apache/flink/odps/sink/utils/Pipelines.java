/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.sink.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

/**
 * Utilities to generate all kinds of sub-pipelines.
 */
public class Pipelines {

    public static DataStreamSink<RowData> dummySink(DataStream<RowData> dataStream) {
        return dataStream.addSink(Pipelines.DummySink.INSTANCE)
                .setParallelism(1)
                .name("dummy");
    }

    public static String opName(String operatorN, String tablePath) {
        return operatorN + ": " + tablePath;
    }

    public static String opUID(String operatorN, String tablePath) {
        return "uid_" + operatorN + "_" + tablePath;
    }

    /**
     * Dummy sink that does nothing.
     */
    public static class DummySink implements SinkFunction<RowData> {
        private static final long serialVersionUID = 1L;
        public static DummySink INSTANCE = new DummySink();
    }
}