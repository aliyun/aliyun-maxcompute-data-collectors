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

package org.apache.flink.odps.test.output;

import org.apache.flink.odps.output.stream.DateTimePartitionAssigner;
import org.apache.flink.odps.output.stream.PartitionComputer;
import org.apache.flink.odps.output.stream.TablePartitionAssigner;
import org.apache.flink.odps.output.writer.WriterContext;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;

import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.junit.Assert.assertTrue;

public class PartitionAssignerTest {

    @Test
    public void testTablePartitionAssigner() throws Exception {
        PartitionComputer<Row> computer = new PartitionComputer<>(
                "defaultPartValue",
                Arrays.asList("c1", "c2", "p1", "p2"),
                Arrays.asList("p1", "p2"),
                "p1=1"
        );
        TablePartitionAssigner<Row> assigner = new TablePartitionAssigner<>(computer);
        String partition;
        partition = assigner.getPartitionSpec(Row.of("c1v", "c2v", "p1v", "p2v"), null);
        Assert.assertEquals(partition, "p1='1',p2='p2v'");

        partition = assigner.getPartitionSpec(Row.of("c1v", "c2v", "p1v", ""), null);
        Assert.assertEquals(partition, "p1='1',p2='defaultPartValue'");
    }

    @Test
    public void testTablePartitionAssigner2() throws Exception {
        PartitionComputer<Row> computer = new PartitionComputer<>(
                "defaultPartValue",
                Arrays.asList("p1", "c1", "p2", "c2"),
                Arrays.asList("p1", "p2"),
                "p1=1"
        );
        TablePartitionAssigner<Row> assigner = new TablePartitionAssigner<>(computer);
        String partition;
        partition = assigner.getPartitionSpec(Row.of("c1v", "c2v", "p1v", "p2v"), null);
        Assert.assertEquals(partition, "p1='1',p2='p1v'");
    }

    @Test
    public void testTablePartitionAssigner3() throws Exception {
        PartitionComputer<Row> computer = new PartitionComputer<>(
                "defaultPartValue",
                Arrays.asList("p1", "c1", "p2", "c2"),
                Arrays.asList("p2", "p1"),
                "p3=1"
        );
        TablePartitionAssigner<Row> assigner = new TablePartitionAssigner<>(computer);
        String partition;
        partition = assigner.getPartitionSpec(Row.of("c1v", "c2v", "p1v", "p2v"), null);
        Assert.assertEquals(partition, "p3='1',p2='p1v',p1='c1v'");
    }

    @Test
    public void testTablePartitionAssigner4() throws Exception {
        PartitionComputer<Row> computer = new PartitionComputer<>(
                "defaultPartValue",
                Arrays.asList("p1", "c1", "p2", "c2", "p3"),
                Arrays.asList("p2", "p3", "p1"),
                "p3=1"
        );
        TablePartitionAssigner<Row> assigner = new TablePartitionAssigner<>(computer);
        String partition;
        partition = assigner.getPartitionSpec(Row.of("c1v", "c2v", "p1v", "p2v", "p3v"), null);
        Assert.assertEquals(partition, "p3='1',p2='p1v',p1='c1v'");
    }

    @Test
    public void testTablePartitionAssigner5() throws Exception {
        String expectedMsg = "Invalid partition columns";
        PartitionComputer<Row> computer = new PartitionComputer<>(
                "defaultPartValue",
                Arrays.asList("p1", "c1", "p2", "c2", "p3"),
                Arrays.asList("p3", "p2", "p4"),
                "p3=1"
        );
        TablePartitionAssigner<Row> assigner = new TablePartitionAssigner<>(computer);
        try {
            assigner.getPartitionSpec(Row.of("c1v", "c2v", "p1v", "p2v", "p3v"), null);
        } catch (Exception e) {
            assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
        }
    }

    @Test
    public void testDatetimePartitionAssigner() throws Exception {
        DateTimePartitionAssigner<Row> assigner = new DateTimePartitionAssigner<>();
        String partition;
        WriterContext writerContext;
        Timestamp timestamp;
        writerContext = new WriterContext("");
        timestamp = Timestamp.valueOf("2016-06-22 19:10:25.123456");

        writerContext.update(null, 0, timestamp.getTime());
        partition = assigner.getPartitionSpec(Row.of("c1v", "c2v", "p1v", "p2v"), writerContext);
        Assert.assertEquals(partition, "dt='2016-06-22--19'");

        timestamp = Timestamp.valueOf("2017-11-11 00:00:00.123");
        writerContext = new WriterContext("p1=3, p2=4");
        writerContext.update(null, 0, timestamp.getTime());
        partition = assigner.getPartitionSpec(Row.of("c1v", "c2v", "p1v", "p2v"), writerContext);
        Assert.assertEquals(partition, "p1='3',p2='4',dt='2017-11-11--00'");
    }

    @Test
    public void testDatetimePartitionAssigner2() throws Exception {
        DateTimePartitionAssigner<Row> assigner = new DateTimePartitionAssigner<>(
                "date",
                "yyyy-MM-dd"
        );
        String partition;
        WriterContext writerContext;
        Timestamp timestamp;
        writerContext = new WriterContext("");
        timestamp = Timestamp.valueOf("2016-06-22 19:10:25.123456");

        writerContext.update(null, 0, timestamp.getTime());
        partition = assigner.getPartitionSpec(Row.of("c1v", "c2v", "p1v", "p2v"), writerContext);
        Assert.assertEquals(partition, "date='2016-06-22'");

        timestamp = Timestamp.valueOf("2017-11-11 00:00:00.123");
        writerContext = new WriterContext("p1=3, p2=4");
        writerContext.update(null, 0, timestamp.getTime());
        partition = assigner.getPartitionSpec(Row.of("c1v", "c2v", "p1v", "p2v"), writerContext);
        Assert.assertEquals(partition, "p1='3',p2='4',date='2017-11-11'");
    }

}
