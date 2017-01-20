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

package com.aliyun.odps.ogg.handler.datahub;

import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import org.dom4j.DocumentException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by lyf0429 on 16/5/24.
 */
public class ConfigureReaderTest {

    @Test
    public void test() throws DocumentException {
        ConfigureReader.reader("src/test/resources/configure.xml");


        try{
            ConfigureReader.reader("src/test/resources/configure_miss_param.xml");
            Assert.assertTrue(false);
        } catch (RuntimeException e){
            Assert.assertTrue(true);
        }

        try{
            ConfigureReader.reader("src/test/resources/configure_datahub_error.xml");
            Assert.assertTrue(false);
        } catch (RuntimeException e){
            Assert.assertTrue(true);
        }

        try{
            ConfigureReader.reader("src/test/resources/configure_topic_error.xml");
            Assert.assertTrue(false);
        } catch (RuntimeException e){
            Assert.assertTrue(true);
        }

        try{
            ConfigureReader.reader("src/test/resources/configure_field_error.xml");
            Assert.assertTrue(false);
        } catch (RuntimeException e){
            Assert.assertTrue(true);
        }

        try{
            Configure configure = ConfigureReader.reader("src/test/resources/configure_no_active_shard.xml");
            DataHubWriter.reInit(configure);
            Assert.assertTrue(false);
        } catch (RuntimeException e){
            Assert.assertTrue(true);
        }
    }


}
