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

/**
 * Created by ouyangzhe on 16/12/1.
 */
public class HandlerInfoManager {
    private static final int MS_RECORD_MAX = 10000;

    private long recordId;

    private static final HandlerInfoManager handlerInfoManager = new HandlerInfoManager();

    public static HandlerInfoManager instance() {
        return handlerInfoManager;
    }

    private HandlerInfoManager() {
    }

    // 不会并发访问，所以没有线程安全问题
    public String genRecordId(long time) {
        long nid = time * MS_RECORD_MAX;
        if (recordId < nid) {
            recordId = nid;
        } else {
            recordId++;
        }
        return Long.toString(recordId);
    }

}
