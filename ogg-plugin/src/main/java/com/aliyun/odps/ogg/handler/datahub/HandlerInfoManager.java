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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Created by ouyangzhe on 16/12/1.
 */
public class HandlerInfoManager {
    private final static Logger logger = LoggerFactory
            .getLogger(HandlerInfoManager.class);

    private static final int MS_RECORD_MAX = 10000;

    private Configure configure;

    private String handlerInfoFileName;

    private long recordId;

    private String sendPosition;

    public long getRecordId() {
        return recordId;
    }

    public void setRecordId(long recordId) {
        this.recordId = recordId;
    }

    public String getSendPosition() {
        return sendPosition;
    }

    public void setSendPosition(String sendPosition) {
        this.sendPosition = sendPosition;
    }

    private static HandlerInfoManager handlerInfoManager;

    public static HandlerInfoManager instance() {
        return handlerInfoManager;
    }

    public static void init(Configure configure) {
        if (handlerInfoManager == null) {
            handlerInfoManager = new HandlerInfoManager(configure);
        }
    }

    public void updateHandlerInfos(long time, String position) {
        long nid = time * MS_RECORD_MAX;
        if (recordId < nid) {
            recordId = nid;
        } else {
            recordId++;
        }
        this.sendPosition = position;
    }

    public void saveHandlerInfos() {
        if (configure.isCheckPointFileDisable()) {
            return;
        }
        DataOutputStream out = null;
        try {
            out = new DataOutputStream(new FileOutputStream(handlerInfoFileName, false));
            out.writeLong(recordId);
            out.writeInt(sendPosition.length());
            out.writeBytes(sendPosition);
        } catch (IOException e) {
            logger.error("Error writing handler info file. recordId=" + recordId + ", "
                    + "sendPosition=" + sendPosition, e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    logger.error("Close handler info file failed. recordId=" + recordId + ", "
                            + "sendPosition=" + sendPosition, e);
                }
            }
        }
    }

    private HandlerInfoManager(Configure configure) {
        this.configure = configure;
        handlerInfoFileName = configure.getCheckPointFileName();
        restoreHandlerInfos(handlerInfoFileName);
        logger.info("initial recordId: " + recordId + ", sendPosition: " + sendPosition);
    }

    private void restoreHandlerInfos(String fileName) {
        File handlerInfoFile = new File(fileName);
        if (handlerInfoFile.exists() && !handlerInfoFile.isDirectory()) {
            DataInputStream in = null;
            try {
                in = new DataInputStream(new FileInputStream(handlerInfoFile));
                recordId = in.readLong();
                int length = in.readInt();
                byte[] buffer = new byte[1024];
                in.read(buffer, 0, length);
                sendPosition = new String(buffer, 0, length, Charset.forName("UTF-8"));
            } catch (IOException e) {
                logger.error("Error reading handler info file, may cause duplication.", e);
                throw new RuntimeException("Error reading handler info file, may cause duplication ", e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.warn("Close handler info file failed. ", e);
                    }
                }
            }
        } else {
            // the file not exits, start from scratch
            logger.info("handler info file not exists, init recordId [1] sendPosition ['']");
            recordId = 0;
            sendPosition = "";
        }
    }
}
