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

package org.apache.spark.sql.odps.arrow;

import org.apache.arrow.vector.types.pojo.ArrowType;

public abstract class OdpsExtensionType extends ArrowType.ExtensionType {

    private final ArrowType storageType;
    private final String extensionName;

    public OdpsExtensionType(ArrowType storageType, String extensionName) {
        this.storageType = storageType;
        this.extensionName = extensionName;
    }

    @Override
    public ArrowType storageType() {
        return this.storageType;
    }

    @Override
    public String extensionName() {
        return this.extensionName;
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
        return extensionName.equals(other.extensionName());
    }

    @Override
    public String serialize() {
        return extensionName;
    }
}




