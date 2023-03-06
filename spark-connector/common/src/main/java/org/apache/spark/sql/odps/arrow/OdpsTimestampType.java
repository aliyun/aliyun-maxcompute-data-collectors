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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;

public class OdpsTimestampType extends OdpsExtensionType {

    public static final ExtensionType INSTANCE = new OdpsTimestampType();
    public static final String EXTENSION_NAME = "odps_timestamp";

    static {
        ExtensionTypeRegistry.register(INSTANCE);
    }

    public OdpsTimestampType() {
        super(Struct.INSTANCE, EXTENSION_NAME);
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
        if (!serializedData.equals(EXTENSION_NAME)) {
            throw new IllegalArgumentException("OdpsTimeStampType deserialize invalid data " + serializedData);
        }
        return new OdpsTimestampType();
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        StructVector vector = new StructVector(name, allocator, fieldType, null);
        vector.addOrGet("sec", FieldType.nullable(Types.MinorType.BIGINT.getType()), BigIntVector.class);
        vector.addOrGet("nano", FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
        return vector;
    }

    public static java.util.List<Field> getChildrenFields() {
        java.util.List<Field> childrenFields = new ArrayList<>();
        childrenFields.add(new Field("sec", FieldType.nullable(Types.MinorType.BIGINT.getType()), new ArrayList<>()));
        childrenFields.add(new Field("nano", FieldType.nullable(Types.MinorType.INT.getType()), new ArrayList<>()));
        return childrenFields;
    }

    public static BigIntVector getSecondsVector(FieldVector arrowVector) {
        return (BigIntVector) ((StructVector)arrowVector).getChild("sec");
    }

    public static IntVector getNanosVector(FieldVector arrowVector) {
        return (IntVector) ((StructVector)arrowVector).getChild("nano");
    }
}