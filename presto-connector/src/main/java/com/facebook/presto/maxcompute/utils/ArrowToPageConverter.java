/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.maxcompute.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.ArrayBlockBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.maxcompute.MaxComputeColumnHandle;
import com.facebook.presto.maxcompute.MaxComputeErrorCode;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ArrowToPageConverter
{
    private final Map<String, TypeInfo> odpsTypeMap;
    private final List<ColumnHandle> requireColumns;
    private final Map<String, String> columnNameConvertMap;

    public ArrowToPageConverter(List<ColumnHandle> requireColumns, List<Column> schema)
    {
        this.odpsTypeMap = requireNonNull(schema, "schema is null").stream().collect(Collectors.toMap(Column::getName, Column::getTypeInfo));
        this.requireColumns = requireNonNull(requireColumns, "requireColumns is null");

        // It seems that Presto is not case-sensitive (all columns are automatically converted to lowercase), but MaxCompute is,
        // so we use columnNameConvertMap to do the conversion to convert the lowercase column names in the ColumnHandle to the correct column names.
        this.columnNameConvertMap = schema.stream().collect(Collectors.toMap(c -> c.getName().toLowerCase(), Column::getName));
    }

    public void convert(PageBuilder pageBuilder, VectorSchemaRoot vectorSchemaRoot)
    {
        pageBuilder.declarePositions(vectorSchemaRoot.getRowCount());
        for (int column = 0; column < requireColumns.size(); column++) {
            String requireColumnName = ((MaxComputeColumnHandle) requireColumns.get(column)).getName();
            String filedName = columnNameConvertMap.getOrDefault(requireColumnName, requireColumnName);
            FieldVector vector = vectorSchemaRoot.getVector(filedName);

            ArrowVectorAccessor dataAccessor = ArrowUtils.createColumnVectorAccessor(vector, odpsTypeMap.get(filedName));
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(column);
            Type prestoType = ((MaxComputeColumnHandle) requireColumns.get(column)).getType();
            TypeInfo odpsType = odpsTypeMap.get(filedName);
            transferData(dataAccessor, blockBuilder, prestoType, odpsType, vector.getValueCount());
        }
    }

    private void transferData(ArrowVectorAccessor dataAccessor, BlockBuilder blockBuilder, Type prestoType, TypeInfo odpsType, int valueCount)
    {
        Class<?> javaType = prestoType.getJavaType();
        for (int index = 0; index < valueCount; index++) {
            Object data = ArrowUtils.getData(dataAccessor, odpsType, index);
            if (data == null) {
                blockBuilder.appendNull();
            }
            else if (javaType == boolean.class) {
                prestoType.writeBoolean(blockBuilder, (Boolean) data);
            }
            else if (javaType == long.class) {
                prestoType.writeLong(blockBuilder, (Long) data);
            }
            else if (javaType == double.class) {
                prestoType.writeDouble(blockBuilder, (Double) data);
            }
            else if (javaType == Slice.class) {
                prestoType.writeSlice(blockBuilder, (Slice) data);
            }
            else if (javaType == Block.class && prestoType instanceof ArrayType) {
                ArrayType arrayType = (ArrayType) prestoType;
                ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) blockBuilder;
                BlockBuilder elementBlockBuilder = arrayBlockBuilder.getElementBlockBuilder();
                List object = (List) data;
                transferData(new SimpleDataAccessor(object), elementBlockBuilder, arrayType.getElementType(), TypeInfoFactory.UNKNOWN, object.size());
            }
            else {
                // TODO: support map and struct
                throw new PrestoException(MaxComputeErrorCode.MAXCOMPUTE_CONNECTOR_ERROR, "Unsupported type: " + javaType);
            }
        }
    }
}
