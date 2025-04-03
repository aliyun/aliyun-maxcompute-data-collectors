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

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;

import java.util.Collection;
import java.util.List;

public class SimpleDataAccessor
        extends ArrowVectorAccessor
{

    private final Collection data;

    public SimpleDataAccessor(Collection data)
    {
        super(new BitVector("ignore", new RootAllocator(0L)));
        this.data = data;
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        if (data instanceof List) {
            return ((List) data).get(rowId) == null;
        }
        else if (data instanceof SimpleStruct) {
            return ((SimpleStruct) data).getFieldValue(rowId) == null;
        }
        else {
            return false;
        }
    }

    public Object get(int rowId)
    {
        if (data instanceof List) {
            return ((List) data).get(rowId);
        }
        else {
            return null;
        }
    }
}
