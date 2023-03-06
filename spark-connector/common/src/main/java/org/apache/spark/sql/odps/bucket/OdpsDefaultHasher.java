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

package org.apache.spark.sql.odps.bucket;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.List;

public class OdpsDefaultHasher {
    public String getName() {
        return "default";
    }

    /*
     * basic hash function for long
     */
    static int basicLongHasher(long val) {
        long l = val;
        l = (~l) + (l << 18);
        l ^= (l >> 31);
        l *= 21;
        l ^= (l >> 11);
        l += (l << 6);
        l ^= (l >> 22);
        return (int) (l);
    }

    public static int hashInt(Integer val) {
        if (val == null) {
            return 0;
        }
        return basicLongHasher(val.longValue());
    }

    public static int hashLong(Long val) {
        if (val == null) {
            return 0;
        }
        return basicLongHasher(val.longValue());
    }

    public static int hashFloat(Float val) {
        if (val == null) {
            return 0;
        }
        return basicLongHasher((long) Float.floatToIntBits(val));
    }

    public static int hashDouble(Double val) {
        if (val == null) {
            return 0;
        }
        return basicLongHasher(Double.doubleToLongBits(val));
    }

    public static int hashBoolean(Boolean val) {
        if (val == null) {
            return 0;
        }
        //it's magic number
        if (val) {
            return 0x172ba9c7;
        } else {
            return -0x3a59cb12;
        }
    }

    public static int hashString(String val) {
        Charset UTF8 = Charset.forName("UTF8");
        if (val == null) {
            return 0;
        }

        byte[] chars = val.getBytes(UTF8);
        int hashVal = 0;
        for (int i = 0; i < chars.length; ++i) {
            hashVal += chars[i];
            hashVal += (hashVal << 10);
            hashVal ^= (hashVal >> 6);
        }

        hashVal += (hashVal << 3);
        hashVal ^= (hashVal >> 11);
        hashVal += (hashVal << 15);

        return hashVal;
    }

    public static int hashString(UTF8String val) {
        return hashUnsafeBytes(val.getBytes(), Platform.BYTE_ARRAY_OFFSET, val.numBytes());
    }

    public static int hashUnsafeBytes(Object base, long offset, int lengthInBytes) {
        assert (lengthInBytes >= 0) : "lengthInBytes cannot be negative";
        int hashVal = 0;
        for (int i = 0; i < lengthInBytes; ++i) {
            hashVal += (int) Platform.getByte(base, offset + i);
            hashVal += (hashVal << 10);
            hashVal ^= (hashVal >> 6);
        }

        hashVal += (hashVal << 3);
        hashVal ^= (hashVal >> 11);
        hashVal += (hashVal << 15);

        return hashVal;
    }

    public static int hashTimestamp(Timestamp val) {
        if (val == null) {
            return 0;
        }
        long millis = val.getTime();
        long seconds = (millis >= 0 ? millis : millis - 999) / 1000;
        int nanos = val.getNanos();
        seconds <<= 30;
        seconds |= nanos;
        return basicLongHasher(seconds);
    }

    public static int hashBigDecimal(BigDecimal val) {
        if (val == null) {
            return 0;
        }
        BigDecimal[] divideAndRemainder =
                val.divideAndRemainder(new BigDecimal(1).scaleByPowerOfTen(9));
        long totalSec = divideAndRemainder[0].longValue();
        int nanos = divideAndRemainder[1].intValue();
        totalSec <<= 30;
        totalSec |= nanos;
        return basicLongHasher(totalSec);
    }

    public static int hashDecimal(BigDecimal val) {
        throw new RuntimeException("Not supported decimal type:" + val);
    }

    /**
     * Get the hash val for one row
     *
     * @param hashVals
     * @return: the final combine hash val for the row
     */
    public static int CombineHashVal(int hashVals[]) {
        int combineHashVal = 0;
        for (int hashVal : hashVals) {
            combineHashVal += hashVal;
        }
        return (combineHashVal ^ (combineHashVal >> 8));
    }

    public static int CombineHashVal(List<Integer> hashVals) {
        int combineHashVal = 0;
        for (int hashVal : hashVals) {
            combineHashVal += hashVal;
        }
        return (combineHashVal ^ (combineHashVal >> 8));
    }
}

