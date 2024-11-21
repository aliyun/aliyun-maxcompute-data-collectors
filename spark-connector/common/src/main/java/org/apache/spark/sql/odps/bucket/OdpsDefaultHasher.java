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
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_SECOND;
import static org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MICROS;

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

    public static int hashTinyInt(Byte val) {
        if (val == null) {
            return 0;
        }
        return basicLongHasher(val.longValue());
    }

    public static int hashSmallInt(Short val) {
        if (val == null) {
            return 0;
        }
        return basicLongHasher(val.longValue());
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

    public static int hashDate(int date) {
        return hashLong(LocalDate.ofEpochDay(date).atStartOfDay(ZoneOffset.UTC).toEpochSecond());
    }

    public static int hashTimestamp(long timestamp) {
        long timestampInSeconds = MICROSECONDS.toSeconds(timestamp);
        long nanoSecondsPortion = (timestamp % MICROS_PER_SECOND) * NANOS_PER_MICROS;
        long result = timestampInSeconds;
        result <<= 30;
        result |= nanoSecondsPortion;
        return OdpsDefaultHasher.hashLong(result);
    }

    public static int hashDecimal(BigDecimal val, int precision, int scale) {
        if (val == null) {
            return 0;
        }
        BigInteger bi = castBigDecimal2BigInteger(val.toString(), precision, scale);
        if (isDecimal128(precision)) {
            // Reference to task/sql_task/execution_engine/ir/hash_ir.cpp:HashInt1284Row
            return basicLongHasher(bi.longValue()) + basicLongHasher(bi.shiftRight(64).longValue());
        }
        return basicLongHasher(bi.longValue());
    }

    // Reference to include/runtime_decimal_val.h:isDecimal128
    private static boolean isDecimal128(int precision) {
        return precision > 18;
    }

    // Reference to the code in common/util/runtime_decimal_val_funcs.cpp::RuntimeDecimalValFuncs::doCastTo.
    // This function converts decimal into an int128_t variable (= 16 Bytes).
    private static BigInteger castBigDecimal2BigInteger(String input, int resultPrecision, int resultScale)
            throws IllegalArgumentException {
        // trim
        input = input.trim();
        int len = input.length();
        int ptr = 0;

        // check negative
        boolean isNegative = false;
        if (len > 0) {
            if (input.charAt(ptr) == '-') {
                isNegative = true;
                ptr++;
                len--;
            } else if (input.charAt(ptr) == '+') {
                ptr++;
                len--;
            }
        }

        // ignore leading zeros
        while (len > 0 && input.charAt(ptr) == '0') {
            ptr++;
            len--;
        }

        // check decimal format and analyze precison and scale
        int valueScale = 0;
        boolean foundDot = false;
        boolean foundExponent = false;
        for (int i = 0; i < len; i++) {
            char c = input.charAt(ptr + i);
            if (Character.isDigit(c)) {
                if (foundDot) {
                    valueScale++;
                }
            } else if (c == '.' && !foundDot) {
                foundDot = true;
            } else if ((c == 'e' || c == 'E') && i + 1 < len) {
                foundExponent = true;
                int exponent = Integer.parseInt(input.substring(ptr + i + 1));
                valueScale -= exponent;
                len = ptr + i;
                break;
            } else {
                throw new IllegalArgumentException("Invalid decimal format: " + input);
            }
        }

        // get result value
        String
                numberWithoutExponent =
                foundExponent ? input.substring(ptr, len) : input.substring(ptr);
        if (foundDot) {
            numberWithoutExponent = numberWithoutExponent.replace(".", "");
        }
        if (numberWithoutExponent.isEmpty()) {
            return BigInteger.ZERO;
        }
        BigInteger tmpResult = new BigInteger(numberWithoutExponent);
        if (valueScale > resultScale) {
            tmpResult = tmpResult.divide(BigInteger.TEN.pow(valueScale - resultScale));
            if (numberWithoutExponent.charAt(
                    numberWithoutExponent.length() - (valueScale - resultScale)) >= '5') {
                tmpResult = tmpResult.add(BigInteger.ONE);
            }
        } else if (valueScale < resultScale) {
            tmpResult = tmpResult.multiply(BigInteger.TEN.pow(resultScale - valueScale));
        }
        if (isNegative) {
            tmpResult = tmpResult.negate();
        }

        // TODO: check overflow
        // if (tmpResult.toString().length() - (isNegative ? 1 : 0) > resultPrecision) {
        //     throw new IllegalArgumentException("Result precision overflow.");
        // }

        return tmpResult;
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

