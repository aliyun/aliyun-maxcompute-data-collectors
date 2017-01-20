/**
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
package com.cloudera.sqoop.lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Serialize BigDecimal classes to/from DataInput and DataOutput objects.
 *
 * BigDecimal is comprised of a BigInteger with an integer 'scale' field.
 * The BigDecimal/BigInteger can also return itself as a 'long' value.
 *
 * We serialize in one of two formats:
 *
 *  First, check whether the BigInt can fit in a long:
 *  boolean b = BigIntegerPart &gt; LONG_MAX || BigIntegerPart &lt; LONG_MIN
 *
 *  [int: scale][boolean: b == false][long: BigInt-part]
 *  [int: scale][boolean: b == true][string: BigInt-part.toString()]
 *
 * TODO(aaron): Get this to work with Hadoop's Serializations framework.
 *
 * @deprecated use org.apache.sqoop.lib.BigDecimalSerializer instead.
 * @see org.apache.sqoop.lib.BigDecimalSerializer
 */
public final class BigDecimalSerializer {

  private BigDecimalSerializer() { }

  static final BigInteger LONG_MAX_AS_BIGINT =
      org.apache.sqoop.lib.BigDecimalSerializer.LONG_MAX_AS_BIGINT;
  static final BigInteger LONG_MIN_AS_BIGINT =
      org.apache.sqoop.lib.BigDecimalSerializer.LONG_MIN_AS_BIGINT;

  public static void write(BigDecimal d, DataOutput out) throws IOException {
    org.apache.sqoop.lib.BigDecimalSerializer.write(d, out);
  }

  public static BigDecimal readFields(DataInput in) throws IOException {
    return org.apache.sqoop.lib.BigDecimalSerializer.readFields(in);
  }
}
