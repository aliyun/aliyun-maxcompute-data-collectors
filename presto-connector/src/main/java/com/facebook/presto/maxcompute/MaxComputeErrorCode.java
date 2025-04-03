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
package com.facebook.presto.maxcompute;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;

import static com.facebook.presto.common.ErrorType.EXTERNAL;
import static com.facebook.presto.common.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.common.ErrorType.USER_ERROR;

public enum MaxComputeErrorCode
        implements ErrorCodeSupplier
{
    /**
     * maxcompute connector error, such as npe, type convert error etc.
     */
    MAXCOMPUTE_CONNECTOR_ERROR(0, INTERNAL_ERROR),
    /**
     * maxcompute user error, such as resource not exit, ak error, type not support etc.
     */
    MAXCOMPUTE_USER_ERROR(1, USER_ERROR),
    /**
     * maxcompute external error, such as network error.
     */
    MAXCOMPUTE_EXTERNAL_ERROR(2, EXTERNAL);
    public static final int ERROR_CODE_MASK = 0x0510_0000;

    private final ErrorCode errorCode;

    MaxComputeErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + ERROR_CODE_MASK, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
