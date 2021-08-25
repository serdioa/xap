/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.sql.datagateway.netty.exception;

import com.gigaspaces.sql.datagateway.netty.utils.ErrorCodes;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;

public class ExceptionUtil {
    public static ProtocolException wrapException(String baseMessage, Throwable cause) {
        if (cause instanceof Error)
            throw (Error) cause;

        if (cause instanceof ProtocolException)
            return (ProtocolException) cause;

        if (cause instanceof CalciteContextException) {
            CalciteContextException ce = (CalciteContextException) cause;
            cause = ce.getCause();
            assert cause != null;

            int line = ce.getPosLine();
            int column = ce.getPosColumn();

            String message = String.format("%s. Line %d column %d: %s",
                baseMessage, line, column, cause.getMessage());

            return new NonBreakingException(ErrorCodes.SYNTAX_ERROR, message, cause);
        }

        if (cause instanceof SqlParseException) {
            String message = baseMessage + ": " + cause.getMessage();
            return new NonBreakingException(ErrorCodes.SYNTAX_ERROR, message, cause);
        }

        String message = baseMessage + ": " + cause.getMessage();
        return new NonBreakingException(ErrorCodes.INTERNAL_ERROR, message, cause);
    }
}
