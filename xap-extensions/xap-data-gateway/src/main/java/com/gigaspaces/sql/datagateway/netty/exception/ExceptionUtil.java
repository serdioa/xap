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
