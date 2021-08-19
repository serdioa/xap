package com.gigaspaces.sql.datagateway.netty.exception;

import com.gigaspaces.jdbc.SqlErrorCodes;
import com.gigaspaces.sql.datagateway.netty.utils.ErrorCodes;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;

import java.sql.SQLException;

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
        return new NonBreakingException(resolveInternalError(cause), message, cause);
    }

    /**
     * Convert JDBC error code into ODBC error.
     */
    private static String resolveInternalError(Throwable cause) {
        int errorCode = 0;
        if (cause == null) {
            return ErrorCodes.INTERNAL_ERROR;
        }

        if (cause instanceof SQLException) {
            errorCode = ((SQLException) cause).getErrorCode();
        } else if (cause.getCause() instanceof SQLException){
            errorCode = ((SQLException) cause.getCause()).getErrorCode();
        }

        switch (errorCode) {
            case (SqlErrorCodes._378):
                return ErrorCodes.BAD_DATETIME_FORMAT;
            default:
                return ErrorCodes.INTERNAL_ERROR;
        }
    }
}
