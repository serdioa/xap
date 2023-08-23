package com.gigaspaces.internal.server.space.mvcc.exception;

public class MVCCGenerationConflictRuntimeException extends RuntimeException {

    private static final long serialVersionUID = -7959722857217558503L;

    public MVCCGenerationConflictRuntimeException() {
        super();
    }

    public MVCCGenerationConflictRuntimeException(String message) {
        super(message);
    }

    public MVCCGenerationConflictRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public MVCCGenerationConflictRuntimeException(Throwable cause) {
        super(cause);
    }

    protected MVCCGenerationConflictRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
