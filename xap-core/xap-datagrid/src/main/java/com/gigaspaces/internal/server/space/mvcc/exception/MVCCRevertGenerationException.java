package com.gigaspaces.internal.server.space.mvcc.exception;

public class MVCCRevertGenerationException extends MVCCGenerationConflictRuntimeException {

    public MVCCRevertGenerationException(String message) {
        super(message);
    }
}
