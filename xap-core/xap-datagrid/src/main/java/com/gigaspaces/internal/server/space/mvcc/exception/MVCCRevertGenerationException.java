package com.gigaspaces.internal.server.space.mvcc.exception;

public class MVCCRevertGenerationException extends MVCCGenerationConflictRuntimeException {

    private static final long serialVersionUID = -602018298200803915L;

    public MVCCRevertGenerationException(String message) {
        super(message);
    }
}
