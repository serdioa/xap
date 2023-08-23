package com.gigaspaces.internal.server.space.mvcc.exception;

/**
 * @author Sagiv Michael
 * @since 16.3
 */
public class MVCCGenerationStateException extends RuntimeException {
    private static final long serialVersionUID = 913779341516565242L;

    public MVCCGenerationStateException() {
    }

    public MVCCGenerationStateException(String message) {
        super(message);
    }

    public MVCCGenerationStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public MVCCGenerationStateException(Throwable cause) {
        super(cause);
    }
}
