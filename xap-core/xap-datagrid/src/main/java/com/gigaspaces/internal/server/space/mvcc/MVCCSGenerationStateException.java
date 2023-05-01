package com.gigaspaces.internal.server.space.mvcc;

/**
 * @author Sagiv Michael
 * @since 16.3
 */
public class MVCCSGenerationStateException extends RuntimeException {
    private static final long serialVersionUID = 913779341516565242L;

    public MVCCSGenerationStateException() {
    }

    public MVCCSGenerationStateException(String message) {
        super(message);
    }

    public MVCCSGenerationStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public MVCCSGenerationStateException(Throwable cause) {
        super(cause);
    }
}
