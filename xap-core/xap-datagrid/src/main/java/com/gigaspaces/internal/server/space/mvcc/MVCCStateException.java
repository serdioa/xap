package com.gigaspaces.internal.server.space.mvcc;

/**
 * @author Sagiv Michael
 * @since 16.3
 */
public class MVCCStateException extends RuntimeException {
    private static final long serialVersionUID = 913779341516565242L;

    public MVCCStateException() {
    }

    public MVCCStateException(String message) {
        super(message);
    }

    public MVCCStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public MVCCStateException(Throwable cause) {
        super(cause);
    }
}
