package com.gigaspaces.internal.server.space.mvcc;

/**
 * @author Tomer Shapira
 * @since 16.4
 */
public class MVCCIllegalStateException extends IllegalStateException{
    private static final long serialVersionUID = -8900584441059692872L;

    public MVCCIllegalStateException() {
    }

    public MVCCIllegalStateException(String message) {
        super(message);
    }

    public MVCCIllegalStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public MVCCIllegalStateException(Throwable cause) {
        super(cause);
    }
}
