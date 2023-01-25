package org.openspaces.core.executor.mvcc.exception;

/**
 * @author Sagiv Michael
 * @since 16.3.0
 */
public class MVCCRetryTaskException extends Exception {
    private static final long serialVersionUID = 7382997159809192328L;

    public MVCCRetryTaskException() {
    }

    public MVCCRetryTaskException(String message) {
        super(message);
    }

    public MVCCRetryTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
