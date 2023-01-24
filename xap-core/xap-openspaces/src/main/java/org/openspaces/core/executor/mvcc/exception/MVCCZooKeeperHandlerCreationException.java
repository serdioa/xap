package org.openspaces.core.executor.mvcc.exception;

/**
 * @author Sagiv Michael
 * @since 16.3.0
 */
public class MVCCZooKeeperHandlerCreationException extends RuntimeException {

    private static final long serialVersionUID = 5687101084072415426L;

    public MVCCZooKeeperHandlerCreationException() {
    }

    public MVCCZooKeeperHandlerCreationException(String message) {
        super(message);
    }

    public MVCCZooKeeperHandlerCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
