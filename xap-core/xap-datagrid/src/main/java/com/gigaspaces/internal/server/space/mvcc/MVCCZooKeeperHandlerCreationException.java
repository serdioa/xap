package com.gigaspaces.internal.server.space.mvcc;

/**
 * @author Davyd Savitskyi
 * @since 16.4.0
 */
public class MVCCZooKeeperHandlerCreationException extends RuntimeException {

    private static final long serialVersionUID = 5687101084072415429L;

    public MVCCZooKeeperHandlerCreationException() {
    }

    public MVCCZooKeeperHandlerCreationException(String message) {
        super(message);
    }

    public MVCCZooKeeperHandlerCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}

