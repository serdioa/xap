package com.gigaspaces.internal.server.space.tiered_storage.transaction;


/**
 * result an operation executed as part of a bulk on tiered storage
 *
 * @since 16.2
 */
public class TieredStorageBulkOperationResult {
    private final Integer rowsAffected;
    private final Throwable exception;

    public TieredStorageBulkOperationResult(Integer rowsAffected) {
        this(rowsAffected, null);
    }

    public TieredStorageBulkOperationResult(Integer rowsAffected, Throwable exception) {
        this.rowsAffected = rowsAffected;
        this.exception = exception;
    }

    public Integer getRowsAffected() {
        return rowsAffected;
    }

    public Throwable getException() {
        return exception;
    }
}
