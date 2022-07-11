package com.gigaspaces.internal.server.space.tiered_storage.transaction;


import com.gigaspaces.internal.server.storage.IEntryHolder;

/**
 * base for an operation to execute as part of a bulk on tiered storage
 *
 * @since 16.2
 */
public abstract class TieredStorageBulkOperationRequest {
    private final IEntryHolder entryHolder;


    TieredStorageBulkOperationRequest(IEntryHolder entryHolder) {
        this.entryHolder = entryHolder;
    }

    public IEntryHolder getEntryHolder() {
        return entryHolder;
    }

    public boolean isInsertOperation() {return false;};
    public boolean isUpdateOperation() {return false;};
    public boolean isRemoveOperation() {return false;};
    public boolean isGetOperation() {return false;};
}
