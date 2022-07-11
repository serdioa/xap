package com.gigaspaces.internal.server.space.tiered_storage.transaction;


import com.gigaspaces.internal.server.storage.IEntryHolder;

@com.gigaspaces.api.InternalApi
public class TieredStorageInsertBulkOperationRequest extends TieredStorageBulkOperationRequest {

    public TieredStorageInsertBulkOperationRequest(IEntryHolder entryHolder) {
        super(entryHolder);
    }

    @Override
    public boolean isInsertOperation() {
        return true;
    }
}
