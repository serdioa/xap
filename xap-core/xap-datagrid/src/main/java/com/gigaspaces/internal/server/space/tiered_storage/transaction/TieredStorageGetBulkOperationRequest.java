package com.gigaspaces.internal.server.space.tiered_storage.transaction;


import com.gigaspaces.internal.server.storage.IEntryHolder;

@com.gigaspaces.api.InternalApi
public class TieredStorageGetBulkOperationRequest extends TieredStorageBulkOperationRequest {
    public TieredStorageGetBulkOperationRequest(IEntryHolder entryHolder) {
        super(entryHolder);
    }

    @Override
    public boolean isGetOperation() {
        return true;
    }
}
